import os
import shutil
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from datetime import datetime
from logger_config import logger, log_time

# Logger is now imported from logger_config

# ============================================================
# LOAD ENVIRONMENT VARIABLES
# ============================================================
load_dotenv()

# ============================================================
# CONFIGURATION
# ============================================================
# SOURCE_DIR = "/home/caypro/Documents/supremePdfMapper/samepl/real_estate_"
SOURCE_DIR = "/data/scraping_data_doc"
# DEST_PARENT_DIR = "/home/caypro/Documents/supremePdfMapper/samepl/restructured_data"
DEST_PARENT_DIR = "/data/scraping_data_doc/restructure_data"
BATCH_SIZE = 2000

# constant english district value required by you
CHRDISTRICT_ENGLISH_CONST = "Mumbai District"

# ============================================================
# DATABASE CONFIGURATION
# ============================================================
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

# ============================================================
# MAPPING DICTIONARIES (extend as needed)
# - Only mapped values are transformed, others remain unchanged
# ============================================================
DISTRICT_MAP = {
    "मुंबई_जिल्हा": "मुंबई जिल्हा",
}

SRO_MAP = {
    "Joint_S.R._Mumbai_2_(Mumbai_City_2_(Worli))": "Joint S.R. Mumbai 2 (Mumbai City 2 (Worli))",
    "Joint_S.R._Mumbai_1_(Mumbai_City_1_(Fort))": "Joint S.R. Mumbai 1 (Mumbai City 1 (Fort))",
    "Joint_S.R._Mumbai_3_(Joint_Sub-Registrar_Mumbai_City_3)": "Joint S.R. Mumbai 3 (Joint Sub-Registrar Mumbai City 3)"
}

# ============================================================
# DATABASE UTILITIES
# ============================================================
def get_db_connection():
    """Create and return a database connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Database connection established successfully")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

def check_duplicate_ongoing(cursor, chrdistrict, chrsro, intstartrange, intendrange, chryear):
    """
    Check if a record exists in tblongoingdocno with the given combination.
    Returns intongoingid if found, None otherwise.
    """
    query = """
        SELECT intongoingid 
        FROM tblongoingdocno 
        WHERE chrdistrict = %s 
          AND chrsro = %s 
          AND intstartrange = %s 
          AND intendrange = %s 
          AND chryear = %s
        LIMIT 1
    """
    cursor.execute(query, (chrdistrict, chrsro, intstartrange, intendrange, chryear))
    result = cursor.fetchone()
    return result[0] if result else None

def insert_ongoing_docno(cursor, chrdistrict, chrdistrictenglish, chrsro, intstartrange, intendrange, chryear):
    """
    Insert a new record into tblongoingdocno and return the generated intongoingid.
    """
    query = """
        INSERT INTO tblongoingdocno 
        (chrdistrict, chrdistrictenglish, chrsro, intstartrange, intendrange, chryear, chrip, enmstatus, dtmaddedon, dtmupdatedon)
        VALUES (%s, %s, %s, %s, %s, %s, NULL, 'Completed Webhook Recieved', NOW(), NOW())
        RETURNING intongoingid
    """
    cursor.execute(query, (chrdistrict, chrdistrictenglish, chrsro, intstartrange, intendrange, chryear))
    result = cursor.fetchone()
    return result[0]

def check_duplicate_record(cursor, intongoingid, intdocno):
    """
    Check if a record exists in tblongoingdocnorecords with the given combination.
    Returns True if exists, False otherwise.
    """
    query = """
        SELECT 1 
        FROM tblongoingdocnorecords 
        WHERE intongoingid = %s 
          AND intdocno = %s
        LIMIT 1
    """
    cursor.execute(query, (intongoingid, intdocno))
    return cursor.fetchone() is not None

def insert_docno_record(cursor, intongoingid, chrdistrict, chrdistrictenglish, 
                       chrregistrationtype, chrsro, intdocno, chryear, 
                       intstartrange, intendrange, crawling_status, extraction_status):
    """
    Insert a record into tblongoingdocnorecords.
    """
    query = """
        INSERT INTO tblongoingdocnorecords 
        (intongoingid, chrdistrict, chrdistrictenglish, chrregistrationtype, chrsro, 
         intdocno, chryear, intstartrange, intendrange, crawling_status, extraction_status,
         text_htmlpath, text_jsonpath, text_error, dtmaddedon, dtmupdatedon)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, NULL, NULL, NOW(), NOW())
    """
    cursor.execute(query, (intongoingid, chrdistrict, chrdistrictenglish, chrregistrationtype,
                          chrsro, intdocno, chryear, intstartrange, intendrange,
                          crawling_status, extraction_status))

# ============================================================
# UTILITIES
# ============================================================
def map_district(d):
    return DISTRICT_MAP.get(d, d)

def map_sro(s):
    return SRO_MAP.get(s, s)

# ============================================================
# PARSE FOLDER NAME
# ============================================================
def parse_folder_name(folder_name):
    parts = folder_name.split('_')

    # iSarita 2.0 special case
    if len(parts) > 2 and parts[0] == 'iSarita' and parts[1] == '2.0':
        reg_type = "iSarita_2.0"
        remaining_parts = parts[2:]
    else:
        reg_type = parts[0]
        remaining_parts = parts[1:]

    if len(remaining_parts) < 4:
        return None

    dist = f"{remaining_parts[0]}_{remaining_parts[1]}"
    year = remaining_parts[-2]

    try:
        doc_no = int(remaining_parts[-1])
    except:
        return None

    sro = "_".join(remaining_parts[2:-2])

    return {
        "original_name": folder_name,
        "reg_type": reg_type,
        "dist": dist,
        "sro": sro,
        "year": year,
        "doc_no": doc_no
    }

# ============================================================
# PROCESS BATCH WITH DATABASE
# ============================================================
@log_time
def process_batch_with_db(conn, batch_id, batch_data, meta, copy_count_ref):
    """
    Process a single batch:
    1. Check for duplicate in tblongoingdocno
    2. Insert or reuse intongoingid
    3. Create folder with intongoingid
    4. Copy folders
    5. Insert records into tblongoingdocnorecords
    
    All within a transaction that will rollback on any error.
    Returns: (success: bool, intongoingid: int or None, error_msg: str or None)
    """
    cursor = conn.cursor()
    
    try:
        # Prepare metadata
        chrdistrict_mapped = map_district(meta["dist"])
        chrsro_mapped = map_sro(meta["sro"])
        range_min = meta["range_min"]
        range_max = meta["range_max"]
        year = meta["year"]
        
        logger.info(f"Processing batch {batch_id}: {chrdistrict_mapped}, {chrsro_mapped}, {year}, Range: {range_min}-{range_max}")
        
        # Step 1: Check for duplicate in tblongoingdocno
        existing_id = check_duplicate_ongoing(cursor, chrdistrict_mapped, chrsro_mapped, 
                                              range_min, range_max, year)
        
        if existing_id:
            logger.info(f"Found existing intongoingid: {existing_id} for batch {batch_id}")
            intongoingid = existing_id
            is_new_parent = False
        else:
            # Step 2: Insert new record into tblongoingdocno
            intongoingid = insert_ongoing_docno(cursor, chrdistrict_mapped, CHRDISTRICT_ENGLISH_CONST,
                                               chrsro_mapped, range_min, range_max, year)
            logger.info(f"Created new intongoingid: {intongoingid} for batch {batch_id}")
            is_new_parent = True
        
        # Step 3: Create folder with intongoingid
        batch_folder = os.path.join(DEST_PARENT_DIR, str(intongoingid))
        os.makedirs(batch_folder, exist_ok=True)
        logger.info(f"Created/verified folder: {batch_folder}")
        
        # Step 4: Copy folders and rename files
        items_map = batch_data["items"]
        for item in items_map.values():
            sp = os.path.join(SOURCE_DIR, item['original_name'])
            
            # Create simplified folder name: {registration_type}_{doc_number}
            new_folder_name = f"{item['reg_type']}_{item['doc_no']}"
            dp = os.path.join(batch_folder, new_folder_name)
            
            if not os.path.exists(dp):
                try:
                    shutil.copytree(sp, dp)
                    copy_count_ref[0] += 1
                    logger.debug(f"Copied and renamed folder: {item['original_name']} -> {new_folder_name}")
                    
                    # Rename files inside the copied folder
                    old_folder_name = item['original_name']
                    files_renamed = 0
                    
                    for filename in os.listdir(dp):
                        file_path = os.path.join(dp, filename)
                        
                        # Only process files, not directories
                        if os.path.isfile(file_path):
                            # Check if filename starts with the old folder name
                            if filename.startswith(old_folder_name):
                                # Extract the suffix (everything after the old folder name)
                                suffix = filename[len(old_folder_name):]
                                
                                # Create new filename: {registration_type}_{doc_number}{suffix}
                                new_filename = f"{item['reg_type']}_{item['doc_no']}{suffix}"
                                new_file_path = os.path.join(dp, new_filename)
                                
                                # Rename the file
                                os.rename(file_path, new_file_path)
                                files_renamed += 1
                                logger.debug(f"  Renamed file: {filename} -> {new_filename}")
                    
                    if files_renamed > 0:
                        logger.info(f"Renamed {files_renamed} files in folder {new_folder_name}")
                    
                except Exception as e:
                    error_msg = f"Failed to copy folder {item['original_name']} to {new_folder_name}: {e}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
            else:
                logger.debug(f"Folder already exists, skipping: {new_folder_name}")
        
        # Step 5: Insert records into tblongoingdocnorecords
        records_inserted = 0
        records_skipped = 0
        
        for doc_no in range(range_min, range_max + 1):
            # Determine if document exists or is a gap
            if doc_no in items_map:
                # Document exists
                item = items_map[doc_no]
                
                if item['reg_type'] == "iSarita_2.0":
                    chrregistrationtype_val = "iSarita 2.0"
                    crawling_status_val = "Not Found"
                else:
                    chrregistrationtype_val = item['reg_type']
                    crawling_status_val = "Found"
                
                dist_val = item['dist']
                sro_val = item['sro']
                year_val = item['year']
            else:
                # Document missing (gap)
                chrregistrationtype_val = "iSarita 2.0"
                crawling_status_val = "Not Found"
                dist_val = meta['dist']
                sro_val = meta['sro']
                year_val = meta['year']
            
            # Map values
            chrdistrict_mapped = map_district(dist_val)
            chrsro_mapped = map_sro(sro_val)
            chrdistrictenglish_val = CHRDISTRICT_ENGLISH_CONST
            extraction_status_val = "Pending"
            
            # Check for duplicate (only if parent is not new)
            if not is_new_parent:
                if check_duplicate_record(cursor, intongoingid, doc_no):
                    logger.debug(f"Record already exists: intongoingid={intongoingid}, docno={doc_no}")
                    records_skipped += 1
                    continue
            
            # Insert record
            insert_docno_record(cursor, intongoingid, chrdistrict_mapped, chrdistrictenglish_val,
                              chrregistrationtype_val, chrsro_mapped, doc_no, year_val,
                              range_min, range_max, crawling_status_val, extraction_status_val)
            records_inserted += 1
        
        logger.info(f"Batch {batch_id} (intongoingid={intongoingid}): Inserted {records_inserted} records, Skipped {records_skipped} duplicates")
        
        # Commit transaction
        conn.commit()
        logger.info(f"Successfully committed batch {batch_id}")
        
        cursor.close()
        return (True, intongoingid, None)
        
    except Exception as e:
        # Rollback on any error
        conn.rollback()
        error_msg = f"Error processing batch {batch_id}: {e}"
        logger.error(error_msg)
        logger.info(f"Rolled back transaction for batch {batch_id}")
        cursor.close()
        return (False, None, error_msg)

# ============================================================
# MAIN PROCESS
# ============================================================
@log_time
def main():
    logger.info("=" * 80)
    logger.info("Starting restructure with database integration...")
    logger.info("=" * 80)

    # Sanity check source exists
    if not os.path.exists(SOURCE_DIR):
        logger.error(f"Source directory not found: {SOURCE_DIR}")
        return

    # Create destination directory
    os.makedirs(DEST_PARENT_DIR, exist_ok=True)
    logger.info(f"Destination directory: {DEST_PARENT_DIR}")

    # Get database connection
    try:
        conn = get_db_connection()
    except Exception as e:
        logger.error(f"Cannot proceed without database connection. Exiting.")
        return

    # List and parse folders
    items = os.listdir(SOURCE_DIR)
    parsed_items = []
    skipped = 0
    
    logger.info(f"Scanning source directory: {SOURCE_DIR}")
    for folder in items:
        full = os.path.join(SOURCE_DIR, folder)
        if not os.path.isdir(full):
            continue
        meta = parse_folder_name(folder)
        if meta:
            parsed_items.append(meta)
        else:
            skipped += 1

    logger.info(f"Found {len(parsed_items)} valid folders. Skipped {skipped} invalid items.")

    if len(parsed_items) == 0:
        logger.warning("No valid folders to process. Exiting.")
        conn.close()
        return

    # GROUP KEY = (dist, sro, year)
    groups = {}
    for item in parsed_items:
        key = (item["dist"], item["sro"], item["year"])
        groups.setdefault(key, []).append(item)

    logger.info(f"Grouped folders into {len(groups)} groups by (district, sro, year)")

    # Organize into batches
    batch_map = {}
    local_batch_id = 0
    copy_count_ref = [0]  # Using list to make it mutable for reference passing

    batches_to_process = []  # List of (local_batch_id, batch_data, meta)

    for key, lst in groups.items():
        lst.sort(key=lambda x: x["doc_no"])

        for item in lst:
            doc_no = item["doc_no"]
            logical = (doc_no - 1) // BATCH_SIZE
            map_key = (key, logical)

            if map_key not in batch_map:
                local_batch_id += 1
                batch_map[map_key] = local_batch_id

            bid = batch_map[map_key]
            item['sub_dir_id'] = bid

            # Compute 2000-range boundaries
            range_min = logical * BATCH_SIZE + 1
            range_max = (logical + 1) * BATCH_SIZE
            item['range_min'] = range_min
            item['range_max'] = range_max

    # Group items by batch
    batches = {}
    for item in parsed_items:
        bid = item['sub_dir_id']
        if bid not in batches:
            batches[bid] = {
                "items": {},
                "meta": {
                    "dist": item["dist"],
                    "sro": item["sro"],
                    "year": item["year"],
                    "range_min": item["range_min"],
                    "range_max": item["range_max"]
                }
            }
        batches[bid]["items"][item["doc_no"]] = item

    logger.info(f"Total batches to process: {len(batches)}")
    logger.info("=" * 80)

    # Process each batch
    successful_batches = 0
    failed_batches = 0

    for bid in sorted(batches.keys()):
        batch_data = batches[bid]
        meta = batch_data["meta"]
        
        success, intongoingid, error_msg = process_batch_with_db(conn, bid, batch_data, meta, copy_count_ref)
        
        if success:
            successful_batches += 1
        else:
            failed_batches += 1
            logger.error(f"STOPPING PROCESS due to batch {bid} failure: {error_msg}")
            break

    # Close database connection
    conn.close()
    logger.info("Database connection closed")

    # Final summary
    logger.info("=" * 80)
    logger.info("PROCESS SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total batches: {len(batches)}")
    logger.info(f"Successful batches: {successful_batches}")
    logger.info(f"Failed batches: {failed_batches}")
    logger.info(f"Total folders copied: {copy_count_ref[0]}")
    
    if failed_batches > 0:
        logger.warning("Process stopped due to errors. Previous successful batches remain in database.")
    else:
        logger.info("All batches processed successfully!")
    
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
