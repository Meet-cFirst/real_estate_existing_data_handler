import os
import re
import subprocess
import sys
import time

import traceback
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from logger_config import logger, log_time

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

DISTRICT_MAP = {
    "मुंबई_जिल्हा": "Mumbai_District",
    "मुंबई जिल्हा": "Mumbai_District",
}

def map_district(d):
    return DISTRICT_MAP.get(d, d)

# Import parse_folder_name from the existing script
try:
    from restructure_data import parse_folder_name
except ImportError:
    # Fallback if running from a different location or if import fails
    def parse_folder_name(folder_name):
        parts = folder_name.split('_')
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

# Logger is now imported from logger_config

class S3Transfer:
    def __init__(self, bucket_name, db_conn=None, dry_run=False):
        self.bucket_name = bucket_name
        self.db_conn = db_conn
        self.dry_run = dry_run
        # Only initialize client if not in dry_run mode to avoid credential errors during testing

    def generate_timestamped_html_name(self, cnr_no, local_file_path):
        """
        Generates a timestamped filename for HTML files.
        Format: {filename}_{timestamp}.html
        """
        filename = os.path.basename(local_file_path)
        name, ext = os.path.splitext(filename)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{name}_{timestamp}{ext}"

    def generate_timestamped_html_name(self, diary_no):
        """
        Generate timestamped HTML filename.

        Args:
            cnr_no (str): CNR number
            original_html_path (str): Path to original HTML file

        Returns:
            str: New filename with timestamp
        """
        try:
            timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
            new_filename = f"{diary_no}_{timestamp}.html"
            logger.info(f"Generated timestamped HTML name: {new_filename}")
            return new_filename

        except Exception as e:
            logger.error(f"Error generating timestamped name for CNR {diary_no}: {e}")
            return f"{diary_no}.html"  # Fallback to original format

    def list_s3_files(self, s3_base_path):
        result = subprocess.run(
            ["s3cmd", "ls", s3_base_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        if result.returncode == 0:
            # Extract file names from s3cmd output
            return set(re.findall(r'\s+(\S+)$', result.stdout, re.MULTILINE))
        else:
            logger.error(f"Failed to list S3 directory: {result.stderr}")
            return set()

    def cache_s3_files_and_cleanup(self, s3_base_path, local_folder_path, cache_dir="/tmp/s3_cache"):
        """
        Cache S3 file listing and verify all local files match S3.
        If all files match, delete the local folder.
        
        Args:
            s3_base_path (str): S3 base path to list files from
            local_folder_path (str): Local folder path to compare and potentially delete
            cache_dir (str): Directory to store cache files
            
        Returns:
            bool: True if folder was deleted, False otherwise
        """
        try:
            # Create cache directory if it doesn't exist
            os.makedirs(cache_dir, exist_ok=True)
            
            # Generate cache filename based on s3 path
            cache_filename = s3_base_path.replace("s3://", "").replace("/", "_") + ".txt"
            cache_file_path = os.path.join(cache_dir, cache_filename)
            
            # Get all S3 files recursively in one command
            logger.info(f"Fetching S3 file listing for {s3_base_path}")
            result = subprocess.run(
                ["s3cmd", "ls", "-r", s3_base_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            if result.returncode != 0:
                logger.error(f"Failed to list S3 files: {result.stderr}")
                logger.error("CRITICAL ERROR: Cannot verify S3 files. Exiting.")
                sys.exit(1)
            
            # Save to cache file
            with open(cache_file_path, 'w') as f:
                f.write(result.stdout)
            logger.info(f"Cached S3 file listing to {cache_file_path}")
            
            # Parse S3 files from cache
            s3_files = set(re.findall(r'\s+(\S+)$', result.stdout, re.MULTILINE))
            logger.info(f"Found {len(s3_files)} files in S3")
            
            # Get all local files
            local_files = set()
            for root, dirs, files in os.walk(local_folder_path):
                for file in files:
                    local_file_path = os.path.join(root, file)
                    relative_path = os.path.relpath(local_file_path, local_folder_path)
                    # Convert to S3 path format
                    s3_equivalent_path = s3_base_path + relative_path.replace(os.sep, '/')
                    local_files.add(s3_equivalent_path)
            
            logger.info(f"Found {len(local_files)} files in local folder")
            
            # Check if all local files exist in S3
            missing_in_s3 = local_files - s3_files
            
            if missing_in_s3:
                logger.error(f"CRITICAL: Not all local files are in S3. Missing {len(missing_in_s3)} files:")
                for missing_file in list(missing_in_s3)[:10]:  # Log first 10
                    logger.error(f"  - {missing_file}")
                logger.error("CRITICAL ERROR: Files missing in S3. Cannot delete local folder. Exiting.")
                sys.exit(1)
            
            # All files match - safe to delete local folder
            logger.info(f"All {len(local_files)} local files verified in S3. Deleting local folder: {local_folder_path}")
            
            import shutil
            shutil.rmtree(local_folder_path)
            logger.info(f"Successfully deleted local folder: {local_folder_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"CRITICAL ERROR in cache_s3_files_and_cleanup: {e}")
            logger.error(traceback.format_exc())
            logger.error("Exiting due to critical error.")
            sys.exit(1)


    @log_time
    def transfer_file_to_s3(self, local_file_path, s3_destination_path, max_retries=3):
        """
        Transfer a single file to S3 with retry mechanism.

        Args:
            local_file_path (str): Local file path
            s3_destination_path (str): S3 destination path
            max_retries (int): Maximum retry attempts

        Returns:
            bool: True if successful, False otherwise
        """
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Attempt {attempt}: Transferring {local_file_path} to {s3_destination_path}")

                result = subprocess.run([
                    "s3cmd", "put", local_file_path, s3_destination_path
                ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

                if result.returncode == 0:
                    logger.info(f"Successfully transferred file on attempt {attempt}")
                    return True
                else:
                    logger.error(f"Transfer failed on attempt {attempt}: {result.stderr}")

            except Exception as e:
                logger.error(f"Transfer attempt {attempt} failed with exception: {e}")

            if attempt < max_retries:
                time.sleep(2)  # Wait before retry

        logger.error(f"Failed to transfer file after {max_retries} attempts: {local_file_path}")
        return False


    @log_time
    def transfer_folder_to_s3(self, folder_path, diary_no, do_id):
        """
        Transfer entire folder to S3 with proper structure and verification.

        Args:
            folder_path (str): Local folder path to transfer
            diary_no (str): Diary number (folder name)
            do_id (str): Data Order ID (intongoingid)

        Returns:
            tuple: (success: bool, s3_base_path: str, s3_html_file_path: str)
        """
        try:
            folder_name = os.path.basename(folder_path)

            # Query database to get district, sro, year using do_id (intongoingid)
            if not self.db_conn:
                logger.error("Database connection not provided to S3Transfer")
                return False, None, None
            
            cursor = self.db_conn.cursor(cursor_factory=RealDictCursor)
            
            query = """
                SELECT chrdistrict, chrdistrictenglish, chrsro, chryear 
                FROM tblongoingdocno 
                WHERE intongoingid = %s
                LIMIT 1
            """
            
            cursor.execute(query, (do_id,))
            result = cursor.fetchone()
            cursor.close()
            
            if not result:
                logger.error(f"No metadata found in tblongoingdocno for intongoingid: {do_id}")
                return False, None, None
            
            # Extract metadata from database
            district_raw = result['chrdistrict']
            district_english = result['chrdistrictenglish']
            sro_raw = result['chrsro']
            year = result['chryear']
            
            # Use English district if available, otherwise map the Marathi district
            district = district_english.replace(" ", "_") if district_english else map_district(district_raw)
            sro = sro_raw.replace(" ", "_")
            
            logger.info(f"Retrieved metadata from DB for do_id {do_id}: district={district}, sro={sro}, year={year}")

            s3_base_path = f"s3://calysosro/marshal/{district}/{sro}/{year}/{do_id}/{folder_name}/"
            logger.info(f"Starting S3 transfer for CNR {diary_no} to path: {s3_base_path}")

            if not os.path.exists(folder_path):
                logger.error(f"Source folder does not exist: {folder_path}")
                return False, None, None

            files_to_transfer = []
            s3_html_file_path = None



            for root, dirs, files in os.walk(folder_path):
                for file in files:
                    local_file_path = os.path.join(root, file)
                    relative_path = os.path.relpath(local_file_path, folder_path)

                    s3_file_path = s3_base_path + relative_path.replace(os.sep, '/')

                    files_to_transfer.append((local_file_path, s3_file_path))

            logger.info(f"Found {len(files_to_transfer)} files to transfer for CNR {diary_no}")

            # Transfer each file with retry mechanism
            successful_transfers = 0
            failed_files = []

            for local_file, s3_destination in files_to_transfer:
                if self.transfer_file_to_s3(local_file, s3_destination):
                    successful_transfers += 1
                else:
                    failed_files.append(local_file)

            # Get base S3 directory (remove file name from path)
            s3_base_path = os.path.dirname(files_to_transfer[0][1]) + "/"
            s3_files = self.list_s3_files(s3_base_path)

            verification_failures = []
            for local_file, s3_destination in files_to_transfer:
                logger.debug(f"local_file: {local_file}")
                logger.debug(f"s3_destination: {s3_destination}")
                if local_file not in failed_files:
                    s3_file_name = s3_destination
                    if s3_file_name not in s3_files:
                        verification_failures.append(s3_file_name)

            total_files = len(files_to_transfer)
            if successful_transfers == total_files and len(verification_failures) == 0:
                logger.info(f"Successfully transferred and verified all {total_files} files for CNR {diary_no}")
                
                # Cache S3 files and cleanup local folder if all files match
                cleanup_success = self.cache_s3_files_and_cleanup(s3_base_path, folder_path)
                if cleanup_success:
                    logger.info(f"Local folder deleted after successful S3 verification: {folder_path}")
                else:
                    logger.warning(f"Could not delete local folder (verification failed or error occurred): {folder_path}")
                
                return True, s3_base_path, s3_html_file_path
            else:
                logger.error(f"Transfer incomplete for CNR {diary_no}: {successful_transfers}/{total_files} successful, {len(verification_failures)} verification failures")
                logger.error(f"Failed files: {failed_files}")
                logger.error(f"Verification failures: {verification_failures}")
                return False, s3_base_path, s3_html_file_path

        except Exception as e:
            logger.error(f"Error in S3 transfer for CNR {diary_no}: {e}")
            logger.error(traceback.format_exc())
            return False, None, None



@log_time
def main():
    SOURCE_DIR =  "/data/scraping_data_doc/restructure_data"
    BUCKET_NAME = "calysosro"
    
    if not os.path.exists(SOURCE_DIR):
        logger.error(f"Source directory not found: {SOURCE_DIR}")
        return

    # Establish database connection
    try:
        db_conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Database connection established successfully")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        logger.error("Cannot proceed without database connection. Exiting.")
        return

    # Create S3Transfer instance with database connection
    uploader = S3Transfer(BUCKET_NAME, db_conn=db_conn)
    
    # Iterate through do_id folders (1, 2, 3...)
    try:
        do_ids = sorted([d for d in os.listdir(SOURCE_DIR) if os.path.isdir(os.path.join(SOURCE_DIR, d))], key=lambda x: int(x) if x.isdigit() else 999999)
    except ValueError:
        do_ids = [d for d in os.listdir(SOURCE_DIR) if os.path.isdir(os.path.join(SOURCE_DIR, d))]

    logger.info(f"Found {len(do_ids)} batches (do_ids) to process.")

    for do_id in do_ids:
        do_id_path = os.path.join(SOURCE_DIR, do_id)
        logger.info(f"\nProcessing Batch (do_id): {do_id}")

        # Iterate through case folders inside the do_id folder
        case_folders = os.listdir(do_id_path)

        for case_folder in case_folders:
            case_path = os.path.join(do_id_path, case_folder)
            if not os.path.isdir(case_path):
                continue

            # Use the folder name as cnr_no/case identifier for logging
            cnr_no = case_folder

            success, s3_path, html_path = uploader.transfer_folder_to_s3(case_path, cnr_no, do_id)

            if success:
                logger.info(f"[SUCCESS] {case_folder} -> {s3_path}")
            else:
                logger.error(f"[FAILED] {case_folder}")
    
    # Close database connection
    db_conn.close()
    logger.info("Database connection closed")

if __name__ == "__main__":
    main()