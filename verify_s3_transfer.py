import os
import subprocess
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("verify_s3_transfer.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DISTRICT_MAP = {
    "मुंबई जिल्हा": "Mumbai_District",
}

def map_district(d):
    return DISTRICT_MAP.get(d, d)


class S3Verifier:
    def __init__(self):
        # Load .env file from the Real_Estate_Extraction directory
        env_path = "/root/Documents/Real_Estate_Extraction/.env"
        load_dotenv(dotenv_path=env_path)
        self.db_config = {
            'host': os.getenv('DATABASE_HOST'),
            'port': os.getenv('DATABASE_PORT'),
            'user': os.getenv('DATABASE_USER'),
            'password': os.getenv('DATABASE_PASSWORD'),
            'dbname': os.getenv('DATABASE_NAME')
        }
        
        self.matched_log = "/root/Documents/real_estate_existing_data_handler/logs/matched.log"
        self.not_matched_log = "/root/Documents/real_estate_existing_data_handler/logs/not_matched.log"
        
        # Locks for thread-safe file writing
        self.matched_lock = Lock()
        self.not_matched_lock = Lock()
        
        # Clear previous logs
        with open(self.matched_log, 'w') as f:
            f.write(f"# S3 Verification Matched - {datetime.now()}\n")
            f.write("# Format: do_id | local_path | s3_path | status\n\n")
        
        with open(self.not_matched_log, 'w') as f:
            f.write(f"# S3 Verification Not Matched - {datetime.now()}\n")
            f.write("# Format: do_id | local_path | s3_path | reason\n\n")

    def get_metadata_from_database(self, do_id):
        """Fetch district, SRO, and year from database for a given do_id."""
        try:
            conn = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                dbname=self.db_config['dbname']
            )
            
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            query = """
                SELECT chrdistrict, chrsro, chryear 
                FROM tblongoingdocno 
                WHERE intongoingid = %s
            """
            cursor.execute(query, (int(do_id),))
            row = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            if row:
                return row['chrdistrict'], row['chrsro'], row['chryear']
            else:
                logger.warning(f"No metadata found in database for do_id {do_id}")
                return None, None, None
                
        except Exception as e:
            logger.error(f"Failed to fetch metadata from database for do_id {do_id}: {e}")
            return None, None, None

    def get_local_files(self, folder_path):
        """Get all files from local folder recursively."""
        local_files = []
        if not os.path.exists(folder_path):
            logger.error(f"Local folder does not exist: {folder_path}")
            return None
        
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                local_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_file_path, folder_path)
                local_files.append(relative_path)
        
        return sorted(local_files)

    def get_all_s3_files_from_paths(self, s3_paths_file, cache_file, force_refresh=False):
        """Get ALL files from S3 paths listed in a file, with disk caching.
        
        Args:
            s3_paths_file: File containing S3 paths to query (one per line)
            cache_file: File to save/load cached S3 file list
            force_refresh: If True, ignore cache and fetch fresh from S3
            
        Returns:
            dict: Dictionary mapping full S3 paths to True for fast lookups
        """
        # Check if cache exists and we can use it
        if not force_refresh and os.path.exists(cache_file):
            logger.info(f"Loading S3 file cache from: {cache_file}")
            try:
                s3_files = {}
                with open(cache_file, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            s3_files[line] = True
                logger.info(f"Successfully loaded {len(s3_files)} S3 files from cache")
                return s3_files
            except Exception as e:
                logger.warning(f"Failed to load cache: {e}. Will fetch from S3.")
        
        # Read S3 paths from file
        logger.info(f"Reading S3 paths from: {s3_paths_file}")
        with open(s3_paths_file, 'r') as f:
            s3_paths = [line.strip() for line in f if line.strip()]
        
        logger.info(f"Found {len(s3_paths)} S3 paths to query")
        logger.info("Fetching files from each path... This may take a few moments.")
        
        s3_files = {}
        total_files = 0
        
        # Query each S3 path
        for idx, s3_path in enumerate(s3_paths, 1):
            logger.info(f"[{idx}/{len(s3_paths)}] Querying: {s3_path}")
            
            try:
                result = subprocess.run(
                    ["s3cmd", "ls", "-r", s3_path],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=300
                )
                
                if result.returncode != 0:
                    logger.warning(f"Failed to list {s3_path}: {result.stderr}")
                    continue
                
                # Parse output
                path_file_count = 0
                for line in result.stdout.strip().split('\n'):
                    if line.strip():
                        parts = line.split()
                        if len(parts) >= 4 and parts[-1].startswith('s3://'):
                            s3_file_path = parts[-1]
                            s3_files[s3_file_path] = True
                            path_file_count += 1
                
                total_files += path_file_count
                logger.info(f"  Found {path_file_count} files in this path")
                
            except subprocess.TimeoutExpired:
                logger.error(f"Timeout while querying {s3_path}")
            except Exception as e:
                logger.error(f"Error querying {s3_path}: {e}")
        
        logger.info(f"Total S3 files found: {total_files}")
        
        # Save to cache file
        logger.info(f"Saving S3 file list to cache: {cache_file}")
        try:
            with open(cache_file, 'w') as f:
                for s3_file in sorted(s3_files.keys()):
                    f.write(f"{s3_file}\n")
            logger.info("Cache saved successfully")
        except Exception as e:
            logger.error(f"Failed to save cache: {e}")
        
        return s3_files
    
    def get_s3_files_for_folder(self, s3_folder_path, s3_cache):
        """Extract files for a specific folder from the cached S3 file list.
        
        Args:
            s3_folder_path: The S3 folder path to check (e.g., s3://bucket/path/to/folder/)
            s3_cache: Dictionary of all S3 files from get_all_s3_files
            
        Returns:
            list: List of relative file paths within this folder
        """
        if s3_cache is None:
            return None
        
        folder_files = []
        for s3_path in s3_cache.keys():
            if s3_path.startswith(s3_folder_path):
                # Extract relative path
                relative = s3_path.replace(s3_folder_path, '').lstrip('/')
                if relative and '/' not in relative:  # Only files directly in this folder
                    folder_files.append(relative)
                elif relative:  # Files in subfolders
                    folder_files.append(relative)
        
        return sorted(folder_files)

    def verify_folder(self, do_id, s3_cache, local_base_path="/data/scraping_data_doc/restructure_data"):
        """Verify a single folder against S3 using cached S3 file list.
        
        Args:
            do_id: Data order ID
            s3_cache: Dictionary of all S3 files (from get_all_s3_files)
            local_base_path: Base path for local folders
        """
        logger.info(f"Processing do_id: {do_id}")
        
        # Find the folder for this do_id
        local_folder = os.path.join(local_base_path, str(do_id))
        
        if not os.path.exists(local_folder):
            msg = f"{do_id} | {local_folder} | N/A | Local folder does not exist\n"
            logger.warning(f"Local folder does not exist: {local_folder}")
            with self.not_matched_lock:
                with open(self.not_matched_log, 'a') as f:
                    f.write(msg)
            return False
        
        # Get metadata from database
        district, sro, year = self.get_metadata_from_database(do_id)
        
        if not district or not sro or not year:
            msg = f"{do_id} | {local_folder} | N/A | Could not fetch metadata from database\n"
            logger.error(f"Could not fetch metadata for do_id: {do_id}")
            with self.not_matched_lock:
                with open(self.not_matched_log, 'a') as f:
                    f.write(msg)
            return False
        
        # Map district and format sro
        district = map_district(district)
        sro = sro.replace(" ", "_")
        
        # Find subdirectories in the do_id folder
        subdirs = [d for d in os.listdir(local_folder) 
                   if os.path.isdir(os.path.join(local_folder, d))]
        
        if not subdirs:
            msg = f"{do_id} | {local_folder} | N/A | No subdirectories found\n"
            logger.warning(f"No subdirectories found in: {local_folder}")
            with self.not_matched_lock:
                with open(self.not_matched_log, 'a') as f:
                    f.write(msg)
            return False
        
        all_matched = True
        
        # Check each subdirectory
        for folder_name in subdirs:
            local_full_path = os.path.join(local_folder, folder_name)
            s3_path = f"s3://calysosro/marshal/{district}/{sro}/{year}/{do_id}/{folder_name}/"
            
            logger.info(f"Comparing {local_full_path} with {s3_path}")
            
            # Get local files
            local_files = self.get_local_files(local_full_path)
            if local_files is None:
                msg = f"{do_id} | {local_full_path} | {s3_path} | Failed to read local files\n"
                with self.not_matched_lock:
                    with open(self.not_matched_log, 'a') as f:
                        f.write(msg)
                all_matched = False
                continue
            
            # Get S3 files from cache (no network request!)
            s3_files = self.get_s3_files_for_folder(s3_path, s3_cache)
            if s3_files is None:
                msg = f"{do_id} | {local_full_path} | {s3_path} | Failed to read S3 files from cache\n"
                with self.not_matched_lock:
                    with open(self.not_matched_log, 'a') as f:
                        f.write(msg)
                all_matched = False
                continue
            
            # Compare file lists
            local_set = set(local_files)
            s3_set = set(s3_files)
            
            missing_in_s3 = local_set - s3_set
            extra_in_s3 = s3_set - local_set
            
            if missing_in_s3 or extra_in_s3:
                all_matched = False
                msg = f"{do_id} | {local_full_path} | {s3_path} | Mismatch - Missing in S3: {len(missing_in_s3)}, Extra in S3: {len(extra_in_s3)}\n"
                with self.not_matched_lock:
                    with open(self.not_matched_log, 'a') as f:
                        f.write(msg)
                        if missing_in_s3:
                            f.write(f"  Missing files: {missing_in_s3}\n")
                        if extra_in_s3:
                            f.write(f"  Extra files: {extra_in_s3}\n")
                logger.warning(f"Mismatch for {folder_name}: Missing={len(missing_in_s3)}, Extra={len(extra_in_s3)}")
            else:
                msg = f"{do_id} | {local_full_path} | {s3_path} | MATCHED ({len(local_files)} files)\n"
                with self.matched_lock:
                    with open(self.matched_log, 'a') as f:
                        f.write(msg)
                logger.info(f"MATCHED: {folder_name} ({len(local_files)} files)")
        
        return all_matched

    def verify_batch(self, batch_file, s3_paths_file, cache_file, max_workers=5, force_refresh=False):
        """Verify all do_ids from batch file using multithreading.
        
        Args:
            batch_file: Path to file containing DO IDs
            s3_paths_file: Path to file containing S3 paths to query
            cache_file: Path to file for caching S3 file list
            max_workers: Maximum number of concurrent threads (default: 5)
            force_refresh: If True, ignore cache and fetch fresh from S3
        """
        logger.info(f"Reading batch file: {batch_file}")
        
        with open(batch_file, 'r') as f:
            do_ids = [line.strip() for line in f if line.strip()]
        
        logger.info(f"Found {len(do_ids)} DO IDs to verify")
        
        # Fetch all S3 files from paths (with caching support)
        logger.info("=" * 80)
        logger.info("Step 1: Fetching S3 files from specified paths")
        logger.info("=" * 80)
        s3_cache = self.get_all_s3_files_from_paths(s3_paths_file, cache_file, force_refresh)
        
        if s3_cache is None or len(s3_cache) == 0:
            logger.error("Failed to fetch S3 files or cache is empty. Aborting verification.")
            return
        
        logger.info("=" * 80)
        logger.info(f"Step 2: Verifying {len(do_ids)} DO IDs using {max_workers} worker threads")
        logger.info("=" * 80)
        
        matched_count = 0
        not_matched_count = 0
        
        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks with the cached S3 file list
            future_to_do_id = {
                executor.submit(self.verify_folder, do_id, s3_cache): do_id 
                for do_id in do_ids
            }
            
            # Process completed tasks
            for future in as_completed(future_to_do_id):
                do_id = future_to_do_id[future]
                try:
                    if future.result():
                        matched_count += 1
                    else:
                        not_matched_count += 1
                except Exception as e:
                    logger.error(f"Error verifying do_id {do_id}: {e}")
                    not_matched_count += 1
        
        logger.info("=" * 80)
        logger.info(f"Verification complete!")
        logger.info(f"Total DO IDs: {len(do_ids)}")
        logger.info(f"Matched: {matched_count}")
        logger.info(f"Not Matched: {not_matched_count}")
        logger.info(f"Matched log: {self.matched_log}")
        logger.info(f"Not Matched log: {self.not_matched_log}")
        logger.info("=" * 80)


if __name__ == "__main__":
    import sys
    
    batch_file = "/root/Documents/real_estate_existing_data_handler/logs/batch_lines.log"
    s3_paths_file = "/root/Documents/real_estate_existing_data_handler/logs/s3_path"
    cache_file = "/root/Documents/real_estate_existing_data_handler/logs/s3_files_cache.txt"
    
    # Parse command line arguments
    max_workers = 5  # default
    force_refresh = False  # default: use cache if available
    
    if len(sys.argv) > 1:
        try:
            max_workers = int(sys.argv[1])
            logger.info(f"Using custom worker count: {max_workers}")
        except ValueError:
            logger.warning(f"Invalid worker count '{sys.argv[1]}', using default: 5")
    
    if len(sys.argv) > 2 and sys.argv[2].lower() in ['--refresh', '-r', 'refresh']:
        force_refresh = True
        logger.info("Force refresh enabled: will ignore cache and fetch fresh from S3")
    
    verifier = S3Verifier()
    verifier.verify_batch(batch_file, s3_paths_file, cache_file, max_workers, force_refresh)


