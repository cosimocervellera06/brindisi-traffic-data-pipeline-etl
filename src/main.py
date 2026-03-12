import logging
import os
from dotenv import load_dotenv
from extract import Extractor
from transform import Transformer
from load import Loader

# Load environment variables from .env file
load_dotenv()

# Database connection settings from environment variables
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS")
}

# Configure logging to both file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)

def run_pipeline():
    """
    Orchestrates the ETL process: Extracts raw CSV files, 
    Transforms them into a normalized format, and Loads them into PostgreSQL.
    """
    logging.info("--- INTEGRATED ETL START: BRINDISI TRAFFIC PROJECT ---")
    
    # Initialize ETL components
    extractor = Extractor("data_raw/sorted")
    transformer = Transformer()
    loader = Loader(DB_CONFIG)
    
    # Discovery phase
    file_catalog = extractor.discover_files()
    logging.info(f"Extraction Phase: {len(file_catalog)} files cataloged.")

    count_success = 0
    for file_info in file_catalog:
        try:
            # 1. EXTRACT
            raw_df = extractor.load_raw_data(file_info)
            
            # 2. TRANSFORM (Cleaning, Hashing, Normalization)
            clean_df = transformer.clean_and_normalize(raw_df, file_info)
            
            if clean_df.empty:
                logging.warning(f"File {file_info['name']} skipped: no valid data after transformation.")
                continue

            # 3. LOAD (Register data source and insert observations)
            source_id = loader.register_source(file_info)
            loader.upload_data(clean_df, source_id)
            
            count_success += 1
            if count_success % 50 == 0:
                logging.info(f"Progress: {count_success}/{len(file_catalog)} files uploaded...")
            
        except Exception as e:
            logging.error(f"FAILURE on file {file_info['name']}: {e}")

    # Resource cleanup
    loader.close()
    logging.info(f"--- ETL COMPLETE: {count_success} files inserted successfully ---")

if __name__ == "__main__":
    run_pipeline()