import pandas as pd
import re
from pathlib import Path

class Extractor:
    """
    Handles the discovery and initial ingestion of raw CSV files.
    Identifies data schemas and extracts temporal metadata from filenames.
    """
    def __init__(self, data_path):
        self.data_path = Path(data_path)

    def get_reference_date(self, filename):
        """
        Extracts the reference date from the filename using regex patterns.
        Primary pattern: YYYYMMDD (e.g., 20251106).
        Fallback pattern: YYMMDD (e.g., 251106).
        """
        # Search for 8 consecutive digits starting with 2025
        match = re.search(r'(2025)(\d{2})(\d{2})', filename)
        if match:
            yyyy, mm, dd = match.groups()
            return f"{yyyy}-{mm}-{dd}"
        
        # Fallback for alternative formats (e.g., 251106)
        match_short = re.search(r'(\d{2})(\d{2})(\d{2})', filename)
        if match_short:
            yy, mm, dd = match_short.groups()
            # Safety check: ensure we don't misinterpret '20' '25' from '2025' filenames
            if yy == '20' and len(filename.split('2025')) > 1:
                 # If '2025' is present, this specific short regex shouldn't trigger
                 pass
            else:
                return f"20{yy}-{mm}-{dd}"
                
        return None

    def discover_files(self):
        """
        Scans the data directory and catalogs files based on their schema version.
        Schema 1: 12 columns (Legacy/standard format)
        Schema 2: 19 columns (Extended sensor format)
        """
        files_found = list(self.data_path.rglob("*.csv"))
        catalog = []

        for f in files_found:
            # Skip hidden or temporary system files
            if f.name.startswith('.'): 
                continue
            
            try:
                # Read only headers to determine schema version efficiently
                df_sample = pd.read_csv(f, nrows=0)
                cols = len(df_sample.columns)
                
                # Schema version identification logic
                schema_ver = 1 if cols == 12 else 2
                
                ref_date = self.get_reference_date(f.name)
                
                if not ref_date:
                    print(f"[WARNING] Unable to extract reference date from filename: {f.name}")
                    continue

                catalog.append({
                    "path": f,
                    "name": f.name,
                    "schema": schema_ver,
                    "ref_date": ref_date
                })
            except Exception as e:
                print(f"[ERROR] Failed to scan file {f.name}: {e}")
                
        return catalog

    def load_raw_data(self, file_info):
        """Loads the full CSV content into a pandas DataFrame."""
        return pd.read_csv(file_info["path"])