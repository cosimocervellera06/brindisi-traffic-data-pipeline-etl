import pandas as pd
import os
from pathlib import Path
from tqdm import tqdm

class DatasetValidator:
    """
    Performs cross-file validation to ensure data consistency and 
    verify primary key integrity across the entire dataset.
    """
    def __init__(self, data_path):
        self.data_path = Path(data_path)
        self.all_files = list(self.data_path.rglob("*.csv"))
        self.road_metadata_cache = []

    def run_validation(self):
        print(f"\n--- STARTING DEEP VALIDATION ON {len(self.all_files)} FILES ---")
        
        for file_path in tqdm(self.all_files, desc="Processing files"):
            try:
                # Load file - analyzing schemas to detect inconsistencies
                df = pd.read_csv(file_path)
                
                # Standardize column naming across different schemas
                id_col = 'id' if 'id' in df.columns else 'fid'
                name_col = 'name'
                osm_col = 'osm_id'
                
                if osm_col in df.columns and id_col in df.columns:
                    # Capture unique road-segment mappings
                    cols_to_keep = [id_col, osm_col, name_col]
                    if 'xy_start' in df.columns: 
                        cols_to_keep.append('xy_start')
                    
                    subset = df[cols_to_keep].drop_duplicates()
                    subset['file_source'] = file_path.name
                    self.road_metadata_cache.append(subset)
                    
            except Exception as e:
                print(f"[ERROR] Failed to process {file_path.name}: {e}")

        # Consolidate all metadata for global analysis
        full_df = pd.concat(self.road_metadata_cache, ignore_index=True)

        print("\n" + "="*60)
        print("DATA CONSISTENCY ANALYSIS RESULTS")
        print("="*60)

        # 1. GLOBAL IDENTIFIER TEST
        # Checking if local IDs (1, 2, 3...) overlap across different OSM entities
        id_check = full_df.groupby('id' if 'id' in full_df.columns else 'fid')['osm_id'].nunique()
        non_unique_ids = id_check[id_check > 1]
        
        if not non_unique_ids.empty:
            print("[WARNING] Identifier collision detected: local IDs are NOT globally unique.")
            print("Action taken: RoadSegment must be modeled as a WEAK ENTITY.")
            print("Implementation: A composite or surrogate Hash ID is required.")
        else:
            print("[OK] CSV identifiers appear to be globally consistent.")

        # 2. ROAD METADATA TEST (OSM_ID vs NAME)
        # Verifying if the same OSM_ID references different street names
        name_consistency = full_df.groupby('osm_id')['name'].nunique()
        inconsistent_roads = name_consistency[name_consistency > 1]
        
        if not inconsistent_roads.empty:
            print(f"\n[NOTICE] {len(inconsistent_roads)} OSM_IDs have inconsistent names across files.")
            print("Sample of inconsistent metadata:")
            print(full_df[full_df['osm_id'].isin(inconsistent_roads.index[:3].tolist())][['osm_id', 'name']].drop_duplicates())
        else:
            print("\n[OK] Metadata consistency: Each OSM_ID maps to a unique street name.")

        print("="*60 + "\n")
        
        self._save_to_file(non_unique_ids.empty, len(inconsistent_roads))

    def _save_to_file(self, ids_ok, inconsistent_count):
        report_path = Path("docs/analysis/validation_summary.md")
        os.makedirs("docs/analysis", exist_ok=True)
        with open(report_path, "w") as f:
            f.write("# Dataset Validation Summary\n")
            f.write(f"ID Uniqueness: {'PASSED' if ids_ok else 'FAILED - Weak Entity Model Required'}\n")
            f.write(f"Inconsistent Road Names: {inconsistent_count}\n")
        print(f"[INFO] Validation summary saved to: {report_path}")

if __name__ == "__main__":
    # Path configured for Docker environment
    validator = DatasetValidator("data_raw/sorted")
    validator.run_validation()