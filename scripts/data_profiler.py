import os
import pandas as pd
import json
from pathlib import Path
from datetime import datetime

class TrafficDataProfiler:
    """
    Data Profiling tool for Brindisi Traffic Dataset.
    Identifies schema variations, data types, and quality issues across multiple CSV files.
    """

    def __init__(self, data_path):
        self.data_path = Path(data_path)
        self.schemas = {}
        self.osm_mapping_data = [] 
        self.report_stats = {
            "total_files_processed": 0,
            "corrupted_files": [],
            "start_time": datetime.now().isoformat(),
            "unique_schemas_found": 0
        }

    def get_column_signature(self, df):
        """Creates a unique signature based on sorted column names."""
        return tuple(sorted(df.columns.tolist()))

    def profile_dataset(self):
        """Scans the directory and groups files by schema signature."""
        print(f"Starting profiling at: {self.data_path}")
        
        all_csv_files = list(self.data_path.rglob("*.csv"))
        
        for file_path in all_csv_files:
            try:
                df = pd.read_csv(file_path, nrows=500)
                self.report_stats["total_files_processed"] += 1

                if 'osm_id' in df.columns and 'id' in df.columns:
                    temp_df = df[['osm_id', 'id', 'name']].drop_duplicates()
                    self.osm_mapping_data.append(temp_df)
                
                signature = self.get_column_signature(df)
                
                if signature not in self.schemas:
                    self.schemas[signature] = {
                        "column_list": list(df.columns),
                        "column_count": len(df.columns),
                        "files": [],
                        "data_types": df.dtypes.apply(lambda x: str(x)).to_dict(),
                        "null_thresholds": df.isnull().mean().to_dict()
                    }
                
                self.schemas[signature]["files"].append(str(file_path.relative_to(self.data_path.parent)))
                
            except Exception as e:
                self.report_stats["corrupted_files"].append({"file": str(file_path), "error": str(e)})

        self.report_stats["unique_schemas_found"] = len(self.schemas)
        self._print_professional_report()
        self.analyze_osm_consistency()

    def analyze_osm_consistency(self):
        """
        Verifica se ad un singolo osm_id corrispondono più segmenti (id).
        Fondamentale per giustificare il modello EER.
        """
        print("\n" + "="*60)
        print("CONSISTENCY ANALYSIS: OSM_ID vs SEGMENT_ID")
        print("="*60)
        
        if not self.osm_mapping_data:
            print("Insufficient data for OSM consistency check.")
            return

        full_mapping = pd.concat(self.osm_mapping_data).drop_duplicates()

        analysis = full_mapping.groupby('osm_id').agg({
            'id': 'nunique',
            'name': 'unique'
        }).rename(columns={'id': 'segment_count'})

        multi_segments = analysis[analysis['segment_count'] > 1]

        if not multi_segments.empty:
            print(f"CRITICAL FINDING: {len(multi_segments)} OSM_IDs are split into multiple segments.")
            print("This proves that osm_id CANNOT be the Primary Key.")
            print("\nExample of multi-segment OSM IDs:")
            print(multi_segments.head(5))
        else:
            print("OSM_ID seems to have a 1:1 mapping with segments in this sample.")

    def _print_professional_report(self):
        """Outputs a structured technical summary of the data profiling."""
        print("-" * 60)
        print("DATA ENGINEERING PROFILING REPORT - BRINDISI TRAFFIC PROJECT")
        print("-" * 60)
        print(f"Total Files Scanned: {self.report_stats['total_files_processed']}")
        print(f"Unique Schemas Detected: {self.report_stats['unique_schemas_found']}")
        print("-" * 60)

        schema_index = 1
        for sig, info in self.schemas.items():
            print(f"\nSCHEMA VERSION {schema_index}")
            print(f"Column Count: {info['column_count']}")
            print(f"Associated Files: {len(info['files'])}")
            print(f"Columns: {info['column_list']}")
            schema_index += 1

        if self.report_stats["corrupted_files"]:
            print("\nWARNING: Corrupted Files Detected")
            for entry in self.report_stats["corrupted_files"]:
                print(f"  - {entry['file']}: {entry['error']}")

    def export_json_report(self, output_file="profiling_report.json"):
        """Exports report metadata."""
        report = {
            "metadata": self.report_stats,
            "schemas": [
                {
                    "id": i,
                    "columns": info["column_list"],
                    "file_count": len(info["files"])
                } for i, info in enumerate(self.schemas.values())
            ]
        }
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=4)
        print(f"\nFull metadata report exported to: {output_file}")

if __name__ == "__main__":
    DATA_DIRECTORY = "data_raw/sorted"
    OUTPUT_FILE = "docs/analysis/profiling_report.json"
    
    os.makedirs("docs/analysis", exist_ok=True)
    
    profiler = TrafficDataProfiler(DATA_DIRECTORY)
    profiler.profile_dataset()
    profiler.export_json_report(OUTPUT_FILE)