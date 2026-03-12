import pandas as pd
import json
import logging
from pathlib import Path

class SchemaAuditor:
    """
    Metadata Auditor to detect schema drift and validate structural 
    consistency across the entire traffic dataset.
    """

    def __init__(self, source_dir: str, report_dir: str):
        self.source_dir = Path(source_dir)
        self.report_dir = Path(report_dir)
        self.schema_registry = {}
        
        # Ensure report directory exists
        self.report_dir.mkdir(parents=True, exist_ok=True)

    def perform_audit(self):
        """
        Iterates through files to identify unique data structures
        and generate statistical metadata.
        """
        files = list(self.source_dir.rglob("*.csv"))
        print(f"[INFO] Auditing {len(files)} files for structural consistency...")

        for file_path in files:
            try:
                # Load header and sample to analyze structure
                df = pd.read_csv(file_path, nrows=100)
                
                # Create a signature based on sorted column names
                cols = sorted(df.columns.tolist())
                schema_signature = hash(tuple(cols))

                if schema_signature not in self.schema_registry:
                    self.schema_registry[schema_signature] = {
                        "column_count": len(df.columns),
                        "column_names": df.columns.tolist(),
                        "data_types": df.dtypes.astype(str).to_dict(),
                        "null_ratio": df.isnull().mean().to_dict(),
                        "samples": [file_path.name],
                        "occurrence_count": 1
                    }
                else:
                    self.schema_registry[schema_signature]["occurrence_count"] += 1
                    if len(self.schema_registry[schema_signature]["samples"]) < 3:
                        self.schema_registry[schema_signature]["samples"].append(file_path.name)
            
            except Exception as e:
                print(f"[ERROR] Failed to audit {file_path.name}: {e}")

        self._export_audit_report()

    def _export_audit_report(self):
        """Saves the audit findings to a JSON file for documentation."""
        report_path = self.report_dir / "schema_audit_report.json"
        with open(report_path, "w") as f:
            json.dump(self.schema_registry, f, indent=4)
        
        print(f"\n[SUCCESS] Audit complete. Report generated at: {report_path}")
        print(f"Total Unique Schemas Detected: {len(self.schema_registry)}")

if __name__ == "__main__":
    # Paths configured for the project structure
    SOURCE = "data_raw/sorted"
    REPORTS = "docs/analysis"
    
    auditor = SchemaAuditor(SOURCE, REPORTS)
    auditor.perform_audit()