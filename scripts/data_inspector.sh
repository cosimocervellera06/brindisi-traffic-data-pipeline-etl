#!/bin/bash

# Define paths relative to the project root
DATA_DIR="./data_raw/sorted"
REPORT_DIR="./docs/analysis"
REPORT_FILE="$REPORT_DIR/raw_dataset_report.txt"

# Ensure the report directory exists
mkdir -p "$REPORT_DIR"

echo "=== BRINDISI TRAFFIC DATASET INSPECTION REPORT ===" > "$REPORT_FILE"
echo "Analysis timestamp: $(date)" >> "$REPORT_FILE"
echo "------------------------------------------------" >> "$REPORT_FILE"

# Count total CSV files
total_files=$(find "$DATA_DIR" -name "*.csv" | wc -l)
echo "Total files found: $total_files" | tee -a "$REPORT_FILE"

# Iterate through CSV files and perform structural analysis
find "$DATA_DIR" -name "*.csv" -print0 | while IFS= read -r -d '' file; do
    echo "Inspecting: $file"
    
    # 1. Extract header row
    header=$(head -n 1 "$file")
    
    # 2. Count lines (excluding header)
    lines=$(($(wc -l < "$file") - 1))
    
    # 3. Check for missing fields (basic check for empty commas)
    missing_data=$(grep -c ",," "$file")
    
    # Write findings to the report file
    echo "FILE: $(basename "$file")" >> "$REPORT_FILE"
    echo "  - Row Count: $lines" >> "$REPORT_FILE"
    echo "  - Columns Detected: $header" >> "$REPORT_FILE"
    
    if [ "$missing_data" -gt 0 ]; then
        echo "  - WARNING: $missing_data rows with potential missing values detected!" >> "$REPORT_FILE"
    fi
    echo "------------------------------------------------" >> "$REPORT_FILE"
done

echo "Inspection complete. See results in: $REPORT_FILE"