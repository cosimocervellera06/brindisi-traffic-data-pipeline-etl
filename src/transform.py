import pandas as pd
import hashlib
import logging

class Transformer:
    """
    Handles data cleaning, normalization, and feature engineering.
    Converts heterogeneous raw schemas into a unified format for database ingestion.
    """
    def __init__(self):
        # Specific mapping for Schema 2 (19 columns format) to match the unified schema
        self.schema_2_mapping = {
            'fclass': 'road_class',
            'Distanza': 'length',
            'bridge': 'is_bridge',
            'tunnel': 'is_tunnel',
            'maxspeed': 'max_speed',
            'output_speed km/h': 'speed',
            'output_distanza in metri': 'distance',
            'output_tempo percorrenza in secondi': 'travel_time',
            'startpoint': 'xy_start',
            'Endpoint': 'xy_end'
        }

    def generate_segment_hash(self, row):
        """
        Generates a deterministic unique surrogate key for each road segment.
        Uses a combination of OSM_ID and coordinates to ensure global uniqueness.
        """
        unique_str = f"{row['osm_id']}_{row['xy_start']}_{row['xy_end']}"
        return hashlib.md5(unique_str.encode()).hexdigest()

    def clean_and_normalize(self, df, file_info):
        """
        Executes the full transformation pipeline:
        1. Column Standardization
        2. Data Sanitization & Type Conversion
        3. Metadata & Surrogate Key Generation
        4. Quality Control (Outlier Filtering)
        """
        
        # --- PHASE 1: COLUMN STANDARDIZATION ---
        if file_info["schema"] == 2:
            df = df.rename(columns=self.schema_2_mapping)
        else:
            # Schema 1: rename 'highway' to unified 'road_class'
            df = df.rename(columns={'highway': 'road_class'})
            # Inject default values for columns missing in Schema 1
            df['max_speed'] = 0
            df['is_bridge'] = 'f'  # PostgreSQL boolean format
            df['is_tunnel'] = 'f'

        # --- PHASE 2: DATA CLEANING AND TYPE CONVERSION ---
        # Normalize bridge/tunnel indicators into boolean values
        if 'is_bridge' in df.columns:
            df['is_bridge'] = df['is_bridge'].apply(
                lambda x: True if str(x).lower() in ['t', 'true', 'y', 'bridge'] else False
            )
        if 'is_tunnel' in df.columns:
            df['is_tunnel'] = df['is_tunnel'].apply(
                lambda x: True if str(x).lower() in ['t', 'true', 'y', 'tunnel'] else False
            )

        # Remove rows missing critical identification or metric data
        df = df.dropna(subset=['osm_id', 'speed', 'xy_start', 'xy_end'])

        # --- PHASE 3: METADATA GENERATION ---
        # Generate the unique hash ID for the road segment
        df['segment_hash'] = df.apply(self.generate_segment_hash, axis=1)

        # Handle temporal metadata: inject reference date if 'datetime' is missing (Schema 2)
        if 'datetime' not in df.columns:
            df['timestamp'] = pd.to_datetime(file_info['ref_date'])
        else:
            df['timestamp'] = pd.to_datetime(df['datetime'])
            
        # Feature Engineering: Determine day phase based on hour
        if 'daytime' not in df.columns:
            df['day_phase'] = df['timestamp'].dt.hour.apply(
                lambda x: 'day' if 6 <= x <= 20 else 'night'
            )
        else:
            df['day_phase'] = df['daytime']

        # --- PHASE 4: QUALITY CONTROL ---
        # Filter speed outliers based on logical thresholds (0-150 km/h)
        df = df[(df['speed'] >= 0) & (df['speed'] <= 150)]

        # --- PHASE 5: FINAL STRUCTURE ENFORCEMENT ---
        # Ensure all mandatory columns for the database Loader exist
        required_cols = [
            'osm_id', 'name', 'road_class', 'segment_hash', 'length', 
            'xy_start', 'xy_end', 'max_speed', 'is_bridge', 'is_tunnel', 
            'timestamp', 'speed', 'travel_time', 'day_phase'
        ]
        
        for col in required_cols:
            if col not in df.columns:
                df[col] = None

        # Return the DataFrame with columns in the specific order required by the Loader
        return df[required_cols]