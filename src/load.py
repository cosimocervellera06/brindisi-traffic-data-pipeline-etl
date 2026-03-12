import psycopg2
from psycopg2.extras import execute_values
import logging

class Loader:
    """
    Handles data persistence into the PostgreSQL database.
    Ensures referential integrity by loading data according to table hierarchies.
    """
    def __init__(self, db_config):
        self.conn = psycopg2.connect(**db_config)
        self.conn.autocommit = True

    def register_source(self, file_info):
        """
        Registers the ingested file in the data_sources registry.
        Returns the source ID to maintain data lineage for each observation.
        """
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO data_sources (name, schema_version, reference_date)
                VALUES (%s, %s, %s) RETURNING id;
            """, (file_info['name'], file_info['schema'], file_info['ref_date']))
            return cur.fetchone()[0]

    def upload_data(self, df, source_id):
        """
        Performs bulk insertion for Roads, Segments, and Observations.
        Uses UPSERT logic (ON CONFLICT) to handle recurring geographic entities.
        """
        with self.conn.cursor() as cur:
            # 1. Road Ingestion: Avoid duplicates for recurring OSM_IDs
            roads = df[['osm_id', 'name', 'road_class']].drop_duplicates()
            execute_values(cur, """
                INSERT INTO roads (osm_id, name, class) VALUES %s
                ON CONFLICT (osm_id) DO NOTHING;
            """, roads.values.tolist())

            # 2. Segment Ingestion: Use the surrogate Hash ID generated during transformation
            segments = df[['segment_hash', 'osm_id', 'length', 'xy_start', 'xy_end', 'max_speed', 'is_bridge', 'is_tunnel']].drop_duplicates()
            execute_values(cur, """
                INSERT INTO road_segments (id, osm_id, length, geom_start, geom_end, max_speed, is_bridge, is_tunnel)
                VALUES %s ON CONFLICT (id) DO NOTHING;
            """, segments.values.tolist())

            # 3. Observation Ingestion: High-frequency traffic metrics
            observations = []
            for _, row in df.iterrows():
                observations.append((
                    row['segment_hash'], source_id, row['timestamp'],
                    row['speed'], row['travel_time'], row['day_phase']
                ))
            
            # Perform bulk insertion for performance optimization
            execute_values(cur, """
                INSERT INTO traffic_observations (segment_id, source_id, timestamp, speed_kmh, travel_time_sec, day_phase)
                VALUES %s;
            """, observations)

    def close(self):
        """Safely closes the database connection."""
        self.conn.close()