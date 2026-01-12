import duckdb
import yaml
import os
from typing import Optional

def load_config(config_path: str = "config/icij.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def get_connection(database: str = ":memory:") -> duckdb.DuckDBPyConnection:
    return duckdb.connect(database)

def load_data(
    config_path: str = "config/icij.yaml", 
    conn: Optional[duckdb.DuckDBPyConnection] = None
) -> duckdb.DuckDBPyConnection:
    config = load_config(config_path)
    
    if conn is None:
        conn = get_connection()
        
    for source in config.get("sources", []):
        table_name = source["table"]
        file_path = source["path"]
        
        # Ensure path is relative to workspace root if running from elsewhere, 
        # or assume script is run from root. 
        # Here we just use the path as defined in config.
        
        print(f"Loading {table_name} from {file_path}...")
        
        if table_name == "relationships":
            # Relationships must be a TABLE for DuckPGQ
            conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM '{file_path}'")
        else:
            # Others can be VIEWS (Zero-Copy)
            conn.execute(f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM '{file_path}'")
    
    # Enable DuckPGQ
    print("Enabling DuckPGQ extension...")
    conn.execute("INSTALL duckpgq FROM community")
    conn.execute("LOAD duckpgq")
    
    # Create unified nodes table for Property Graph (Materialized for DuckPGQ)
    print("Creating unified 'nodes_all' table...")
    # Select common columns and add type
    conn.execute("""
        CREATE OR REPLACE TABLE nodes_all AS 
        SELECT node_id, name, 'Entity' as type FROM nodes_entities
        UNION ALL
        SELECT node_id, address as name, 'Address' as type FROM nodes_addresses
        UNION ALL
        SELECT node_id, name, 'Intermediary' as type FROM nodes_intermediaries
        UNION ALL
        SELECT node_id, name, 'Officer' as type FROM nodes_officers
    """)

    # Create Property Graph
    print("Defining Property Graph 'icij_graph'...")
    conn.execute("""
        CREATE OR REPLACE PROPERTY GRAPH icij_graph
        VERTEX TABLES (
            nodes_all LABEL IcijNode
        )
        EDGE TABLES (
            relationships 
            SOURCE KEY (node_id_start) REFERENCES nodes_all (node_id)
            DESTINATION KEY (node_id_end) REFERENCES nodes_all (node_id)
            LABEL related_to
        )
    """)
    
    return conn

if __name__ == "__main__":
    # Test run
    con = load_data()
    print("Data loaded and Graph created successfully.")
    print("Tables:", con.execute("SHOW TABLES").fetchall())

