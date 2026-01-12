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
        # Use CREATE VIEW to avoid copying data (Zero-Copy)
        conn.execute(f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM '{file_path}'")
        
    return conn

if __name__ == "__main__":
    # Test run
    con = load_data()
    print("Data loaded successfully.")
    print("Tables:", con.execute("SHOW TABLES").fetchall())
