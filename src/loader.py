import duckdb
import os

def load_data() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    
    # Path resolution
    data_dir = os.environ.get("DATA_DIR", "data")
    
    nodes_path = os.path.join(data_dir, "nodes.parquet")
    edges_path = os.path.join(data_dir, "edges.parquet")
    
    # Verify existence
    if not os.path.exists(nodes_path):
        raise FileNotFoundError(f"nodes.parquet not found at {nodes_path}")
    if not os.path.exists(edges_path):
        raise FileNotFoundError(f"edges.parquet not found at {edges_path}")

    print(f"Mounting nodes from {nodes_path} as VIEW...")
    conn.execute(f"CREATE OR REPLACE VIEW nodes AS SELECT * FROM '{nodes_path}'")
    
    print(f"Mounting edges from {edges_path} as VIEW...")
    conn.execute(f"CREATE OR REPLACE VIEW edges AS SELECT * FROM '{edges_path}'")
    
    # Indexes generally cannot be created on Views backed by Parquet files in DuckDB
    # We rely on Parquet's internal statistics and DuckDB's pushdown optimization.

    
    print("Data loaded successfully.")
    return conn

if __name__ == "__main__":
    load_data()

