import sys
import pytest
import os
import shutil
import tempfile
import duckdb
from typing import Generator

# Ensure src module is importable
sys.path.append(os.getcwd())

@pytest.fixture(scope="session")
def test_data_dir() -> Generator[str, None, None]:
    """
    Creates a temporary directory with test data converted to parquet.
    """
    # Create temp dir
    temp_dir = tempfile.mkdtemp()
    data_dir = os.path.join(temp_dir, "data")
    os.makedirs(data_dir, exist_ok=True)
    
    source_dir = os.path.join(os.getcwd(), "tests/data")
    
    # We need to convert CSVs to nodes.parquet and edges.parquet
    # Matching schema: 
    # nodes: id (BIGINT), display_name (VARCHAR), node_type (VARCHAR)
    # edges: id (UINTEGER), source_id (BIGINT), target_id (BIGINT), edge_type (VARCHAR)
    
    conn = duckdb.connect(":memory:")
    
    # 1. NODES
    # Union all node CSVs
    # Mappings: 
    # entity: node_id->id, name->display_name, 'entity'->node_type
    # address: node_id->id, address->display_name, 'address'->node_type
    # officer: node_id->id, name->display_name, 'officer'->node_type
    # intermediary: node_id->id, name->display_name, 'intermediary'->node_type
    
    # Helper to load and standardize
    def load_node_csv(filename, ntype, name_col="name"):
        path = os.path.join(source_dir, filename)
        if not os.path.exists(path):
            return 
        conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE tmp_{ntype} AS 
            SELECT node_id as id, {name_col} as display_name, '{ntype}' as node_type 
            FROM read_csv_auto('{path}')
        """)
        
    load_node_csv("nodes-entities.csv", "entity", "name")
    load_node_csv("nodes-addresses.csv", "address", "address")
    load_node_csv("nodes-officers.csv", "officer", "name")
    load_node_csv("nodes-intermediaries.csv", "intermediary", "name")
    
    # Union all tmp tables
    tables = conn.execute("SHOW TABLES").fetchall()
    node_tables = [t[0] for t in tables if t[0].startswith("tmp_")]
    
    if node_tables:
        q = " UNION ALL ".join([f"SELECT * FROM {t}" for t in node_tables])
        conn.execute(f"CREATE TABLE all_nodes AS {q}")
        conn.execute(f"COPY all_nodes TO '{os.path.join(data_dir, 'nodes.parquet')}' (FORMAT PARQUET)")

    # 2. EDGES
    rels_path = os.path.join(source_dir, "relationships.csv")
    if os.path.exists(rels_path):
        # relationships.csv: node_id_start, node_id_end, rel_type ...
        # map: node_id_start->source_id, node_id_end->target_id, rel_type->edge_type
        # generate id? row_number()
        conn.execute(f"""
            CREATE TABLE all_edges AS
            SELECT 
                row_number() OVER () as id, 
                node_id_start as source_id, 
                node_id_end as target_id, 
                rel_type as edge_type 
            FROM read_csv_auto('{rels_path}')
        """)
        conn.execute(f"COPY all_edges TO '{os.path.join(data_dir, 'edges.parquet')}' (FORMAT PARQUET)")

        
    conn.close()
    
    yield data_dir
    
    # Cleanup
    shutil.rmtree(temp_dir)

@pytest.fixture
def api_client(test_data_dir):
    """
    Creates a TestClient with overridden dependencies using test data.
    """
    from src.main import app
    from src.deps import get_db
    from fastapi.testclient import TestClient
    
    # Override DATA_DIR
    os.environ["DATA_DIR"] = test_data_dir
    
    # Reset/Reload singleton if needed (though we create fresh connection in override)
    # But get_db uses a global singleton variable in deps.py.
    # We should override the dependency directly.
    
    # Using real loader which uses DATA_DIR
    from src.loader import load_data
    
    # Create a fresh connection for this test
    # Note: load_data() now expects env var DATA_DIR to be set.
    conn = load_data()
    
    def override_get_db():
        return conn

    app.dependency_overrides[get_db] = override_get_db
    
    client = TestClient(app)
    yield client
    
    app.dependency_overrides = {}
    conn.close()
