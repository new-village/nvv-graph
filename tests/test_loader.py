import pytest
from unittest.mock import patch
import os
from src.loader import load_data

def test_load_data_creates_tables(test_data_dir):
    """
    Test that load_data function correctly loads nodes and edges tables.
    """
    # Override DATA_DIR env var to point to test_data_dir
    with patch.dict(os.environ, {"DATA_DIR": test_data_dir}):
        conn = load_data()
        
        # 1. Verify Tables Exist
        tables = conn.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        assert "nodes" in table_names
        assert "edges" in table_names
        
        # 2. Verify Content
        n_count = conn.execute("SELECT count(*) FROM nodes").fetchone()[0]
        assert n_count > 0
        
        e_count = conn.execute("SELECT count(*) FROM edges").fetchone()[0]
        assert e_count > 0

def test_edge_query(test_data_dir):
    """
    Test that we can join nodes and edges.
    """
    with patch.dict(os.environ, {"DATA_DIR": test_data_dir}):
        conn = load_data()
        
        # Query: Officer A (12000001) linked to Entity X (11000001)
        # In test data: Officer A -> Entity X
        # edges.parquet should have this link.
        
        query = """
        SELECT n_source.display_name, n_target.display_name
        FROM edges e
        JOIN nodes n_source ON e.source_id = n_source.id
        JOIN nodes n_target ON e.target_id = n_target.id
        WHERE n_source.display_name = 'Officer A' AND n_target.display_name = 'Entity X'
        """
        results = conn.execute(query).fetchall()
        
        assert len(results) > 0
