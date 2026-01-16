
import pytest
import duckdb
from unittest.mock import patch
from src.loader import load_data

def test_load_data_creates_graph(mock_config):
    """
    Test that load_data function correctly loads tables and defines the property graph.
    """
    # Mock load_config to return our test data config
    with patch("src.loader.load_config", return_value=mock_config):
        conn = load_data(config_path="dummy_path")  # Path is ignored due to mock
        
        # 1. Verify Tables Exist
        tables = conn.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        assert "nodes_entities" in table_names
        assert "nodes_officers" in table_names
        
        # 2. Verify Graph Exists (implicit check via query)
        # Note: DuckDB doesn't have a simple "SHOW GRAPHS" command standardized yet easily parsable?
        # We try to run a graph query.
        
        # Simple Graph Query: Match all Officer nodes
        res = conn.execute("SELECT count(*) FROM GRAPH_TABLE (icij_graph MATCH (n:officer))").fetchone()[0]
        assert res > 0

def test_graph_query_relationships(mock_config):
    """
    Test a graph query that traverses a relationship.
    """
    with patch("src.loader.load_config", return_value=mock_config):
        conn = load_data(config_path="dummy_path")
        
        # Path: Officer -> Entity
        # We know Officer A -> Entity X exists from test data
        query = """
        SELECT o_name, e_name 
        FROM GRAPH_TABLE (icij_graph 
            MATCH (o:officer)-[r:related_to_officer_entity]->(e:entity)
            COLUMNS (o.name as o_name, e.name as e_name)
        )
        """
        results = conn.execute(query).fetchall()
        
        found = False
        for row in results:
            if row[0] == 'Officer A' and row[1] == 'Entity X':
                found = True
                break
        
        assert found, "Relationship Officer A -> Entity X not found in graph query"

def test_graph_query_shortest_path(mock_config):
    """
    Test a shortest path query (if DuckPGQ supports it or standard path match).
    Checking 2-hop: Officer -> Entity -> Address
    """
    with patch("src.loader.load_config", return_value=mock_config):
        conn = load_data(config_path="dummy_path")
        
        # Path: Officer A -> Entity X -> Address 1
        # DuckPGQ strict mode requires edge labels or variable patterns.
        # We'll use a specific query with defined edge variables to avoid "All patterns must bind to a label"
        query = """
        SELECT o_name, a_name
        FROM GRAPH_TABLE (icij_graph 
            MATCH (o:officer)-[r1:related_to_officer_entity]->(e:entity)-[r2:related_to_entity_address]->(a:address)
            COLUMNS (o.name as o_name, a.name as a_name)
        )
        """
        results = conn.execute(query).fetchall()
        
        found = False
        for row in results:
            if row[0] == 'Officer A' and row[1] == 'Address 1':
                found = True
                break
        
        assert found, "2-hop path Officer A -> Entity X -> Address 1 not found"
