import pytest
from fastapi.testclient import TestClient
from src.main import app
from src.deps import get_db, get_config
import duckdb
import os

@pytest.fixture
def api_client(test_data_dir, mock_config):
    conn = duckdb.connect(":memory:")
    # Helper to load data exactly as loader does for consistent state in tests
    # But for mocking simplicity, we do manual table creation
    for source in mock_config["sources"]:
        conn.execute(f"CREATE OR REPLACE TEMP TABLE {source['table']} AS SELECT * FROM '{source['path']}'")

    # Manually run graph creation DDL because we are bypassing loader in test harness for speed/control??
    # Actually, we should call load_data or replicate its graph logic logic.
    # The API query depends on `icij_graph` existing.
    # Replicating graph creation logic here is redundant and brittle.
    # BETTER: Use src.loader.load_data logic but with our mock config.
    from src.loader import load_data
    from unittest.mock import patch
    
    # We need to construct a config object from mock_config
    # loader.load_data expects path or we can mock load_config
    with patch("src.loader.load_config", return_value=mock_config):
         # Also we need to make sure 'processor' logic ran?
         # In mock_config, paths point to CSVs. 
         # load_data calls processor if defined.
         # icij_processor converts csv to parquet and enriches relationships using DuckDB logic.
         # This might be heavy for unit test but necessary for Graph.
         # Let's trust load_data doing its job.
         conn = load_data(config_path="dummy", conn=conn)

    def override_get_db():
        return conn
        
    def override_get_config():
        return mock_config
        
    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_config] = override_get_config
    
    client = TestClient(app)
    yield client
    
    app.dependency_overrides = {}
    conn.close()

def test_get_neighbors_officer_both(api_client):
    """
    Officer A (12000001) -> Entity X (11000001)
    Direction 'both' should find Entity X.
    Start node (Officer A) should NOT be in 'nodes' list.
    """
    response = api_client.get("/api/v1/nodes/officer/12000001/neighbors?direction=both")
    assert response.status_code == 200
    res = response.json()
    
    # Verify Nodes
    # Expect 1 node: Entity X
    node_ids = {n["id"] for n in res["nodes"]}
    assert "11000001" in node_ids
    assert "12000001" not in node_ids
    assert len(res["nodes"]) == 1
    
    # Verify Edges
    assert len(res["edges"]) == 1
    edge = res["edges"][0]
    assert edge["source"] == "12000001"
    assert edge["target"] == "11000001"
    assert "related_to_officer_entity" in edge["type"]

def test_get_neighbors_direction_out(api_client):
    # Officer A -> Entity X
    response = api_client.get("/api/v1/nodes/officer/12000001/neighbors?direction=out")
    assert response.status_code == 200
    res = response.json()
    assert len(res["nodes"]) == 1
    assert res["nodes"][0]["id"] == "11000001"
    assert len(res["edges"]) == 1

def test_get_neighbors_direction_in(api_client):
    # Officer A -> Entity X
    # In to Officer A should find nothing
    response = api_client.get("/api/v1/nodes/officer/12000001/neighbors?direction=in")
    assert response.status_code == 200
    res = response.json()
    assert len(res["edges"]) == 0
    assert len(res["nodes"]) == 0

def test_get_neighbors_entity_in(api_client):
    # Entity X (11000001) <- Officer A
    # In to Entity X should find Officer A
    response = api_client.get("/api/v1/nodes/entity/11000001/neighbors?direction=in")
    assert response.status_code == 200
    res = response.json()
    node_ids = {n["id"] for n in res["nodes"]}
    assert "12000001" in node_ids
    assert "11000001" not in node_ids
    assert len(res["nodes"]) == 1
    assert len(res["edges"]) == 1

def test_get_neighbors_invalid_node_type(api_client):
    # Expect 400
    response = api_client.get("/api/v1/nodes/invalid/123/neighbors")
    assert response.status_code == 400

def test_get_neighbors_count_officer(api_client):
    # Officer A -> Entity X
    response = api_client.get("/api/v1/nodes/officer/12000001/neighbors/count?direction=both")
    assert response.status_code == 200
    res = response.json()
    assert res["count"] == 1
    assert res["details"].get("entity") == 1

def test_get_neighbors_count_entity_in(api_client):
    # Entity X <- Officer A
    response = api_client.get("/api/v1/nodes/entity/11000001/neighbors/count?direction=in")
    assert response.status_code == 200
    res = response.json()
    assert res["count"] == 1
    assert res["details"].get("officer") == 1

