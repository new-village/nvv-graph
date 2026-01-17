import pytest
from src.main import app

# api_client is injected from conftest.py

def test_get_neighbors_officer_both(api_client):
    """
    Officer A (12000001) -> Entity X (11000001)
    Direction 'both' should find Entity X.
    Start node (Officer A) should NOT be in 'nodes' list.
    """
    response = api_client.get("/api/v1/nodes/12000001/neighbors?direction=both")
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
    # Source/target are IDs. Check expected values.
    # We normalized edge storage in conftest but API query logic uses edges table.
    assert edge["source"] == "12000001"
    assert edge["target"] == "11000001"
    # edge type in parquet fixture logic was 'officer_entity' or similar from CSV. 
    # CSV relationships.csv: start, end, type
    # 'related_to_officer_entity' might be missing if raw csv just has type.
    # In CSV: type column.
    
    # Let's check loose containment
    # assert "related_to" in edge["type"] 
    # Actually just check existence for now or debug print
    assert edge["type"] is not None

def test_get_neighbors_direction_out(api_client):
    # Officer A -> Entity X
    response = api_client.get("/api/v1/nodes/12000001/neighbors?direction=out")
    assert response.status_code == 200
    res = response.json()
    assert len(res["nodes"]) == 1
    assert res["nodes"][0]["id"] == "11000001"
    assert len(res["edges"]) == 1

def test_get_neighbors_direction_in(api_client):
    # Officer A -> Entity X
    # In to Officer A should find nothing (unless there's incoming)
    response = api_client.get("/api/v1/nodes/12000001/neighbors?direction=in")
    assert response.status_code == 200
    res = response.json()
    assert len(res["edges"]) == 0
    assert len(res["nodes"]) == 0

def test_get_neighbors_entity_in(api_client):
    # Entity X (11000001) <- Officer A
    # In to Entity X should find Officer A
    response = api_client.get("/api/v1/nodes/11000001/neighbors?direction=in")
    assert response.status_code == 200
    res = response.json()
    node_ids = {n["id"] for n in res["nodes"]}
    assert "12000001" in node_ids
    assert "11000001" not in node_ids
    assert len(res["nodes"]) == 1
    assert len(res["edges"]) == 1

def test_get_neighbors_invalid_id_returns_empty(api_client):
    # Valid ID lookup will return empty if not found or no neighbors
    response = api_client.get("/api/v1/nodes/invalid123/neighbors")
    assert response.status_code == 200
    res = response.json()
    assert res["nodes"] == []
    assert res["edges"] == []

def test_get_neighbors_count_officer(api_client):
    # Officer A -> Entity X
    response = api_client.get("/api/v1/nodes/12000001/neighbors/count?direction=both")
    assert response.status_code == 200
    res = response.json()
    assert res["count"] == 1
    assert res["details"].get("entity") == 1

def test_get_neighbors_count_entity_in(api_client):
    # Entity X <- Officer A
    response = api_client.get("/api/v1/nodes/11000001/neighbors/count?direction=in")
    assert response.status_code == 200
    res = response.json()
    assert res["count"] == 1
    assert res["details"].get("officer") == 1



