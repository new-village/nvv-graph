import pytest
from src.main import app

# api_client is now self-contained in conftest.py

def test_get_node_officer_found(api_client):
    """
    Test getting an existing Officer node.
    Officer A has node_id 12000001
    """
    response = api_client.get("/api/v1/nodes/12000001")
    assert response.status_code == 200
    res = response.json()
    assert res["count"] == 1
    # Note: id in parquet from conftest is BIGINT. API returns what DB has.
    # We should check loosely or cast.
    assert str(res["data"]["id"]) == "12000001"
    assert res["data"]["display_name"] == "Officer A"

def test_get_node_entity_found(api_client):
    """
    Test getting an existing Entity node.
    Entity X has node_id 11000001
    """
    response = api_client.get("/api/v1/nodes/11000001")
    assert response.status_code == 200
    res = response.json()
    assert res["count"] == 1
    assert str(res["data"]["id"]) == "11000001"
    assert res["data"]["display_name"] == "Entity X"
    # jurisdiction is no longer available in the simplified nodes.parquet schema
    # assert res["data"]["jurisdiction"] == "BVI"

def test_get_node_not_found(api_client):
    """
    Test getting a non-existent node.
    """
    response = api_client.get("/api/v1/nodes/99999999")
    # Should now return 200 with count 0
    assert response.status_code == 200
    res = response.json()
    assert res["count"] == 0
    assert res["data"] is None

def test_get_node_invalid_id_not_found(api_client):
    """
    Test getting a node with invalid ID (e.g. string vs int check).
    Since ID is generic string in path, it just won't be found.
    """
    response = api_client.get("/api/v1/nodes/invalid123")
    assert response.status_code == 200
    res = response.json()
    assert res["count"] == 0
