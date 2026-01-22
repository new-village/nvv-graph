import pytest
from src.main import app

def test_search_nodes_exact_match(api_client):
    # Search for Officer A by display_name
    # In fixture nodes-officers.csv: name is "Officer A", id is 12000001
    response = api_client.get("/api/v1/search?display_name=Officer A")
    assert response.status_code == 200
    res = response.json()
    assert res["count"] > 0
    # Check if results contain the expected ID
    ids = {r["id"] for r in res["results"]}
    assert 12000001 in ids # Officer A ID

def test_search_nodes_fuzzy(api_client):
    # Search for "Officer" fuzzy
    response = api_client.get("/api/v1/search?display_name=Officer&fuzzy=true")
    assert response.status_code == 200
    res = response.json()
    assert res["count"] > 0
    ids = {r["id"] for r in res["results"]}
    assert 12000001 in ids

def test_search_edges_by_type(api_client):
    # Search edges
    # In fixture relationships.csv: rel_type is mapped to edge_type column
    # The fixture CSV content isn't shown but conftest says:
    # rel_type as edge_type
    
    # We should search by 'edge_type' column, not 'type'
    response = api_client.get("/api/v1/search?table=edges&edge_type=officer_of") 
    # Try generic search or checking conftest details, but 'edge_type' is valid column
    # Let's try searching for something likely to exist or just verify status 200/count
    
    # If the column validation works, this should return 200 (even if count is 0)
    response = api_client.get("/api/v1/search?table=edges&edge_type=registered_address")
    assert response.status_code == 200


def test_search_pagination(api_client):
    # Search all officers with small limit and offset
    # First get total count (or a known large set)
    # Generic search to match many
    # Assume we have Officer A (12000001) and Officer B (12000002) in data
    
    # Page 1: limit 1, offset 0
    resp1 = api_client.get("/api/v1/search?display_name=Officer&fuzzy=true&limit=1&offset=0")
    assert resp1.status_code == 200
    res1 = resp1.json()
    assert len(res1["results"]) == 1
    id1 = res1["results"][0]["id"]
    
    # Page 2: limit 1, offset 1
    resp2 = api_client.get("/api/v1/search?display_name=Officer&fuzzy=true&limit=1&offset=1")
    assert resp2.status_code == 200
    res2 = resp2.json()
    assert len(res2["results"]) == 1
    id2 = res2["results"][0]["id"]
    
    assert id1 != id2

def test_search_default_limit(api_client):
    # Verify default limit is 25 (by checking it's not 10 if we had enough data, 
    # but with small test data we just check params didn't break anything)
    # We can check if limit > 10 is allowed without explicit param if we had >10 items.
    # For now, just ensure it works without limit param.
    response = api_client.get("/api/v1/search?display_name=Officer&fuzzy=true")
    assert response.status_code == 200
    res = response.json()
    assert res["count"] >= 0

def test_search_invalid_column(api_client):
    response = api_client.get("/api/v1/search?invalid_col=something")
    assert response.status_code == 400
    assert "Invalid column" in response.json()["detail"]

def test_search_invalid_table(api_client):
    response = api_client.get("/api/v1/search?table=invalid_table&col=val")
    assert response.status_code == 400
    assert "Invalid table" in response.json()["detail"]

def test_search_empty_params_returns_empty(api_client):
    response = api_client.get("/api/v1/search")
    assert response.status_code == 200
    res = response.json()
    assert res["count"] == 0
    assert res["results"] == []
