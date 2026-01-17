
import pytest
from fastapi.testclient import TestClient
from src.main import app
from src.deps import get_db, get_config
from src.loader import load_data
import duckdb

# Since we use dependency injection, we can override dependencies.
# However, our loader builds a complex in-memory state.
# For API tests, we want the app to be fully initialized with test data.


@pytest.fixture
def api_client(test_data_dir, mock_config):
    """
    Creates a TestClient with overridden dependencies using test data.
    """
    # Initialize a test connection with data loaded
    conn = duckdb.connect(":memory:")
    
    # We load tables manually from mock_config
    for source in mock_config["sources"]:
        conn.execute(f"CREATE OR REPLACE TEMP TABLE {source['table']} AS SELECT * FROM '{source['path']}'")
    
    # We typically don't need to manually inject id_field anymore because mock_config already has it
    # thanks to our previous edit in conftest.py. 
    # But let's verify mock_config structure in conftest.
    # The fixture api_client was overriding/adding id_field. Now it should respect mock_config or just use it.
    
    # Mock config now has node_type and id_field.
    api_config = mock_config
        
    # Overrides
    def override_get_db():
        return conn
        
    def override_get_config():
        return api_config
        
    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_config] = override_get_config
    
    client = TestClient(app)
    yield client
    
    app.dependency_overrides = {}
    conn.close()

def test_get_node_officer_found(api_client):
    """
    Test getting an existing Officer node.
    Officer A has node_id 12000001
    """
    response = api_client.get("/api/v1/nodes/officer/12000001")
    assert response.status_code == 200
    res = response.json()
    assert res["count"] == 1
    assert res["data"]["node_id"] == 12000001
    assert res["data"]["name"] == "Officer A"

def test_get_node_entity_found(api_client):
    """
    Test getting an existing Entity node.
    Entity X has node_id 11000001
    """
    response = api_client.get("/api/v1/nodes/entity/11000001")
    assert response.status_code == 200
    res = response.json()
    assert res["count"] == 1
    assert res["data"]["node_id"] == 11000001
    assert res["data"]["name"] == "Entity X"
    assert res["data"]["jurisdiction"] == "BVI"

def test_get_node_not_found(api_client):
    """
    Test getting a non-existent node.
    """
    response = api_client.get("/api/v1/nodes/officer/99999999")
    # Should now return 200 with count 0
    assert response.status_code == 200
    res = response.json()
    assert res["count"] == 0
    assert res["data"] is None

def test_get_node_invalid_type(api_client):
    """
    Test getting a node with invalid type.
    """
    response = api_client.get("/api/v1/nodes/invalidtype/123")
    assert response.status_code == 400
    assert "Invalid node_type" in response.json()["detail"]
