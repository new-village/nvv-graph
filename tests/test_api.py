
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
    # We need to patch load_config used by load_data OR pass our mock config.
    # load_data takes config_path.
    # But wait, we can just use the mock_config dictionary if we adjust load_data or manually load tables.
    # Actually, let's reuse load_data logic but inject our mock config.
    
    # We can mock src.loader.load_config via patch, but since we are calling load_data directly here...
    # Let's manually set up the connection similar to conftest/load_data logic but using mock_config.
    
    # Re-using logic from loader by mocking yaml.safe_load? Or just manually doing it is safer.
    # Let's manually load tables from test_data_dir into conn.
    
    for source in mock_config["sources"]:
        conn.execute(f"CREATE OR REPLACE TEMP TABLE {source['table']} AS SELECT * FROM '{source['path']}'")
    
    # We also need to add id_field to mock_config for API to work!
    # Updating mock_config in place or copy
    api_config = mock_config.copy()
    for source in api_config["sources"]:
        source["id_field"] = "node_id"
        
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
    response = api_client.get("/node/Officer/12000001")
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
    response = api_client.get("/node/Entity/11000001")
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
    response = api_client.get("/node/Officer/99999999")
    # Should now return 200 with count 0
    assert response.status_code == 200
    res = response.json()
    assert res["count"] == 0
    assert res["data"] is None

def test_get_node_invalid_type(api_client):
    """
    Test getting a node with invalid type.
    """
    response = api_client.get("/node/InvalidType/123")
    assert response.status_code == 400
    assert "Invalid node_type" in response.json()["detail"]
