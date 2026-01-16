
import sys
import pytest
import os
import shutil
import tempfile
from typing import Generator

# Ensure src module is importable
sys.path.append(os.getcwd())

@pytest.fixture(scope="session")
def test_data_dir() -> Generator[str, None, None]:
    """
    Creates a temporary directory with test data.
    Copies ./tests/data CSVs into a unique temp dir for each test run session.
    """
    # Create temp dir
    temp_dir = tempfile.mkdtemp()
    
    # Source data (the fixtures we just created)
    # Assuming tests are run from workspace root
    source_dir = os.path.join(os.getcwd(), "tests/data")
    
    # Copy files
    if os.path.exists(source_dir):
        for f in os.listdir(source_dir):
            shutil.copy(os.path.join(source_dir, f), temp_dir)
            
    # Also create a data subdirectory inside temp_dir and copy there as the code expects data/ prefix in some places or we adjust config
    # Ideally config points to paths. Let's replicate strict structure if needed.
    # The loader config defaults to "data/..." paths. We will need to override this config in tests.
    
    yield temp_dir
    
    # Cleanup
    shutil.rmtree(temp_dir)

@pytest.fixture
def mock_config(test_data_dir: str):
    """
    Returns a configuration dictionary pointing to the temp test data.
    """
    return {
        "processor": "src.icij_processor",
        "sources": [
            {
                "table": "nodes_entities",
                "path": os.path.join(test_data_dir, "nodes-entities.csv"),
                "label": "Entity"
            },
            {
                "table": "nodes_addresses",
                "path": os.path.join(test_data_dir, "nodes-addresses.csv"),
                "label": "Address"
            },
            {
                "table": "nodes_officers",
                "path": os.path.join(test_data_dir, "nodes-officers.csv"),
                "label": "Officer"
            },
             {
                "table": "nodes_intermediaries",
                "path": os.path.join(test_data_dir, "nodes-intermediaries.csv"),
                "label": "Intermediary"
            },
            {
                "table": "relationships",
                "path": os.path.join(test_data_dir, "relationships.csv")
            }
        ]
    }
