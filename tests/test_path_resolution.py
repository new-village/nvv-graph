
import os
import unittest
from unittest.mock import patch, MagicMock
from src.icij_processor import process_data

class TestPathResolution(unittest.TestCase):
    @patch('src.icij_processor.duckdb')
    @patch('src.icij_processor.os')
    def test_default_data_dir_fallback(self, mock_os, mock_duckdb):
        # Test default behavior: DATA_DIR defaults to "/data"
        mock_os.getcwd.return_value = "/app"
        
        # Mock os.environ.get to return default if key not found
        def environ_get(key, default=None):
            return default
        mock_os.environ.get.side_effect = environ_get
        
        # Scenario: data/foo.csv missing, but /data/foo.csv exists
        def exists_side_effect(path):
            if path == "/data/foo.csv": return True
            return False
            
        mock_os.path.exists.side_effect = exists_side_effect
        mock_os.path.join.side_effect = os.path.join
        mock_os.path.basename.side_effect = os.path.basename
        
        # Config uses relative path
        config = {
            "sources": [
                {"table": "foo", "path": "data/foo.csv", "node_type": "foo"}
            ]
        }
        result = process_data(config)
        
        # Should resolve to /data/foo.parquet (because CSV is converted)
        processed_path = result["sources"][0]["path"]
        print(f"Original path: data/foo.csv -> Resolved path: {processed_path}")
        self.assertEqual(processed_path, "/data/foo.parquet")

    @patch('src.icij_processor.duckdb')
    @patch('src.icij_processor.os')
    def test_env_var_fallback(self, mock_os, mock_duckdb):
        # Scenario: DATA_DIR set to /custom/data
        mock_os.getcwd.return_value = "/app"
        
        def environ_get(key, default=None):
            if key == "DATA_DIR": return "/custom/data"
            return default
        mock_os.environ.get.side_effect = environ_get
        
        # /custom/data/foo.csv exists
        def exists_side_effect(path):
            if path == "/custom/data/foo.csv": return True
            return False
            
        mock_os.path.exists.side_effect = exists_side_effect
        mock_os.path.join.side_effect = os.path.join
        mock_os.path.basename.side_effect = os.path.basename
        
        config = {
            "sources": [
                {"table": "foo", "path": "data/foo.csv", "node_type": "foo"}
            ]
        }
        result = process_data(config)
        
        # Verify resolution + conversion
        processed_path = result["sources"][0]["path"]
        self.assertEqual(processed_path, "/custom/data/foo.parquet")

    @patch('src.icij_processor.duckdb')
    @patch('src.icij_processor.os')
    def test_env_var_parquet_fallback(self, mock_os, mock_duckdb):
        # Scenario: CSV missing, but Parquet exists in custom DATA_DIR
        def environ_get(key, default=None):
            if key == "DATA_DIR": return "/custom/data"
            return default
        mock_os.environ.get.side_effect = environ_get

        def exists_side_effect(path):
            if path == "/custom/data/foo.parquet": return True
            return False
            
        mock_os.path.exists.side_effect = exists_side_effect
        mock_os.path.join.side_effect = os.path.join
        mock_os.path.basename.side_effect = os.path.basename
        
        config = {
            "sources": [
                {"table": "foo", "path": "data/foo.csv", "node_type": "foo"}
            ]
        }
        result = process_data(config)
        
        processed_path = result["sources"][0]["path"]
        print(f"Original path: data/foo.csv -> Resolved path: {processed_path}")
        self.assertEqual(processed_path, "/custom/data/foo.parquet")

    @patch('src.icij_processor.duckdb')
    @patch('src.icij_processor.os')
    def test_reverse_fallback_postgres_to_csv(self, mock_os, mock_duckdb):
        # Scenario: Config says .parquet (data/foo.parquet).
        # File missing.
        # But data/foo.csv exists.
        # Processor should find CSV, convert it, and point to .parquet.
        
        mock_os.getcwd.return_value = "/app"
        # Return default env
        def environ_get(key, default=None):
            return default
        mock_os.environ.get.side_effect = environ_get

        def exists_side_effect(path):
            if path == "data/foo.parquet": return False
            if path == "data/foo.csv": return True
            return False
            
        mock_os.path.exists.side_effect = exists_side_effect
        mock_os.path.join.side_effect = os.path.join
        mock_os.path.basename.side_effect = os.path.basename
        
        # Config uses .parquet
        config = {
            "sources": [
                {"table": "foo", "path": "data/foo.parquet", "node_type": "foo"}
            ]
        }
        result = process_data(config)
        
        # Expectation: 
        # 1. Finds data/foo.csv
        # 2. Converts to data/foo.parquet
        # 3. Returns data/foo.parquet as path
        processed_path = result["sources"][0]["path"]
        self.assertEqual(processed_path, "data/foo.parquet")

if __name__ == '__main__':
    unittest.main()
