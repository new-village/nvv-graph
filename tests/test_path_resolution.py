
import os
import unittest
from unittest.mock import patch, MagicMock
from src.icij_processor import process_data

class TestPathResolution(unittest.TestCase):
    @patch('src.icij_processor.duckdb')
    @patch('src.icij_processor.os')
    def test_absolute_path_fallback(self, mock_os, mock_duckdb):
        # Setup mock behavior
        mock_os.getcwd.return_value = "/app"
        
        # Scenario: data/foo.csv missing, but /data/foo.csv exists
        def exists_side_effect(path):
            if path == "data/foo.csv": return False
            if path == "/data/foo.csv": return True
            if path == "data/foo.parquet": return False
            if path == "/data/foo.parquet": return False
            return False
            
        mock_os.path.exists.side_effect = exists_side_effect
        mock_os.path.join.side_effect = os.path.join
        mock_os.path.dirname.return_value = "data"
        
        # Execute
        config = {
            "sources": [
                {"table": "foo", "path": "data/foo.csv", "node_type": "foo"}
            ]
        }
        result = process_data(config)
        
        # Verify
        processed_path = result["sources"][0]["path"]
        print(f"Original path: data/foo.csv -> Resolved path: {processed_path}")
        # The processor converts/points to the parquet file
        self.assertEqual(processed_path, "/data/foo.parquet")

    @patch('src.icij_processor.duckdb')
    @patch('src.icij_processor.os')
    def test_absolute_parquet_fallback(self, mock_os, mock_duckdb):
        # Scenario: data/foo.csv missing, /data/foo.csv missing, but /data/foo.parquet exists
        def exists_side_effect(path):
            if path == "/data/foo.parquet": return True
            return False
            
        mock_os.path.exists.side_effect = exists_side_effect
        mock_os.path.join.side_effect = os.path.join
        
        config = {
            "sources": [
                {"table": "foo", "path": "data/foo.csv", "node_type": "foo"}
            ]
        }
        result = process_data(config)
        
        processed_path = result["sources"][0]["path"]
        print(f"Original path: data/foo.csv -> Resolved path: {processed_path}")
        self.assertEqual(processed_path, "/data/foo.parquet")

if __name__ == '__main__':
    unittest.main()
