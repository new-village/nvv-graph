
import os
import duckdb
from src.icij_processor import process_data

def test_process_data_parquet_conversion(mock_config, test_data_dir):
    """
    Test that CSV files are correctly converted to Parquet.
    """
    # Run processor
    result_config = process_data(mock_config)
    
    # Check if parquet files were created
    for source in result_config["sources"]:
        path = source["path"]
        assert path.endswith(".parquet")
        assert os.path.exists(path)
        
        # Verify content basic check
        conn = duckdb.connect(":memory:")
        count = conn.execute(f"SELECT count(*) FROM '{path}'").fetchone()[0]
        assert count > 0, f"File {path} is empty"

def test_process_data_enrichment(mock_config, test_data_dir):
    """
    Test that relationships are enriched with start_label and end_label.
    """
    # Run processor
    result_config = process_data(mock_config)
    
    # Find relationships file
    rel_source = next(s for s in result_config["sources"] if s["table"] == "relationships")
    rel_path = rel_source["path"]
    
    assert rel_path.endswith("relationships.parquet") or "relationships_typed" in rel_path
    
    # Query enriched data
    conn = duckdb.connect(":memory:")
    df = conn.execute(f"SELECT * FROM '{rel_path}'").df()
    
    # Check columns exist
    assert "start_label" in df.columns
    assert "end_label" in df.columns
    
    # Verify specific row content (Officer -> Entity)
    # Officer = 12000001 (Officer A) -> Entity = 11000001 (Entity X)
    row = df[(df["node_id_start"] == 12000001) & (df["node_id_end"] == 11000001)]
    assert not row.empty
    assert row.iloc[0]["start_label"] == "Officer"
    assert row.iloc[0]["end_label"] == "Entity"
