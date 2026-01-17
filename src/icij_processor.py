import duckdb
import os
import importlib
from typing import Dict, List, Any

def process_data(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    ICIJ Specific Processor.
    1. Convert CSV to Parquet.
    2. Enrich relationships with labels and save as 'relationships.parquet'.
    """
    print("Running ICIJ Data Processor...")
    conn = duckdb.connect(":memory:")
    
    processed_sources = []
    
    # Track labels for enrichment
    node_types = {}
    
    # 1. Convert Nodes CSV -> Parquet
    for source in config.get("sources", []):
        table_name = source["table"]
        file_path = source["path"]
        n_type = source.get("node_type")
        
        if table_name == "relationships":
            # Will handle specifically for enrichment
            continue

        # Debug: Print CWD once
        if len(processed_sources) == 0:
             print(f"DEBUG: Current Working Directory: {os.getcwd()}")
             print(f"DEBUG: DATA_DIR: {os.environ.get('DATA_DIR', '/data')}")

        # Resolve path using DATA_DIR environment variable
        data_dir = os.environ.get("DATA_DIR", "/data")
        
        # If path is relative (e.g. data/nodes.csv), try to find it in DATA_DIR
        # We strip 'data/' prefix if present to avoid data/data duplication if DATA_DIR ends with data
        # But simpler: just basename. 
        # config path: "data/nodes-entities.csv" -> basename: "nodes-entities.csv"
        # composed: $DATA_DIR/nodes-entities.csv
        
        filename = os.path.basename(file_path)
        env_path = os.path.join(data_dir, filename)
        
        # Priority 1: Check $DATA_DIR/filename
        if os.path.exists(env_path):
             print(f"Notice: Found file in DATA_DIR: {env_path}")
             file_path = env_path
             source["path"] = env_path
        # Priority 2: Check $DATA_DIR/filename.parquet (fallback if config was csv)
        elif file_path.endswith(".csv"):
             env_parquet = os.path.join(data_dir, filename.replace(".csv", ".parquet"))
             if os.path.exists(env_parquet):
                 print(f"Notice: Found parquet in DATA_DIR: {env_parquet}")
                 file_path = env_parquet
                 source["path"] = env_parquet
        # Priority 2b: Check $DATA_DIR/filename.csv (fallback if config is parquet but we only have csv)
        elif file_path.endswith(".parquet"):
             env_csv = os.path.join(data_dir, filename.replace(".parquet", ".csv"))
             if os.path.exists(env_csv):
                 print(f"Notice: Configured parquet missing, but found CSV in DATA_DIR: {env_csv}")
                 # We don't change source["path"] yet, because we want to convert it to the requested parquet path
                 # But for 'file_path' variable which tracks "source input", we use the CSV.
                 # The conversion logic block below needs to know we intend to output to 'path' (parquet) from 'file_path' (csv).
                 
                 # Let's say config path is `data/nodes.parquet`.
                 # We found `data/nodes.csv`.
                 # We want to read `data/nodes.csv` and write to `data/nodes.parquet`.
                 
                 # Current logic below:
                 # if file_path.endswith(".csv"): ... convert ...
                 
                 # So if we set file_path = env_csv, the block below will trigger.
                 # But we also need to make sure the OUTPUT path (parquet_path below) is correct.
                 file_path = env_csv
                 # We do NOT update source["path"] to the CSV, because the rest of the app might expect parquet if configured so?
                 # Actually, config source["path"] is used by loader to READ.
                 # If we convert it here, we should point source["path"] to the resulting PARQUET file.
                 # The original config was ALREADY parquet. So source["path"] is already correct (parquet).
                 # modifying file_path to csv ensures existence check passes.

        
        # Priority 3: Original path (relative) - Fallthrough to existing logic check


        # Check file existence
        found = False
        if os.path.exists(file_path):
            found = True
        elif file_path.endswith(".csv"):
             # Fallback to parquet
             parquet_path = file_path.replace(".csv", ".parquet")
             if os.path.exists(parquet_path):
                 print(f"Notice: {file_path} not found. Using {parquet_path}...")
                 file_path = parquet_path
                 source["path"] = parquet_path
                 found = True
        elif file_path.endswith(".parquet"):
              # Fallback to csv (Reverse fallback for relative paths)
              csv_path = file_path.replace(".parquet", ".csv")
              if os.path.exists(csv_path):
                  print(f"Notice: {file_path} not found. Using {csv_path} for conversion...")
                  file_path = csv_path
                  found = True

        if not found:
            # DEBUG: Log directory contents to help diagnose Cloud Run mount issues
            dir_path = os.path.dirname(file_path) or "."
            # If we fall back to DATA_DIR logging
            if not os.path.exists(dir_path) and os.environ.get("DATA_DIR"):
                 dir_path = os.environ.get("DATA_DIR")
            
            print(f"ERROR: File not found: {file_path}")
            print(f"DEBUG: DATA_DIR: {os.environ.get('DATA_DIR', '/data')}")

            if os.path.exists(dir_path):
                print(f"DEBUG: Contents of directory '{dir_path}':")
                try:
                    for item in os.listdir(dir_path):
                        print(f"  - {item}")
                except Exception as e:
                    print(f"  (Could not list directory: {e})")
            else:
                print(f"DEBUG: Directory '{dir_path}' does not exist.")
            
            raise FileNotFoundError(f"Required data file not found: {file_path}. See log for directory contents.")

        if file_path.endswith(".csv"):
            # Determine target parquet path
            # If config was originally parquet (e.g. data/nodes.parquet), file_path is currently data/nodes.csv
            # We want to write to data/nodes.parquet
            parquet_path = file_path.replace(".csv", ".parquet")
            
            # Simple check: if csv exists, convert it.
            # In production, check mtime.
            print(f"Converting {file_path} to {parquet_path}...")
            conn.execute(f"CREATE OR REPLACE TABLE raw_{table_name} AS SELECT * FROM read_csv_auto('{file_path}')")
            conn.execute(f"COPY raw_{table_name} TO '{parquet_path}' (FORMAT PARQUET)")
            
            # Update source to point to parquet
            source_copy = source.copy()
            source_copy["path"] = parquet_path
            processed_sources.append(source_copy)
            
            if n_type:
                node_types[n_type] = parquet_path
        else:
            # Already parquet or other format
            # Ensure source points to the valid parquet path if we started with configured parquet
            processed_sources.append(source)
            if n_type:
                node_types[n_type] = file_path

    # 2. Process Relationships
    rel_source = next((s for s in config.get("sources", []) if s["table"] == "relationships"), None)
    if rel_source:
        rel_path = rel_source["path"]
        
        # Resolve path using DATA_DIR environment variable
        data_dir = os.environ.get("DATA_DIR", "/data")
        filename = os.path.basename(rel_path)
        env_path = os.path.join(data_dir, filename)
        
        # Priority 1: Check $DATA_DIR/filename
        if os.path.exists(env_path):
             print(f"Notice: Found relationship file in DATA_DIR: {env_path}")
             rel_path = env_path
        # Priority 2: Check $DATA_DIR/filename.parquet
        elif rel_path.endswith(".csv"):
             env_parquet = os.path.join(data_dir, filename.replace(".csv", ".parquet"))
             if os.path.exists(env_parquet):
                 print(f"Notice: Found relationship parquet in DATA_DIR: {env_parquet}")
                 rel_path = env_parquet

        # Check existence and fallback
        found_rel = False
        if os.path.exists(rel_path):
            found_rel = True
        elif rel_path.endswith(".csv"):
             parquet_path = rel_path.replace(".csv", ".parquet")
             if os.path.exists(parquet_path):
                 print(f"Notice: {rel_path} not found. Using {parquet_path}...")
                 rel_path = parquet_path
                 found_rel = True
        
        if not found_rel:
             dir_path = os.path.dirname(rel_path) or "."
             print(f"ERROR: Relationships file not found: {rel_path}")
             if os.path.exists(dir_path):
                print(f"DEBUG: Contents of directory '{dir_path}':")
                try:
                    for item in os.listdir(dir_path):
                        print(f"  - {item}")
                except Exception as e:
                    print(f"  (Could not list directory: {e})")
             else:
                print(f"DEBUG: Directory '{dir_path}' does not exist.")
             raise FileNotFoundError(f"Required relationships file not found: {rel_path}")
        # Output as relationships.parquet as requested (overwriting if it was the input? No, Input is csv)
        # If input is .csv, output is .parquet.
        # If input is already .parquet, we might be enriching it again? 
        # Plan says: "出力ファイル名は relationships.parquet とします"
        
        output_rel_path = "data/relationships.parquet"
        
        print(f"Enriching relationships from {rel_path} to {output_rel_path}...")
        
        # Load Raw Relationships
        if rel_path.endswith(".csv"):
            conn.execute(f"CREATE OR REPLACE TABLE relationships_raw AS SELECT * FROM read_csv_auto('{rel_path}')")
        else:
            conn.execute(f"CREATE OR REPLACE TABLE relationships_raw AS SELECT * FROM '{rel_path}'")

        # Create Mapping Table from the just-processed node files
        union_parts = []
        for n_type, path in node_types.items():
            # Use n_type (lowercase from config) as node_type in the map
            union_parts.append(f"SELECT node_id, '{n_type}' as node_type FROM '{path}'")
        
        if union_parts:
            create_map_query = f"CREATE OR REPLACE TABLE nodes_map AS {' UNION ALL '.join(union_parts)}"
            conn.execute(create_map_query)
            
            # Enrich
            enrich_query = """
                CREATE OR REPLACE TABLE relationships_typed AS 
                SELECT 
                    r.*,
                    s.node_type as start_node_type,
                    e.node_type as end_node_type
                FROM relationships_raw r
                JOIN nodes_map s ON r.node_id_start = s.node_id
                JOIN nodes_map e ON r.node_id_end = e.node_id
            """
            conn.execute(enrich_query)
            
            # Save
            conn.execute(f"COPY relationships_typed TO '{output_rel_path}' (FORMAT PARQUET)")
            
            # Add to processed sources
            rel_source_copy = rel_source.copy()
            rel_source_copy["path"] = output_rel_path
            processed_sources.append(rel_source_copy)
        
        else:
            print("Warning: No node labels found, cannot enrich relationships.")
            processed_sources.append(rel_source)

    return {"sources": processed_sources}
