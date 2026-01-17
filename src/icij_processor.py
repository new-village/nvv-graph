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

        # Check for absolute path fallback (e.g. if config says 'data/foo.csv' but file is at '/data/foo.csv')
        if not os.path.exists(file_path) and not file_path.startswith("/"):
             abs_path = os.path.join("/", file_path)
             if os.path.exists(abs_path):
                 print(f"Notice: Found file at absolute path {abs_path}")
                 file_path = abs_path
                 source["path"] = abs_path
             # Also check if the parquet version exists at absolute path even if csv doesn't
             elif file_path.endswith(".csv"):
                 abs_parquet = os.path.join("/", file_path.replace(".csv", ".parquet"))
                 if os.path.exists(abs_parquet):
                     print(f"Notice: Found parquet at absolute path {abs_parquet}")
                     file_path = abs_parquet
                     source["path"] = abs_parquet

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
                 # Update config reference
                 source["path"] = parquet_path
                 found = True

        if not found:
            # DEBUG: Log directory contents to help diagnose Cloud Run mount issues
            dir_path = os.path.dirname(file_path) or "."
            print(f"ERROR: File not found: {file_path}")
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
            processed_sources.append(source)
            if n_type:
                node_types[n_type] = file_path

    # 2. Process Relationships
    rel_source = next((s for s in config.get("sources", []) if s["table"] == "relationships"), None)
    if rel_source:
        rel_path = rel_source["path"]
        
        # Check for absolute path fallback
        if not os.path.exists(rel_path) and not rel_path.startswith("/"):
             abs_path = os.path.join("/", rel_path)
             if os.path.exists(abs_path):
                 print(f"Notice: Found relationship file at absolute path {abs_path}")
                 rel_path = abs_path
             elif rel_path.endswith(".csv"):
                 abs_parquet = os.path.join("/", rel_path.replace(".csv", ".parquet"))
                 if os.path.exists(abs_parquet):
                     print(f"Notice: Found relationship parquet at absolute path {abs_parquet}")
                     rel_path = abs_parquet

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
