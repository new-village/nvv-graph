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
    node_labels = {}
    
    # 1. Convert Nodes CSV -> Parquet
    for source in config.get("sources", []):
        table_name = source["table"]
        file_path = source["path"]
        label = source.get("label")
        
        if table_name == "relationships":
            # Will handle specifically for enrichment
            continue

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
            
            if label:
                node_labels[label] = parquet_path
        else:
            processed_sources.append(source)
            if label:
                node_labels[label] = file_path

    # 2. Process Relationships
    rel_source = next((s for s in config.get("sources", []) if s["table"] == "relationships"), None)
    if rel_source:
        rel_path = rel_source["path"]
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
        for label, path in node_labels.items():
            union_parts.append(f"SELECT node_id, '{label}' as label FROM '{path}'")
        
        if union_parts:
            create_map_query = f"CREATE OR REPLACE TABLE nodes_map AS {' UNION ALL '.join(union_parts)}"
            conn.execute(create_map_query)
            
            # Enrich
            enrich_query = """
                CREATE OR REPLACE TABLE relationships_typed AS 
                SELECT 
                    r.*,
                    s.label as start_label,
                    e.label as end_label
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
