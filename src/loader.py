import duckdb
import yaml
import os
from typing import Optional

def load_config(config_path: str = "config/icij.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def get_connection(database: str = ":memory:") -> duckdb.DuckDBPyConnection:
    return duckdb.connect(database)

def load_data(
    config_path: str = "config/icij.yaml", 
    conn: Optional[duckdb.DuckDBPyConnection] = None
) -> duckdb.DuckDBPyConnection:
    config = load_config(config_path)
    
    if conn is None:
        conn = get_connection()
    
    # Enable DuckPGQ first
    print("Enabling DuckPGQ extension...")
    conn.execute("INSTALL duckpgq FROM community")
    conn.execute("LOAD duckpgq")

    node_labels = {}
    
    # 1. Load Tables
    for source in config.get("sources", []):
        table_name = source["table"]
        file_path = source["path"]
        label = source.get("label")
        
        print(f"Loading {table_name} from {file_path}...")
        
        # Always use TABLE for now to support DuckPGQ fully without view restrictions
        # (Though future versions might support views, relying on tables is safer per user req)
        conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM '{file_path}'")
        
        if label:
            node_labels[label] = table_name

    # 2. Create mapping table (NodeID -> Label) for edge enrichment
    # This assumes node_ids are unique across all node tables.
    print("Creating 'nodes_map' for edge type resolution...")
    
    union_parts = []
    for label, table in node_labels.items():
        union_parts.append(f"SELECT node_id, '{label}' as label FROM {table}")
    
    create_map_query = f"CREATE OR REPLACE TABLE nodes_map AS {' UNION ALL '.join(union_parts)}"
    conn.execute(create_map_query)
    
    # Index to speed up join
    # conn.execute("CREATE INDEX idx_nodes_map_id ON nodes_map (node_id)")

    # 3. Enrich relationships table with start/end labels
    print("Creating typed relationships table 'relationships_typed'...")
    conn.execute("""
        CREATE OR REPLACE TABLE relationships_typed AS 
        SELECT 
            r.*,
            s.label as start_label,
            e.label as end_label
        FROM relationships r
        JOIN nodes_map s ON r.node_id_start = s.node_id
        JOIN nodes_map e ON r.node_id_end = e.node_id
    """)

    # 4. Generate Dynamic Property Graph DDL
    print("Defining Property Graph 'icij_graph'...")
    
    # Vertex Tables definition
    vertex_defs = []
    for label, table in node_labels.items():
        vertex_defs.append(f"{table} LABEL {label}")
    
    # Edge Tables definition
    # We need to find all existing combinations of (start_label -> end_label)
    # to define specific edge tables (subsets of relationships_typed).
    # DuckPGQ DDL requires specifying source/dest tables.
    
    combinations = conn.execute("""
        SELECT DISTINCT start_label, end_label FROM relationships_typed
    """).fetchall()
    
    edge_defs = []
    for start_label, end_label in combinations:
        # Define a source-destination specific label, e.g., Entity_to_Address
        # Or just use generic 'related_to' label for all, but broken down by tables.
        
        # We need to act as if we have separate tables or use a WHERE clause if supported?
        # DuckPGQ's CREATE PROPERTY GRAPH EDGE TABLES usually points to a dataset.
        # If we use one table `relationships_typed`, we can perhaps rely on the FKs?
        # But `relationships_typed` links to `nodes_map`? No, DDL SOURCE KEY REFERENCES T(k).
        # T needs to be a Vertex Table.
        
        # Correct approach for multi-typed single edge table in DuckPGQ (if supported):
        # We might need views or DDL entries for EACH type pair if we want to reference specific vertex tables.
        # But wait, DuckPGQ allows `SOURCE KEY (s) REFERENCES T(k)`.
        # If we have multiple vertex tables, we can't reference "one of them" from a single FK column easily
        # unless we split the edge table itself or the CREATE statement supports predicates.
        
        # Workaround:
        # Create a VIEW or Sub-TABLE for each pair type to act as the "Edge Table" for that pair.
        # e.g. edges_Entity_Address
        
        rel_subname = f"rel_{start_label}_{end_label}"
        conn.execute(f"""
            CREATE OR REPLACE TABLE {rel_subname} AS 
            SELECT * FROM relationships_typed 
            WHERE start_label = '{start_label}' AND end_label = '{end_label}'
        """)
        
        start_table = node_labels[start_label]
        end_table = node_labels[end_label]
        
        edge_defs.append(f"""
            {rel_subname}
            SOURCE KEY (node_id_start) REFERENCES {start_table} (node_id)
            DESTINATION KEY (node_id_end) REFERENCES {end_table} (node_id)
            LABEL related_to_{start_label}_{end_label}
        """)

    ddl = f"""
        CREATE OR REPLACE PROPERTY GRAPH icij_graph
        VERTEX TABLES (
            {', '.join(vertex_defs)}
        )
        EDGE TABLES (
            {', '.join(edge_defs)}
        )
    """
    
    # print(ddl) # Debug
    conn.execute(ddl)
    
    return conn

if __name__ == "__main__":
    # Test run
    con = load_data()
    print("Data loaded and Graph created successfully.")
