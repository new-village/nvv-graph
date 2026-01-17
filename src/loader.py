import duckdb
import yaml
import os
import importlib
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
    
    # Generic Processor Handler
    # If a processor is defined in config, dynamically import and execute it.
    processed_config = config
    if "processor" in config:
        processor_name = config["processor"]
        try:
            print(f"Invoking processor: {processor_name}...")
            # Assuming processor module has a function process_data taking config dict
            module = importlib.import_module(processor_name)
            processed_config = module.process_data(config)
        except Exception as e:
            print(f"Error executing processor {processor_name}: {e}")
            raise e
    
    if conn is None:
        conn = get_connection()
    
    # Enable DuckPGQ extensions
    print("Enabling DuckPGQ extension...")
    # Extension is pre-installed in Docker image (offline mode)
    conn.execute("LOAD duckpgq")

    # Rename node_labels to node_types for clarity, but logic remains same
    node_types = {}
    relationships_table = ""
    
    # 1. Load Tables from Config (Generic)
    for source in processed_config.get("sources", []):
        table_name = source["table"]
        file_path = source["path"]
        # CHANGE: label -> node_type
        n_type = source.get("node_type")
        
        print(f"Loader: Loading {table_name} from {file_path}...")
        
        # Consistent approach: Create TEMP TABLE for everything
        conn.execute(f"CREATE OR REPLACE TEMP TABLE {table_name} AS SELECT * FROM '{file_path}'")
        
        if n_type:
            node_types[n_type] = table_name
        # Heuristic: if no label and table name suggests relationships, mark it
        elif table_name == "relationships":
            relationships_table = table_name

    # 2. Dynamic Property Graph Definition
    if relationships_table and node_types:
        print("Loader: Defining Property Graph...")
        
        # Vertex Tables
        vertex_defs = []
        for n_type, table in node_types.items():
            # DuckPGQ LABEL matches node_type (now lowercase)
            vertex_defs.append(f"{table} LABEL {n_type}")
        
        # Edge Tables
        try:
            combinations = conn.execute(f"""
                SELECT DISTINCT start_node_type, end_node_type FROM {relationships_table}
            """).fetchall()
            
            edge_defs = []
            for start_type, end_type in combinations:
                # The data in 'relationships' table might still use old capitalized labels
                # unless processor is also updated. 
                # We updated processor to use lowercase node_type.
                
                # Let's normalize lookup to lowercase for safety
                start_key = start_type.lower() 
                end_key = end_type.lower()
                
                if start_key not in node_types or end_key not in node_types:
                   print(f"Warning: Skipping edge {start_type}->{end_type} because node types not found in config.")
                   continue

                rel_subname = f"rel_{start_key}_{end_key}"
                
                # Materialize sub-tables (TEMP) for DuckPGQ compliance
                conn.execute(f"""
                    CREATE OR REPLACE TEMP TABLE {rel_subname} AS 
                    SELECT * FROM {relationships_table} 
                    WHERE start_node_type = '{start_type}' AND end_node_type = '{end_type}'
                """)
                
                start_table = node_types[start_key]
                end_table = node_types[end_key]
                
                edge_defs.append(f"""
                    {rel_subname}
                    SOURCE KEY (node_id_start) REFERENCES {start_table} (node_id)
                    DESTINATION KEY (node_id_end) REFERENCES {end_table} (node_id)
                    LABEL related_to_{start_key}_{end_key}
                """)
            
            if edge_defs:
                ddl = f"""
                    CREATE OR REPLACE PROPERTY GRAPH icij_graph
                    VERTEX TABLES (
                        {', '.join(vertex_defs)}
                    )
                    EDGE TABLES (
                        {', '.join(edge_defs)}
                    )
                """
                conn.execute(ddl)
                print("Loader: Graph 'icij_graph' created successfully.")
                
        except Exception as e:
            print(f"Loader Warning: Could not define dynamic edges based on labels. Graph might be incomplete. Error: {e}")

    return conn

if __name__ == "__main__":
    # Test run
    con = load_data()
    print("Data loaded and Graph created successfully.")
