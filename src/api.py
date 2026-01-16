from fastapi import APIRouter, Depends, HTTPException
import duckdb
from src.deps import get_db, get_config

router = APIRouter()

@router.get("/node/{node_type}/{id}")
def get_node(
    node_type: str, 
    id: str,
    conn: duckdb.DuckDBPyConnection = Depends(get_db),
    config: dict = Depends(get_config)
):
    
    # 1. Resolve node_type to table and id_field
    target_source = None
    for source in config.get("sources", []):
        if source.get("node_type") == node_type:
            target_source = source
            break
    
    if not target_source:
        raise HTTPException(status_code=400, detail=f"Invalid node_type: {node_type}")
    
    table_name = target_source["table"]
    id_field = target_source.get("id_field")
    
    if not id_field:
        raise HTTPException(status_code=500, detail=f"Configuration error: id_field not defined for {node_type}")

    # 2. Query DuckDB
    # Use localized parameter binding to prevent SQL injection
    # Note: table_name and id_field come from trusted config, but node_id is user input.
    # Bind node_id as parameter.
    
    query = f"SELECT * FROM {table_name} WHERE {id_field} = ?"
    
    try:
        # fetchone returns a tuple. We need to map it to columns.
        # described query allows getting column names.
        
        # Execute to get result
        df = conn.execute(query, [id]).df()
        
        if df.empty:
            return {"count": 0, "data": None}
        
        # Convert first row to dict
        record = df.iloc[0].replace({float('nan'): None}).to_dict()
        
        return {"count": 1, "data": record}

    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        print(f"Database Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
@router.get("/node/{node_type}/{id}/neighbors")
def get_node_neighbors(
    node_type: str,
    id: str,
    depth: int = 1,
    direction: str = "both",
    limit: int = 100,
    conn: duckdb.DuckDBPyConnection = Depends(get_db),
    config: dict = Depends(get_config)
):
    # 1. Validate node_type
    n_type_lower = node_type.lower()
    target_source = None
    for source in config.get("sources", []):
        if source.get("node_type") == n_type_lower:
            target_source = source
            break
    
    if not target_source:
        raise HTTPException(status_code=400, detail=f"Invalid node_type: {node_type}")

    # 2. Construct Query
    # DuckPGQ strict mode requires binding to specific labels.
    # We must discover valid relationships for this node_type from the DB schema
    
    # Get all tables to find relationship tables: rel_{src}_{tgt}
    # This is a heuristic based on loader.py convention.
    try:
        tables_res = conn.execute("SHOW TABLES").fetchall()
        all_tables = [t[0] for t in tables_res]
    except Exception:
        # Fallback if SHOW TABLES fails (unlikely)
        all_tables = []

    union_queries = []
    
    # We need to know all node types to cross check?
    # Actually just parsing table names is enough.
    # Format: rel_{start_node_type}_{end_node_type}
    
    prefix = "rel_"
    valid_rels = [t for t in all_tables if t.startswith(prefix)]
    
    unique_types = set()
    node_source_types = set(s.get("node_type") for s in config["sources"] if s.get("node_type"))

    for t_name in valid_rels:
        # t_name = rel_officer_entity
        parts = t_name[len(prefix):].split("_")
        # Handle cases where type name might contain underscores? 
        # Our loader assumes simple types. If multiple underscores, this logic is fragile.
        # But we controlled config to lowercase simple strings?
        # Let's assume types don't have underscores for MVP or we try to match known types.
        
        # Robust parsing: Try to match start_type from known types
        # This is a bit combinatorial if types have underscores.
        # Given our current data, types are simple.
        # But for robustness:
        # start_type is one of known types. end_type is one of known types.
        # Find split point.
        
        start_match = None
        end_match = None
        
        content = t_name[len(prefix):]
        
        # Try to find split that matches two known types
        found = False
        for i in range(1, len(content)):
            s = content[:i]
            e = content[i+1:] # skip underscore
            if content[i] == '_' and s in node_source_types and e in node_source_types:
                start_match = s
                end_match = e
                found = True
                break
        
        if not found:
            continue
            
        edge_label = f"related_to_{start_match}_{end_match}"
        
        # Check direction
        if start_match == n_type_lower and (direction == "out" or direction == "both"):
            # (MyNode)-[Edge]->(Neighbor)
            union_queries.append(f"""
                SELECT 
                    source_id, '{start_match}' as source_type,
                    target_id, '{end_match}' as target_type,
                    '{edge_label}' as edge_type
                FROM GRAPH_TABLE (icij_graph
                    MATCH (a:{start_match})-[r:{edge_label}]->(b:{end_match})
                    WHERE a.node_id = '{id}'
                    COLUMNS (a.node_id AS source_id, b.node_id AS target_id)
                )
            """)
            
        if end_match == n_type_lower and (direction == "in" or direction == "both"):
            # (Neighbor)-[Edge]->(MyNode)
            # We want source_id to be 'b' (Neighbor) and target_id to be 'a' (MyNode)
            # effectively reversing the topological edge for the "source/target" response fields?
            # User wants "edges": [{source, target}]...
            # Usually edges are directed (source->target).
            # So if (b)->(a) exists, source is b, target is a.
            # My logic: 
            # SELECT b.node_id AS source_id, a.node_id AS target_id
            union_queries.append(f"""
                SELECT 
                    source_id, '{start_match}' as source_type,
                    target_id, '{end_match}' as target_type,
                    '{edge_label}' as edge_type
                FROM GRAPH_TABLE (icij_graph
                    MATCH (b:{start_match})-[r:{edge_label}]->(a:{end_match})
                    WHERE a.node_id = '{id}'
                    COLUMNS (b.node_id AS source_id, a.node_id AS target_id)
                )
            """)

    if not union_queries:
        # No relationships found or node isolated
        # Return empty result properly
        return {"nodes": [], "edges": []}
        
    full_query = " UNION ALL ".join(union_queries)
    # Apply limit to the total result
    full_wrapper = f"SELECT * FROM ({full_query}) LIMIT {limit}"
    
    try:
        # Execute
        df = conn.execute(full_wrapper).df()
        
        # Include self node in nodes list? 
        # API "neighbors" usually implies neighbors + self to visualize context?
        # User requirement: "nodes": [...]
        # Let's include self if we find edges, OR explicitly fetch self.
        # Fetching self is safer to ensure valid response even if no edges.
        # So populate nodes_map with self first.
        
        nodes_map = {}
        nodes_map[id] = {"id": id, "node_type": n_type_lower}

        edges_list = []
        
        # 3. Process Topology
        for _, row in df.iterrows():
            src_id = str(row["source_id"])
            src_type = row["source_type"]
            tgt_id = str(row["target_id"])
            tgt_type = row["target_type"]
            edge_type = row["edge_type"]
            
            # Nodes
            nodes_map[src_id] = {"id": src_id, "node_type": src_type}
            nodes_map[tgt_id] = {"id": tgt_id, "node_type": tgt_type}
                
            # Edge
            edge_id = f"rel_{src_id}_{tgt_id}" 
            edges_list.append({
                "id": edge_id,
                "source": src_id,
                "target": tgt_id,
                "type": edge_type
            })
            
        # 4. Fetch Properties
        # Group by type
        ids_by_type = {}
        for nid, data in nodes_map.items():
            nt = data["node_type"]
            if nt not in ids_by_type:
                ids_by_type[nt] = []
            ids_by_type[nt].append(nid)
            
        nodes_list = []
        for n_type, ids in ids_by_type.items():
            # Find table for n_type
            target_src = next((s for s in config["sources"] if s.get("node_type") == n_type), None)
            if not target_src:
                continue
            table = target_src["table"]
            id_field = target_src["id_field"]
            
            # Batch Select
            placeholders = ','.join(['?'] * len(ids))
            q_props = f"SELECT * FROM {table} WHERE {id_field} IN ({placeholders})"
            props_df = conn.execute(q_props, ids).df()
            
            for _, prow in props_df.iterrows():
                p_dict = prow.replace({float('nan'): None}).to_dict()
                # Remove id from props if redundant? Or keep it.
                nid = str(p_dict[id_field])
                if nid in nodes_map:
                    # Construct node object
                    nodes_list.append({
                        "id": nid,
                        "node_type": n_type,
                        "properties": p_dict
                    })
        
        return {
            "nodes": nodes_list,
            "edges": edges_list
        }

    except Exception as e:
        print(f"Graph Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
