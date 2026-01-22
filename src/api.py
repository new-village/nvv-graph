from fastapi import APIRouter, Depends, HTTPException
import duckdb
from src.deps import get_db
from src.schemas import NodeResponse, NeighborsResponse, NeighborsCountResponse, Node, Edge

router = APIRouter()

@router.get("/nodes/{id}", response_model=NodeResponse)
def get_node(
    id: str,
    conn: duckdb.DuckDBPyConnection = Depends(get_db)
):
    """
    Fetch a node by ID directly from the nodes table.
    """
    try:
        # Validate ID is a number to match DB schema (BIGINT)
        if not id.isdigit():
            return {"count": 0, "data": None}

        # Convert input id to int for safe comparisons/usage if needed, 
        # though DuckDB query param handles string digits fine usually. 
        # But consistency is good.
        node_id_int = int(id)

        query = "SELECT * FROM nodes WHERE id = ?"
        df = conn.execute(query, [node_id_int]).df()
        
        if df.empty:
            return {"count": 0, "data": None}
        
        # Convert first row to dict and handle None/NaN
        record = df.iloc[0].replace({float('nan'): None}).to_dict()
        
        # Ensure ID is treated consistently
        # The parquet schema has ID as BIGINT.
        
        return {"count": 1, "data": record}

    except Exception as e:
        print(f"Database Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@router.get("/nodes/{id}/neighbors", response_model=NeighborsResponse)
def get_node_neighbors(
    id: str,
    depth: int = 1,
    direction: str = "both",
    conn: duckdb.DuckDBPyConnection = Depends(get_db)
):
    """
    Fetch neighbors specifically from edges table.
    We need to join edges with nodes table to get neighbor details.
    """
    
    # Validate ID is a number
    if not id.isdigit():
        return {"nodes": [], "edges": []}

    node_id_int = int(id)

    # We are looking for edges where source_id = id OR target_id = id
    # And we need to filter by direction.
    
    try:
        # Base query for edges
        # We need two parts: Outgoing and Incoming
        
        queries = []
        
        # Outgoing: (Me) -> (Neighbor)
        # source_id = Me, target_id = Neighbor
        if direction in ["out", "both"]:
            queries.append("""
                SELECT 
                    'out' as dir,
                    e.id as edge_id, e.edge_type, 
                    n.id as neighbor_id, n.node_type as neighbor_type, n.display_name as neighbor_name,
                    n.* EXCLUDE (id, node_type, display_name)
                FROM edges e
                JOIN nodes n ON e.target_id = n.id
                WHERE e.source_id = ?
            """)
            
        # Incoming: (Neighbor) -> (Me)
        # source_id = Neighbor, target_id = Me
        if direction in ["in", "both"]:
            queries.append("""
                SELECT 
                    'in' as dir,
                    e.id as edge_id, e.edge_type,
                    n.id as neighbor_id, n.node_type as neighbor_type, n.display_name as neighbor_name,
                    n.* EXCLUDE (id, node_type, display_name)
                FROM edges e
                JOIN nodes n ON e.source_id = n.id
                WHERE e.target_id = ?
            """)
            
        if not queries:
             return {"nodes": [], "edges": []}

        full_query = " UNION ALL ".join(queries)
        
        # Params: we need to pass 'id' for each query part
        params = [node_id_int] * len(queries)
        
        df = conn.execute(full_query, params).df()
        
        nodes_list = []
        edges_list = []
        
        # Track unique nodes to simple list
        seen_nodes = set()
        
        for _, row in df.iterrows():
            # Process Neighbor Node
            # neighbor_id comes as int (BIGINT) from DuckDB DF if schema is correct
            nid = row["neighbor_id"]
            
            # Key for set should be consistent (int)
            if nid not in seen_nodes:
                # Let's simplify row processing
                row_dict = row.replace({float('nan'): None}).to_dict()
                
                # Construct node object
                node_obj = {
                    "id": nid, # Keep as int
                    "node_type": row_dict["neighbor_type"],
                    "display_name": row_dict["neighbor_name"],
                    "properties": {k: v for k, v in row_dict.items() if k not in ["dir", "edge_id", "edge_type", "neighbor_id", "neighbor_type", "neighbor_name"]}
                }
                nodes_list.append(node_obj)
                seen_nodes.add(nid)
            
            row_dict = row.replace({float('nan'): None}).to_dict()
            
            # Edge
            # edge_id comes as int
            edge_id = row_dict["edge_id"]
            
            # Determine source/target using numeric IDs
            # If dir is out, source is Me (node_id_int), target is Neighbor (nid)
            src_val = node_id_int if row_dict["dir"] == 'out' else nid
            tgt_val = nid if row_dict["dir"] == 'out' else node_id_int
            
            edge_obj = {
                "id": edge_id,
                "type": row_dict["edge_type"],
                "source": src_val,
                "target": tgt_val
            }
            edges_list.append(edge_obj)
            
        return {
            "nodes": nodes_list,
            "edges": edges_list
        }

    except Exception as e:
        print(f"Graph Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/nodes/{id}/neighbors/count", response_model=NeighborsCountResponse)
def get_node_neighbors_count(
    id: str,
    direction: str = "both",
    conn: duckdb.DuckDBPyConnection = Depends(get_db)
):
    try:
        # Validate ID
        if not id.isdigit():
             return {"count": 0, "details": {}}
             
        node_id_int = int(id)

        # Aggregation of neighbors by type
        # Similarly, OUT and IN
        
        queries = []
        
        # Outgoing: neighbor is target
        if direction in ["out", "both"]:
            queries.append("""
                SELECT n.node_type, COUNT(*) as cnt
                FROM edges e
                JOIN nodes n ON e.target_id = n.id
                WHERE e.source_id = ?
                GROUP BY n.node_type
            """)
            
        # Incoming: neighbor is source
        if direction in ["in", "both"]:
            queries.append("""
                SELECT n.node_type, COUNT(*) as cnt
                FROM edges e
                JOIN nodes n ON e.source_id = n.id
                WHERE e.target_id = ?
                GROUP BY n.node_type
            """)
            
        full_query = " UNION ALL ".join(queries)
        params = [node_id_int] * len(queries)
        
        df = conn.execute(full_query, params).df()
        
        breakdown = {}
        total = 0
        
        if not df.empty:
            # Group by node_type again because UNION might split them
            grouped = df.groupby("node_type")["cnt"].sum()
            total = int(grouped.sum())
            breakdown = grouped.to_dict()
            
        return {
            "count": total,
            "details": breakdown
        }

    except Exception as e:
        print(f"Graph Count Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

