from fastapi import APIRouter, Depends, HTTPException
import duckdb
from src.deps import get_db, get_config

router = APIRouter()

@router.get("/node/{node_type}/{node_id}")
def get_node(
    node_type: str, 
    node_id: str,
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
        df = conn.execute(query, [node_id]).df()
        
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
