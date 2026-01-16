import sys
import os
import duckdb

# Add workspace root to python path to allow importing src
sys.path.append(os.getcwd())

from src.loader import load_data

def verify_graph():
    print("Starting Graph Verification...")
    try:
        conn = load_data()
        
        print("\n--- Testing Graph Query (SQL/PGQ) ---")
        
        # 1. Typed Match: Find Entity connected to Address
        print("Query: MATCH (e:Entity)-[r:related_to_Entity_Address]->(a:Address)")
        q1 = """
        SELECT e_name, r_link, a_name 
        FROM GRAPH_TABLE (icij_graph 
            MATCH (e:Entity)-[r:related_to_Entity_Address]->(a:Address)
            COLUMNS (e.name as e_name, r.link as r_link, a.name as a_name)
        ) 
        LIMIT 5
        """
        res1 = conn.execute(q1).fetchall()
        for row in res1:
            print(row)
            
        if not res1:
            print("No simple Entity->Address relationships found. Checking reverse...")
            
        # 2. Typed Match: Officer -> Entity
        # Note: Directions might vary, checking Officer connected to Entity.
        # In source data: Officer usually START, Entity usually END for "director of" relationships etc.
        print("\nQuery: MATCH (o:Officer)-[r:related_to_Officer_Entity]->(e:Entity)")
        q2 = """
        SELECT o_name, r_link, e_name 
        FROM GRAPH_TABLE (icij_graph 
            MATCH (o:Officer)-[r:related_to_Officer_Entity]->(e:Entity)
            COLUMNS (o.name as o_name, r.link as r_link, e.name as e_name)
        ) 
        LIMIT 5
        """
        res2 = conn.execute(q2).fetchall()
        for row in res2:
            print(row)

        if not res1 and not res2:
             raise Exception("No relationships found in typed queries.")

        # 3. Path Finding with mixed types
        # Officer -> Entity -> Address
        print("\nQuery: Find 2-hop paths (o)-[]->(e)-[]->(a)")
        q3 = """
        SELECT count(*)
        FROM GRAPH_TABLE (icij_graph 
            MATCH (o:Officer)-[r1:related_to_Officer_Entity]->(e:Entity)-[r2:related_to_Entity_Address]->(a:Address)
            COLUMNS (o.node_id)
        )
        LIMIT 1
        """
        count = conn.execute(q3).fetchone()[0]
        print(f"Sample 2-hop check count (limit 1): {count}")
        
        print("\nGraph Verification PASSED.")
            
    except Exception as e:
        print(f"\nGraph Verification FAILED with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    verify_graph()
