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
        
        # 1. Simple Match: Find any 5 relationships
        print("Query: Find 5 relationships (a)-[r]->(b)")
        q1 = """
        SELECT a_name, r_link, b_name 
        FROM GRAPH_TABLE (icij_graph 
            MATCH (a:IcijNode)-[r:related_to]->(b:IcijNode)
            COLUMNS (a.name as a_name, r.link as r_link, b.name as b_name)
        ) 
        LIMIT 5
        """
        res1 = conn.execute(q1).fetchall()
        for row in res1:
            print(row)
            
        if not res1:
            raise Exception("No relationships found in graph query.")

        # 2. Path Finding: Find a 2-hop path
        print("\nQuery: Find 2-hop paths (a)-[]->(b)-[]->(c)")
        q2 = """
        SELECT count(*)
        FROM GRAPH_TABLE (icij_graph 
            MATCH (a:IcijNode)-[r1:related_to]->(b:IcijNode)-[r2:related_to]->(c:IcijNode)
            COLUMNS (a.node_id)
        )
        LIMIT 1
        """
        # Just check if it runs without error and returns a count
        count = conn.execute(q2).fetchone()[0]
        print(f"Sample 2-hop check count (limit 1): {count}")
        
        print("\nGraph Verification PASSED.")
            
    except Exception as e:
        print(f"\nGraph Verification FAILED with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    verify_graph()
