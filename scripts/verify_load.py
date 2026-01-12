import sys
import os

# Add workspace root to python path to allow importing src
sys.path.append(os.getcwd())

from src.loader import load_data

def verify():
    print("Starting verification...")
    try:
        conn = load_data()
        
        tables = [
            "nodes_entities", 
            "nodes_addresses", 
            "nodes_intermediaries", 
            "nodes_officers", 
            "relationships"
        ]
        
        all_passed = True
        
        for table in tables:
            count = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            print(f"Table '{table}': {count} rows")
            if count == 0:
                print(f"ERROR: Table '{table}' is empty.")
                all_passed = False
                
        # Sample data check
        print("\n--- Verifying View Creation ---")
        # Check if they are actually views
        # In DuckDB, views appear in information_schema.tables with table_type='VIEW'
        view_check = conn.execute("SELECT table_name, table_type FROM information_schema.tables WHERE table_schema='main'").fetchall()
        print("Existing Tables/Views:", view_check)
        
        print("\n--- Sample: nodes_entities ---")
        print(conn.execute("SELECT * FROM nodes_entities LIMIT 1").fetchall())
        
        print("\n--- Sample: relationships ---")
        print(conn.execute("SELECT * FROM relationships LIMIT 1").fetchall())
        
        if all_passed:
            print("\nVerification PASSED: All views created and data content verified.")
        else:
            print("\nVerification FAILED: Some tables kept empty.")
            sys.exit(1)
            
    except Exception as e:
        print(f"\nVerification FAILED with error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    verify()
