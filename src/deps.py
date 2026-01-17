import duckdb
from src.loader import load_data

# Singleton connection
_db_connection = None

def get_db():
    """
    Dependency to get the DuckDB connection.
    Lazy loads if not already initialized.
    """
    global _db_connection
    if _db_connection is None:
        _db_connection = load_data()
    return _db_connection

