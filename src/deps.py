import duckdb

# Global State
conn: duckdb.DuckDBPyConnection = None
config: dict = None

def get_db():
    if conn is None:
        raise RuntimeError("Database not initialized")
    return conn

def get_config():
    if config is None:
        raise RuntimeError("Config not initialized")
    return config
