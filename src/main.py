import contextlib
from fastapi import FastAPI
from src.loader import load_data, load_config
from src.api import router as api_router
from src import deps
import duckdb

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    print("Startup: Loading Data and Graph...")
    # Load config separate so we can access it in API
    deps.config = load_config()
    # Load data (this also handles processor and graph creation)
    deps.conn = load_data(conn=duckdb.connect(":memory:"))
    yield
    print("Shutdown: Closing connection...")
    if deps.conn:
        deps.conn.close()

app = FastAPI(title="NVV Graph API", description="DuckDB/DuckPGQ backed Graph API", version="0.1.0", lifespan=lifespan)

app.include_router(api_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
