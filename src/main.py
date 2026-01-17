import contextlib
from fastapi import FastAPI
from src.loader import load_data
from src.api import router as api_router
from src import deps

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    print("Startup: Loading Data...")
    # Initialize the singleton connection
    # In simpler model, load_data returns connection, we store it in deps? 
    # Actually deps.py has lazy loading. 
    # But for startup verification:
    
    # We can force load here. 
    # But deps.py handles singleton `get_db`.
    # Let's ensure it's loaded.
    deps.get_db()
    
    yield
    print("Shutdown: Closing connection...")
    if deps._db_connection:
        deps._db_connection.close()

app = FastAPI(title="Yata Graph API", description="Parquet-backed Graph API", version="0.1.0", lifespan=lifespan)

# Update prefix per user request for lowercase URLs? No, API structure changes to /nodes/...
# The user's manual verification plan had "/nodes/...". 
# The current api.py has router mounted on "/nodes".
# If we include it with prefix "/api/v1", the result is "/api/v1/nodes/...".
# The user requested verification of "/nodes/officer/..." (root level?).
# The plan verified "/nodes/Officer/..." but user corrected it to "/nodes/officer/...".
# The original main.py had prefix "/api/v1".
# I should probably align with user's implicit expectation of root or explain.
# Let's stick to "/api/v1" as it is standard and user only corrected the case of node type.
# But "GET /nodes/Officer/12000001" implies root.
# I will expose it at root or keep /api/v1.
# Let's keep /api/v1 as it is safer to preserve versioning, but allow root redirection if asked. 
# Re-reading plan:
# "- GET /nodes/officer/12000001"
# It doesn't say /api/v1/nodes.
# I'll create a root router or mount it at root for simplicity as per "Simpler" requirement?
# No, let's keep /api/v1 but also allow /? or just stick to /api/v1. 
# actually the user feedback correction was just about casing "nodes/Officer" -> "nodes/officer".
# Unclear if they meant /api/v1/nodes/officer or /nodes/officer.
# The original code had /api/v1. I'll stick to /api/v1 but mention it.

app.include_router(api_router, prefix="/api/v1")

# Also mount at root as the user examples omitted /api/v1 prefix often?
# Or just stick to one. I will stick to /api/v1. (Actually user examples in previous prompts might have omitted it for brevity).

