# Dev Container Setup

This directory contains the VS Code Dev Container configuration for the yata-graph project.

## Features

- **Python 3.12** - Latest Python version for optimal performance
- **Pre-installed packages**:
  - FastAPI (>=0.115.0) - Modern, fast web framework
  - Uvicorn (>=0.32.0) - Lightning-fast ASGI server
  - DuckDB (>=1.1.0) - High-performance analytical database
  - SQLAlchemy (>=2.0.35) - Database toolkit and ORM
  - Pandas & PyArrow - Data processing optimization
  - Pydantic - Type validation

- **Data Volume Mount**: Host's `/data` directory is automatically mounted to `/workspaces/yata-graph/data`
- **Port Forwarding**: Port 8000 is forwarded for FastAPI application
- **Base Image**: Python 3.14 (Bullseye)
- **Docker-in-Docker**: Supported for container operations
- **Data Volume Mount**: Host's `/data` directory is automatically mounted to `/workspaces/yata-graph/data`

## Usage

1. Install [VS Code](https://code.visualstudio.com/) and the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
2. Open this repository in VS Code
3. Press `F1` and select "Dev Containers: Reopen in Container"
4. Wait for the container to build and dependencies to install
5. Start developing!

## Mount Configuration

The Dev Container is configured to mount the host's `/data` directory to `/workspaces/yata-graph/data` for DuckDB/SQLite data processing.

If you are on Windows/Mac or need a different path:
1. Open `devcontainer.json`
2. Update the `mounts` section:
   ```json
   // Example for Windows
   "mounts": [
       "source=C:\\data,target=/workspaces/yata-graph/data,type=bind"
   ]
   ```
   or
   ```json
   // Example for Custom Linux Path
   "mounts": [
    "source=/your/custom/path,target=/workspaces/yata-graph/data,type=bind"
   ]
   ```

### Linux/macOS Setup

If you don't have a `/data` directory, create it first:
```bash
sudo mkdir -p /data
sudo chmod 755 /data
```

### Windows Setup

On Windows, the `/data` path won't work directly. You have two options:

1. **Modify the mount in `devcontainer.json`** to use a Windows path:
   ```json
   "mounts": [
       "source=C:\\data,target=/workspaces/yata-graph/data,type=bind"
   ]
   ```

2. **Use WSL2** and create the directory in WSL:
   ```bash
   sudo mkdir -p /data
   sudo chmod 755 /data
   ```

### Custom Data Directory

To use a different location, modify line 38 in `.devcontainer/devcontainer.json`:
```json
"mounts": [
    "source=/your/custom/path,target=/workspaces/yata-graph/data,type=bind"
]
```
