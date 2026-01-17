# yata-graph

[Japanese](./README_ja.md)

`yata-graph` is a backend container service optimized for analytical and graph queries. It leverages DuckDB (with DuckPGQ extension) for high-speed graph traversals and SQLite for authentication and audit logging, all wrapped in a modern FastAPI application.

## 🚀 Project Goal

- **High-Performance Graph Search**: Utilize DuckDB/DuckPGQ to perform complex graph queries on confidential data with minimal latency.
- **Data Sovereignty**: Remove dependencies on external managed DB services. Keep all data within your own environment.
- **Portable & Secure**: Designed for deployment anywhere (Cloud Run, On-premise Docker) with a "Security First" approach.

## 🏗 Architecture

### Technology Stack
- **Language**: Python 3.14
- **Web Framework**: FastAPI
- **Main Engine**: DuckDB (DuckPGQ extension) - Direct Parquet file querying
- **Auth/Audit Store**: SQLite (managed via SQLAlchemy)
- **Containerization**: Dev Container / Docker

### Key Features
- **Configurable Data Sources**: Source paths and schemas are defined in `config/sources.yaml`, separating configuration from code.
- **Secure Authentication**: OAuth2 + JWT based authentication.
- **Persistence**: Usage of external volume mounts for persisting Auth/Audit logs while keeping the compute container stateless.

## 🛠 Getting Started

### Prerequisites
- **Dev Container**: This project is designed to be developed inside a Dev Container. Ensure you have Docker and VS Code with the Dev Containers extension installed.

### Installation & Running
1. Open the project in VS Code.
2. Reopen in Container when prompted.
3. The environment will automatically set up Python 3.14 and install dependencies.
