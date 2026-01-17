# Build stage
FROM python:3.14-slim AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Create virtualenv and install dependencies
RUN python -m venv /app/venv
ENV PATH="/app/venv/bin:$PATH"
RUN pip install --no-cache-dir -r requirements.txt

# Runtime stage
FROM python:3.14-slim AS runtime

WORKDIR /app

# Create non-root user
RUN groupadd -r appuser && useradd -r -m -g appuser appuser

# Copy virtualenv from builder
COPY --from=builder /app/venv /app/venv
ENV PATH="/app/venv/bin:$PATH"

# Copy application code
COPY src /app/src
COPY config /app/config
# Copy data directory (assuming it's populated or volume mounted)
# For production image, data is usually external volume. 
# But we need the directory existence.
RUN mkdir -p /app/data && chown -R appuser:appuser /app/data

# Switch to non-root user
USER appuser

# Pre-install DuckDB extensions (Best Practice for Cloud Run / Containers)
# efficiently caches the extension in the image provided HOME is writable (checked by -m earlier)
RUN python -c "import duckdb; con = duckdb.connect(); con.execute('INSTALL duckpgq FROM community'); con.execute('LOAD duckpgq')"

# Expose port
EXPOSE 8080

# Environment variables
ENV PYTHONUNBUFFERED=1

# Command to run the application
# Use src.main:app, host 0.0.0.0 and port 8080 (Cloud Run default)
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080"]
