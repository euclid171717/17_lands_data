# 17lands Data Project - Python + dbt-duckdb
FROM python:3.11-slim-bookworm

WORKDIR /app

# Install system deps (minimal for DuckDB)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir dbt-duckdb>=1.8.0

# dbt packages (profiles.yml needed for profile resolution)
COPY dbt_project.yml packages.yml profiles.yml ./
COPY macros/ macros/
COPY models/ models/
RUN dbt deps

# Project code
COPY scripts/ scripts/
COPY config/ config/

# Default: run shell for interactive use
CMD ["/bin/bash"]
