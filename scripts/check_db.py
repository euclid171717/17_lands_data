"""Quick check: list tables in the active DuckDB (env DUCKDB_PATH or config default)."""
import os
import sys
from pathlib import Path

root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))

import duckdb

from scripts.ingest.config import get_paths, load_config

env_db = os.environ.get("DUCKDB_PATH")
if env_db:
    db = Path(env_db)
else:
    try:
        cfg = load_config()
        db = get_paths(cfg)["db"]
    except (FileNotFoundError, ValueError):
        db = root / "data" / "db" / "17lands.duckdb"

if not db.exists():
    print(f"Database not found: {db}")
    print("Set DUCKDB_PATH or run ingest (e.g. --helpers --set MKM; --set MKM).")
    sys.exit(1)

conn = duckdb.connect(str(db), read_only=True)
rows = conn.execute(
    """
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    ORDER BY 1, 2
"""
).fetchall()
conn.close()

if not rows:
    print("No tables found.")
else:
    for schema, name in rows:
        print(f"  {schema}.{name}")
