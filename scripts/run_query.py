#!/usr/bin/env python3
"""
Run an ad-hoc SQL file against the 17lands DuckDB database.

Usage (from repo root):
  $env:DUCKDB_PATH = "data/db/MKM.duckdb"   # optional; else uses config/datasets.yaml
  python -m scripts.run_query queries/01_table_list.sql

Docker (if you use compose):
  docker compose run --rm app python -m scripts.run_query queries/example.sql

Edit .sql files in your IDE, then run with the command above or the DuckDB extension (see queries/README.md).
"""

import sys
from pathlib import Path

# Add project root for imports
root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))

import os

import duckdb

from scripts.ingest.config import get_paths, load_config


def _segment_has_sql(segment: str) -> bool:
    """True if segment contains at least one line that is not blank and not a full-line -- comment."""
    for line in segment.splitlines():
        t = line.strip()
        if not t:
            continue
        if t.startswith("--"):
            continue
        return True
    return False


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python -m scripts.run_query <path-to.sql>", file=sys.stderr)
        print("Example: python -m scripts.run_query queries/example.sql", file=sys.stderr)
        sys.exit(1)

    sql_path = Path(sys.argv[1])
    if not sql_path.exists():
        print(f"File not found: {sql_path}", file=sys.stderr)
        sys.exit(1)

    config = load_config()
    env_db = os.environ.get("DUCKDB_PATH")
    if env_db:
        db_path = Path(env_db)
    else:
        db_path = get_paths(config)["db"]

    if not db_path.exists():
        print(f"Database not found: {db_path}", file=sys.stderr)
        print("Run ingest first: docker compose run --rm app python -m scripts.ingest.cli --set MKM", file=sys.stderr)
        sys.exit(1)

    sql = sql_path.read_text(encoding="utf-8")
    conn = duckdb.connect(str(db_path), read_only=True)

    # Split into statements (semicolon + newline; skip empty and comment-only segments)
    sql_normalized = sql.replace("\r\n", "\n")
    statements = [
        s.strip()
        for s in sql_normalized.split(";\n")
        if s.strip() and _segment_has_sql(s)
    ]
    if not statements:
        statements = [sql.strip()] if sql.strip() and _segment_has_sql(sql) else []

    try:
        for i, stmt in enumerate(statements, 1):
            if not stmt or not _segment_has_sql(stmt):
                continue
            result = conn.execute(stmt)
            rows = result.fetchall()
            cols = result.description

            if len(statements) > 1:
                print(f"\n--- Result {i} ---\n")

            if cols and rows:
                col_names = [c[0] for c in cols]
                widths = [max(len(str(col_names[i])), *(len(str(r[i])) for r in rows)) for i in range(len(col_names))]
                fmt = "  ".join(f"{{:<{w}}}" for w in widths)
                print(fmt.format(*col_names))
                print("-" * (sum(widths) + 2 * (len(col_names) - 1)))
                for row in rows:
                    print(fmt.format(*[str(v) if v is not None else "" for v in row]))
            elif cols:
                print("(0 rows)")
            else:
                print("Done")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
