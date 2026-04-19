#!/usr/bin/env python3
"""Print row counts for raw + dbt schemas in a DuckDB file (read-only)."""
import argparse
import sys
from pathlib import Path

import duckdb


def _default_db_argument(root: Path) -> str:
    """Prefer config-resolved path (per-set or single DB); fall back to 17lands.duckdb."""
    try:
        sys.path.insert(0, str(root))
        from scripts.ingest.config import get_paths, load_config

        p = get_paths(load_config())["db"]
        if p.exists():
            return str(p)
    except Exception:
        pass
    return str(root / "data" / "db" / "17lands.duckdb")


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    p = argparse.ArgumentParser()
    p.add_argument(
        "db",
        nargs="?",
        default=_default_db_argument(root),
        help="Path to .duckdb file (default: from config, else data/db/17lands.duckdb)",
    )
    args = p.parse_args()
    db = Path(args.db)
    if not db.exists():
        print(f"Not found: {db}", file=sys.stderr)
        return 1

    c = duckdb.connect(str(db), read_only=True)
    rows = c.execute(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name
    """
    ).fetchall()
    print(f"Database: {db}\n")
    for sch, name in rows:
        try:
            n = c.execute(f'SELECT COUNT(*) FROM "{sch}"."{name}"').fetchone()[0]
            print(f"  {sch}.{name}: {n:,} rows")
        except Exception as e:
            print(f"  {sch}.{name}: error {e}")
    c.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
