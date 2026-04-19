-- Legacy entry point — prefer the numbered files in this folder (see queries/README.md).
-- If your DuckDB extension does not auto-open a database, attach once (adjust path to match DUCKDB_PATH / your set):

-- ATTACH 'data/db/MKM.duckdb' AS mk (READ_ONLY);
-- Then qualify tables: mk.raw.draft, etc.

SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
ORDER BY 1, 2;
