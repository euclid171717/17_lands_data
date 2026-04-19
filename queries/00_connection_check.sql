-- Quick sanity check: run this first in the DuckDB extension or via run_query.
SELECT
  current_database() AS database_name,
  'connected' AS status;
