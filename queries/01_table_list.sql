-- All user tables (schemas + names). Safe to run on any ingested DB.
SELECT
  table_catalog,
  table_schema,
  table_name
FROM information_schema.tables
WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
ORDER BY table_schema, table_name;
