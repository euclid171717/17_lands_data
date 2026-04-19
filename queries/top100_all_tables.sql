-- Deprecated: `SELECT *` on raw.draft / raw.game is too wide for the editor and slow.
-- Use instead:
--   queries/05_describe_core_tables.sql  — column layout
--   queries/06_sample_rows_narrow.sql    — readable samples
--   queries/02_columns_all_tables.sql    — full column list from information_schema

SELECT 'See queries/README.md' AS use_instead;
