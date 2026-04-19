-- Row counts for dbt staging views (only after `dbt run` on this database).
-- Omit this file if you have not run dbt yet.

SELECT 'main_staging.stg_draft' AS table_path, COUNT(*) AS row_count FROM main_staging.stg_draft
UNION ALL SELECT 'main_staging.stg_draft_core', COUNT(*) FROM main_staging.stg_draft_core
UNION ALL SELECT 'main_staging.stg_game', COUNT(*) FROM main_staging.stg_game
UNION ALL SELECT 'main_staging.stg_abilities', COUNT(*) FROM main_staging.stg_abilities
UNION ALL SELECT 'main_staging.stg_cards', COUNT(*) FROM main_staging.stg_cards
UNION ALL SELECT 'main_staging.stg_dungeon', COUNT(*) FROM main_staging.stg_dungeon;
