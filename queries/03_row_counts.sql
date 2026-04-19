-- Row counts for tables created by ingest (raw + helpers + card lists).
-- After `dbt run`, also run `queries/04_row_counts_dbt.sql` for main_staging.*.

SELECT 'raw.draft' AS table_path, COUNT(*) AS row_count FROM raw.draft
UNION ALL SELECT 'raw.game', COUNT(*) FROM raw.game
UNION ALL SELECT 'raw_helpers.dungeon', COUNT(*) FROM raw_helpers.dungeon
UNION ALL SELECT 'raw_card_data.abilities', COUNT(*) FROM raw_card_data.abilities
UNION ALL SELECT 'raw_card_data.cards', COUNT(*) FROM raw_card_data.cards;
