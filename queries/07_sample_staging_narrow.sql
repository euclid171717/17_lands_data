-- Samples from dbt staging (requires `dbt run` first).

SELECT
  expansion,
  event_type,
  draft_id,
  pack_number,
  pick_number,
  pick
FROM main_staging.stg_draft
LIMIT 10;

SELECT
  expansion,
  event_type,
  draft_id,
  game_time,
  game_number,
  won
FROM main_staging.stg_game
LIMIT 10;

SELECT * FROM main_staging.stg_dungeon LIMIT 20;

SELECT * FROM main_staging.stg_abilities LIMIT 5;

SELECT * FROM main_staging.stg_cards LIMIT 5;
