-- Readable samples: key columns only (no per-card pack_card_* / pool_* / drawn_* wide fields).

SELECT
  expansion,
  event_type,
  draft_id,
  draft_time,
  rank,
  pack_number,
  pick_number,
  pick,
  pick_maindeck_rate,
  pick_sideboard_in_rate
FROM raw.draft
LIMIT 10;

SELECT
  expansion,
  event_type,
  draft_id,
  game_time,
  game_number,
  won
FROM raw.game
LIMIT 10;

SELECT * FROM raw_helpers.dungeon LIMIT 20;

SELECT id, text FROM raw_card_data.abilities LIMIT 10;

SELECT * FROM raw_card_data.cards LIMIT 10;
