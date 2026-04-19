{{
  config(
    materialized='table',
    tags=['heavy'],
    enabled=var('build_pack_pool_mart', false),
  )
}}
-- Long-format pack_card_* / pool_* columns (one row per pick × card column).
SELECT
  u.draft_id,
  u.expansion,
  u.event_type,
  u.pack_number,
  u.pick_number,
  u.pick,
  u.col AS source_column,
  CASE
    WHEN u.col LIKE 'pack_card_%' THEN 'pack_card'
    WHEN u.col LIKE 'pool_%' THEN 'pool'
  END AS role,
  CASE
    WHEN u.col LIKE 'pack_card_%' THEN regexp_replace(u.col, '^pack_card_', '')
    WHEN u.col LIKE 'pool_%' THEN regexp_replace(u.col, '^pool_', '')
  END AS card_name_in_column,
  TRY_CAST(u.v AS INTEGER) AS present
FROM (
  UNPIVOT (
    SELECT * FROM {{ source('raw', 'draft') }}
  ) ON COLUMNS(c -> c LIKE 'pack_card_%' OR c LIKE 'pool_%')
  INTO NAME col VALUE v
) u
