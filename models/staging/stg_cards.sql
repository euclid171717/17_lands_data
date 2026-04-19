{{ config(materialized='table') }}
-- Staging: MTGA card IDs to card names and metadata
-- Columns: id, expansion, name, rarity, color_identity, mana_value, types, is_booster

select
  CAST(id AS INTEGER) AS card_id,
  {{ empty_to_null('expansion') }} AS expansion,
  {{ empty_to_null('name') }} AS name,
  {{ empty_to_null('rarity') }} AS rarity,
  {{ empty_to_null('color_identity') }} AS color_identity,
  TRY_CAST(mana_value AS INTEGER) AS mana_value,
  {{ empty_to_null('types') }} AS types,
  CASE
    WHEN TRIM(CAST(is_booster AS VARCHAR)) = '' OR CAST(is_booster AS VARCHAR) = '~'
    THEN NULL
    ELSE LOWER(CAST(is_booster AS VARCHAR)) IN ('true', '1', 'yes')
  END AS is_booster
from {{ source('raw_card_data', 'cards') }}
