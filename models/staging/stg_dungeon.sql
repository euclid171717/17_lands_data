{{
  config(
    materialized='view'
  )
}}
-- Staging: dungeon abilities for AFR replay data
-- Applies empty_to_null to strings, sensible types for ids

select
  CAST(abilityId AS INTEGER) AS ability_id,
  {{ empty_to_null('dungeon') }} AS dungeon,
  {{ empty_to_null('room') }} AS room,
  {{ empty_to_null('effect') }} AS effect
from {{ source('raw_helpers', 'dungeon') }}
