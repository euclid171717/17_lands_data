{{
  config(
    materialized='table',
    enabled=var('has_replay', false),
  )
}}
-- Staging: replay data (one row per turn)
-- Filter by expansion, event_type. Add column-specific transforms as needed.

select
  * EXCLUDE (expansion, event_type),
  {{ empty_to_null('expansion') }} AS expansion,
  {{ empty_to_null('event_type') }} AS event_type
from {{ source('raw', 'replay') }}
