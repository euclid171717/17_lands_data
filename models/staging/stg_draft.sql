{{ config(materialized='table') }}
-- Staging: draft data (one row per pick)
-- Filter by expansion, event_type. Apply empty_to_null to key string columns.
-- Add more column transforms as needed for your analysis.

select
  * EXCLUDE (expansion, event_type),
  {{ empty_to_null('expansion') }} AS expansion,
  {{ empty_to_null('event_type') }} AS event_type
from {{ source('raw', 'draft') }}
