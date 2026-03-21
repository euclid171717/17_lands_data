{{
  config(
    materialized='view'
  )
}}
-- Staging: MTGA ability IDs to names for replay data
-- Adjust columns to match raw_card_data.abilities schema

select
  CAST(id AS INTEGER) AS ability_id,
  {{ empty_to_null('name') }} AS name
from {{ source('raw_card_data', 'abilities') }}
