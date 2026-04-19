{{ config(materialized='table') }}
-- Staging: MTGA ability IDs to names for replay data
-- Adjust columns to match raw_card_data.abilities schema

select
  CAST(a.id AS INTEGER) AS ability_id,
  {{ empty_to_null('a.text') }} AS name
from {{ source('raw_card_data', 'abilities') }} a
