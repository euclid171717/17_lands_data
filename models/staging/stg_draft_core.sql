{{ config(materialized='table') }}
-- Cross-set stable columns only (see macros/draft_stable.sql).
-- Card-specific presence columns remain on raw.draft / a set-specific wide model.

select
{% for col in get_draft_stable_columns() %}
  {{ empty_to_null(col) }} as {{ col }}{{ "," if not loop.last }}
{% endfor %}
from {{ source('raw', 'draft') }}
