{# Column names that appear in both MKM and LCI PremierDraft draft CSV headers (Feb 2026).
   Most other columns are set-specific: pack_card_<exact card name>, pool_<exact card name>.
   Re-run a header diff when 17lands adds global columns; extend this list if needed. #}

{% macro get_draft_stable_columns() %}
  {{ return([
    'expansion',
    'event_type',
    'draft_id',
    'draft_time',
    'rank',
    'event_match_wins',
    'event_match_losses',
    'pack_number',
    'pick_number',
    'pick',
    'pick_maindeck_rate',
    'pick_sideboard_in_rate',
    'user_game_win_rate_bucket',
    'user_n_games_bucket',
    'pack_card_Forest',
    'pack_card_Island',
    'pack_card_Mountain',
    'pack_card_Plains',
    'pack_card_Swamp',
    'pool_Forest',
    'pool_Island',
    'pool_Mountain',
    'pool_Plains',
    'pool_Swamp',
  ]) }}
{% endmacro %}
