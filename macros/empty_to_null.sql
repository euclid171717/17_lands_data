{% macro empty_to_null(column_name) %}
  CASE
    WHEN TRIM(CAST({{ column_name }} AS VARCHAR)) = ''
      OR CAST({{ column_name }} AS VARCHAR) = '~'
    THEN NULL
    ELSE {{ column_name }}
  END
{% endmacro %}
