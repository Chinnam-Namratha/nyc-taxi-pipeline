/*
  Macro: generate_surrogate_key
  ──────────────────────────────
  Creates an MD5 hash from a list of fields, concatenated with a pipe
  delimiter to avoid hash collisions across different field-value combinations.

  Compatible with DuckDB (MD5, CONCAT_WS) and Snowflake (MD5, CONCAT_WS).

  Usage:
    {{ generate_surrogate_key(['pickup_datetime', 'dropoff_datetime', 'PULocationID']) }}
*/

{% macro generate_surrogate_key(field_list) %}

    MD5(
        CONCAT_WS(
            '|',
            {% for field in field_list %}
                CAST({{ field }} AS VARCHAR)
                {%- if not loop.last %},{% endif %}
            {% endfor %}
        )
    )

{% endmacro %}
