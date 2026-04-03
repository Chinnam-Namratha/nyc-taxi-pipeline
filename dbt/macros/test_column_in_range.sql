/*
  Generic test: column_in_range
  ──────────────────────────────
  Assert that all non-NULL values of `column_name` fall within the
  closed interval [min_value, max_value].

  Parametrised so the same test can be applied to any numeric column
  on any model.

  Usage in schema.yml:
    columns:
      - name: trip_duration_minutes
        tests:
          - column_in_range:
              arguments:
                min_value: 1
                max_value: 180

  Any row returned by this query is a test failure.
*/

{% test column_in_range(model, column_name, min_value, max_value) %}

SELECT
    {{ column_name }} AS actual_value
FROM  {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND (
        {{ column_name }} < {{ min_value }}
     OR {{ column_name }} > {{ max_value }}
  )

{% endtest %}
