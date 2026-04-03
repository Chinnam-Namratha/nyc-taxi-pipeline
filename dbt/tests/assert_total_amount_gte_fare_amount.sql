/*
  Singular test: assert_total_amount_gte_fare_amount
  ──────────────────────────────────────────────────
  Business rule: total_amount = fare + tip + tolls + surcharges + extras.
  It can NEVER be less than the base fare.

  Any row returned by this query is a test failure.
  Rows that fail indicate data corruption, sign errors in the source feed,
  or a bug in the staging casts.

  Note: we allow total_amount == fare_amount (a $0-tip cash trip with no
  surcharges is valid), so the condition is strictly < rather than <=.
*/

SELECT
    trip_id,
    fare_amount,
    total_amount,
    ROUND(total_amount - fare_amount, 4) AS difference  -- how far off it is; useful when debugging patterns in failures
FROM {{ ref('fct_trips') }}
WHERE total_amount < fare_amount
