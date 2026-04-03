/*
  Singular test: assert_agg_zone_performance_unique_key
  ──────────────────────────────────────────────────────
  agg_zone_performance has a composite primary key:
    (pickup_location_id, pickup_year, pickup_month)

  Any row returned by this query indicates a duplicate combination,
  which would mean the aggregation grouped incorrectly.
*/

SELECT
    pickup_location_id,
    pickup_year,
    pickup_month,
    COUNT(*) AS row_count
FROM {{ ref('agg_zone_performance') }}
GROUP BY
    pickup_location_id,
    pickup_year,
    pickup_month
HAVING COUNT(*) > 1
