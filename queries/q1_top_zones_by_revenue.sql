/*
  Q1 — Top 10 pickup zones by revenue in each month of 2023

  Performance approach: aggregate to zone × month grain before windowing.
  This reduces ~38M rows to roughly 265 zones × 12 months = ~3,180 rows,
  so RANK() operates on a tiny result set rather than the full fact table.

  On Snowflake, the WHERE YEAR(...) = 2023 filter triggers partition pruning
  because fct_trips is clustered by pickup_date — partitions outside 2023 are
  skipped entirely. Selecting only 6 of the 25 columns also reduces I/O since
  Snowflake's columnar storage reads only the referenced columns per partition.

  Expected runtime on an X-Small warehouse: under 30 seconds on first run,
  near-instant on repeat runs due to Snowflake result caching.
*/

WITH monthly_zone_revenue AS (

    -- Step 1: aggregate to zone × month grain.
    -- This gives us ~3,180 rows before the window function runs.
    SELECT
        DATE_TRUNC('month', pickup_datetime)        AS trip_month,
        YEAR(pickup_datetime)                       AS trip_year,
        MONTH(pickup_datetime)                      AS trip_month_num,
        pickup_location_id,
        pickup_zone,
        pickup_borough,
        COUNT(*)                                    AS total_trips,
        ROUND(SUM(total_amount), 2)                 AS total_revenue

    FROM fct_trips                              -- prefix with schema in prod: marts.fct_trips
    WHERE YEAR(pickup_datetime) = 2023          -- partition pruning hint

    GROUP BY
        DATE_TRUNC('month', pickup_datetime),
        YEAR(pickup_datetime),
        MONTH(pickup_datetime),
        pickup_location_id,
        pickup_zone,
        pickup_borough

),

ranked AS (

    -- Step 2: rank within each month over the small aggregated result set.
    SELECT
        *,
        RANK() OVER (
            PARTITION BY trip_year, trip_month_num
            ORDER BY     total_revenue DESC
        ) AS revenue_rank

    FROM monthly_zone_revenue

)

SELECT
    trip_month,
    trip_year,
    trip_month_num,
    pickup_location_id,
    pickup_zone,
    pickup_borough,
    total_trips,
    total_revenue,
    revenue_rank

FROM   ranked
WHERE  revenue_rank <= 10
ORDER BY
    trip_year,
    trip_month_num,
    revenue_rank;
