{{
    config(
        materialized = 'table',
        tags         = ['marts', 'aggregate', 'zones']
    )
}}

/*
  agg_zone_performance
  ────────────────────
  Per-zone, per-month performance metrics with a revenue rank.

  On the revenue rank question — I went with ranking within each month rather
  than a global rank across the whole year. A global rank is basically useless
  here because Manhattan zones dominate every single month without exception.
  You'd look at it and see Midtown at #1 in January, #1 in February, #1 in
  March... and so on. There's no signal in that.

  Monthly ranking is what actually matters operationally. It tells you which
  zones are picking up momentum, which ones drop off in winter, and whether a
  zone that was mid-table in Q1 started climbing by Q3. You can also trend a
  zone's rank over time as a time series, which you simply can't do with a
  frozen annual rank.

  If someone needs a global annual rank downstream, it's a one-liner GROUP BY
  on top of this model. But the monthly version is the useful one.
*/

WITH zone_monthly AS (

    SELECT
        pickup_year,
        pickup_month,
        pickup_location_id,
        pickup_zone,
        pickup_borough,

        COUNT(*)                               AS total_trips,
        ROUND(AVG(trip_distance), 2)           AS avg_trip_distance,
        ROUND(AVG(fare_amount),   2)           AS avg_fare,
        ROUND(SUM(total_amount),  2)           AS monthly_revenue

    FROM {{ ref('fct_trips') }}

    GROUP BY
        pickup_year,
        pickup_month,
        pickup_location_id,
        pickup_zone,
        pickup_borough

),

ranked AS (

    SELECT
        *,

        -- Revenue rank within each calendar month (see rationale above).
        RANK() OVER (
            PARTITION BY pickup_year, pickup_month
            ORDER BY     monthly_revenue DESC
        )                                      AS revenue_rank_in_month,

        -- High-demand flag: zones that handled >10,000 trips in the month.
        -- Useful for capacity planning and targeted incentive programs.
        CASE
            WHEN total_trips > 10000 THEN TRUE
            ELSE FALSE
        END                                    AS is_high_demand_zone

    FROM zone_monthly

)

SELECT *
FROM   ranked
ORDER BY
    pickup_year,
    pickup_month,
    revenue_rank_in_month
