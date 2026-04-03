{{
    config(
        materialized = 'table',
        tags         = ['marts', 'aggregate', 'revenue']
    )
}}

/*
  agg_daily_revenue
  ─────────────────
  Daily revenue aggregation.  One row per calendar day in 2023.
*/

SELECT
    pickup_date,
    pickup_year,
    pickup_month,

    COUNT(*)                                                    AS total_trips,
    ROUND(SUM(fare_amount),   2)                                AS total_fare,
    ROUND(AVG(fare_amount),   2)                                AS avg_fare,
    ROUND(SUM(tip_amount),    2)                                AS total_tips,
    ROUND(SUM(total_amount),  2)                                AS total_revenue,

    -- Tip rate = total tips / total fares (credit-card trips only have tip data,
    -- so this under-counts the true tip rate for cash trips)
    ROUND(
        SUM(tip_amount) / NULLIF(SUM(fare_amount), 0) * 100,
        2
    )                                                           AS tip_rate_pct

FROM {{ ref('fct_trips') }}

GROUP BY
    pickup_date,
    pickup_year,
    pickup_month

ORDER BY
    pickup_date
