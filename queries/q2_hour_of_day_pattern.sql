/*
  Q2 — Hour-of-day demand pattern
  ────────────────────────────────
  For each hour of the day (0–23):
    • total_trips             — total trip count across all 2023 data
    • avg_fare                — average base fare
    • avg_tip_pct             — average tip as % of fare (credit-card trips only;
                                cash-trip tips are not recorded in the TLC data)
    • rolling_3h_avg_trips    — centred 3-hour rolling average of trip count
                                (1 hour before + current + 1 hour after)

  Sorted by hour ascending.
*/

WITH hourly_stats AS (

    SELECT
        EXTRACT(HOUR FROM pickup_datetime)::INTEGER     AS hour_of_day,
        COUNT(*)                                        AS total_trips,
        ROUND(AVG(fare_amount), 2)                      AS avg_fare,

        -- Tip % = tip / fare — only meaningful for credit-card trips.
        -- Exclude rows where fare_amount = 0 to avoid division errors.
        ROUND(
            AVG(
                CASE
                    WHEN fare_amount > 0
                    THEN tip_amount / fare_amount * 100.0
                    ELSE NULL
                END
            ),
            2
        )                                               AS avg_tip_pct

    FROM fct_trips                                      -- prefix: marts.fct_trips
    GROUP BY EXTRACT(HOUR FROM pickup_datetime)

),

with_rolling AS (

    SELECT
        hour_of_day,
        total_trips,
        avg_fare,
        avg_tip_pct,

        -- Centred 3-hour rolling average: window covers the hour before,
        -- current hour, and hour after.  At the edges (hour 0 and 23) the
        -- window is naturally truncated to the available rows.
        ROUND(
            AVG(total_trips) OVER (
                ORDER BY hour_of_day
                ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
            ),
            0
        )                                               AS rolling_3h_avg_trips

    FROM hourly_stats

)

SELECT
    hour_of_day,
    total_trips,
    avg_fare,
    avg_tip_pct,
    rolling_3h_avg_trips
FROM   with_rolling
ORDER BY hour_of_day;
