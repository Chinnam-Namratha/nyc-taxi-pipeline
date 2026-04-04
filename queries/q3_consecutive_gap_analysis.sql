/*
  Q3 — Consecutive trip gap analysis
  ────────────────────────────────────
  For each day × pickup zone in 2023, find the maximum gap in minutes
  between the end of one trip and the start of the next trip from the
  same zone. This identifies periods of unusually low demand per zone.

  Output columns:
    trip_date            — calendar date
    pickup_location_id   — zone ID
    pickup_zone          — zone name
    max_gap_minutes      — longest intra-zone gap on that day

  Performance strategy for Snowflake (~38M rows):

  Clustering key — the most impactful change. Adding
    ALTER TABLE fct_trips CLUSTER BY (pickup_location_id, pickup_date)
  physically co-locates rows by zone and date. The LEAD() window function
  partitions by (date, location_id), so each partition resolves to a small
  set of micro-partitions rather than scanning all 38M rows.

  Search Optimization Service — enables faster equality lookups on
  pickup_location_id when Snowflake builds the partition list for each
  LEAD() window. Enabled via:
    ALTER TABLE fct_trips ADD SEARCH OPTIMIZATION

  Result caching — this query runs over static historical data so the result
  is identical on every run. Snowflake caches it for 24 hours at no warehouse
  cost. Running it once nightly via the Airflow pipeline is sufficient.

  Incremental materialisation — wrapping this in a dbt incremental model
  (unique_key = trip_date + pickup_location_id) means each daily run only
  processes yesterday's data and appends it, avoiding a full re-scan of the
  historical dataset every time.
*/

WITH trips_ordered AS (

    SELECT
        DATE(pickup_datetime)                               AS trip_date,
        pickup_location_id,
        pickup_zone,
        pickup_datetime,
        dropoff_datetime,

        -- Next trip's pickup time within the same zone on the same calendar day.
        LEAD(pickup_datetime) OVER (
            PARTITION BY DATE(pickup_datetime), pickup_location_id
            ORDER BY     pickup_datetime
        )                                                   AS next_pickup_datetime

    FROM fct_trips                                          -- prefix: marts.fct_trips

),

gaps AS (

    SELECT
        trip_date,
        pickup_location_id,
        pickup_zone,

        -- Gap = next trip's start minus this trip's end.
        -- A positive gap means the zone had idle time between these two trips.
        DATEDIFF(
            'minute',
            dropoff_datetime,
            next_pickup_datetime
        )                                                   AS gap_minutes

    FROM trips_ordered

    WHERE
        next_pickup_datetime IS NOT NULL                    -- last trip of the day has no "next"
        AND next_pickup_datetime > dropoff_datetime         -- exclude overlapping / concurrent trips

)

SELECT
    trip_date,
    pickup_location_id,
    pickup_zone,
    MAX(gap_minutes)                                        AS max_gap_minutes

FROM   gaps

GROUP BY
    trip_date,
    pickup_location_id,
    pickup_zone

ORDER BY
    trip_date,
    max_gap_minutes DESC;
