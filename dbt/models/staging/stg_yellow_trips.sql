{{
    config(
        materialized = 'view',
        tags         = ['staging', 'yellow_taxi']
    )
}}

/*
  stg_yellow_trips
  ────────────────
  Rename raw columns to snake_case, enforce data types, and derive
  trip_duration_minutes.  NO filtering happens here — business-rule filtering
  is deferred to the intermediate layer so that data quality tests in the
  staging schema are evaluated against the full raw dataset.
*/

WITH source AS (

    SELECT * FROM {{ source('nyc_tlc', 'yellow_tripdata') }}

),

renamed AS (

    SELECT
        -- ── Surrogate key ───────────────────────────────────────────────────
        {{ generate_surrogate_key([
            'tpep_pickup_datetime',
            'tpep_dropoff_datetime',
            'PULocationID',
            'DOLocationID',
            'fare_amount'
        ]) }}                                           AS trip_id,

        -- ── Timestamps ─────────────────────────────────────────────────────
        CAST(tpep_pickup_datetime  AS TIMESTAMP)        AS pickup_datetime,
        CAST(tpep_dropoff_datetime AS TIMESTAMP)        AS dropoff_datetime,

        -- ── Computed: trip duration ─────────────────────────────────────────
        DATEDIFF(
            'minute',
            CAST(tpep_pickup_datetime  AS TIMESTAMP),
            CAST(tpep_dropoff_datetime AS TIMESTAMP)
        )                                               AS trip_duration_minutes,

        -- ── Date partitioning helpers ───────────────────────────────────────
        CAST(tpep_pickup_datetime AS DATE)              AS pickup_date,
        EXTRACT('year'  FROM tpep_pickup_datetime)::INTEGER AS pickup_year,
        EXTRACT('month' FROM tpep_pickup_datetime)::INTEGER AS pickup_month,
        EXTRACT('hour'  FROM tpep_pickup_datetime)::INTEGER AS pickup_hour,

        -- ── Integer dimensions ──────────────────────────────────────────────
        CAST(passenger_count AS INTEGER)                AS passenger_count,
        CAST(PULocationID    AS INTEGER)                AS pickup_location_id,
        CAST(DOLocationID    AS INTEGER)                AS dropoff_location_id,
        CAST(payment_type    AS INTEGER)                AS payment_type,

        -- ── Numeric measures ────────────────────────────────────────────────
        CAST(trip_distance          AS DOUBLE)          AS trip_distance,
        CAST(fare_amount            AS DOUBLE)          AS fare_amount,
        CAST(tip_amount             AS DOUBLE)          AS tip_amount,
        CAST(tolls_amount           AS DOUBLE)          AS tolls_amount,
        CAST(extra                  AS DOUBLE)          AS extra,
        CAST(mta_tax                AS DOUBLE)          AS mta_tax,
        CAST(improvement_surcharge  AS DOUBLE)          AS improvement_surcharge,
        CAST(congestion_surcharge   AS DOUBLE)          AS congestion_surcharge,
        CAST(total_amount           AS DOUBLE)          AS total_amount

    FROM source

),

deduplicated AS (

    -- The TLC source data contains genuinely duplicate rows (identical
    -- pickup/dropoff times, locations, and fare). Keep only the first
    -- occurrence of each duplicate group so trip_id is unique.
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY trip_id
            ORDER BY     pickup_datetime
        ) AS _row_num
    FROM renamed

)

SELECT * EXCLUDE (_row_num)
FROM   deduplicated
WHERE  _row_num = 1
