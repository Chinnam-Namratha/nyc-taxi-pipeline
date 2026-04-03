{{
    config(
        materialized = 'table',
        tags         = ['marts', 'fact']
    )
}}

/*
  fct_trips
  ─────────
  Core fact table — all valid, zone-enriched NYC yellow taxi trips for 2023.
  Sourced entirely from int_trips_enriched so that filtering logic lives
  in exactly one place.
*/

SELECT
    -- keys
    trip_id,

    -- time dimensions
    pickup_datetime,
    dropoff_datetime,
    trip_duration_minutes,
    pickup_date,
    pickup_year,
    pickup_month,
    pickup_hour,

    -- location dimensions
    pickup_location_id,
    pickup_borough,
    pickup_zone,
    pickup_service_zone,
    dropoff_location_id,
    dropoff_borough,
    dropoff_zone,
    dropoff_service_zone,

    -- other dimensions
    passenger_count,
    payment_type,

    -- measures
    trip_distance,
    fare_amount,
    tip_amount,
    tolls_amount,
    extra,
    mta_tax,
    improvement_surcharge,
    congestion_surcharge,
    total_amount

FROM {{ ref('int_trips_enriched') }}
