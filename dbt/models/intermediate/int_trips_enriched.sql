{{
    config(
        materialized = 'view',
        tags         = ['intermediate', 'yellow_taxi']
    )
}}

/*
  int_trips_enriched
  ──────────────────
  Applies business-rule filters to remove invalid trips, then joins each trip
  to the zone dimension for both pickup and dropoff locations.

  Filter rules (enforced here so that fct_trips is always "clean"):
    • trip_distance       > 0   — a trip must cover some distance
    • fare_amount         > 0   — zero/negative fares are data errors or comps
    • passenger_count    >= 1   — dispatched trips with no passengers are errors
    • trip_duration_minutes  BETWEEN 1 AND 180
                               — < 1 min is a recording error; > 3 hrs is an outlier
    • pickup_datetime    IN 2023 — scope to the dataset we loaded
*/

WITH trips AS (

    SELECT * FROM {{ ref('stg_yellow_trips') }}

),

zones AS (

    SELECT * FROM {{ ref('stg_taxi_zones') }}

),

filtered AS (

    SELECT *
    FROM   trips
    WHERE  trip_distance          > 0
      AND  fare_amount            > 0
      AND  passenger_count       >= 1
      AND  trip_duration_minutes  BETWEEN 1 AND 180
      AND  pickup_datetime       >= '2023-01-01'
      AND  pickup_datetime        < '2024-01-01'

),

enriched AS (

    SELECT
        filtered.*,

        -- Pickup zone attributes
        pu_zone.borough      AS pickup_borough,
        pu_zone.zone_name    AS pickup_zone,
        pu_zone.service_zone AS pickup_service_zone,

        -- Dropoff zone attributes
        do_zone.borough      AS dropoff_borough,
        do_zone.zone_name    AS dropoff_zone,
        do_zone.service_zone AS dropoff_service_zone

    FROM      filtered

    LEFT JOIN zones AS pu_zone
           ON filtered.pickup_location_id  = pu_zone.location_id

    LEFT JOIN zones AS do_zone
           ON filtered.dropoff_location_id = do_zone.location_id

)

SELECT * FROM enriched
