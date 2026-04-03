{{
    config(
        materialized = 'view',
        tags         = ['staging', 'zones']
    )
}}

/*
  stg_taxi_zones
  ──────────────
  Thin rename/cast layer over the taxi_zone_lookup seed.
  Downstream models join on location_id.
*/

WITH source AS (

    SELECT * FROM {{ ref('taxi_zone_lookup') }}

),

renamed AS (

    SELECT
        CAST(LocationID AS INTEGER)  AS location_id,
        Borough                      AS borough,
        Zone                         AS zone_name,
        service_zone                 AS service_zone
    FROM source

)

SELECT * FROM renamed
