{{
    config(
        materialized = 'table',
        tags         = ['marts', 'dimension']
    )
}}

/*
  dim_zones
  ─────────
  Conformed dimension for NYC TLC taxi zones.
  Downstream fact tables join on location_id.
*/

SELECT
    location_id,
    borough,
    zone_name,
    service_zone
FROM {{ ref('stg_taxi_zones') }}
