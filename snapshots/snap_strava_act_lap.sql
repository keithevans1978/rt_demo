{{ 
    config(materialized='ephemeral'
    ,tags=["strava"])
}}

select *
from {{ ref('land_strava_act_lap_raw') }}