{{ 
    config(materialized='ephemeral') 
}}

select * 
from {{ ref('land_strava_act_lap_raw') }}