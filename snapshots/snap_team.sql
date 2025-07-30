{{ 
    config(materialized='ephemeral'
    ,tags=["strava"])
}}

select * 
from {{ ref('land_strava_activies_raw') }}