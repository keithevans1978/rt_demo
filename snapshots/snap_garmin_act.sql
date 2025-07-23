{{ 
    config(materialized='ephemeral'
    ,tags=["garmin"])
}}

select * 
from {{ ref('land_garmin_activies') }}