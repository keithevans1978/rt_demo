{{ 
    config(materialized='ephemeral') 
}}

select * 
from {{ ref('land_spotify_recently_played') }}