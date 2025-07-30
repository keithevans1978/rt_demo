{{ 
    config(materialized='ephemeral')
}}

select * 
from {{ ref('land_game') }}