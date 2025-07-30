{{ 
    config(materialized='view')
}}

with source_ as (
        select {{dbt_utils.star(
                          from=source('KANSAS_BASKETBALL', 'VW_TEAM')
                            )}}
    from {{ source('KANSAS_BASKETBALL', 'VW_GAME_RESULTS') }}
)


select {{ dbt_utils.generate_surrogate_key(['OPPONENT','GAME_DATE']) }} game_pk
      ,*
from source_
