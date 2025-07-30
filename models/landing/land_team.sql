{{ 
    config(materialized='view')
}}

with source_ as (
        select {{dbt_utils.star(
                          from=source('KANSAS_BASKETBALL', 'VW_TEAM')
                            )}}
                            ,'strava' as record_source
    from {{ source('KANSAS_BASKETBALL', 'VW_TEAM') }}
)


select {{ dbt_utils.generate_surrogate_key(['team_name']) }} team_name_pk
      ,*
from source_
