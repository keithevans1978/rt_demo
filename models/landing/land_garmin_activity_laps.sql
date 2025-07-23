{{ config(materialized='view'
        ,tags=["garmin"])
     }}

with source_ as (
        select {{dbt_utils.star(
                          from=source('kevans_grant_garmin', 'activity_laps')
                            )}}
                ,'garmin' as record_source
    from {{ source('kevans_grant_garmin', 'activity_laps') }}
)

-- keith evans today

select      {{ dbt_utils.generate_surrogate_key(['activity_id','lap','record_source']) }}     activity_lap_pk
            ,{{ dbt_utils.generate_surrogate_key(['activity_id','record_source']) }}          activity_pk
            ,*
from source_