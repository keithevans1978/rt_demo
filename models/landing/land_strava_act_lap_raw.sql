{{ config(materialized='view'
        ,tags=["strava"])
 }}

with source_ as (
        select {{dbt_utils.star(
                          from=source('kevans_grant_strava', 'strava_activity_lap_raw')
                            )}}
                        ,'strava' as record_source
    from {{ source('kevans_grant_strava', 'strava_activity_lap_raw') }}
)

select      {{ dbt_utils.generate_surrogate_key(['activity_id', 'lap_number','record_source']) }}     activity_lap_pk
            ,{{ dbt_utils.generate_surrogate_key(['activity_id','record_source']) }}                  activity_pk
            ,*
from source_