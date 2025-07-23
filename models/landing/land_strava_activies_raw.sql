{{ config(materialized='view'
        ,tags=["strava"])
     }}

with source_ as (
        select {{dbt_utils.star(
                          from=source('kevans_grant_strava', 'strava_activities_raw')
                            )}}
                            ,'strava' as record_source
    from {{ source('kevans_grant_strava', 'strava_activities_raw') }}
)

-- keith evans today

select {{ dbt_utils.generate_surrogate_key(['activity_id','record_source']) }} activity_pk
      ,*
from source_
