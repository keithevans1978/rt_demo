{{ config(materialized='view'
        ,tags=["strava"])
     }}

with source_ as (
        select {{dbt_utils.star(
                          from=source('kevans_grant_strava', 'STRAVA_ACTIVITIES_RAW')
                            )}}
    from {{ source('kevans_grant_strava', 'STRAVA_ACTIVITIES_RAW') }}
)

-- Keith Evans today

select {{ dbt_utils.generate_surrogate_key(['ACTIVITY_ID']) }} ACTIVITY_LAP_ID
      ,*
from source_
