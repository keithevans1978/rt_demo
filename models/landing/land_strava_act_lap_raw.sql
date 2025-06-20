{{ config(materialized='view'
        ,tags=["strava"])
 }}

with source_ as (
        select {{dbt_utils.star(
                          from=source('kevans_grant_strava', 'STRAVA_ACTIVITY_LAP_RAW')
                            )}}
    from {{ source('kevans_grant_strava', 'STRAVA_ACTIVITY_LAP_RAW') }}
)

select {{ dbt_utils.generate_surrogate_key(['ACTIVITY_ID', 'LAP_NUMBER']) }} ACTIVITY_LAP_ID
      ,*
from source_