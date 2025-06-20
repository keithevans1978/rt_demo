{{ config(materialized='view'
        ,tags=["strava"])
     }}

with source_ as (
        select {{dbt_utils.star(
                          from=source('kevans_grant_spotify', 'RAW_SPOTIFY_RECENTLY_PLAYED')
                            )}}
    from {{ source('kevans_grant_spotify', 'RAW_SPOTIFY_RECENTLY_PLAYED') }}
)

-- Keith Evans today
, final_ as (
select {{ dbt_utils.generate_surrogate_key(['TRACK_ID','PLAYED_AT']) }}
        ,{{dbt_utils.star(
                          from=source('kevans_grant_spotify', 'RAW_SPOTIFY_RECENTLY_PLAYED')
                            )}}
from source_
)

select distinct *
from final_
