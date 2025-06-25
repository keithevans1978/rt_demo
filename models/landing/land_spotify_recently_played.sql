{{ config(materialized='view'
        ,tags=["strava"])
     }}

with source_ as (
        select {{dbt_utils.star(
                          from=source('kevans_grant_spotify', 'RAW_SPOTIFY_RECENTLY_PLAYED')
                            )}}
    from {{ source('kevans_grant_spotify', 'RAW_SPOTIFY_RECENTLY_PLAYED') }}
)


, final_ as (
select {{ dbt_utils.generate_surrogate_key(['TRACK_ID','PLAYED_AT']) }} as snapshot_spotify_id
        ,{{dbt_utils.star(
                          from=source('kevans_grant_spotify', 'RAW_SPOTIFY_RECENTLY_PLAYED')
                            )}}
from source_
)

select distinct snapshot_spotify_id
        ,"TRACK_ID",
  "TRACK_NAME",
  "ARTIST_NAME",
  "ALBUM_NAME",
  "DURATION_MS",
  "EXPLICIT",
  "PLAYED_AT",
  "URI",
  "RAW_JSON",
  "METADATA_LOAD_TS",
  "METADATA_SOURCE_FILE",
  "METADATA_ROW_NUMBER"
from final_
