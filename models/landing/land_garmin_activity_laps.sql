{{ config(materialized='view'
        ,tags=["garmin"])
     }}

with source_ as (
        select {{dbt_utils.star(
                          from=source('kevans_grant_garmin', 'ACTIVITY_LAPS')
                            )}}
    from {{ source('kevans_grant_garmin', 'ACTIVITY_LAPS') }}
)

-- Keith Evans today

select {{ dbt_utils.generate_surrogate_key(['ACTIVITY_ID','LAP']) }} ACTIVITY_LAP_PK
      ,*
from source_