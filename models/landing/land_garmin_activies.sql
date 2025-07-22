{{ config(materialized='view'
        ,tags=["garmin"])
     }}

with source_ as (
        select {{dbt_utils.star(
                          from=source('kevans_grant_garmin', 'ACTIVITIES')
                            )}}
    from {{ source('kevans_grant_garmin', 'ACTIVITIES') }}
)

-- Keith Evans today

select {{ dbt_utils.generate_surrogate_key(['ACTIVITY_ID']) }} ACTIVITY_PK
      ,*
from source_
