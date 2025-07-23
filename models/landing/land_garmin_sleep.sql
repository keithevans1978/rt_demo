{{ config(materialized='view'
        ,tags=["garmin"])
     }}

with source_ as (
        select {{dbt_utils.star(
                          from=source('kevans_grant_garmin', 'SLEEP')
                            )}}
    from {{ source('kevans_grant_garmin', 'SLEEP') }}
)

-- Keith Evans today

select {{ dbt_utils.generate_surrogate_key(['DAY']) }} ACTIVITY_PK
      ,*
from source_
