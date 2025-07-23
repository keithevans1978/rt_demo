{{ config(materialized='view'
        ,tags=["garmin"])
     }}

with source_ as (
        select {{dbt_utils.star(
                          from=source('kevans_grant_garmin', 'WEIGHT')
                            )}}
    from {{ source('kevans_grant_garmin', 'WEIGHT') }}
)

-- Keith Evans today

select {{ dbt_utils.generate_surrogate_key(['WEIGHT']) }} ACTIVITY_PK
      ,*
from source_
