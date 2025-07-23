{{ config(materialized='view'
        ,tags=["garmin"])
     }}

with source_ as (
        select {{dbt_utils.star(
                          from=source('kevans_grant_garmin', 'activities')
                            )}}
                ,'garmin' as record_source
    from {{ source('kevans_grant_garmin', 'activities') }}
)

-- keith evans today

select {{ dbt_utils.generate_surrogate_key(['activity_id','record_source']) }} activity_pk
      ,*
from source_
