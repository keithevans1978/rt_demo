{{ config(
    materialized = 'view'
    ,tags=["strava"]
) }}


select *
        ,{{ sat_record_start_date('load_date') }}
        ,{{ sat_record_end_date('activity_lap_hk', 'load_date') }}
        ,{{ sat_current_ind('activity_lap_hk', 'load_date') }}
        ,{{ sat_version_number('activity_lap_hk', 'load_date') }}
        ,{{ sat_reverse_version_number('activity_lap_hk', 'load_date') }}
from {{ ref('sat_strava_activity_lap') }}



