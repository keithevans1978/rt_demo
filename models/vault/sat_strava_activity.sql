{{ config(
    materialized = 'incremental',
    unique_key = ['activity_hk','load_date'],
    incremental_strategy = 'insert_overwrite'
    ,tags=["strava"]
) }}

-- Source 1: Strava
with strava_source as (
    select  activity_id
            activity_lap_id
            activity_name
            activity_type
            load_date
            raw_json
            start_date
        'stage_strava_activity' as record_source
    from {{ ref('stage_strava_act') }}
    where is_current = 1
),

-- Source 2: Garmin
strava_lap_source as (
    select
        activity_id,
       activity_hk as activity_hk,
        current_timestamp as load_date,
        'stage_strava_act_lap' as record_source
    from {{ ref('stage_strava_act_lap') }}
),


-- Union the sources
unioned as (
    select * from strava_source
    union all
    select * from strava_lap_source
),

deduplicated as (
    select  activity_id
            , activity_hk
            , max(load_date)        load_date
            , max(record_source)    record_source
    from unioned
    group by all 
)

-- Insert-only logic: skip anything already loaded
select *
from deduplicated

{% if is_incremental() %}
where activity_hk not in (
    select activity_hk from {{ this }}
)
{% endif %}