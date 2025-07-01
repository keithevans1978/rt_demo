{{ config(
    materialized = 'incremental',
    unique_key = 'activity_hk',
    incremental_strategy = 'append'
    ,tags=["strava"]
) }}

-- Source 1: Strava
with strava_lap_source as (
    select
        activity_id
        , lap_number
        , activity_lap_hk      
        , load_date
        , record_source
    from {{ ref('stage_strava_act_lap') }}
        where is_current = 1
),

deduplicated as (
    select  activity_id
            , lap_number
            , activity_lap_hk
            , max(load_date)        load_date
            , max(record_source)    record_source
    from strava_lap_source
    group by all 
)

-- Insert-only logic: skip anything already loaded
select *
from deduplicated

{% if is_incremental() %}
where activity_lap_hk not in (
    select activity_lap_hk from {{ this }}
)
{% endif %}