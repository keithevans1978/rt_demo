{{ config(
    materialized = 'incremental',
    unique_key = 'activity_hk',
    incremental_strategy = 'append'
    ,tags=["strava"]
) }}

-- Source 1: Strava
with strava_source as (
    select
       activity_id
       ,activity_hk
       ,load_date
       ,record_source
    from {{ ref('stage_strava_act') }}
    where is_current = 1
),

-- Source 2: Garmin
strava_lap_source as (
    select
        activity_id
        ,activity_hk
        ,load_date
        ,record_source
    from {{ ref('stage_strava_act_lap') }}
        where is_current = 1
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