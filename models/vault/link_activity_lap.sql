{{ config(
    materialized = 'incremental',
    unique_key = 'activity_hk',
    incremental_strategy = 'insert_overwrite'
    ,tags=["strava"]
) }}

-- Source 1: Strava
with strava_lap_source as (
    select
        activity_id
        ,lap_number
        ,activity_lap_id as activity_lap_hk
        ,activity_hk
        ,{{ dbt_utils.generate_surrogate_key(['activity_hk','activity_lap_id']) }} activity_lap_lk
        ,load_date
        ,record_source
    from {{ ref('stage_strava_act_lap') }}
        where is_current = 1
),

deduplicated as (
    select  activity_id
            , lap_number
            , activity_lap_lk
            , activity_lap_hk
            , activity_hk
            , max(load_date)        load_date
            , max(record_source)    record_source
    from strava_lap_source
    group by all 
)

-- Insert-only logic: skip anything already loaded
select *
from deduplicated

{% if is_incremental() %}
where activity_lap_lk not in (
    select activity_lap_lk from {{ this }}
)
{% endif %}