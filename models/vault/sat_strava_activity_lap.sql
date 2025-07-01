{{ config(
    materialized = 'incremental',
    unique_key = ['activity_lap_hk','load_date'],
    incremental_strategy = 'insert_overwrite'
    ,tags=["strava"]
) }}

-- Source 1: Strava
with strava_lap_source as (
    select      activity_hk
	           ,activity_id
	           ,start_date
	           ,activity_type
	           ,activity_name
	           ,raw_json
	           ,load_date
               ,change_hk
               ,record_source
    from {{ ref('stage_strava_act_lap') }}
    where is_current = 1
)

,strava_lap_current as (
    {% if is_incremental() %}

    select
        activity_lap_hk
        ,load_date
        ,change_hk
    from {{ this }}
    qualify lead(load_date) over (
        partition by activity_lap_hk
        order by load_date
    ) is null

    {% else %}

    -- Return an empty result so model builds successfully on first run
    select
        null as activity_hk
        ,null as load_date
        ,null as change_hk
    where false

    {% endif %}
                )

,new_records as (
    select s.*
    from strava_source s
    left join strava_current c
      on s.activity_hk = c.activity_hk
      and s.load_date = c.load_date
     and s.change_hk = c.change_hk  -- change hash match
    where c.activity_hk is null  -- no match → new or changed
)

select *
from new_records