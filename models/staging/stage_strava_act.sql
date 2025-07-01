{{ 
    config(
        materialized = 'view',tags=["strava"]
    )
}}

SELECT
    activity_lap_id  activity_hk,
	activity_id,
	start_date,
	activity_type,
	activity_name,
	raw_json,
	load_date,
	dbt_scd_id,
	dbt_updated_at,
	dbt_valid_from,
	dbt_valid_to,
    {{ dbt_utils.generate_surrogate_key(['activity_id','activity_lap_id','activity_name','activity_type','raw_json','start_date']) }} change_hk
    ,CASE 
        WHEN CAST(dbt_valid_to AS DATE) = '8888-12-31' THEN 1
        ELSE 0
    END AS is_current
    ,'snap_strava_act'  record_source
FROM {{ ref('snap_strava_act') }}