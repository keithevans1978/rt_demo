{{ 
    config(
        materialized = 'view',tags=["strava"]
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['ACTIVITY_ID']) }}                                                                     activity_hk
	,activity_lap_id                                                                                                            activity_lap_hk
    ,activity_id
	,lap_number
	,lap_start_date
	,raw_json
	,load_date
	,dbt_scd_id
	,dbt_updated_at
	,dbt_valid_from
	,dbt_valid_to      
    ,{{ dbt_utils.generate_surrogate_key(['activity_id','activity_lap_id','lap_number','lap_start_date','raw_json']) }}         change_hk
    ,CASE 
        WHEN CAST(dbt_valid_to AS DATE) = '8888-12-31' THEN 1
        ELSE 0
    END AS is_current
    ,'strava_act_lap'  record_source
FROM {{ ref('snap_strava_act_lap') }}