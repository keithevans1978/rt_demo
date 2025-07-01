{{ 
    config(
        materialized = 'view',tags=["strava"]
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['ACTIVITY_ID']) }} activity_hk,
    *,
    CASE 
        WHEN dbt_valid_to IS NULL THEN TRUE
        ELSE FALSE
    END AS is_current
FROM {{ ref('snap_strava_act_lap') }}