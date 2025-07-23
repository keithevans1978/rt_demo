{{ 
    config(
        materialized = 'view',
        post_hook = [
            "TRUNCATE TABLE KEVANS_GRANT_SRC.SPOTIFY_API.RAW_SPOTIFY_RECENTLY_PLAYED"
        ]
    )
}}

SELECT
    *,
    CASE 
        WHEN dbt_valid_to IS NULL THEN TRUE
        ELSE FALSE
    END AS is_current
FROM {{ ref('snap_spotify_recently_played') }}