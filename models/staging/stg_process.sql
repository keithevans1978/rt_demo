{{ 
    config(
        materialized='view',
        post_hook="""
            UPDATE KANSAS_BASKETBALL.PUBLIC.PROCESS_DATE
            SET process_flag = 'Y'
            WHERE process_date = (
                SELECT MIN(process_date)
                FROM KANSAS_BASKETBALL.PUBLIC.PROCESS_DATE
            );
        """
    )
}}

select 1 from {{ ref('snap_game') }} where 1=0
union
select 1 from {{ ref('snap_team') }} where 1=0
union
select 1 from {{ ref('snap_team_del') }} where 1=0