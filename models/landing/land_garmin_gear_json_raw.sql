{{ config(materialized='view'
        ,tags=["garmin"])
     }}

with source_ as (
        select parse_json(raw) gear_json 
        from {{ source('kevans_grant_garmin', 'GEAR_JSON_RAW') }}
)

-- Keith Evans today

SELECT
  gear_json:gearPk::INT AS gear_pk,
  gear_json:uuid::STRING AS uuid,
  gear_json:userProfilePk::INT AS user_profile_pk,
  gear_json:gearStatusName::STRING AS gear_status,
  gear_json:customMakeModel::STRING AS make_model,
  gear_json:dateBegin::DATE AS date_begin,
  gear_json:dateEnd::DATE AS date_end,
  gear_json:maximumMeters::FLOAT AS max_meters,
  gear_json:notified::BOOLEAN AS notified,
  gear_json:createDate::DATE AS create_date,
  gear_json:updateDate::DATE AS update_date
from source_
