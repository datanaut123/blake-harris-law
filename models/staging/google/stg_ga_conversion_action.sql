WITH actions AS (
  SELECT * FROM {{ source("google_ads", "conversion_action") }}
)

select distinct 
conversion_action_id,
conversion_action_name,
conversion_action_type,
conversion_action_category

from actions