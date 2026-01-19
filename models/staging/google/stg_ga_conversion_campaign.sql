WITH conv_campaign AS (
  SELECT * FROM {{ source("google_ads", "conversion_campaign") }}
)

select 
campaign_id,
campaign_name,
segments_date as date,
metrics_conversions,
metrics_conversions_value,
REGEXP_EXTRACT(segments_conversion_action, r'conversionActions/(\d+)$') AS conversion_action_id

from conv_campaign