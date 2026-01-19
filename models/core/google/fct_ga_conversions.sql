WITH conv_camp AS (
  SELECT
    campaign_id,
    campaign_name,
    date,
    metrics_conversions,
    metrics_conversions_value,
    safe_cast(conversion_action_id AS string) AS conversion_action_id

  FROM {{ ref("stg_ga_conversion_campaign") }}
),

conv_action AS (

  SELECT
    conversion_action_name,
    conversion_action_type,
    conversion_action_category,
    safe_cast(conversion_action_id AS string) AS conversion_action_id

  FROM {{ ref("stg_ga_conversion_action") }}

),

data_join AS (
  SELECT
    a.campaign_id,
    a.campaign_name,
    a.date,
    a.metrics_conversions,
    a.metrics_conversions_value,
    conversion_action_name,
    conversion_action_type,
    conversion_action_category,
    coalesce(a.conversion_action_id, b.conversion_action_id)
      AS conversion_action_id


  FROM conv_camp AS a
  LEFT JOIN conv_action AS b ON a.conversion_action_id = b.conversion_action_id
),

call_requests AS (
  SELECT
    date,
    campaign_id,
    campaign_name,
    sum(metrics_conversions) AS calls

  FROM data_join
WHERE
  conversion_action_name IN (
    'Call Tracking Lead'
  )
  GROUP BY
    date,
    campaign_id,
    campaign_name

),

form_sub AS (
  SELECT
    date,
    campaign_id,
    campaign_name,
    sum(metrics_conversions) AS forms

  FROM data_join
  WHERE
  conversion_action_name IN (
    'Contact Form Submission - GTM'
  )
  GROUP BY
    date,
    campaign_id,
    campaign_name
),

campaigns AS (
  SELECT DISTINCT
    campaign_id,
    campaign_name,
    date

  FROM data_join
)

SELECT
  camp.date,
  camp.campaign_id,
  camp.campaign_name,
  calls,
  forms


FROM campaigns AS camp
LEFT JOIN
  call_requests AS cr
  ON (camp.campaign_id = cr.campaign_id AND camp.date = cr.date)
LEFT JOIN
  form_sub AS fs
  ON (camp.campaign_id = fs.campaign_id AND camp.date = fs.date)