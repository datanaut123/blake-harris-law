select
    safe_cast(campaign_id as int64) as campaign_id,
    campaign_name,
    clicks,
    impressions,
    spend,
    date_start as date,
    c.status,
    0 as conversions

from {{ source("meta_ads", "ads_insights") }} as a
left join {{ source("meta_ads", "campaigns") }} as c on a.campaign_id = c.id
