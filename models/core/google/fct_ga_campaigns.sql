select 
    campaign_id,
    campaign_name,
    date,
    sum(clicks) as clicks,
    status,
    sum(conversions) as conversions,
    sum(spend) as spend,
    sum(impressions) as impressions

    from {{ref('stg_ga_campaigns')}}
    group by 
    campaign_id,
    campaign_name,
    date,
    status