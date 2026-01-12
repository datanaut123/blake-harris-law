select 
    campaign_id,
    campaign_name,
    date,
    clicks,
    status,
    conversions,
    spend,
    impressions,
    'Google' as platform,
    'Paid' as channel

    from {{ref('fct_ga_campaigns')}}