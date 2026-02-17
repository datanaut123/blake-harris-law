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

    union all 

select 
    campaign_id,
    campaign_name,
    date,
    clicks,
    status,
    conversions,
    spend,
    impressions,
    'Bing' as platform,
    'Paid' as channel

    from {{ref('stg_ba_campaigns')}}