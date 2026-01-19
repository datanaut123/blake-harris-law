with
    campaigns as (
        select
            campaign_id,
            campaign_name,
            date,
            sum(clicks) as clicks,
            status,
            sum(conversions) as conversions,
            sum(spend) as spend,
            sum(impressions) as impressions

        from {{ ref('fct_ga_campaigns') }}
        group by campaign_id, campaign_name, date, status

    ),

    actions as (
        select date, campaign_id, campaign_name, calls, forms

        from {{ ref('fct_ga_conversions') }}
    )

select
    camp.campaign_id,
    camp.campaign_name,
    coalesce(camp.date, act.date) as date,
    clicks,
    status,
    conversions,
    spend,
    impressions,
    calls,
    forms

from campaigns as camp
left join actions as act on camp.campaign_id = act.campaign_id and camp.date = act.date
