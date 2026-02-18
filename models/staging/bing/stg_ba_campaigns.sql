select
    spend as spend,
    clicks as clicks,
    accountid as account_id,
    campaignid as campaign_id,
    conversions as conversions,
    impressions as impressions,
    campaignname as campaign_name,
    campaigntype as campaign_type,
    currencycode as currency_code,
    allconversions as all_conversions,
    campaignlabels as campaign_labels,
    campaignstatus as campaign_status,
    timeperiod as date,
    CampaignStatus as status

from {{ source("bing_ads", "campaign_performance_report_daily") }}
where accountid = 187180504
