with
    deals as (
        select
            deal_id,
            created_date,
            stage_change_date,
            stage_name,
            first_touch_url,
            utm_campaign,
            utm_term,
            utm_source,
            utm_medium,
            utm_content,
            lead_source,
            platform,
            channel,
            gclid_value,
            fbclid,
            closed_won_amount,
            opportunity_amount,
            is_14_day_waiting_period,
            is_24_hour_waiting_period,
            is_30_day_waiting_period_1,
            is_30_day_waiting_period_2,
            is_48_hour_waiting_period,
            is_60_day_follow_up,
            is_7_day_waiting_period,
            is_agreement_pending,
            is_closed_lost,
            is_closed_lost_to_competition,
            is_closed_won,
            is_consult_scheduled,
            is_contacted,
            is_contract_out,
            is_follow_up_required,
            is_id_decision_makers,
            is_needs_analysis,
            is_negotiation_review,
            is_new_lead,
            is_proposal_price_quote,
            is_qualification,
            is_qualified_lead,
            is_value_proposition,
            is_contacted_scheduled,
            is_junk_leads,
            is_deal,
            is_lead,
            total_leads

        from {{ ref('mrt_leads_deals_unfreeze') }}
    ),

    ads as (    
        select
            campaign_id,
            campaign_name,
            date,
            platform,
            channel,
            sum(clicks) as clicks,
            sum(spend) as spend,
            sum(impressions) as impressions,

        from {{ ref('mrt_ads') }}
        group by campaign_id, campaign_name, date, platform, channel
    ),

    data_join as (
        select
            deal_id,
            created_date,
            coalesce(stage_change_date, date) as date,
            stage_name,
            first_touch_url,
            coalesce(de.utm_campaign, ad.campaign_name) as utm_campaign,
            utm_term,
            utm_source,
            utm_medium,
            utm_content,
            lead_source,
            coalesce(de.platform, ad.platform) as platform,
            coalesce(de.channel, ad.channel) as channel,
            gclid_value,
            fbclid,
            ad.campaign_id as a_campaign_id,
            ad.date as a_date,
            clicks,
            impressions,
            spend,
            closed_won_amount,
            opportunity_amount,
            is_14_day_waiting_period,
            is_24_hour_waiting_period,
            is_30_day_waiting_period_1,
            is_30_day_waiting_period_2,
            is_48_hour_waiting_period,
            is_60_day_follow_up,
            is_7_day_waiting_period,
            is_agreement_pending,
            is_closed_lost,
            is_closed_lost_to_competition,
            is_closed_won,
            is_consult_scheduled,
            is_contacted,
            is_contract_out,
            is_follow_up_required,
            is_id_decision_makers,
            is_needs_analysis,
            is_negotiation_review,
            is_new_lead,
            is_proposal_price_quote,
            is_qualification,
            is_qualified_lead,
            is_value_proposition,
            is_contacted_scheduled,
            is_junk_leads,
            is_deal,
            is_lead,
            total_leads

        from deals as de
        full join
            ads as ad
            on de.stage_change_date = ad.date
            and de.utm_campaign = ad.campaign_name
            and de.channel = ad.channel
            and de.platform = ad.platform
    ),

    deduplication as (
        select
            *,
            row_number() over (
                partition by a_campaign_id, a_date order by a_date desc
            ) as rn

        from data_join

    )

select
    * except (rn, a_campaign_id, a_date, spend, impressions, clicks),
    case when rn = 1 then spend else 0 end as spend,
    case when rn = 1 then impressions else 0 end as impressions,
    case when rn = 1 then clicks else 0 end as clicks

from deduplication



