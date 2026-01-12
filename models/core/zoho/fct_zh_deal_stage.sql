with
    deals as (
        select
            deal_id,
            deal_name,
            account_name,
            account_id,
            contact_name,
            contact_id,
            type as deal_type,
            description as deal_description,
            date(closing_date) as deal_closing_date,
            date(created_time) as created_date,
            amount,
            owner_id,
            owner_name,
            owner_email,
            last_activity_time,
            exchange_rate,
            currency,
            review_process_delegate,
            review_process_approve,
            review_process_reject,
            review_process_resubmit,
            approval_delegate,
            approval_approve,
            approval_reject,
            approval_resubmit,
            campaign_source_name,
            expected_revenue,
            approved,
            in_merge,
            editable,
            orchestration,
            followed,
            last_utm_medium,
            tag,
            first_touch_url,
            first_utm_campaign,
            first_utm_term,
            first_utm_source as utm_source,
            first_utm_medium,
            first_utm_content,
            lead_source,
            last_utm_term,
            last_utm_campaign,
            last_touch_url,
            last_utm_content,
            last_utm_source,
            lead_type,
            lead_channel,
            gclid_value,
            gclid,
            referring_url,
            tracking_source,
            fbclid,
            channel_source,
            ad_click_date,
            ad_group_name,
            ad,
            ad_network,
            ad_campaign_name,
            keyword

        from {{ ref('stg_zh_deals') }}
    ),

    stage_history as (
        select deal_id, amount, stage_change_date, close_date, stage_name

        from {{ ref('stg_zh_deal_stage_history') }}
    )

select
    de.deal_id,
    deal_name,
    account_name,
    account_id,
    contact_name,
    contact_id,
    deal_type,
    deal_description,
    coalesce(date(de.deal_closing_date), date(sg.close_date)) as deal_closing_date,
    created_date,
    stage_change_date,
    sg.stage_name,
    coalesce(de.amount, sg.amount) as amount,
    owner_id,
    owner_name,
    owner_email,
    last_activity_time,
    exchange_rate,
    currency,
    review_process_delegate,
    review_process_approve,
    review_process_reject,
    review_process_resubmit,
    approval_delegate,
    approval_approve,
    approval_reject,
    approval_resubmit,
    campaign_source_name,
    expected_revenue,
    approved,
    in_merge,
    editable,
    orchestration,
    followed,
    last_utm_medium,
    tag,
    first_touch_url,
    first_utm_campaign,
    first_utm_term,
    utm_source,
    first_utm_medium,
    first_utm_content,
    lead_source,
    last_utm_term,
    last_utm_campaign,
    last_touch_url,
    last_utm_content,
    last_utm_source,
    lead_type,
    lead_channel,
    gclid_value,
    gclid,
    referring_url,
    tracking_source,
    fbclid,
    channel_source,
    ad_click_date,
    ad_group_name,
    ad,
    ad_network,
    ad_campaign_name,
    keyword,
    case
        -- Google platforms
        when
            lower(lead_source) = 'google'
            or lower(utm_source) like '%google%'
            or lower(utm_source) like '%www.google.%'
            or lower(utm_source) = 'google'
            or lower(lead_source) = 'google adwords'
            or lower(utm_source) = 'youtube'
            or lower(utm_source) = 'www.youtube.com'
            or lower(lead_source) = 'youtube'
        then 'Google'

        -- Meta platforms
        when
            lower(lead_source) = 'meta'
            or lower(utm_source) = 'meta'
            or lower(lead_source) = 'facebook'
            or lower(utm_source) like '%facebook%'
            or lower(utm_source) = 'fb'
            or lower(utm_source) like 'fb-%'
            or lower(lead_source) = 'instagram'
            or lower(utm_source) like '%instagram%'
            or lower(utm_source) = 'ig'
        then 'Meta'

        -- TikTok
        when
            lower(lead_source) = 'tiktok'
            or lower(utm_source) like '%tiktok%'
            or lower(utm_source) = 'tiktok'
        then 'TikTok'

        -- LinkedIn
        when lower(utm_source) like '%linkedin%'
        then 'LinkedIn'

        -- Microsoft/Bing
        when
            lower(lead_source) = 'bing'
            or lower(utm_source) like '%bing%'
            or lower(utm_source) = 'bing'
            or lower(lead_source) = 'microsoft copilot'
            or lower(utm_source) like '%microsoft%'
        then 'Microsoft'

        -- Yahoo
        when lower(lead_source) = 'yahoo' or lower(utm_source) like '%yahoo%'
        then 'Yahoo'

        -- Twitter/X
        when lower(lead_source) = 'twitter' or lower(utm_source) = 't.co'
        then 'Twitter'

        else 'Others'
    end as platform,

    -- Channel calculation
    case
        -- Paid channels
        when lower(lead_channel) = 'paid social'
        then 'Paid Social'
        when lower(lead_channel) = 'paid search'
        then 'Paid Search'

        -- Organic channels
        when lower(lead_channel) = 'organic search'
        then 'Organic Search'
        when lower(lead_channel) = 'organic social'
        then 'Organic Social'

        -- Direct channels
        when lower(lead_channel) = 'direct'
        then 'Direct'
        when lower(lead_channel) = 'in-person'
        then 'In-Person'

        -- Referral/Marketing
        when lower(lead_channel) = 'referral'
        then 'Referral'
        when lower(lead_channel) = 'email marketing'
        then 'Email Marketing'

        -- Fallback logic when lead_channel is empty
        when lead_channel is null or trim(lead_channel) = ''
        then
            case
                -- Paid indicators from utm_source/lead_source
                when lower(lead_source) in ('meta', 'google adwords')
                then 'Paid'
                when
                    lower(utm_source) in ('meta', 'google')
                    and lower(lead_source) in ('meta', 'google')
                then 'Paid'

                -- Organic search engines
                when
                    lower(lead_source)
                    in ('google', 'bing', 'yahoo', 'duckduckgo', 'brave')
                    and lower(utm_source) not in ('meta', 'google')
                then 'Organic Search'
                when
                    lower(utm_source) like '%google%'
                    or lower(utm_source) like '%bing%'
                    or lower(utm_source) like '%yahoo%'
                then 'Organic Search'

                -- Social platforms (assume organic when not specified)
                when
                    lower(utm_source)
                    in ('facebook', 'instagram', 'youtube', 'tiktok', 'ig')
                    or lower(lead_source)
                    in ('facebook', 'instagram', 'youtube', 'tiktok')
                then 'Organic Social'

                -- Referral sites
                when
                    lower(utm_source) like '%.com'
                    or lower(utm_source) like '%.org'
                    or lower(utm_source) like '%.ai'
                then 'Referral'

                -- Direct
                when lower(lead_source) = 'direct' or lower(utm_source) = 'direct'
                then 'Direct'

                else 'Unknown'
            end

        else 'Unknown'
    end as channel

from deals as de
left join stage_history as sg on de.deal_id = sg.deal_id
