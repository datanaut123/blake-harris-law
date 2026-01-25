with deals_data as (select
    id as deal_id,
    owner_id,
    owner_name,
    owner_email,
    last_activity_time,
    exchange_rate,
    currency,
    date(created_time) as created_date,
    date(modified_time) as stage_change_date,
    created_by_id,
    created_by_name,
    created_by_email,
    modified_by_id,
    modified_by_name,
    modified_by_email,
    review_process_delegate,
    review_process_approve,
    review_process_reject,
    review_process_resubmit,
    approval_delegate,
    approval_approve,
    approval_reject,
    approval_resubmit,
    deal_name,
    amount,
    stage as stage_name,
    probability,
    closing_date,
    type,
    description,
    campaign_source_name,
    campaign_source_id,
    lead_conversion_time,
    expected_revenue,
    overall_sales_duration,
    next_step,
    sales_cycle_duration,
    account_name,
    account_id,
    contact_name,
    contact_id,
    _fivetran_deleted,
    approved,
    approval_state,
    currency_symbol,
    in_merge,
    process_flow,
    editable,
    orchestration,
    followed,
    _fivetran_synced,
    custom_last_utm_medium as last_utm_medium,
    custom_conversion_export_status as conversion_export_status,
    custom_cost_per_click as cost_per_click,
    custom_tag as tag,
    custom_first_touch_url as first_touch_url,
    custom_first_utm_term as first_utm_term,
    custom_lead_source as lead_source,
    custom_locked_for_me as locked_for_me,
    custom_last_utm_term as last_utm_term,
    custom_first_utm_source as utm_source,
    custom_layout_id as layout_id,
    custom_first_utm_medium as first_utm_medium,
    custom_first_utm_content as first_utm_content,
    custom_reason_for_loss_s as reason_for_loss_s,
    custom_lead_type as lead_type,
    custom_cost_per_conversion as cost_per_conversion,
    custom_lead_channel as lead_channel,
    custom_first_utm_campaign as first_utm_campaign,
    custom_locked_s as locked_s,
    custom_gclid_value as gclid_value,
    custom_last_utm_campaign as last_utm_campaign,
    custom_last_touch_url as last_touch_url,
    custom_last_utm_content as last_utm_content,
    custom_gclid as gclid,
    custom_entry_date as entry_date,
    custom_last_utm_source as last_utm_source,
    custom_referring_url as referring_url,
    custom_contact_email as contact_email,
    custom_tracking_source as tracking_source,
    custom_contact_state as contact_state,
    custom_contact_city as contact_city,
    custom_postal_zip_code as postal_zip_code,
    custom_phone_1 as phone_1,
    custom_fbclid as fbclid,
    custom_ctm_first_name as ctm_first_name,
    custom_ctm_last_name as ctm_last_name,
    custom_zia_owner_assignment as zia_owner_assignment,
    custom_channel_source as channel_source,
    custom_awareness_source as awareness_source,
    custom_next_contact_formula as next_contact_formula,
    custom_last_contact_success as last_contact_success,
    custom_last_contact_attempt as last_contact_attempt,
    custom_last_contact_notes as last_contact_notes,
    custom_last_contact_result as last_contact_result,
    custom_ad_click_date as ad_click_date,
    custom_ad_group_name as ad_group_name,
    custom_ad as ad,
    custom_conversion_exported_on as conversion_exported_on,
    custom_ad_network as ad_network,
    custom_ad_campaign_name as ad_campaign_name,
    custom_click_type as click_type,
    custom_device_type as device_type,
    custom_keyword as keyword,
    custom_reason_for_conversion_failure as reason_for_conversion_failure
from {{ source("zoho", "deal") }}
)

select *,
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
            or gclid_value is not null
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
            or fbclid is not null
        then 'Meta'

        -- TikTok
        when
            lower(lead_source) = 'tiktok'
            or lower(utm_source) like '%tiktok%'
            or lower(utm_source) = 'tiktok'
        then 'TikTok'

        -- LinkedIn
        when lower(utm_source) like '%linkedin%' or lower(lead_source) like '%linkedin%'
        then 'LinkedIn'

        -- Microsoft/Bing
        when
            lower(lead_source) = 'bing'
            or lower(utm_source) like '%bing%'
            or lower(utm_source) = 'bing'
            or lower(lead_source) = 'microsoft copilot'
            or lower(utm_source) like '%microsoft%'
        then 'Microsoft'

        else 'Others'
    end as platform,

    -- Channel calculation (Organic or Paid only)
    case
        -- Paid channels
        when lower(lead_channel) in ('paid social', 'paid search')
        then 'Paid'

        -- Organic channels
        when lower(lead_channel) in ('organic search', 'organic social')
        then 'Organic'

        -- Fallback logic when lead_channel is empty or other values
        when
            lead_channel is null
            or trim(lead_channel) = ''
            or lower(lead_channel)
            not in ('paid social', 'paid search', 'organic search', 'organic social')
        then
            case
                -- Paid indicators from utm_source/lead_source
                when lower(lead_source) in ('meta', 'google adwords')
                then 'Paid'
                when gclid_value is not null or fbclid is not null
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
                then 'Organic'
                when
                    lower(utm_source) like '%google%'
                    or lower(utm_source) like '%bing%'
                    or lower(utm_source) like '%yahoo%'
                then 'Organic'

                -- Social platforms (assume organic when not specified)
                when
                    lower(utm_source)
                    in ('facebook', 'instagram', 'youtube', 'tiktok', 'ig', 'linkedin')
                    or lower(lead_source)
                    in ('facebook', 'instagram', 'youtube', 'tiktok', 'linkedin')
                then 'Organic'

                else 'Others'
            end

        else 'Others'
    end as channel

from deals_data