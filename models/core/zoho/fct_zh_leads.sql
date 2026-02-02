with
    leads_data as (
        select
            lead_id,
            deal_id,
            created_date,
            modified_date,
            entry_date,
            lead_status,
            first_touch_url,
            coalesce(ad_campaign_name, first_utm_campaign) as utm_campaign,
            coalesce(keyword, first_utm_term) as utm_term,
            first_utm_source,
            first_utm_medium as utm_medium,
            first_utm_content as utm_content,
            lead_source,
            coalesce(gclid_value, gclid) as gclid_value,
            fbclid,
            concat(first_name, ' ',last_name) as full_name,
            email,
            case
                -- Google platforms
                when
                    lower(lead_source) = 'google'
                    or lower(first_utm_source) like '%google%'
                    or lower(first_utm_source) like '%www.google.%'
                    or lower(first_utm_source) = 'google'
                    or lower(lead_source) = 'google adwords'
                    or lower(first_utm_source) = 'youtube'
                    or lower(first_utm_source) = 'www.youtube.com'
                    or lower(lead_source) = 'youtube'
                    or coalesce(gclid_value, gclid) is not null
                then 'Google'

                -- Meta platforms
                when
                    lower(lead_source) = 'meta'
                    or lower(first_utm_source) = 'meta'
                    or lower(lead_source) = 'facebook'
                    or lower(first_utm_source) like '%facebook%'
                    or lower(first_utm_source) = 'fb'
                    or lower(first_utm_source) like 'fb-%'
                    or lower(lead_source) = 'instagram'
                    or lower(first_utm_source) like '%instagram%'
                    or lower(first_utm_source) = 'ig'
                    or fbclid is not null
                then 'Meta'

                -- TikTok
                when
                    lower(lead_source) = 'tiktok'
                    or lower(first_utm_source) like '%tiktok%'
                    or lower(first_utm_source) = 'tiktok'
                then 'TikTok'

                -- LinkedIn
                when
                    lower(first_utm_source) like '%linkedin%'
                    or lower(lead_source) like '%linkedin%'
                then 'LinkedIn'

                -- Microsoft/Bing
                when
                    lower(lead_source) = 'bing'
                    or lower(first_utm_source) like '%bing%'
                    or lower(first_utm_source) = 'bing'
                    or lower(lead_source) = 'microsoft copilot'
                    or lower(first_utm_source) like '%microsoft%'
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

                when gclid_value is not null or fbclid is not null or gclid is not null
                then 'Paid'

                -- Fallback logic when lead_channel is empty or other values
                when
                    lead_channel is null
                    or trim(lead_channel) = ''
                    or lower(lead_channel) not in (
                        'paid social', 'paid search', 'organic search', 'organic social'
                    )
                then
                    case
                        -- Paid indicators from utm_source/lead_source
                        when lower(lead_source) in ('meta', 'google adwords')
                        then 'Paid'
                        when
                            lower(first_utm_source) in ('meta', 'google')
                            and lower(lead_source) in ('meta', 'google')
                        then 'Paid'

                        -- Organic search engines
                        when
                            lower(lead_source)
                            in ('google', 'bing', 'yahoo', 'duckduckgo', 'brave')
                            and lower(first_utm_source) not in ('meta', 'google')
                        then 'Organic'
                        when
                            lower(first_utm_source) like '%google%'
                            or lower(first_utm_source) like '%bing%'
                            or lower(first_utm_source) like '%yahoo%'
                        then 'Organic'

                        -- Social platforms (assume organic when not specified)
                        when
                            lower(first_utm_source) in (
                                'facebook',
                                'instagram',
                                'youtube',
                                'tiktok',
                                'ig',
                                'linkedin'
                            )
                            or lower(lead_source) in (
                                'facebook', 'instagram', 'youtube', 'tiktok', 'linkedin'
                            )
                        then 'Organic'

                        else 'Others'
                    end

                else 'Others'
            end as channel

        from {{ ref('stg_zh_leads') }}

    )

select
    lead_id,
    full_name,
    email,
    entry_date,
    created_date,
    modified_date,
    lead_status,
    first_touch_url,
    utm_campaign,
    utm_term,
    first_utm_source as utm_source,
    utm_medium,
    utm_content,
    lead_source,
    gclid_value,
    fbclid,
    platform,
    channel,
    1 as is_contacted_scheduled,
    0 as is_junk_leads,
    0 as total_leads,
    0 as active_leads

from leads_data
where lead_status = 'Contacted/Scheduled'

union all

select
    lead_id,
    full_name,
    email,
    entry_date,
    created_date,
    modified_date,
    lead_status,
    first_touch_url,
    utm_campaign,
    utm_term,
    first_utm_source as utm_source,
    utm_medium,
    utm_content,
    lead_source,
    gclid_value,
    fbclid,
    platform,
    channel,
    0 as is_contacted_scheduled,
    1 as is_junk_leads,
    0 as total_leads,
    0 as active_leads

from leads_data
where lead_status = 'Junk Lead'

union all

select distinct
    lead_id,
    full_name,
    email,
    entry_date,
    created_date,
    modified_date,
    lead_status,
    first_touch_url,
    utm_campaign,
    utm_term,
    first_utm_source as utm_source,
    utm_medium,
    utm_content,
    lead_source,
    gclid_value,
    fbclid,
    platform,
    channel,
    0 as is_contacted_scheduled,
    0 as is_junk_leads,
    1 as total_leads,
    0 as active_leads,

from leads_data

union all

select distinct
    lead_id,
    full_name,
    email,
    entry_date,
    created_date,
    modified_date,
    lead_status,
    first_touch_url,
    utm_campaign,
    utm_term,
    first_utm_source as utm_source,
    utm_medium,
    utm_content,
    lead_source,
    gclid_value,
    fbclid,
    platform,
    channel,
    0 as is_contacted_scheduled,
    0 as is_junk_leads,
    0 as total_leads,
    1 as active_leads
from leads_data
where deal_id is null
and lead_status <> 'Junk Lead' 
and lead_status <> 'Lost Lead'