with
    stage_history as (
        select
            id,
            deal_id,
            probability,
            expected_revenue,
            amount,
            date(last_modified_time) as stage_change_date,
            close_date,
            modified_by_id,
            modified_by_name,
            modified_by_email,
            stage_id,
            duration_days,
            row_number() over (
                partition by deal_id, stage_id order by last_modified_time desc
            ) as rn

        from {{ source('zoho', 'deal_stage_history') }}
        qualify rn = 1
    ),

    stages as (
        select distinct id as stage_id, name as stage_name

        from {{ source('zoho', 'stage') }}
    )

select
    deal_id,
    probability,
    expected_revenue,
    amount,
    stage_change_date,
    close_date,
    modified_by_id,
    modified_by_name,
    modified_by_email,
    duration_days,
    stage_name
from stage_history
left join stages on stage_history.stage_id = stages.stage_id
