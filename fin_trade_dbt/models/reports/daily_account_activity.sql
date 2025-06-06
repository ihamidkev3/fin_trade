{{ config(
    schema= 'rpt',
    materialized= 'incremental',
    incremental_strategy= 'merge',
    unique_key= ['account_key', 'activity_date'],
    on_schema_change='sync_all_columns'
) }}

-- Calculate daily metrics for each account
with daily_new_accounts as (
    select 
        cp.account_key,
        cast(o.create_date as date) as activity_date,
        min(o.create_date) as first_activity_date,
        max(o.create_date) as last_activity_date,
        count(distinct o.natural_key) as total_events,
        sum(case when os.direction_code = 'BUY' then o.base_amount else 0 end) as total_inbound,
        sum(case when os.direction_code = 'SELL' then o.base_amount else 0 end) as total_outbound,
        sum(o.fee_amount) as total_fees
    from {{ ref('orders') }} o
    inner join {{ ref('cust_profile') }} cp 
        on o.account_key = cp.account_key
    inner join {{ ref('ord_side') }} os
        on o.direction_key = os.direction_key
    inner join {{ ref('ord_auth') }} oa
        on o.approval_key = oa.approval_key
    where oa.status_code = 1
    group by 
        cp.account_key,
        cast(o.create_date as date)
),

-- Calculate account lifecycle metrics including activity day numbering
account_metrics as (
    select 
        account_key,
        activity_date,
        first_activity_date,
        last_activity_date,
        total_events,
        total_inbound,
        total_outbound,
        total_fees,
        total_inbound - total_outbound as net_position,
        row_number() over (partition by account_key order by activity_date) as activity_day_number
    from daily_new_accounts
)

-- Final output with account stage classification
select 
    account_key,
    activity_date,
    first_activity_date,
    last_activity_date,
    total_events,
    total_inbound,
    total_outbound,
    total_fees,
    net_position,
    activity_day_number,
    case 
        when activity_day_number = 1 then 'New Account'
        when activity_day_number <= 7 then 'First Week'
        when activity_day_number <= 30 then 'First Month'
        else 'Established'
    end as account_stage
from account_metrics

{% if is_incremental() %}
where activity_date > (select max(activity_date) from {{ this }})
{% endif %} 