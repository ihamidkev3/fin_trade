{{ config(
    schema= 'rpt',
    materialized= 'incremental',
    incremental_strategy= 'merge',
    unique_key= ['account_key', 'status_date'],
    on_schema_change='sync_all_columns'
) }}

with daily_status as (
    select 
        cp.account_key,
        cast(o.create_date as date) as status_date,
        count(distinct o.natural_key) as total_transactions,
        count(distinct case when oa.status_code = 1 then o.natural_key end) as completed_transactions,
        count(distinct case when oa.status_code = 2 then o.natural_key end) as pending_transactions,
        count(distinct case when oa.status_code = 0 then o.natural_key end) as rejected_transactions,
        sum(case when oa.status_code = 1 then o.base_amount else 0 end) as completed_volume,
        sum(case when oa.status_code = 2 then o.base_amount else 0 end) as pending_volume,
        sum(case when oa.status_code = 0 then o.base_amount else 0 end) as rejected_volume
    from {{ ref('orders') }} o
    inner join {{ ref('cust_profile') }} cp 
        on o.account_key = cp.account_key
    inner join {{ ref('ord_auth') }} oa
        on o.approval_key = oa.approval_key
    group by 
        cp.account_key,
        cast(o.create_date as date)
)

select 
    account_key,
    status_date,
    total_transactions,
    completed_transactions,
    pending_transactions,
    rejected_transactions,
    completed_volume,
    pending_volume,
    rejected_volume,
    cast(completed_transactions as float) / nullif(total_transactions, 0) as completion_rate,
    cast(rejected_transactions as float) / nullif(total_transactions, 0) as rejection_rate,
    case 
        when total_transactions = 0 then 'Inactive'
        when cast(completed_transactions as float) / total_transactions >= 0.9 then 'High Success'
        when cast(completed_transactions as float) / total_transactions >= 0.7 then 'Moderate Success'
        else 'Needs Review'
    end as account_status
from daily_status

{% if is_incremental() %}
where status_date > (select max(status_date) from {{ this }})
{% endif %} 