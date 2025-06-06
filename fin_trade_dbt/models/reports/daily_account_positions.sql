{{ config(
    schema= 'rpt',
    materialized= 'incremental',
    incremental_strategy= 'merge',
    unique_key= ['account_key', 'balance_date'],
    on_schema_change='sync_all_columns'
) }}

with daily_balances as (
    select 
        cp.account_key,
        cast(o.create_date as date) as balance_date,
        sum(case when os.direction_code = 'BUY' then o.base_amount else 0 end) as daily_inbound,
        sum(case when os.direction_code = 'SELL' then o.base_amount else 0 end) as daily_outbound,
        sum(o.fee_amount) as daily_fees,
        count(distinct o.natural_key) as daily_transactions
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

running_totals as (
    select 
        account_key,
        balance_date,
        daily_inbound,
        daily_outbound,
        daily_fees,
        daily_transactions,
        sum(daily_inbound) over (partition by account_key order by balance_date) as total_inbound,
        sum(daily_outbound) over (partition by account_key order by balance_date) as total_outbound,
        sum(daily_fees) over (partition by account_key order by balance_date) as total_fees,
        sum(daily_transactions) over (partition by account_key order by balance_date) as total_transactions
    from daily_balances
)

select 
    account_key,
    balance_date,
    daily_inbound,
    daily_outbound,
    daily_fees,
    daily_transactions,
    total_inbound,
    total_outbound,
    total_fees,
    total_transactions,
    total_inbound - total_outbound as current_balance,
    case 
        when total_inbound - total_outbound > 0 then 'Positive'
        when total_inbound - total_outbound < 0 then 'Negative'
        else 'Zero'
    end as balance_status
from running_totals

{% if is_incremental() %}
where balance_date > (select max(balance_date) from {{ this }})
{% endif %} 