{{ config(
    schema= 'rpt',
    materialized= 'incremental',
    incremental_strategy= 'merge',
    unique_key= ['metric_date', 'product_type'],
    on_schema_change='sync_all_columns'
) }}

with daily_metrics as (
    select 
        cast(o.create_date as date) as order_date,
        pc.product_type,
        count(distinct o.account_key) as active_accounts,
        count(distinct o.natural_key) as total_transactions,
        sum(o.base_amount) as total_volume,
        sum(o.fee_amount) as total_fees,
        avg(o.base_amount) as avg_transaction_size,
        count(distinct case when os.direction_code = 'BUY' then o.natural_key end) as buy_transactions,
        count(distinct case when os.direction_code = 'SELL' then o.natural_key end) as sell_transactions,
        sum(case when os.direction_code = 'BUY' then o.base_amount else 0 end) as buy_volume,
        sum(case when os.direction_code = 'SELL' then o.base_amount else 0 end) as sell_volume
    from {{ ref('orders') }} o
    inner join {{ ref('prd_category') }} pc 
        on o.category_key = pc.category_key
    inner join {{ ref('ord_side') }} os
        on o.direction_key = os.direction_key
    inner join {{ ref('ord_auth') }} oa
        on o.approval_key = oa.approval_key
    where oa.status_description = 'COMPLETED'
    group by 
        cast(o.create_date as date),
        pc.product_type
)

select 
    order_date,
    product_type,
    active_accounts,
    total_transactions,
    total_volume,
    total_fees,
    avg_transaction_size,
    buy_transactions,
    sell_transactions,
    buy_volume,
    sell_volume,
    buy_volume - sell_volume as net_flow,
    case 
        when buy_volume > sell_volume then 'Net Inflow'
        when sell_volume > buy_volume then 'Net Outflow'
        else 'Balanced'
    end as flow_status,
    cast(buy_transactions as float) / nullif(total_transactions, 0) as buy_ratio,
    cast(sell_transactions as float) / nullif(total_transactions, 0) as sell_ratio
from daily_metrics

{% if is_incremental() %}
where metric_date > (select max(metric_date) from {{ this }})
{% endif %} 