{{ config(
    schema= 'rpt',
    materialized= 'incremental',
    incremental_strategy= 'merge',
    unique_key= ['account_key', 'detail_date'],
    on_schema_change='sync_all_columns'
) }}

with daily_details as (
    select 
        cp.account_key,
        cast(o.create_date as date) as detail_date,
        pc.product_type,
        os.direction_code,
        count(distinct o.natural_key) as transaction_count,
        sum(o.base_amount) as total_amount,
        sum(o.fee_amount) as total_fees,
        avg(o.base_amount) as avg_transaction_size
    from {{ ref('orders') }} o
    inner join {{ ref('cust_profile') }} cp 
        on o.account_key = cp.account_key
    inner join {{ ref('prd_category') }} pc 
        on o.category_key = pc.category_key
    inner join {{ ref('ord_side') }} os
        on o.direction_key = os.direction_key
    inner join {{ ref('ord_auth') }} oa
        on o.approval_key = oa.approval_key
    where oa.status_code = 1
    group by 
        cp.account_key,
        cast(o.create_date as date),
        pc.product_type,
        os.direction_code
),

account_metrics as (
    select 
        account_key,
        detail_date,
        count(distinct product_type) as unique_products,
        count(distinct direction_code) as unique_directions,
        sum(transaction_count) as total_transactions,
        sum(total_amount) as total_volume,
        sum(total_fees) as total_fees,
        avg(avg_transaction_size) as avg_transaction_size
    from daily_details
    group by 
        account_key,
        detail_date
)

select 
    account_key,
    detail_date,
    unique_products,
    unique_directions,
    total_transactions,
    total_volume,
    total_fees,
    avg_transaction_size,
    case 
        when total_transactions >= 10 and unique_products > 1 then 'Diversified Active'
        when total_transactions >= 10 and unique_products = 1 then 'Single Product Active'
        when total_transactions >= 5 then 'Moderate Activity'
        else 'Low Activity'
    end as activity_profile
from account_metrics

{% if is_incremental() %}
where detail_date > (select max(detail_date) from {{ this }})
{% endif %} 