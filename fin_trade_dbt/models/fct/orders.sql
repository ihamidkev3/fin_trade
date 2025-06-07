{{ config(
    schema='fct',
    materialized= 'incremental',
    incremental_strategy= 'append',
    unique_key=['natural_key'],
    on_schema_change='ignore',
    wrap_in_transaction=False,
    pre_hook= "{{ set_identity_insert(fct.orders, 'ON') }}" -- DBT will automatically turn OFF identity insert after the model runs
) }}

with src_ord as (
    SELECT  
        f.[record_id] as natural_key,
        f.[creation_timestamp] as create_date,
        f.[modification_timestamp] as update_date,
        cp.[account_key],
        oa.[approval_key],
        pc.category_key,
        f.[quote_amount],
        f.[base_amount],
        f.[execution_price],
        os.direction_key,
        ot.operation_key,
        f.[executed_amount],
        f.[total_quote_executed] as gross_amount,
        f.[fee_amount],
        pp.provider_key,
        f.[provider_quoted_price],
        f.[provider_fee],
        case when category_key=19 and [direction_key]=1 then  f.[base_amount] - (f.[fee_amount]/f.[execution_price])
            when category_key=19 and [direction_key]=2 then -f.[base_amount]
            when category_key=18 and [direction_key]=1 then  f.[base_amount]
            when category_key=18 and [direction_key]=2 then -f.[base_amount]
        end as net_position_change,
        f.[run_key_ref]
    FROM {{ ref('stg_orders') }} f
    inner join {{ ref('ord_auth')}} as oa on isnull(oa.[status_description],'0') = isnull(f.status_code,'0')
    inner join {{ ref('prd_category')}} as pc on isnull(pc.category_level_3,'0') = isnull(f.market_symbol,'0') 
        and isnull(pc.product_type,'0') = isnull(f.base_currency,'0')
    inner join {{ ref('ord_side')}} as os on isnull(os.direction_code,'0') = isnull(f.order_direction,'0')
    inner join {{ ref('ord_type')}} as ot on isnull(ot.operation_code,'0') = isnull(f.order_type,'0')
    inner join {{ ref('cust_profile')}} as cp on isnull(cp.[user_identifier],'0') = isnull(f.user_identifier,'0')
    inner join {{ ref('prd_provider')}} as pp on isnull(pp.provider_code,'0') = isnull(f.liquidity_provider,'0')
    left join {{ this }} t on t.natural_key = f.record_id
    where t.natural_key is null
),
numbered_records as (
    select 
        ROW_NUMBER() OVER (ORDER BY create_date) + COALESCE((select max(id) from {{ this }}), 0) as id,
        natural_key,
        create_date,
        update_date,
        account_key,
        approval_key,
        category_key,
        quote_amount,
        base_amount,
        execution_price,
        direction_key,
        operation_key,
        executed_amount,
        gross_amount,
        fee_amount,
        provider_key,
        provider_quoted_price,
        provider_fee,
        net_position_change,
        run_key_ref
    from src_ord
)
select 
    id,
    natural_key,
    create_date,
    update_date,
    account_key,
    approval_key,
    category_key,
    quote_amount,
    base_amount,
    NULL AS current_balance,
    execution_price,
    direction_key,
    operation_key,
    executed_amount,
    gross_amount,
    fee_amount,
    provider_key,
    provider_quoted_price,
    provider_fee,
    net_position_change,
    run_key_ref
from numbered_records
{% if is_incremental() %}
  where create_date > (select coalesce(max(create_date), '1900-01-01') from {{ this }})
{% endif %} 