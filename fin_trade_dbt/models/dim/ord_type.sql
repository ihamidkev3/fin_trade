{{ config(
    schema='dim',
    materialized='incremental',
    incremental_strategy='append',
    unique_key=['operation_code'],
    on_schema_change='ignore',
    wrap_in_transaction=False,
    pre_hook= "{{ set_identity_insert(dim.ord_type, 'ON') }}" -- DBT will automatically turn OFF identity insert after the model runs
) }}

with src_type as (
    select distinct
        f.order_type as operation_code,
        case 
            when f.order_type = 'MARKET' then 'Market Order'
            when f.order_type = 'LIMIT' then 'Limit Order'
            else 'Unknown'
        end as operation_description
    from {{ ref('stg_orders') }} f
    left join {{ this }} t on t.operation_code = f.order_type
    where t.operation_code is null
),
numbered_records as (
    select 
        ROW_NUMBER() OVER (ORDER BY operation_code) + COALESCE((select max(operation_key) from {{ this }}), 0) as operation_key,
        operation_code,
        operation_description
    from src_type
)
select 
    operation_key,
    operation_code,
    operation_description
from numbered_records 