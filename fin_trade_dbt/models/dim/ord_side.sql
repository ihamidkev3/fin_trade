{{ config(
    schema='dim',
    materialized='incremental',
    incremental_strategy='append',
    unique_key=['direction_code'],
    on_schema_change='ignore',
    wrap_in_transaction=False,
    pre_hook= "{{ set_identity_insert(dim.ord_side, 'ON') }}" -- DBT will automatically turn OFF identity insert after the model runs
) }}

with src_side as (
    select distinct
        f.order_direction as direction_code,
        case 
            when f.order_direction = 'BUY' then 'Buy Order'
            when f.order_direction = 'SELL' then 'Sell Order'
            else 'Unknown'
        end as direction_description
    from {{ source('staging', 'staging_financial_orders') }} f
    left join {{ this }} t on t.direction_code = f.order_direction
    where t.direction_code is null
),
numbered_records as (
    select 
        ROW_NUMBER() OVER (ORDER BY direction_code) + COALESCE((select max(direction_key) from {{ this }}), 0) as direction_key,
        direction_code,
        direction_description
    from src_side
)
select 
    direction_key,
    direction_code,
    direction_description
from numbered_records 