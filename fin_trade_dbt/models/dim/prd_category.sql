{{ config(
    schema='dim',
    materialized='incremental',
    incremental_strategy='append',
    unique_key=['product_type', 'category_level_3', 'category_level_4'],
    on_schema_change='ignore',
    wrap_in_transaction=False,
    pre_hook= "{{ set_identity_insert(dim.prd_category, 'ON') }}" -- DBT will automatically turn OFF identity insert after the model runs
) }}

with src_category as (
    select distinct
        a.base_currency as product_type,
        a.base_currency as category_level_1,
        a.quote_currency as category_level_2,
        a.market_symbol as category_level_3,
        a.time_validity as category_level_4
    from {{ source('staging', 'staging_financial_orders') }} a
    left join {{ this }} b 
    on isnull(LTRIM(RTRIM(b.product_type)),'0') = isnull(LTRIM(RTRIM(a.base_currency)),'0')
    and isnull(LTRIM(RTRIM(b.category_level_3)),'0') = isnull(LTRIM(RTRIM(a.market_symbol)),'0')
    and isnull(LTRIM(RTRIM(b.category_level_4)),'0') = isnull(LTRIM(RTRIM(a.time_validity)),'0')
    where b.category_key is null
),
numbered_records as (
    select 
        ROW_NUMBER() OVER (ORDER BY product_type, category_level_3, category_level_4) + COALESCE((select max(category_key) from {{ this }}), 0) as category_key,
        product_type,
        category_level_1,
        category_level_2,
        category_level_3,
        category_level_4
    from src_category
)
select 
    category_key,
    product_type,
    category_level_1,
    category_level_2,
    category_level_3,
    category_level_4
from numbered_records 