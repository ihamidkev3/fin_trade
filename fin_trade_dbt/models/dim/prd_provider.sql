{{ config(
    schema='dim',
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['provider_code'],
    on_schema_change='ignore',
    wrap_in_transaction=False,
    pre_hook= "{{ set_identity_insert(dim.prd_provider, 'ON') }}" -- DBT will automatically turn OFF identity insert after the model runs
) }}

with src_provider as (
    select 
        s.provider_code,
        s.provider_name,
        s.is_active,
        s.insert_timestamp
    from {{ ref('stg_orders') }} s
    left join {{ this }} t on t.provider_code = s.provider_code
    where t.provider_code is null
),
numbered_records as (
    select 
        ROW_NUMBER() OVER (ORDER BY provider_code) + COALESCE((select max(provider_key) from {{ this }}), 0) as provider_key,
        provider_code,
        provider_name,
        is_active,
        insert_timestamp
    from src_provider
)
select 
    provider_key,
    provider_code,
    provider_name,
    is_active,
    insert_timestamp
from numbered_records 