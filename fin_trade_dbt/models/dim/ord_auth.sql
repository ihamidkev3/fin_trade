{{ config(
    schema='dim',
    materialized='incremental',
    incremental_strategy='append',
    unique_key=['status_description'],
    on_schema_change='ignore',
    wrap_in_transaction=False,
    pre_hook= "{{ set_identity_insert(dim.ord_auth, 'ON') }}" -- DBT will automatically turn OFF identity insert after the model runs
) }}

with src_auth as (
    select distinct
        f.status_code,
        case 
            when f.status_code = 0 then 'Cancelled'
            when f.status_code = 1 then 'Completed'
            when f.status_code = 2 then 'Pending'
            else 'Unknown'
        end as status_detail
    from {{ source('staging', 'staging_financial_orders') }} f
    left join {{ this }} t on t.status_description = f.status_code
    where t.status_description is null
),
numbered_records as (
    select 
        ROW_NUMBER() OVER (ORDER BY status_description) + COALESCE((select max(approval_key) from {{ this }}), 0) as approval_key,
        status_code,
        status_detail
    from src_auth
)
select 
    approval_key,
    status_code,
    status_detail
from numbered_records 