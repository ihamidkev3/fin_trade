{{ config(
    schema='dim',
    materialized='incremental',
    incremental_strategy='append',
    unique_key=['user_identifier'],
    on_schema_change='ignore',
    wrap_in_transaction=False,
    pre_hook= "{{ set_identity_insert(dim.cust_profile, 'ON') }}" -- DBT will automatically turn OFF identity insert after the model runs
) }}

with src_profile as (
    select distinct
        f.user_identifier,
        f.user_email
    from {{ source('staging', 'staging_financial_orders') }} f
    left join {{ this }} t on t.user_identifier = f.user_identifier
    where t.account_key is null
),
numbered_records as (
    select 
        ROW_NUMBER() OVER (ORDER BY user_identifier) + COALESCE((select max(account_key) from {{ this }}), 0) as account_key,
        user_identifier,
        user_email
    from src_profile
)
select 
    account_key,
    user_identifier,
    user_email
from numbered_records 