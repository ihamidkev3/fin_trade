with source as (
    select * from {{ source('financial_data', 'staging_financial_orders') }}
),

orders as (
    select
        -- Primary key
        record_id as financial_event_id,
        
        -- Timestamps
        creation_timestamp as created_at,
        modification_timestamp as modified_at,
        deletion_timestamp as deleted_at,
        
        -- User information
        user_identifier,
        user_email,
        
        -- Order status
        status_code,
        
        -- Currency information
        quote_currency,
        base_currency,
        market_symbol as trading_pair,
        
        -- Amount information
        quote_amount,
        base_amount,
        execution_price,
        executed_amount,
        total_quote_executed,
        
        -- Order details
        order_direction,
        order_type,
        time_validity,
        
        -- Fee information
        fee_amount,
        provider_fee,
        
        -- Provider details
        provider_quoted_price,
        liquidity_provider,
        provider_response,
        
        -- Metadata
        _dbt_loaded_at as dbt_loaded_at,
        _dbt_source_relation as dbt_source_relation

    from source
)

select * from orders 