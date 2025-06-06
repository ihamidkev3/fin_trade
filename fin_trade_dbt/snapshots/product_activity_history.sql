{% snapshot product_activity_history %}

{{
    config(
      target_schema='snapshots',
      unique_key='account_key',
      strategy='check',
      check_cols=['activity_profile'],
      invalidate_hard_deletes=True,
      snapshot_meta_column_names= {
        'dbt_valid_from': 'activity_valid_from',
        'dbt_valid_to': 'activity_valid_to'
      }
    )
}}

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
from {{ ref('daily_product_activity') }}

{% endsnapshot %} 