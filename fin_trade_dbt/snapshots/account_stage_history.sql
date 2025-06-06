{% snapshot account_stage_history %}

{{
    config(
      target_schema='snapshots',
      unique_key='account_key',
      strategy='check',
      check_cols=['account_stage'],
      invalidate_hard_deletes=True,
      snapshot_meta_column_names= {
        'dbt_valid_from': 'stage_valid_from',
        'dbt_valid_to': 'stage_valid_to'
      }
    )
}}

select 
    account_key,
    activity_date,
    first_activity_date,
    activity_day_number,
    case 
        when activity_day_number = 1 then 'New Account'
        when activity_day_number <= 7 then 'First Week'
        when activity_day_number <= 30 then 'First Month'
        else 'Established'
    end as account_stage
from {{ ref('daily_account_activity') }}

{% endsnapshot %} 