# Financial Trading Data Warehouse

## Overview
This dbt project implements a data warehouse for financial trading data, focusing on customer activity tracking, product performance, and transaction analysis. The project follows a dimensional modeling approach with fact tables, dimension tables, and derived reporting models, along with SCD Type 2 snapshots for tracking important metric changes over time.

## Project Structure

```
fin_trade/
├── models/
│   ├── dim/           # Dimension tables
│   ├── fct/           # Fact tables
│   └── reports/       # Reporting models
├── snapshots/         # SCD Type 2 tracking
└── docs/             # Documentation
```

## Data Flow

### 1. Source Data
- Source system: `staging_financial_orders`
- Contains raw trading data including:
  - User information
  - Transaction details
  - Product information
  - Trading parameters

### 2. Dimension Tables
Located in `models/dim/`:

#### Customer Profile (`cust_profile`)
```sql
select distinct
    user_identifier,
    user_email
from staging_financial_orders
```

#### Product Category (`prd_category`)
```sql
select distinct
    base_currency as product_type,
    quote_currency,
    market_symbol,
    time_validity
from staging_financial_orders
```

#### Other Dimensions
- `ord_auth`: Order status (COMPLETED, PENDING, CANCELLED, UNKNOWN)
- `ord_side`: Trading direction (BUY, SELL)
- `ord_type`: Order types (MARKET, LIMIT)
- `prd_provider`: Liquidity providers

### 3. Fact Table
Located in `models/fct/`:

#### Orders
Core fact table containing all trading activity:
```sql
select
    record_id as natural_key,
    creation_timestamp as create_date,
    user_identifier,
    base_amount,
    quote_amount,
    fee_amount,
    -- Foreign keys to dimensions
    account_key,
    category_key,
    direction_key,
    approval_key
from staging_financial_orders
```

### 4. Report Models
Located in `models/reports/`:

#### Daily Account Activity
Tracks customer lifecycle stages:
```sql
select
    account_key,
    activity_date,
    case 
        when activity_day_number = 1 then 'New Account'
        when activity_day_number <= 7 then 'First Week'
        when activity_day_number <= 30 then 'First Month'
        else 'Established'
    end as account_stage
from daily_new_accounts
```

#### Daily Product Activity
Monitors trading behavior patterns:
```sql
select
    account_key,
    detail_date,
    case 
        when total_transactions >= 10 and unique_products > 1 then 'Diversified Active'
        when total_transactions >= 10 and unique_products = 1 then 'Single Product Active'
        when total_transactions >= 5 then 'Moderate Activity'
        else 'Low Activity'
    end as activity_profile
from daily_details
```

### 5. Snapshots
Located in `snapshots/`:

#### Account Stage History
Tracks customer lifecycle progression:
```sql
{% snapshot account_stage_history %}
{{
    config(
      strategy='check',
      check_cols=['account_stage']
    )
}}
select 
    account_key,
    activity_date,
    account_stage
from daily_account_activity
{% endsnapshot %}
```

#### Product Activity History
Monitors changes in trading behavior:
```sql
{% snapshot product_activity_history %}
{{
    config(
      strategy='check',
      check_cols=['activity_profile'],
      snapshot_meta_column_names= {
        'dbt_valid_from': 'activity_valid_from',
        'dbt_valid_to': 'activity_valid_to'
      }
    )
}}
select 
    account_key,
    detail_date,
    activity_profile
from daily_product_activity
{% endsnapshot %}
```

## Key Metrics and Analysis

### 1. Customer Lifecycle
- Track progression through stages:
  - New Account → First Week → First Month → Established
- Analyze time spent in each stage
- Monitor customer maturity

### 2. Trading Behavior
- Monitor activity levels:
  - Low Activity → Moderate Activity → Single Product Active → Diversified Active
- Track product diversification
- Analyze trading patterns

### 3. Performance Indicators
- Transaction volumes
- Fee generation
- Product popularity
- Customer engagement

## Usage Examples

### 1. Track Customer Stage Changes
```sql
select 
    account_key,
    account_stage,
    stage_valid_from,
    stage_valid_to
from account_stage_history
order by account_key, stage_valid_from
```

### 2. Analyze Trading Pattern Evolution
```sql
select 
    account_key,
    activity_profile,
    activity_valid_from,
    activity_valid_to
from product_activity_history
order by account_key, activity_valid_from
```

### 3. Monitor Daily Activity
```sql
select 
    activity_date,
    count(*) as total_accounts,
    count(case when account_stage = 'New Account' then 1 end) as new_accounts,
    count(case when activity_profile = 'Diversified Active' then 1 end) as active_traders
from daily_account_activity
group by activity_date
order by activity_date
```

## Testing and Data Quality

### 1. Dimension Tests
- Unique keys
- Not null constraints
- Referential integrity
- Value validation

### 2. Fact Tests
- Transaction completeness
- Balance calculations
- Status transitions

### 3. Report Tests
- Stage calculations
- Activity metrics
- Temporal consistency

### 4. Snapshot Tests
- Valid time periods
- Stage transitions
- Profile changes

## Incremental Processing
Most models use incremental processing to efficiently handle new data:
```sql
{% if is_incremental() %}
where activity_date > (select max(activity_date) from {{ this }})
{% endif %}
```

This ensures that only new or changed records are processed in each run. 