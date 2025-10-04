select
  count(*)                               as total_rows,
  sum( (order_ts_kyiv is not null)::int) as nonnull_order_ts
from dbt_gyb.stg_sales;
