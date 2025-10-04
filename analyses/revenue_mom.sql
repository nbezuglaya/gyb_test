with monthly as (
  select date_trunc('month', coalesce(order_ts_kyiv, subscription_start_ts_kyiv)) as month_kyiv,
         sum(revenue_total) as revenue_total
  from dbt_gyb.fct_sales
  where coalesce(order_ts_kyiv, subscription_start_ts_kyiv) is not null
  group by 1
)
select
  month_kyiv,
  revenue_total,
  lag(revenue_total) over (order by month_kyiv) as prev_rev,
  case
    when lag(revenue_total) over (order by month_kyiv) is null then null
    when lag(revenue_total) over (order by month_kyiv) = 0 then null
    else (revenue_total - lag(revenue_total) over (order by month_kyiv))
         / lag(revenue_total) over (order by month_kyiv) * 100.0
  end as mom_growth_pct
from monthly
order by month_kyiv;
