-- 2) KPI ?? ??????? + ?????
with base as (
  select distinct reference_id, sales_agent_name
  from dbt_gyb.stg_sales
  where sales_agent_name is not null
),
sale_rev as (
  select reference_id, revenue_total, discount_amount
  from dbt_gyb.fct_sales
),
joined as (
  select b.sales_agent_name,
         b.reference_id,
         s.revenue_total,
         s.discount_amount
  from base b
  left join sale_rev s using (reference_id)
),
agent_agg as (
  select
    sales_agent_name,
    count(*)             as sales_count,
    sum(revenue_total)   as total_revenue,
    avg(revenue_total)   as avg_revenue_per_sale,
    avg(discount_amount) as avg_discount_per_sale
  from joined
  group by 1
)
select
  *,
  rank() over (order by total_revenue desc) as revenue_rank
from agent_agg
order by revenue_rank;
