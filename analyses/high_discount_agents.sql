-- 3) ?????? ?? ???????? ???? ??????????? ??????????
with per_sale as (
  select reference_id, max(discount_amount) as discount_amount
  from dbt_gyb.fct_sales
  group by 1
),
global_avg as (
  select avg(discount_amount) as global_avg_discount
  from per_sale
),
agent_avg as (
  select s.sales_agent_name,
         avg(p.discount_amount) as avg_discount
  from dbt_gyb.stg_sales s
  join per_sale p using (reference_id)
  group by 1
)
select
  a.sales_agent_name,
  a.avg_discount,
  g.global_avg_discount,
  (a.avg_discount - g.global_avg_discount) as diff_vs_global
from agent_avg a
cross join global_avg g
where a.avg_discount > g.global_avg_discount
order by diff_vs_global desc;
