with s as (
  select * from {{ ref('stg_sales') }}
),
single_vals as (
  select
    reference_id,

    max(product_name)  as product_name,
    max(product_code)  as product_code,
    max(country)       as country,
    max(campaign_name) as campaign_name,
    max(source)        as source,

    max(order_ts_kyiv)       as order_ts_kyiv,
    max(return_ts_kyiv)      as return_ts_kyiv,
    max(last_rebill_ts_kyiv) as last_rebill_ts_kyiv,

    max(subscription_start_ts_kyiv)   as subscription_start_ts_kyiv,
    max(subscription_end_ts_kyiv)     as subscription_end_ts_kyiv,
    max(subscription_duration_months) as subscription_duration_months,

    bool_or(has_chargeback) as has_chargeback,
    bool_or(has_refund)     as has_refund,

    -- ??????? ????????? ?????? ? ????-????? ????? ??????? (???????? ????????)
    max(total_amount)        as total_amount,
    max(discount_amount)     as discount_amount,
    max(number_of_rebills)   as number_of_rebills,
    max(original_amount)     as original_amount,
    max(returned_amount)     as returned_amount,
    max(total_rebill_amount) as total_rebill_amount
  from s
  group by reference_id
),
agents as (
  select
    reference_id,
    string_agg(distinct sales_agent_name, ', ') as sales_agent_list
  from s
  group by reference_id
)
select
  v.*,
  a.sales_agent_list
from single_vals v
left join agents a using (reference_id)
