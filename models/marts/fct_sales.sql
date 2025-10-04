with b as (
  select * from {{ ref('int_sales_by_reference') }}
)
select
  reference_id,
  product_name,
  product_code,
  country,
  campaign_name,
  source,
  sales_agent_list,

  -- ??????????: Kyiv (?? ?), UTC, New York (????????? ???)
  order_ts_kyiv,
  (order_ts_kyiv at time zone 'Europe/Kyiv')                                 as order_ts_utc,
  ((order_ts_kyiv at time zone 'Europe/Kyiv') at time zone 'America/New_York') as order_ts_ny,

  -- ??????????: Kyiv/UTC/NY
  return_ts_kyiv,
  (return_ts_kyiv at time zone 'Europe/Kyiv')                                 as return_ts_utc,
  ((return_ts_kyiv at time zone 'Europe/Kyiv') at time zone 'America/New_York') as return_ts_ny,

  -- ??????? ???? ??? ???????? ? ???????????
  case
    when return_ts_kyiv is not null and order_ts_kyiv is not null
      then date_part('day', return_ts_kyiv - order_ts_kyiv)
    else null
  end as days_to_refund,

  subscription_start_ts_kyiv,
  subscription_end_ts_kyiv,
  subscription_duration_months,

  has_chargeback,
  has_refund,

  original_amount,
  discount_amount,
  total_amount,
  number_of_rebills,
  total_rebill_amount,
  returned_amount,

  -- ?????????? ??????? ??????
  (coalesce(total_amount,0) + coalesce(total_rebill_amount,0) - coalesce(returned_amount,0)) as revenue_total,
  coalesce(total_rebill_amount,0) as revenue_rebills_only
from b
