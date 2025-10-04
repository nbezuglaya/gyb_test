with parsed as (
  select
    cast(s."Reference ID" as varchar)                 as reference_id,
    coalesce(nullif(s."Country", ''), 'N/A')          as country,
    coalesce(nullif(s."Product Code", ''), 'N/A')     as product_code,
    coalesce(nullif(s."Product Name", ''), 'N/A')     as product_name,

    btrim(s."Subscription Start Date")  as _sub_start_txt,
    btrim(s."Subscription Deactivation Date") as _sub_end_txt,
    btrim(s."Order Date Kyiv")          as _order_txt,
    btrim(s."Return Date Kyiv")         as _return_txt,
    btrim(s."Last Rebill Date Kyiv")    as _last_rebill_txt,

    nullif(s."Subscription Duration Months", '')::int as subscription_duration_months,

    coalesce(nullif(s."Sales Agent Name", ''), 'N/A') as sales_agent_name,
    upper(coalesce(nullif(s."Source", ''), 'N/A'))    as source,
    coalesce(nullif(s."Campaign Name", ''), 'N/A')    as campaign_name,

    nullif(replace(replace(replace(s."Total Amount ($)",    '$',''), ',',''), ' ', ''), '')::numeric as total_amount,
    nullif(replace(replace(replace(s."Discount Amount ($)", '$',''), ',',''), ' ', ''), '')::numeric as discount_amount,
    nullif(s."Number Of Rebills", '')::int                                                         as number_of_rebills,
    nullif(replace(replace(replace(s."Original Amount ($)", '$',''), ',',''), ' ', ''), '')::numeric as original_amount,
    nullif(replace(replace(replace(s."Returned Amount ($)", '$',''), ',',''), ' ', ''), '')::numeric as returned_amount,
    nullif(replace(replace(replace(s."Total Rebill Amount", '$',''), ',',''), ' ', ''), '')::numeric as total_rebill_amount,

    case when lower(coalesce(s."Has Chargeback", '')) in ('true','t','yes','y','1') then true
         when lower(coalesce(s."Has Chargeback", '')) in ('false','f','no','n','0') then false
         else null end as has_chargeback,

    case when lower(coalesce(s."Has Refund", '')) in ('true','t','yes','y','1') then true
         when lower(coalesce(s."Has Refund", '')) in ('false','f','no','n','0') then false
         else null end as has_refund

  from {{ ref('sales_data') }} as s
  where nullif(s."Reference ID", '') is not null
),

-- 1) нормалізація: додаємо поля з кириличним місяцем → номер місяця
norm as (
  select
    p.*,
    lower(p._order_txt) as _order_ru,
    regexp_replace(
      regexp_replace(
      regexp_replace(
      regexp_replace(
      regexp_replace(
      regexp_replace(
      regexp_replace(
      regexp_replace(
      regexp_replace(
      regexp_replace(
      regexp_replace(
      regexp_replace(lower(p._order_txt), '^январ[ья]',  '01'),
                                   '^феврал[ья]', '02'),
                                   '^март',       '03'),
                                   '^апрел[ья]',  '04'),
                                   '^ма[йя]',     '05'),
                                   '^июн[ья]',    '06'),
                                   '^июл[ья]',    '07'),
                                   '^август',     '08'),
                                   '^сентябр[ья]','09'),
                                   '^октябр[ья]', '10'),
                                   '^ноябр[ья]',  '11'),
                                   '^декабр[ья]', '12')
    as _order_ru_mm
  from parsed p
),

-- 2) остаточний парсинг
dt as (
  select
    reference_id, country, product_code, product_name,
    sales_agent_name, source, campaign_name,
    subscription_duration_months,
    total_amount, discount_amount, number_of_rebills,
    original_amount, returned_amount, total_rebill_amount,
    has_chargeback, has_refund,

    _sub_start_txt, _sub_end_txt, _order_txt, _return_txt, _last_rebill_txt,
    _order_ru, _order_ru_mm,

    /* Subscription Start */
    case
      when _sub_start_txt ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?[+-]\d{2}(:\d{2})?$'
        then (replace(_sub_start_txt,'T',' ')::timestamptz at time zone 'Europe/Kyiv')
      when _sub_start_txt ~ '^\d{4}-\d{2}-\d{2}(\s+\d{2}:\d{2}(:\d{2})?)?$'
        then _sub_start_txt::timestamp
      when _sub_start_txt ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?Z$'
        then replace(replace(_sub_start_txt,'T',' '),'Z','')::timestamp
      when _sub_start_txt ~ '^\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}(:\d{2})?\s?(AM|PM)$'
        then to_timestamp(_sub_start_txt, 'MM/DD/YYYY HH12:MI:SS AM')
      when _sub_start_txt ~ '^\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}\s?(AM|PM)$'
        then to_timestamp(_sub_start_txt, 'MM/DD/YYYY HH12:MI AM')
      when _sub_start_txt ~ '^\d{1,2}/\d{1,2}/\d{4}$'
        then to_timestamp(_sub_start_txt, 'MM/DD/YYYY')
      when _sub_start_txt ~ '^\d{2}\.\d{2}\.\d{4}(\s+\d{2}:\d{2}(:\d{2})?)?$'
        then to_timestamp(_sub_start_txt, 'DD.MM.YYYY HH24:MI:SS')
      else null
    end as subscription_start_ts_kyiv,

    /* Subscription End */
    case
      when _sub_end_txt ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?[+-]\d{2}(:\d{2})?$'
        then (replace(_sub_end_txt,'T',' ')::timestamptz at time zone 'Europe/Kyiv')
      when _sub_end_txt ~ '^\d{4}-\d{2}-\d{2}(\s+\d{2}:\d{2}(:\d{2})?)?$'
        then _sub_end_txt::timestamp
      when _sub_end_txt ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?Z$'
        then replace(replace(_sub_end_txt,'T',' '),'Z','')::timestamp
      when _sub_end_txt ~ '^\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}(:\d{2})?\s?(AM|PM)$'
        then to_timestamp(_sub_end_txt, 'MM/DD/YYYY HH12:MI:SS AM')
      when _sub_end_txt ~ '^\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}\s?(AM|PM)$'
        then to_timestamp(_sub_end_txt, 'MM/DD/YYYY HH12:MI AM')
      when _sub_end_txt ~ '^\d{1,2}/\d{1,2}/\d{4}$'
        then to_timestamp(_sub_end_txt, 'MM/DD/YYYY')
      when _sub_end_txt ~ '^\d{2}\.\d{2}\.\d{4}(\s+\d{2}:\d{2}(:\d{2})?)?$'
        then to_timestamp(_sub_end_txt, 'DD.MM.YYYY HH24:MI:SS')
      else null
    end as subscription_end_ts_kyiv,

    /* ORDER (RU-місяці + інші формати) */
    case
      when _order_ru ~ '^(январ|феврал|март|апрел|ма[йя]|июн|июл|август|сентябр|октябр|ноябр|декабр)'
        then case
               when _order_ru_mm ~ ':\d{2}:\d{2}\s?(am|pm)?$'
                 then to_timestamp(_order_ru_mm, 'MM DD, YYYY, HH12:MI:SS AM')
               else to_timestamp(_order_ru_mm, 'MM DD, YYYY, HH12:MI AM')
             end
      when _order_txt ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?[+-]\d{2}(:\d{2})?$'
        then (replace(_order_txt,'T',' ')::timestamptz at time zone 'Europe/Kyiv')
      when _order_txt ~ '^\d{4}-\d{2}-\d{2}(\s+\d{2}:\d{2}(:\d{2})?)?$'
        then _order_txt::timestamp
      when _order_txt ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?Z$'
        then replace(replace(_order_txt,'T',' '),'Z','')::timestamp
      when _order_txt ~ '^\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}(:\d{2})?\s?(AM|PM)$'
        then to_timestamp(_order_txt, 'MM/DD/YYYY HH12:MI:SS AM')
      when _order_txt ~ '^\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}\s?(AM|PM)$'
        then to_timestamp(_order_txt, 'MM/DD/YYYY HH12:MI AM')
      when _order_txt ~ '^\d{1,2}/\d{1,2}/\d{4}$'
        then to_timestamp(_order_txt, 'MM/DD/YYYY')
      when _order_txt ~ '^\d{2}\.\d{2}\.\d{4}(\s+\d{2}:\d{2}(:\d{2})?)?$'
        then to_timestamp(_order_txt, 'DD.MM.YYYY HH24:MI:SS')
      else null
    end as order_ts_kyiv,

    /* Return */
    case
      when _return_txt ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?[+-]\d{2}(:\d{2})?$'
        then (replace(_return_txt,'T',' ')::timestamptz at time zone 'Europe/Kyiv')
      when _return_txt ~ '^\d{4}-\d{2}-\d{2}(\s+\d{2}:\d{2}(:\d{2})?)?$'
        then _return_txt::timestamp
      when _return_txt ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?Z$'
        then replace(replace(_return_txt,'T',' '),'Z','')::timestamp
      when _return_txt ~ '^\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}(:\d{2})?\s?(AM|PM)$'
        then to_timestamp(_return_txt, 'MM/DD/YYYY HH12:MI:SS AM')
      when _return_txt ~ '^\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}\s?(AM|PM)$'
        then to_timestamp(_return_txt, 'MM/DD/YYYY HH12:MI AM')
      when _return_txt ~ '^\d{1,2}/\d{1,2}/\d{4}$'
        then to_timestamp(_return_txt, 'MM/DD/YYYY')
      when _return_txt ~ '^\d{2}\.\d{2}\.\d{4}(\s+\d{2}:\d{2}(:\d{2})?)?$'
        then to_timestamp(_return_txt, 'DD.MM.YYYY HH24:MI:SS')
      else null
    end as return_ts_kyiv,

    /* Last Rebill */
    case
      when _last_rebill_txt ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?[+-]\d{2}(:\d{2})?$'
        then (replace(_last_rebill_txt,'T',' ')::timestamptz at time zone 'Europe/Kyiv')
      when _last_rebill_txt ~ '^\d{4}-\d{2}-\d{2}(\s+\d{2}:\d{2}(:\d{2})?)?$'
        then _last_rebill_txt::timestamp
      when _last_rebill_txt ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?Z$'
        then replace(replace(_last_rebill_txt,'T',' '),'Z','')::timestamp
      when _last_rebill_txt ~ '^\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}(:\d{2})?\s?(AM|PM)$'
        then to_timestamp(_last_rebill_txt, 'MM/DD/YYYY HH12:MI:SS AM')
      when _last_rebill_txt ~ '^\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}\s?(AM|PM)$'
        then to_timestamp(_last_rebill_txt, 'MM/DD/YYYY HH12:MI AM')
      when _last_rebill_txt ~ '^\d{1,2}/\d{1,2}/\d{4}$'
        then to_timestamp(_last_rebill_txt, 'MM/DD/YYYY')
      when _last_rebill_txt ~ '^\d{2}\.\d{2}\.\d{4}(\s+\d{2}:\d{2}(:\d{2})?)?$'
        then to_timestamp(_last_rebill_txt, 'DD.MM.YYYY HH24:MI:SS')
      else null
    end as last_rebill_ts_kyiv

  from norm
)
select
  reference_id, country, product_code, product_name,
  subscription_start_ts_kyiv, subscription_end_ts_kyiv, subscription_duration_months,
  order_ts_kyiv, return_ts_kyiv, last_rebill_ts_kyiv,
  has_chargeback, has_refund,
  sales_agent_name, source, campaign_name,
  total_amount, discount_amount, number_of_rebills,
  original_amount, returned_amount, total_rebill_amount
from dt
