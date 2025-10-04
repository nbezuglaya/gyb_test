select distinct "Order Date Kyiv"
from dbt_gyb.sales_data
where coalesce("Order Date Kyiv",'') <> ''
limit 20;
