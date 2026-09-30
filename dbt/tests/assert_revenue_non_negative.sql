select * from {{ ref('fct_daily_store_sales') }} where net_revenue < 0
