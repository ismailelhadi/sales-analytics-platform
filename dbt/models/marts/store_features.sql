-- One row per store with the inputs to the tiering model, computed over the scoring window.
{% set start = var('scoring_start') %}
{% set end = var('scoring_end') %}
with sales as (
    select * from {{ ref('fct_daily_store_sales') }}
    where txn_date between '{{ start }}' and '{{ end }}'
),
totals as (
    select
        store_id,
        sum(net_revenue)                                            as revenue,
        sum(transactions)                                           as transactions,
        sum(iphone_units)                                           as iphone_units,
        sum(accessory_units)                                        as accessory_units,
        sum(case when txn_date < date '{{ start }}' + 30 then net_revenue end) as revenue_first_30d,
        sum(case when txn_date > date '{{ end }}' - 30 then net_revenue end)   as revenue_last_30d
    from sales
    group by 1
),
activation as (
    select pos_store_id as store_id,
           avg(case when recon_status = 'matched' then 1.0 else 0.0 end) as activation_rate
    from {{ ref('fct_activation_reconciliation') }}
    where pos_store_id is not null
      and event_date between '{{ start }}' and '{{ end }}'
    group by 1
)
select
    t.store_id,
    t.revenue,
    t.transactions,
    t.revenue / nullif(t.transactions, 0)                          as avg_basket,
    t.revenue_last_30d / nullif(t.revenue_first_30d, 0) - 1        as revenue_growth,
    t.accessory_units::numeric / nullif(t.iphone_units, 0)         as accessory_attach_rate,
    coalesce(a.activation_rate, 0)                                 as activation_rate
from totals t
left join activation a using (store_id)
