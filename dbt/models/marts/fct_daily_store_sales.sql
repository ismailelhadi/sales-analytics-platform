select
    txn_date,
    store_id,
    count(distinct txn_id)                                          as transactions,
    sum(qty)                                                        as units,
    sum(net_revenue)                                                as net_revenue,
    sum(case when category = 'iPhone' then qty else 0 end)          as iphone_units,
    sum(case when category = 'Accessories' then qty else 0 end)     as accessory_units
from {{ ref('stg_pos_transactions') }}
group by 1, 2
