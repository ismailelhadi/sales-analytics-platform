select
    txn_id,
    txn_ts,
    txn_ts::date                                         as txn_date,
    store_id,
    sku,
    case
        when sku like 'IPH-%' then 'iPhone'
        when sku like 'MAC-%' then 'Mac'
        when sku like 'IPD-%' then 'iPad'
        when sku like 'WAT-%' then 'Watch'
        when sku like 'AIR-%' then 'AirPods'
        else 'Accessories'
    end                                                  as category,
    qty,
    unit_price,
    discount_pct,
    round(qty * unit_price * (1 - discount_pct), 2)      as net_revenue,
    payment_type,
    load_date
from {{ source('raw', 'pos_transactions') }}
