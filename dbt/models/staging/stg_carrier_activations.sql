select
    activation_id,
    txn_id,
    store_id,
    carrier,
    sku,
    activation_ts,
    activation_ts::date as activation_date,
    load_date
from {{ source('raw', 'carrier_activations') }}
