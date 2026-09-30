# Data Sources

## POS transactions
File: `pos_transactions.csv`, delivered daily per date folder. One row per line item. Columns: txn_id, txn_ts, store_id, sku, qty, unit_price, discount_pct, payment_type. Loaded into `raw.pos_transactions`.

## Carrier activations
File: `carrier_activations.csv`, delivered daily by the carrier partner. One row per iPhone activation. Columns: activation_id, txn_id, store_id, carrier, sku, activation_ts. Loaded into `raw.carrier_activations`. Three carriers are reported, anonymised as Carrier A, Carrier B and Carrier C.

## Store master
File: `stores.json`, the reference list of stores from the CRM. Columns: store_id, store_name, province, city, channel, sq_ft, open_date. Channels are Flagship, Retail Partner and Carrier Store. Loaded into `raw.stores` and exposed as `dim_store`.

## Product categories
SKUs map to categories by prefix: IPH is iPhone, MAC is Mac, IPD is iPad, WAT is Watch, AIR is AirPods, and ACC is Accessories.

## Coverage
The dataset covers 120 stores across six provinces (ON, QC, BC, AB, MB, NS) from 2026-01-01 to 2026-06-29. All data in this project is synthetic.
