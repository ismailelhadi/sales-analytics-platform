# Metric Definitions

## Net revenue
Net revenue is quantity times unit price, minus the line discount: `qty * unit_price * (1 - discount_pct)`. It is calculated per POS line item in `stg_pos_transactions` and summed in `fct_daily_store_sales`. Revenue is in Canadian dollars (CAD) and excludes tax.

## Transactions
A transaction is one distinct `txn_id` in the POS system. An accessory added to an iPhone sale is recorded as its own transaction line with its own `txn_id`.

## Average basket
Average basket is net revenue divided by the number of transactions for the same store and period. It measures how much a customer spends per visit.

## Accessory attach rate
Accessory attach rate is accessory units divided by iPhone units for the same store and period. A rate of 0.30 means 30 accessories were sold for every 100 iPhones. Cases, chargers and cables count as accessories.

## Activation rate
Activation rate is the share of iPhone sales in POS that have a matching carrier activation for the same transaction at the same store. It is computed from `fct_activation_reconciliation` as matched sales divided by all iPhone sales for the store.

## Revenue growth
Revenue growth compares the last 30 days of the scoring window with the first 30 days: `revenue_last_30d / revenue_first_30d - 1`. A value of 0.12 means revenue grew 12%.
