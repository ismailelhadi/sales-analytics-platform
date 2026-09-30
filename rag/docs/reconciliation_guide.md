# Activation Reconciliation Guide

## Why reconcile
Two systems record iPhone sales: the POS system records the sale, and the carrier partner reports the activation. They should agree. When they disagree, commissions, inventory and sales reporting can be wrong.

## Statuses
The model `fct_activation_reconciliation` joins iPhone POS sales to carrier activations on `txn_id` and gives each record one status:
- matched: the sale and activation agree on transaction and store.
- missing_activation: an iPhone sale in POS has no activation. Often the customer activated later or on another carrier, but a store with many of these should be reviewed.
- orphan_activation: the carrier reported an activation whose `txn_id` does not exist in POS. This usually means a keying error or a sale recorded outside POS.
- store_mismatch: the sale and activation share a `txn_id` but name different stores. This affects which store gets credit.

## Excluded dates
Reconciliation only runs on dates where both the POS file and the carrier file loaded. If either file was quarantined, that date is excluded until it is re-loaded.

## Guardrail
A dbt test fails the build if fewer than 80% of iPhone sales reconcile as matched, because that level of mismatch points to a broken feed rather than normal business noise.
