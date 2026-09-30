-- Reconcile two systems of record: every iPhone sale in POS should have a carrier activation
-- for the same transaction at the same store, and every activation should trace back to a sale.
with pos as (
    select p.txn_id, p.store_id, p.txn_date
    from {{ ref('stg_pos_transactions') }} p
    join {{ ref('stg_loaded_dates') }} d on d.load_date = p.load_date
    where p.category = 'iPhone'
),
act as (
    select a.activation_id, a.txn_id, a.store_id, a.activation_date
    from {{ ref('stg_carrier_activations') }} a
    join {{ ref('stg_loaded_dates') }} d on d.load_date = a.load_date
)
select
    coalesce(pos.txn_id, act.txn_id)                   as txn_id,
    act.activation_id,
    pos.store_id                                       as pos_store_id,
    act.store_id                                       as carrier_store_id,
    coalesce(pos.txn_date, act.activation_date)        as event_date,
    case
        when pos.txn_id is null                 then 'orphan_activation'
        when act.txn_id is null                 then 'missing_activation'
        when pos.store_id <> act.store_id       then 'store_mismatch'
        else 'matched'
    end                                                as recon_status
from pos
full outer join act on act.txn_id = pos.txn_id
