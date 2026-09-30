-- Guardrail: if fewer than 80% of iPhone sales reconcile, something upstream is broken.
select 1
from {{ ref('fct_activation_reconciliation') }}
having avg(case when recon_status = 'matched' then 1.0 else 0 end) < 0.80
