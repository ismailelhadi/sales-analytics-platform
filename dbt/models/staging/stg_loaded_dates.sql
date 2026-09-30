-- Dates where BOTH systems of record loaded cleanly on their latest run. Reconciliation only
-- runs on these, so a quarantined file doesn't show up as thousands of fake discrepancies.
with latest as (
    select distinct on (source, load_date) source, load_date, status
    from {{ source('ops', 'load_log') }}
    order by source, load_date, logged_at desc
)
select load_date
from latest
where status = 'loaded'
group by load_date
having count(*) = 2
