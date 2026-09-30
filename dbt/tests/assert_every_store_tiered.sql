-- Every store with sales in the scoring window must receive a tier.
select s.store_id
from {{ ref('dim_store') }} s
left join {{ ref('store_tiering') }} t using (store_id)
where t.store_id is null
