select
    store_id, store_name, province, city, channel, sq_ft, open_date,
    date_part('year', age(date '{{ var("scoring_end") }}', open_date))::int as store_age_years
from {{ ref('stg_stores') }}
