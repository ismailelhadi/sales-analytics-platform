select store_id, store_name, province, city, channel, sq_ft, open_date
from {{ source('raw', 'stores') }}
