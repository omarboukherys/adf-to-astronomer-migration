-- Typed product versions (prices change over time).
select
    product_id,
    product_name,
    category,
    try_to_decimal(unit_price, 10, 2) as unit_price,
    try_to_timestamp_ntz(updated_at) as updated_at,
    _source_file,
    _loaded_at
from {{ source('raw_airflow', 'products') }}
