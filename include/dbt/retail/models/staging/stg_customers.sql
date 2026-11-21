-- Typed customer versions (one row per version received; a customer can change city or segment).
select
    customer_id,
    city,
    segment,
    try_to_date(signup_date) as signup_date,
    try_to_timestamp_ntz(updated_at) as updated_at,
    _source_file,
    _loaded_at
from {{ source('raw_airflow', 'customers') }}
