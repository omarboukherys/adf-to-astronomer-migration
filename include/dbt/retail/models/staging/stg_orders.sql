-- Typed order versions. A same order arrives several times (status updates, stale versions re-sent,
-- exact duplicates): versions are kept here and resolved in fct_orders.
-- Invalid values become NULL (try_*) and are flagged instead of failing the load.
with typed as (
    select
        order_id,
        customer_id,
        product_id,
        try_to_number(quantity) as quantity,
        try_to_decimal(unit_price, 10, 2) as unit_price,
        try_to_timestamp_ntz(order_ts) as ordered_at,
        upper(status) as status,
        try_to_timestamp_ntz(updated_at) as updated_at,
        channel,
        _source_file,
        _loaded_at
    from {{ source('raw_airflow', 'orders') }}
)

select
    *,
    coalesce(quantity > 0 and ordered_at is not null and unit_price is not null, false) as is_valid
from typed
