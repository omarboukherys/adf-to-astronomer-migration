-- One row per order: its latest version (by updated_at), valid orders only.
-- Removes exact duplicates and stale versions re-sent after a newer one.
with latest as (
    select *
    from {{ ref('stg_orders') }}
    qualify row_number() over (partition by order_id order by updated_at desc, _loaded_at desc) = 1
)

select
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    quantity * unit_price as amount,
    ordered_at,
    ordered_at::date as order_date,
    status,
    channel,
    updated_at,
    _source_file
from latest
where is_valid
