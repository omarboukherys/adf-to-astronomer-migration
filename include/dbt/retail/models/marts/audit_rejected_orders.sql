-- Orders whose latest version is invalid (negative quantity, impossible date...): kept for audit.
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
    ordered_at,
    status,
    _source_file,
    case
        when quantity is null or quantity <= 0 then 'invalid quantity'
        when ordered_at is null then 'invalid order date'
        else 'invalid price'
    end as rejection_reason
from latest
where not is_valid
