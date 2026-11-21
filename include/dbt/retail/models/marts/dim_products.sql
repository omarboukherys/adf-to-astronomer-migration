-- Current version of each product (latest price).
select product_id, product_name, category, unit_price, updated_at, _source_file
from {{ ref('stg_products') }}
qualify row_number() over (partition by product_id order by updated_at desc, _loaded_at desc) = 1
