-- Current version of each customer.
select customer_id, city, segment, signup_date, updated_at, _source_file
from {{ ref('stg_customers') }}
qualify row_number() over (partition by customer_id order by updated_at desc, _loaded_at desc) = 1
