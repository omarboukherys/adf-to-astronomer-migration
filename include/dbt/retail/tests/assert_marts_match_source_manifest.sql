-- Reconciliation: the marts must give exactly the results computed from the source files
-- (manifest.json of the generator). Returns a row, so fails, if any figure differs.
with actual as (
    select
        (select count(*) from {{ ref('dim_customers') }}) as customers,
        (select count(*) from {{ ref('dim_products') }}) as products,
        (select count(*) from {{ ref('fct_orders') }}) as valid_orders,
        (select count(*) from {{ ref('audit_rejected_orders') }}) as rejected_orders,
        (select sum(amount) from {{ ref('fct_orders') }} where status = 'DELIVERED') as delivered_revenue
)

select *
from actual
where customers != {{ var('expected_customers') }}
   or products != {{ var('expected_products') }}
   or valid_orders != {{ var('expected_valid_orders') }}
   or rejected_orders != {{ var('expected_rejected_orders') }}
   or delivered_revenue != {{ var('expected_delivered_revenue') }}
