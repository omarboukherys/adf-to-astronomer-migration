-- Daily sales by order date and channel.
select
    order_date,
    coalesce(channel, 'unknown') as channel,
    count(*) as orders,
    count_if(status = 'DELIVERED') as delivered_orders,
    sum(iff(status = 'DELIVERED', amount, 0)) as delivered_revenue
from {{ ref('fct_orders') }}
group by 1, 2
