with orders as ( 

    select * from {{ ref('stg_jaffle_shop__orders') }}

),

payments as (

    select * from {{ ref('stg_stripe__payment') }}
)

select o.order_id, o.customer_id, sum(case when p.status = 'success' then p.amount else 0 end) as amount
from orders o
join payments p on o.order_id = p.order_id
group by o.order_id, o.customer_id
order by o.order_id