with customers as (

     select * from {{ ref('stg_jaffle_shop__customers') }}

),

orders as ( 

    select * from {{ ref('stg_jaffle_shop__orders') }}

),

 orders_2 as (

    select customer_id, amount from {{ ref('fct_orders') }}
),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from orders

    group by 1

),

final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce (customer_orders.number_of_orders, 0) 
        as number_of_orders

    from customers

    left join customer_orders using (customer_id)

), 

final_2 as (
    select 
        f.customer_id,
        f.first_name,
        f.last_name, 
        f.first_order_date,
        f.most_recent_order_date,
        f.number_of_orders,
        o.amount
    
    from final f 
    join orders_2 o using(customer_id)
)

select *, sum(amount) over(order by customer_id rows between unbounded preceding and current row) as lifetime_value
from final_2
order by lifetime_value