with orders as (
    select * from {{ ref('stg_orders') }}

),
payments as (
    select * from {{ ref('stg_payments') }}
)

select 
    orders.customer_id, 
    orders.order_id, 
    SUM(payments.amount) as amount
from orders
left join payments using(order_id)
group by 
    1, 2
