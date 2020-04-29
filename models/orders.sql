with orders as (
    select * from {{ ref('stg_orders') }}

),
payments as (
    select * from {{ ref('stg_payments') }}
)

SELECT orders.customer_id, orders.order_id, payments.amount
FROM orders
LEFT OUTER JOIN payments
ON orders.order_id = payments.order_id