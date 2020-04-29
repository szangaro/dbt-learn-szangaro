{{
  config(
    materialized='view'
  )
}}

with stg_customers as (
    select * from {{ ref('stg_customers') }}
),
stg_orders as (
    select * from {{ ref('stg_orders') }}
),
orders as (
    select * from {{ ref('orders') }}
),
customer_orders as (
    select
        so.customer_id,
        min(so.order_date) as first_order_date,
        max(so.order_date) as most_recent_order_date,
        count(so.order_id) as number_of_orders,
        sum(o.amount) as lifetime_value
    from stg_orders so
    left join orders o using(customer_id)
    group by 1
),
final as (
    select
        stg_customers.customer_id,
        stg_customers.first_name,
        stg_customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        coalesce(customer_orders.lifetime_value, 0) as lifetime_value
    from stg_customers
    left join customer_orders using (customer_id)
)
select * from final