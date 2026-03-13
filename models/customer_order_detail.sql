{{
    config(
        materialized='table'
    )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

final as (
    select
        orders.order_id,
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customers.email,
        orders.order_date,
        orders.status
    from orders
    inner join customers on orders.customer_id = customers.customer_id
)

select * from final
