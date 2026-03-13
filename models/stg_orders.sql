with source as (
    select * from {{ source('my_sqlite_source', 'raw_orders') }}
),

renamed as (
    select
        id as order_id,           -- rename id to order_id
        user_id as customer_id,   -- rename user_id to customer_id
        order_date,               -- order date
        status,                   -- order status
        amount                    -- transaction amount
    from source
)

select * from renamed