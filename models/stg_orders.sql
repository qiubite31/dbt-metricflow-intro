with source as (
    select * from {{ source('my_duckdb_source', 'raw_orders') }}
),

renamed as (
    select
        id as order_id,
        user_id as customer_id,
        cast(order_date as date) as order_date,
        status,
        amount
    from source
)

select * from renamed
