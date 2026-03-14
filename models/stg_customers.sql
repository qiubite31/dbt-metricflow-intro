with source as (
    select * from {{ source('my_duckdb_source', 'raw_customers') }}
),

renamed as (
    select
        id as customer_id,
        first_name,
        last_name,
        email
    from source
)

select * from renamed
