
-- 引用 stg_customers 模型
with customers as (
    select * from {{ ref('stg_customers') }}
),

-- 引用 stg_orders 模型
orders as (
    select * from {{ ref('stg_orders') }}
),

-- 聚合訂單數據，計算每個客戶的訂單統計
customer_orders as (
    select
        customer_id,
        min(order_date) as first_order_date,           -- 首張訂單日期
        max(order_date) as most_recent_order_date,     -- 最近一張訂單日期
        count(order_id) as number_of_orders            -- 總訂單數量
    from orders
    group by 1
),

-- 合併客戶基本資料與訂單統計
final as (
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders -- 如果沒有訂單，顯示為 0
    from customers
    left join customer_orders using (customer_id) -- 以客戶為主體進行左外連接
)

-- 輸出最終結果
select * from final
