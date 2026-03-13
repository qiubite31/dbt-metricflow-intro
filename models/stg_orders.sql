
-- 定義 source CTE，從原始資料來源讀取 raw_orders
with source as (
    select * from {{ source('my_sqlite_source', 'raw_orders') }}
),

-- 定義 renamed CTE，重命名欄位並建立與客戶的關聯
renamed as (
    select
        id as order_id,           -- 將原始的 id 重新命名為 order_id
        user_id as customer_id,   -- 將 user_id 重新命名為 customer_id 以便關聯
        order_date,               -- 訂單日期
        status                    -- 訂單狀態
    from source
)

-- 最終選擇輸出的結果
select * from renamed
