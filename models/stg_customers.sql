
-- 定義 source CTE，從原始資料來源讀取 raw_customers
with source as (
    select * from {{ source('my_sqlite_source', 'raw_customers') }}
),

-- 定義 renamed CTE，重命名欄位並進行初步清理
renamed as (
    select
        id as customer_id,    -- 將原始的 id 重新命名為 customer_id
        first_name,           -- 客戶名字
        last_name,            -- 客戶姓氏
        email                 -- 客戶電子信箱
    from source
)

-- 最終選擇輸出的結果
select * from renamed
