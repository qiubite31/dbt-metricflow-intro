import sqlite3
import pandas as pd
import os

# Define database path
db_path = 'data.db'

# Check if database exists and remove it to ensure a fresh start
if os.path.exists(db_path):
    print(f"Existing database found at {db_path}. Deleting for fresh initialization...")
    try:
        os.remove(db_path)
    except Exception as e:
        print(f"Warning: Could not delete existing database: {e}")

# Create connection
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Create sample data
customers_data = {
    'id': [1, 2, 3, 4, 5],
    'first_name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
    'last_name': ['Smith', 'Jones', 'Brown', 'Davis', 'Wilson'],
    'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com', 'david@example.com', 'eve@example.com']
}

orders_data = {
    'id': [101, 102, 103, 104, 105, 106],
    'user_id': [1, 1, 2, 3, 3, 4],
    'order_date': ['2023-01-01', '2023-01-15', '2023-02-01', '2023-02-10', '2023-03-01', '2023-03-05'],
    'status': ['placed', 'shipped', 'completed', 'return_pending', 'returned', 'placed']
}

# Convert to DataFrames
df_customers = pd.DataFrame(customers_data)
df_orders = pd.DataFrame(orders_data)

# Write to SQLite
df_customers.to_sql('raw_customers', conn, if_exists='replace', index=False)
df_orders.to_sql('raw_orders', conn, if_exists='replace', index=False)

print("\nSample data in raw_customers:")
cursor.execute("SELECT * FROM raw_customers LIMIT 5;")
print(cursor.fetchall())

print("\nSample data in raw_orders:")
cursor.execute("SELECT * FROM raw_orders LIMIT 5;")
print(cursor.fetchall())

conn.close()
print(f"Database created at {os.path.abspath(db_path)}")
