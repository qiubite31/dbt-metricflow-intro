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

# Expanded orders data to show aggregations and averages
orders_data = {
    'id': [101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115],
    'user_id': [1, 1, 1, 2, 2, 3, 3, 3, 3, 4, 4, 5, 5, 5, 1],
    'order_date': [
        '2023-01-01', '2023-01-15', '2023-02-05', # User 1
        '2023-02-01', '2023-02-20',             # User 2
        '2023-02-10', '2023-03-01', '2023-03-15', '2023-04-01', # User 3
        '2023-03-05', '2023-03-20',             # User 4
        '2023-04-10', '2023-04-20', '2023-05-01', # User 5
        '2023-05-15'                            # User 1 again
    ],
    'status': [
        'placed', 'shipped', 'completed', 
        'completed', 'shipped', 
        'return_pending', 'returned', 'completed', 'placed',
        'placed', 'shipped',
        'completed', 'completed', 'shipped',
        'placed'
    ],
    'amount': [
        100.5, 50.0, 120.0,  # User 1
        75.25, 150.0,       # User 2
        200.0, 150.75, 300.0, 50.0, # User 3
        80.0, 95.0,         # User 4
        250.0, 180.0, 220.0, # User 5
        45.0                # User 1
    ]
}

# Convert to DataFrames
df_customers = pd.DataFrame(customers_data)
df_orders = pd.DataFrame(orders_data)

# Write to SQLite
df_customers.to_sql('raw_customers', conn, if_exists='replace', index=False)
df_orders.to_sql('raw_orders', conn, if_exists='replace', index=False)

print(f"\nCreated {len(df_customers)} customers and {len(df_orders)} orders.")

conn.close()
print(f"Database updated at {os.path.abspath(db_path)}")