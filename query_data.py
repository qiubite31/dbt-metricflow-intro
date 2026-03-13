import sqlite3
import pandas as pd
import os

# list tables_views to verify all tables and views in the database
def list_tables_views(connection):
    """List all tables and views in the SQLite database."""
    cursor = connection.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    print("\nTables in the database:")
    for table in tables:
        print(f"- {table[0]}")

    cursor.execute("SELECT name FROM sqlite_master WHERE type='view';")
    views = cursor.fetchall()
    print("\nViews in the database:")
    for view in views:
        print(f"- {view[0]}")

    return [tables, views]


def query_database():
    # Define database path
    db_path = 'data.db'
    
    if not os.path.exists(db_path):
        print(f"Error: Database file '{db_path}' not found.")
        return

    # 1. Connect to SQLite database
    print(f"Connecting to database: {db_path}...")
    conn = sqlite3.connect(db_path)
    
    try:
        # 2. List all tables
        list_tables_views(conn)

        # 3. Query 'raw_customers' table
        print("\nQuerying 'raw_customers' table:")
        customers_query = "SELECT * FROM raw_customers;"
        df_customers = pd.read_sql_query(customers_query, conn)
        
        # 4. Print results using pandas
        print("\nResults (as Pandas DataFrame):")
        print(df_customers)
        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close connection
        conn.close()
        print("\nDatabase connection closed.")

def query_customers_with_orders():
    # Define paths
    db_path = 'data.db'
    
    if not os.path.exists(db_path):
        print(f"Error: Database file '{db_path}' not found.")
        return

    # 1. Connect to SQLite database
    print(f"\nConnecting to database to execute dbt model query (customers_with_orders)...")
    conn = sqlite3.connect(db_path)
    
    try:
        # 2. Execute a direct SELECT query
        query = "SELECT * FROM customers_with_orders;"
        df_results = pd.read_sql_query(query, conn)
        
        # 3. Print results
        print("\nResults from 'customers_with_orders' (direct SQLite query):")
        print(df_results)
        
    except Exception as e:
        print(f"An error occurred while executing query: {e}")
    finally:
        conn.close()

def query_customer_order_detail():
    # Define paths
    db_path = 'data.db'
    
    if not os.path.exists(db_path):
        print(f"Error: Database file '{db_path}' not found.")
        return

    # 1. Connect to SQLite database
    print(f"\nConnecting to database to execute dbt model query (customer_order_detail)...")
    conn = sqlite3.connect(db_path)
    
    try:
        # 2. Execute a direct SELECT query
        query = "SELECT * FROM customer_order_detail;"
        df_results = pd.read_sql_query(query, conn)
        
        # 3. Print results
        print("\nResults from 'customer_order_detail' (direct SQLite query):")
        print(df_results)
        
    except Exception as e:
        print(f"An error occurred while executing query: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    print("=== Running Basic Database Queries ===")
    query_database()
    
    print("\n--- query_customers_with_orders ---")
    query_customers_with_orders()

    print("\n--- query_customer_order_detail ---")
    query_customer_order_detail()
