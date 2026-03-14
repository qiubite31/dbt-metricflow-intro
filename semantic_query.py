import sys
import io
import subprocess
import os
import duckdb
import pandas as pd

# Fix Windows console UTF-8 printing
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def run_mf_command(cmd_args):
    """Utility to run an mf command and return its stdout"""
    env = os.environ.copy()
    env["CI"] = "true"  # Disable spinners and styling issues on Windows
    env["PYTHONIOENCODING"] = "utf-8"
        
    cmd = ["mf"] + cmd_args
    print(f"Executing: {' '.join(cmd)}")
    
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env=env,
        encoding='utf-8',
        errors='replace'
    )
    
    if result.returncode != 0:
        print(f"Command failed with exit code {result.returncode}")
        print("Error Output (stderr if any):")
        print(result.stderr)
        
        # Sometime mf CLI writes errors to stdout too, so let's log it:
        print("Output (stdout if any):")
        print(result.stdout)
        return None
        
    return result.stdout.strip()

def list_semantic_layer_metadata():
    print("\n" + "="*50)
    print("Listing Metadata using MetricFlow CLI")
    print("="*50)
    
    # List Metrics
    metrics_out = run_mf_command(["list", "metrics"])
    print("\n--- Available Metrics ---")
    print(metrics_out)

    # List Dimensions for metric (Note: in dbt 1.9+ you must specify the metric to see valid dimensions)
    dimensions_out = run_mf_command(["list", "dimensions", "--metrics", "total_orders"])
    print("\n--- Available Dimensions (for metric 'total_orders') ---")
    print(dimensions_out)

def execute_semantic_query(metric_names: list, group_by: list = None):
    print("\n" + "="*50)
    print("Generating & Executing Semantic Query")
    print("="*50)
    
    # Build mf query command
    args = ["query", "--metrics", ",".join(metric_names)]
    if group_by:
        args.extend(["--group-by", ",".join(group_by)])
        
    args.append("--explain") # Use explain to print the SQL query in dbt 1.9+
    
    # Capture pure SQL from standard output
    sql_output = run_mf_command(args)
    
    if not sql_output:
        print("Failed to generate SQL from MetricFlow.")
        return
        
    # Extract the actual SQL query from the explain log
    # It usually appears after: 🔎 SQL (...)
    sql_lines = []
    in_sql_block = False
    for line in sql_output.splitlines():
        if line.strip().upper().startswith('SELECT'):
            in_sql_block = True
            
        if in_sql_block:
            sql_lines.append(line)
            
    # If heuristic failed, fall back
    clean_sql = "\n".join(sql_lines) if sql_lines else sql_output
        
    print("\nGenerated SQL string from MetricFlow:")
    print("-" * 50)
    print(clean_sql)
    print("-" * 50)
    
    # Execute against our DuckDB backend
    db_path = "data.duckdb"
    if not os.path.exists(db_path):
        print(f"\nError: Database file '{db_path}' not found.")
        return

    print(f"\nExecuting generated SQL against DuckDB ({db_path})...")
    try:
        conn = duckdb.connect(db_path)
        df_results = conn.execute(clean_sql).df()
        
        print("\nQuery Results (Pandas DataFrame):")
        print(df_results)
    except Exception as e:
        print(f"\nDuckDB Execution error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == '__main__':
    # 1. Inspect the Semantic Layer metadata
    list_semantic_layer_metadata()
    
    # 2. Execute a real SQL query through the MetricFlow bridge
    print("\n")
    execute_semantic_query(metric_names=["total_orders"], group_by=["order__order_date"])

    print("\n")
    execute_semantic_query(metric_names=["average_order_value"], group_by=["customer__first_name"])


