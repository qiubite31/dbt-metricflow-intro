import os
import sys

# Ensure UTF-8 encoding is used on Windows to prevent UnicodeDecodeError during dbt operations
os.environ["PYTHONUTF8"] = "1"

try:
    from dbt.cli.main import dbtRunner, dbtRunnerResult
except ImportError:
    print("Error: dbt-core is not installed or accessible.")
    print("Please install it using: pip install dbt-core dbt-sqlite")
    sys.exit(1)

def run_dbt_workflow():
    print("=== Starting dbt Automation Workflow ===\n")
    
    # Initialize dbtRunner
    dbt = dbtRunner()
    
    # Define tasks to execute in sequence
    tasks = [
        {
            "name": "dbt run",
            "args": ["run", "--profiles-dir", "."]
        },
        {
            "name": "dbt test",
            "args": ["test", "--profiles-dir", "."]
        },
        {
            "name": "dbt docs generate",
            "args": ["docs", "generate", "--profiles-dir", "."]
        }
    ]
    
    success = True
    
    # Execute each task
    for task in tasks:
        print(f"[{task['name']}] Executing...")
        print("-" * 40)
        
        # Invoke dbt via dbtRunner
        res: dbtRunnerResult = dbt.invoke(task["args"])
        
        print("-" * 40)
        if res.success:
            print(f"[{task['name']}] [OK] Completed Successfully.\n")
        else:
            print(f"[{task['name']}] [FAIL] Failed.\n")
            success = False
            # If a critical step like 'run' fails, we might want to stop the workflow.
            # But let's proceed to see full errors or if following steps can still run.
            # You can add `break` here if you prefer aborting on failure.

    if success:
        print("=== [DONE] All dbt tasks completed successfully! ===")
    else:
        print("=== [WARN] Workflow finished with some errors. Please check the logs above. ===")
        sys.exit(1)

if __name__ == "__main__":
    run_dbt_workflow()
