# test_connection.py
from db_connection import execute_query

# Test query sederhana
result = execute_query("SELECT employee_id, fullname FROM raw.employees LIMIT 5")
if result is not None:
    print("✅ Database connection successful!")
    print(result)
else:
    print("❌ Database connection failed")