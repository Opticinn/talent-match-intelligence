from db_connection import get_db_connection, execute_query

def test_connection():
    conn = get_db_connection()
    if conn:
        print("✅ Connected to Supabase Cloud!")
        
        # Test query
        result = execute_query("SELECT COUNT(*) as count FROM raw.employees")
        if result is not None:
            print(f"✅ Employees count: {result['count'].iloc[0]}")
        else:
            print("❌ Query failed")
        
        conn.close()
    else:
        print("❌ Connection failed")

test_connection()