import snowflake.connector
from snowflake.connector import DictCursor
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def connect_to_snowflake():
    """
    Connect to Snowflake using password authentication
    """
    try:
        # Get credentials from environment variables
        user = os.getenv('SNOWFLAKE_USER')
        password = os.getenv('SNOWFLAKE_PASSWORD')
        account = os.getenv('SNOWFLAKE_ACCOUNT')
        role = os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')
        warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
        database = os.getenv('SNOWFLAKE_DATABASE', 'tasty_bytes_sample_data')
        schema = os.getenv('SNOWFLAKE_SCHEMA', 'raw_pos')
        
        if not all([user, password, account]):
            raise ValueError("Missing required environment variables. Please check your .env file.")
        
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            role=role,
            warehouse=warehouse,
            database=database,
            schema=schema
        )
        print("Successfully connected to Snowflake!")
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        return None

def debug_columns():
    """
    Debug the column structure of SHOW DATABASES and SHOW SCHEMAS
    """
    conn = connect_to_snowflake()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        # Create test database
        cursor.execute("CREATE DATABASE test_debug;")
        cursor.execute("USE DATABASE test_debug;")
        cursor.execute("CREATE SCHEMA test_schema;")
        
        # Debug SHOW DATABASES
        print("=== SHOW DATABASES COLUMNS ===")
        cursor.execute("SHOW DATABASES;")
        columns = [desc[0] for desc in cursor.description]
        print(f"SHOW DATABASES columns: {columns}")
        
        results = cursor.fetchall()
        if results:
            for i, row in enumerate(results):
                if 'test_debug' in str(row).lower():
                    print(f"test_debug row: {row}")
                    for j, value in enumerate(row):
                        print(f"  Column {j} ({columns[j] if j < len(columns) else 'Unknown'}): {value}")
                    break
        
        # Debug SHOW SCHEMAS
        print("\n=== SHOW SCHEMAS COLUMNS ===")
        cursor.execute("SHOW SCHEMAS;")
        columns = [desc[0] for desc in cursor.description]
        print(f"SHOW SCHEMAS columns: {columns}")
        
        results = cursor.fetchall()
        if results:
            for i, row in enumerate(results):
                if 'test_schema' in str(row).lower():
                    print(f"test_schema row: {row}")
                    for j, value in enumerate(row):
                        print(f"  Column {j} ({columns[j] if j < len(columns) else 'Unknown'}): {value}")
                    break
        
        # Debug DESCRIBE DATABASE
        print("\n=== DESCRIBE DATABASE COLUMNS ===")
        cursor.execute("DESCRIBE DATABASE test_debug;")
        columns = [desc[0] for desc in cursor.description]
        print(f"DESCRIBE DATABASE columns: {columns}")
        
        results = cursor.fetchall()
        if results:
            for i, row in enumerate(results):
                if 'test_schema' in str(row).lower():
                    print(f"test_schema row: {row}")
                    for j, value in enumerate(row):
                        print(f"  Column {j} ({columns[j] if j < len(columns) else 'Unknown'}): {value}")
                    break
        
        # Clean up
        cursor.execute("DROP DATABASE test_debug;")
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        if conn:
            conn.close()

if __name__ == "__main__":
    debug_columns()
