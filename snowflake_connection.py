import snowflake.connector
from snowflake.connector import DictCursor
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def connect_to_snowflake():
    """
    Connect to Snowflake using environment variables
    """
    try:
        # Get credentials from environment variables
        user = os.getenv('SNOWFLAKE_USER')
        account = os.getenv('SNOWFLAKE_ACCOUNT')
        password = os.getenv('SNOWFLAKE_PASSWORD')
        role = os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')
        warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
        database = os.getenv('SNOWFLAKE_DATABASE', 'SNOWFLAKE')
        schema = os.getenv('SNOWFLAKE_SCHEMA', 'INFORMATION_SCHEMA')
        
        if not all([user, account, password]):
            raise ValueError("Missing required environment variables. Please check your .env file.")
        
        # Connection parameters from environment variables
        conn = snowflake.connector.connect(
            user=user,
            account=account,
            password=password,
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

def test_connection():
    """
    Test the Snowflake connection by running a simple query
    """
    conn = connect_to_snowflake()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            result = cursor.fetchone()
            print(f"Snowflake version: {result[0]}")
            
            # Test query to show current role and warehouse
            cursor.execute("SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()")
            result = cursor.fetchone()
            print(f"Current role: {result[0]}")
            print(f"Current warehouse: {result[1]}")
            print(f"Current database: {result[2]}")
            
            cursor.close()
            conn.close()
            print("Connection test completed successfully!")
            
        except Exception as e:
            print(f"Error testing connection: {e}")
    else:
        print("Failed to establish connection")

if __name__ == "__main__":
    test_connection()
