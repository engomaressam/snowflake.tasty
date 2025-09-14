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

def explore_database():
    """
    Explore the database structure to find available tables
    """
    conn = connect_to_snowflake()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        # Show databases
        print("=== AVAILABLE DATABASES ===")
        cursor.execute("SHOW DATABASES;")
        databases = cursor.fetchall()
        for db in databases:
            print(f"Database: {db[1]}")
        
        # Show schemas in tasty_bytes_sample_data
        print("\n=== SCHEMAS IN TASTY_BYTES_SAMPLE_DATA ===")
        cursor.execute("USE DATABASE tasty_bytes_sample_data;")
        cursor.execute("SHOW SCHEMAS;")
        schemas = cursor.fetchall()
        for schema in schemas:
            print(f"Schema: {schema[1]}")
        
        # Show tables in raw_pos schema
        print("\n=== TABLES IN RAW_POS SCHEMA ===")
        cursor.execute("USE SCHEMA raw_pos;")
        cursor.execute("SHOW TABLES;")
        tables = cursor.fetchall()
        for table in tables:
            print(f"Table: {table[1]}")
        
        # Check if franchise table exists
        print("\n=== CHECKING FOR FRANCHISE TABLE ===")
        try:
            cursor.execute("SELECT COUNT(*) FROM franchise;")
            franchise_count = cursor.fetchone()[0]
            print(f"Franchise table exists with {franchise_count} rows")
        except Exception as e:
            print(f"Franchise table error: {e}")
        
        # Check if truck table exists
        print("\n=== CHECKING FOR TRUCK TABLE ===")
        try:
            cursor.execute("SELECT COUNT(*) FROM truck;")
            truck_count = cursor.fetchone()[0]
            print(f"Truck table exists with {truck_count} rows")
        except Exception as e:
            print(f"Truck table error: {e}")
        
        # Show sample data from truck table
        print("\n=== SAMPLE TRUCK DATA ===")
        try:
            cursor.execute("SELECT * FROM truck LIMIT 5;")
            truck_data = cursor.fetchall()
            for row in truck_data:
                print(f"Truck data: {row}")
        except Exception as e:
            print(f"Truck data error: {e}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        if conn:
            conn.close()

if __name__ == "__main__":
    explore_database()
