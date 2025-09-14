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

def debug_copy_results():
    """
    Debug the COPY INTO results to find the correct column index
    """
    conn = connect_to_snowflake()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        # Create test database and table
        cursor.execute("USE WAREHOUSE compute_wh;")
        cursor.execute("CREATE DATABASE test_debug;")
        cursor.execute("""CREATE OR REPLACE FILE FORMAT test_debug.public.csv_ff
type = 'csv';""")
        cursor.execute("""CREATE OR REPLACE STAGE test_debug.public.test_stage
url = 's3://sfquickstarts/tasty-bytes-builder-education/raw_pos/truck'
file_format = test_debug.public.csv_ff;""")
        
        # Create table
        cursor.execute("""CREATE OR REPLACE TABLE test_debug.public.truck
(
    truck_id NUMBER(38,0),
    menu_type_id NUMBER(38,0),
    primary_city VARCHAR(16777216),
    region VARCHAR(16777216),
    iso_region VARCHAR(16777216),
    country VARCHAR(16777216),
    iso_country_code VARCHAR(16777216),
    franchise_flag VARCHAR(16777216),
    franchise_id NUMBER(38,0),
    menu_type VARCHAR(16777216),
    country_code_iso_2 VARCHAR(16777216),
    country_code_iso_3 VARCHAR(16777216),
    country_code_iso_numeric VARCHAR(16777216),
    truck_brand_name VARCHAR(16777216)
);""")
        
        # Copy data
        cursor.execute("""COPY INTO test_debug.public.truck
FROM @test_debug.public.test_stage;""")
        
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        print(f"COPY INTO result columns: {columns}")
        
        # Get the actual results
        results = cursor.fetchall()
        print(f"COPY INTO results: {results}")
        
        if results:
            row = results[0]
            print(f"Number of columns in result: {len(row)}")
            for i, value in enumerate(row):
                print(f"Column {i}: {value}")
        
        # Clean up
        cursor.execute("DROP DATABASE test_debug;")
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        if conn:
            conn.close()

if __name__ == "__main__":
    debug_copy_results()
