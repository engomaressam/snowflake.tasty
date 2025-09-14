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

def load_truck_franchise_data():
    """
    Load truck and franchise data with correct table structures
    """
    conn = connect_to_snowflake()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        # Switch to public schema
        cursor.execute("USE SCHEMA public;")
        
        # Create truck table with correct structure (14 columns)
        print("=== CREATING TRUCK TABLE ===")
        cursor.execute("""CREATE OR REPLACE TABLE truck (
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
        print("Truck table created successfully")
        
        # Load truck data
        cursor.execute("COPY INTO truck FROM @blob_stage/raw_pos/truck/;")
        load_result = cursor.fetchall()
        print(f"Truck load result: {load_result}")
        
        # Check truck row count
        cursor.execute("SELECT COUNT(*) FROM truck;")
        truck_count = cursor.fetchone()[0]
        print(f"Truck table has {truck_count} rows")
        
        # Show sample truck data
        cursor.execute("SELECT * FROM truck LIMIT 3;")
        truck_samples = cursor.fetchall()
        print("Sample truck data:")
        for row in truck_samples:
            print(f"  {row}")
        
        # Create franchise table with correct structure (7 columns)
        print("\n=== CREATING FRANCHISE TABLE ===")
        cursor.execute("""CREATE OR REPLACE TABLE franchise (
            franchise_id NUMBER(38,0),
            first_name VARCHAR(16777216),
            last_name VARCHAR(16777216),
            city VARCHAR(16777216),
            country VARCHAR(16777216),
            phone_number VARCHAR(16777216),
            email VARCHAR(16777216)
        );""")
        print("Franchise table created successfully")
        
        # Load franchise data
        cursor.execute("COPY INTO franchise FROM @blob_stage/raw_pos/franchise/;")
        load_result = cursor.fetchall()
        print(f"Franchise load result: {load_result}")
        
        # Check franchise row count
        cursor.execute("SELECT COUNT(*) FROM franchise;")
        franchise_count = cursor.fetchone()[0]
        print(f"Franchise table has {franchise_count} rows")
        
        # Show sample franchise data
        cursor.execute("SELECT * FROM franchise LIMIT 3;")
        franchise_samples = cursor.fetchall()
        print("Sample franchise data:")
        for row in franchise_samples:
            print(f"  {row}")
        
        # Check if we can find Sara Nicholson
        print("\n=== CHECKING FOR SARA NICHOLSON ===")
        cursor.execute("SELECT * FROM franchise WHERE first_name = 'Sara' AND last_name = 'Nicholson';")
        sara_data = cursor.fetchall()
        if sara_data:
            print(f"Sara Nicholson found: {sara_data[0]}")
            sara_franchise_id = sara_data[0][0]
            
            # Find truck for Sara
            cursor.execute(f"SELECT * FROM truck WHERE franchise_id = {sara_franchise_id};")
            sara_truck = cursor.fetchall()
            if sara_truck:
                print(f"Sara's truck: {sara_truck[0]}")
                # The make should be in truck_brand_name or we need to check the actual structure
                print(f"Truck brand name: {sara_truck[0][13]}")
        else:
            print("Sara Nicholson not found")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        if conn:
            conn.close()

if __name__ == "__main__":
    load_truck_franchise_data()
