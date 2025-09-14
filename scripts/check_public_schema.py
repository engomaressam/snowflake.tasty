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

def check_public_schema():
    """
    Check what's in the PUBLIC schema
    """
    conn = connect_to_snowflake()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        # Check PUBLIC schema
        print("=== TABLES IN PUBLIC SCHEMA ===")
        cursor.execute("USE SCHEMA public;")
        cursor.execute("SHOW TABLES;")
        tables = cursor.fetchall()
        for table in tables:
            print(f"Table: {table[1]}")
        
        # Check if there are any stages
        print("\n=== STAGES IN PUBLIC SCHEMA ===")
        cursor.execute("SHOW STAGES;")
        stages = cursor.fetchall()
        for stage in stages:
            print(f"Stage: {stage[1]}")
        
        # Check if we can load truck data
        print("\n=== CHECKING S3 STAGE FOR TRUCK DATA ===")
        try:
            cursor.execute("LIST @blob_stage/raw_pos/truck/;")
            files = cursor.fetchall()
            for file in files:
                print(f"File: {file[0]}")
        except Exception as e:
            print(f"Stage listing error: {e}")
        
        # Try to create truck table and load data
        print("\n=== CREATING TRUCK TABLE ===")
        try:
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
                truck_brand_name VARCHAR(16777216),
                truck_opening_date DATE,
                truck_closing_date DATE,
                truck_id_hashed VARCHAR(16777216),
                truck_name VARCHAR(16777216),
                truck_plan VARCHAR(16777216),
                truck_type_id NUMBER(38,0),
                truck_type VARCHAR(16777216),
                year_built NUMBER(38,0),
                year_retired NUMBER(38,0),
                make VARCHAR(16777216)
            );""")
            print("Truck table created successfully")
            
            # Try to load data
            cursor.execute("COPY INTO truck FROM @blob_stage/raw_pos/truck/;")
            load_result = cursor.fetchall()
            print(f"Load result: {load_result}")
            
            # Check row count
            cursor.execute("SELECT COUNT(*) FROM truck;")
            count = cursor.fetchone()[0]
            print(f"Truck table has {count} rows")
            
        except Exception as e:
            print(f"Truck table creation/loading error: {e}")
        
        # Try to create franchise table
        print("\n=== CREATING FRANCHISE TABLE ===")
        try:
            cursor.execute("""CREATE OR REPLACE TABLE franchise (
                franchise_id NUMBER(38,0),
                first_name VARCHAR(16777216),
                last_name VARCHAR(16777216),
                city VARCHAR(16777216),
                country VARCHAR(16777216),
                phone_number VARCHAR(16777216),
                email VARCHAR(16777216),
                hire_date DATE,
                franchise_flag VARCHAR(16777216),
                franchise_agreement_number VARCHAR(16777216),
                franchise_agreement_date DATE
            );""")
            print("Franchise table created successfully")
            
            # Try to load data
            cursor.execute("COPY INTO franchise FROM @blob_stage/raw_pos/franchise/;")
            load_result = cursor.fetchall()
            print(f"Load result: {load_result}")
            
            # Check row count
            cursor.execute("SELECT COUNT(*) FROM franchise;")
            count = cursor.fetchone()[0]
            print(f"Franchise table has {count} rows")
            
        except Exception as e:
            print(f"Franchise table creation/loading error: {e}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        if conn:
            conn.close()

if __name__ == "__main__":
    check_public_schema()
