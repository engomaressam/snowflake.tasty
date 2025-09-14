import snowflake.connector
from snowflake.connector import DictCursor
import pandas as pd
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
        database = os.getenv('SNOWFLAKE_DATABASE', 'SNOWFLAKE')
        schema = os.getenv('SNOWFLAKE_SCHEMA', 'INFORMATION_SCHEMA')
        
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

def execute_sql_script(conn, sql_script, description):
    """
    Execute a SQL script and return results
    """
    try:
        cursor = conn.cursor()
        print(f"\n--- {description} ---")
        print(f"Executing: {sql_script[:100]}...")
        
        cursor.execute(sql_script)
        
        # Try to fetch results if it's a SELECT statement
        try:
            results = cursor.fetchall()
            if results:
                print(f"Results: {results}")
                return results
            else:
                print("Query executed successfully (no results to display)")
                return []
        except:
            print("Query executed successfully (no results to display)")
            return []
        
        cursor.close()
        return True
        
    except Exception as e:
        print(f"Error executing SQL: {e}")
        return False

def setup_tasty_bytes_data():
    """
    Complete setup for Tasty Bytes sample data
    """
    conn = connect_to_snowflake()
    if not conn:
        return False
    
    # SQL scripts for the assignment
    sql_scripts = [
        {
            "sql": "USE ROLE accountadmin;",
            "description": "Setting role to accountadmin"
        },
        {
            "sql": "USE WAREHOUSE compute_wh;",
            "description": "Setting warehouse to compute_wh"
        },
        {
            "sql": "CREATE OR REPLACE DATABASE tasty_bytes_sample_data;",
            "description": "Creating Tasty Bytes database"
        },
        {
            "sql": "CREATE OR REPLACE SCHEMA tasty_bytes_sample_data.raw_pos;",
            "description": "Creating raw_pos schema"
        },
        {
            "sql": """CREATE OR REPLACE TABLE tasty_bytes_sample_data.raw_pos.menu
(
    menu_id NUMBER(19,0),
    menu_type_id NUMBER(38,0),
    menu_type VARCHAR(16777216),
    truck_brand_name VARCHAR(16777216),
    menu_item_id NUMBER(38,0),
    menu_item_name VARCHAR(16777216),
    item_category VARCHAR(16777216),
    item_subcategory VARCHAR(16777216),
    cost_of_goods_usd NUMBER(38,4),
    sale_price_usd NUMBER(38,4),
    menu_item_health_metrics_obj VARIANT
);""",
            "description": "Creating menu table with proper structure"
        },
        {
            "sql": "SELECT * FROM tasty_bytes_sample_data.raw_pos.menu;",
            "description": "Verifying empty table exists"
        },
        {
            "sql": """CREATE OR REPLACE STAGE tasty_bytes_sample_data.public.blob_stage
url = 's3://sfquickstarts/tastybytes/'
file_format = (type = csv);""",
            "description": "Creating stage referencing S3 blob location"
        },
        {
            "sql": "LIST @tasty_bytes_sample_data.public.blob_stage/raw_pos/menu/;",
            "description": "Querying stage to find menu CSV file"
        },
        {
            "sql": """COPY INTO tasty_bytes_sample_data.raw_pos.menu
FROM @tasty_bytes_sample_data.public.blob_stage/raw_pos/menu/;""",
            "description": "Copying menu data from stage to table"
        }
    ]
    
    # Execute all SQL scripts
    success_count = 0
    for script in sql_scripts:
        if execute_sql_script(conn, script["sql"], script["description"]):
            success_count += 1
    
    # Verification queries
    verification_queries = [
        {
            "sql": "SELECT COUNT(*) AS row_count FROM tasty_bytes_sample_data.raw_pos.menu;",
            "description": "Counting rows in menu table"
        },
        {
            "sql": "SELECT TOP 10 * FROM tasty_bytes_sample_data.raw_pos.menu;",
            "description": "Showing top 10 rows from menu table"
        },
        {
            "sql": """SELECT TRUCK_BRAND_NAME, COUNT(*)
FROM tasty_bytes_sample_data.raw_pos.menu
GROUP BY 1
ORDER BY 2 DESC;""",
            "description": "Grouping by truck brand name"
        },
        {
            "sql": """SELECT
    TRUCK_BRAND_NAME,
    MENU_TYPE,
    COUNT(*)
FROM tasty_bytes_sample_data.raw_pos.menu
GROUP BY 1,2
ORDER BY 3 DESC;""",
            "description": "Grouping by truck brand name and menu type"
        }
    ]
    
    print("\n" + "="*50)
    print("VERIFICATION QUERIES")
    print("="*50)
    
    for query in verification_queries:
        execute_sql_script(conn, query["sql"], query["description"])
    
    print(f"\nSetup completed! {success_count}/{len(sql_scripts)} SQL scripts executed successfully.")
    
    conn.close()
    return True

if __name__ == "__main__":
    print("Starting Tasty Bytes Sample Data Setup with Password Authentication...")
    setup_tasty_bytes_data()
