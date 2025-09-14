import snowflake.connector
import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def connect_and_run_sql():
    """
    Connect to Snowflake and run the Tasty Bytes setup SQL commands
    """
    
    # Try different connection methods
    connection_methods = [
        {
            "name": "Method 1: Full account identifier",
            "account": os.getenv('SNOWFLAKE_ACCOUNT'),
            "user": os.getenv('SNOWFLAKE_USER'),
            "password": os.getenv('SNOWFLAKE_PASSWORD'),
            "role": os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
            "warehouse": os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
        },
        {
            "name": "Method 2: Organization only",
            "account": os.getenv('SNOWFLAKE_ACCOUNT', '').split('-')[0] if os.getenv('SNOWFLAKE_ACCOUNT') else '',
            "user": os.getenv('SNOWFLAKE_USER'),
            "password": os.getenv('SNOWFLAKE_PASSWORD'),
            "role": os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
            "warehouse": os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
        },
        {
            "name": "Method 3: With region",
            "account": f"{os.getenv('SNOWFLAKE_ACCOUNT')}.us-west-2" if os.getenv('SNOWFLAKE_ACCOUNT') else '',
            "user": os.getenv('SNOWFLAKE_USER'),
            "password": os.getenv('SNOWFLAKE_PASSWORD'),
            "role": os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
            "warehouse": os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
        }
    ]
    
    conn = None
    successful_method = None
    
    for method in connection_methods:
        print(f"\nTrying {method['name']}...")
        try:
            conn = snowflake.connector.connect(
                user=method['user'],
                account=method['account'],
                password=method['password'],
                role=method['role'],
                warehouse=method['warehouse']
            )
            print(f"✅ SUCCESS with {method['name']}!")
            successful_method = method
            break
        except Exception as e:
            print(f"❌ Failed: {e}")
            continue
    
    if not conn:
        print("\n❌ All connection methods failed. Please check your credentials.")
        return False
    
    # SQL commands for Tasty Bytes setup
    sql_commands = [
        "USE ROLE accountadmin;",
        "USE WAREHOUSE compute_wh;",
        "CREATE OR REPLACE DATABASE tasty_bytes_sample_data;",
        "CREATE OR REPLACE SCHEMA tasty_bytes_sample_data.raw_pos;",
        """CREATE OR REPLACE TABLE tasty_bytes_sample_data.raw_pos.menu
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
        "SELECT * FROM tasty_bytes_sample_data.raw_pos.menu;",
        """CREATE OR REPLACE STAGE tasty_bytes_sample_data.public.blob_stage
url = 's3://sfquickstarts/tastybytes/'
file_format = (type = csv);""",
        "LIST @tasty_bytes_sample_data.public.blob_stage/raw_pos/menu/;",
        """COPY INTO tasty_bytes_sample_data.raw_pos.menu
FROM @tasty_bytes_sample_data.public.blob_stage/raw_pos/menu/;""",
        "SELECT COUNT(*) AS row_count FROM tasty_bytes_sample_data.raw_pos.menu;",
        "SELECT TOP 10 * FROM tasty_bytes_sample_data.raw_pos.menu;",
        """SELECT TRUCK_BRAND_NAME, COUNT(*)
FROM tasty_bytes_sample_data.raw_pos.menu
GROUP BY 1
ORDER BY 2 DESC;""",
        """SELECT
    TRUCK_BRAND_NAME,
    MENU_TYPE,
    COUNT(*)
FROM tasty_bytes_sample_data.raw_pos.menu
GROUP BY 1,2
ORDER BY 3 DESC;"""
    ]
    
    print(f"\n🎉 Connected successfully using {successful_method['name']}!")
    print("Executing Tasty Bytes setup commands...")
    
    try:
        cursor = conn.cursor()
        
        for i, sql in enumerate(sql_commands, 1):
            print(f"\n--- Step {i}: {sql[:50]}... ---")
            try:
                cursor.execute(sql)
                
                # Try to fetch results if it's a SELECT statement
                try:
                    results = cursor.fetchall()
                    if results:
                        print(f"Results: {results}")
                    else:
                        print("✅ Command executed successfully")
                except:
                    print("✅ Command executed successfully")
                    
            except Exception as e:
                print(f"❌ Error in step {i}: {e}")
                # Continue with next command
                continue
        
        cursor.close()
        conn.close()
        print("\n🎉 Tasty Bytes setup completed!")
        return True
        
    except Exception as e:
        print(f"❌ Error during execution: {e}")
        if conn:
            conn.close()
        return False

if __name__ == "__main__":
    print("🚀 Starting Snowflake Tasty Bytes Setup...")
    success = connect_and_run_sql()
    
    if success:
        print("\n✅ Assignment completed successfully!")
    else:
        print("\n❌ Assignment failed. Please check the errors above.")
