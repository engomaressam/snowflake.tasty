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

def find_truck_makes():
    """
    Find available truck makes and franchisees with specific makes
    """
    conn = connect_to_snowflake()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        # Switch to public schema
        cursor.execute("USE SCHEMA public;")
        
        # Check all unique truck makes
        print("=== ALL UNIQUE TRUCK MAKES ===")
        cursor.execute("SELECT DISTINCT menu_type FROM truck ORDER BY menu_type;")
        makes = cursor.fetchall()
        for make in makes:
            print(f"Make: {make[0]}")
        
        # Find franchisees with specific makes
        print("\n=== FRANCHISEES WITH SPECIFIC MAKES ===")
        
        # Check for Chevrolet
        cursor.execute("""SELECT f.first_name, f.last_name, t.menu_type
FROM truck t
JOIN franchise f ON t.franchise_id = f.franchise_id
WHERE t.menu_type = 'Chevrolet'
LIMIT 5;""")
        chevrolet_data = cursor.fetchall()
        print("Chevrolet trucks:")
        for row in chevrolet_data:
            print(f"  {row[0]} {row[1]} - {row[2]}")
        
        # Check for Volkswagen
        cursor.execute("""SELECT f.first_name, f.last_name, t.menu_type
FROM truck t
JOIN franchise f ON t.franchise_id = f.franchise_id
WHERE t.menu_type = 'Volkswagen'
LIMIT 5;""")
        volkswagen_data = cursor.fetchall()
        print("Volkswagen trucks:")
        for row in volkswagen_data:
            print(f"  {row[0]} {row[1]} - {row[2]}")
        
        # Check for Airstream
        cursor.execute("""SELECT f.first_name, f.last_name, t.menu_type
FROM truck t
JOIN franchise f ON t.franchise_id = f.franchise_id
WHERE t.menu_type = 'Airstream'
LIMIT 5;""")
        airstream_data = cursor.fetchall()
        print("Airstream trucks:")
        for row in airstream_data:
            print(f"  {row[0]} {row[1]} - {row[2]}")
        
        # Check for Nissan
        cursor.execute("""SELECT f.first_name, f.last_name, t.menu_type
FROM truck t
JOIN franchise f ON t.franchise_id = f.franchise_id
WHERE t.menu_type = 'Nissan'
LIMIT 5;""")
        nissan_data = cursor.fetchall()
        print("Nissan trucks:")
        for row in nissan_data:
            print(f"  {row[0]} {row[1]} - {row[2]}")
        
        # If none of the specific makes exist, find someone with any truck
        print("\n=== ANY FRANCHISEE WITH A TRUCK ===")
        cursor.execute("""SELECT f.first_name, f.last_name, t.menu_type
FROM truck t
JOIN franchise f ON t.franchise_id = f.franchise_id
LIMIT 5;""")
        any_truck_data = cursor.fetchall()
        print("Any trucks:")
        for row in any_truck_data:
            print(f"  {row[0]} {row[1]} - {row[2]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        if conn:
            conn.close()

if __name__ == "__main__":
    find_truck_makes()
