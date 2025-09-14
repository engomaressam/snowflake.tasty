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

def check_franchise_matching():
    """
    Check if truck and franchise tables have matching franchise_ids
    """
    conn = connect_to_snowflake()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        # Switch to public schema
        cursor.execute("USE SCHEMA public;")
        
        # Check franchise_id ranges
        print("=== FRANCHISE_ID RANGES ===")
        cursor.execute("SELECT MIN(franchise_id), MAX(franchise_id), COUNT(*) FROM franchise;")
        franchise_range = cursor.fetchone()
        print(f"Franchise table: MIN={franchise_range[0]}, MAX={franchise_range[1]}, COUNT={franchise_range[2]}")
        
        cursor.execute("SELECT MIN(franchise_id), MAX(franchise_id), COUNT(*) FROM truck;")
        truck_range = cursor.fetchone()
        print(f"Truck table: MIN={truck_range[0]}, MAX={truck_range[1]}, COUNT={truck_range[2]}")
        
        # Check for overlapping franchise_ids
        print("\n=== OVERLAPPING FRANCHISE_IDS ===")
        cursor.execute("""SELECT DISTINCT t.franchise_id, t.menu_type, f.first_name, f.last_name
FROM truck t
JOIN franchise f ON t.franchise_id = f.franchise_id
LIMIT 10;""")
        overlapping = cursor.fetchall()
        print("Overlapping franchise_ids:")
        for row in overlapping:
            print(f"  ID {row[0]}: {row[2]} {row[3]} - {row[1]}")
        
        # Check if there are any matches at all
        cursor.execute("""SELECT COUNT(*) FROM truck t
JOIN franchise f ON t.franchise_id = f.franchise_id;""")
        match_count = cursor.fetchone()[0]
        print(f"\nTotal matching records: {match_count}")
        
        # If no matches, let's see what franchise_ids exist in each table
        print("\n=== SAMPLE FRANCHISE_IDS FROM EACH TABLE ===")
        cursor.execute("SELECT franchise_id FROM franchise ORDER BY franchise_id LIMIT 10;")
        franchise_ids = cursor.fetchall()
        print("Franchise IDs from franchise table:")
        for row in franchise_ids:
            print(f"  {row[0]}")
        
        cursor.execute("SELECT franchise_id FROM truck ORDER BY franchise_id LIMIT 10;")
        truck_franchise_ids = cursor.fetchall()
        print("Franchise IDs from truck table:")
        for row in truck_franchise_ids:
            print(f"  {row[0]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        if conn:
            conn.close()

if __name__ == "__main__":
    check_franchise_matching()
