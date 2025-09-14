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

def debug_truck_data():
    """
    Debug the truck data structure and find Sara Nicholson
    """
    conn = connect_to_snowflake()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        # Switch to public schema
        cursor.execute("USE SCHEMA public;")
        
        # Check truck table structure
        print("=== TRUCK TABLE STRUCTURE ===")
        cursor.execute("DESCRIBE TABLE truck;")
        columns = cursor.fetchall()
        for col in columns:
            print(f"Column: {col[0]} - Type: {col[1]}")
        
        # Check sample truck data
        print("\n=== SAMPLE TRUCK DATA ===")
        cursor.execute("SELECT * FROM truck LIMIT 5;")
        truck_data = cursor.fetchall()
        for i, row in enumerate(truck_data):
            print(f"Row {i+1}: {row}")
        
        # Check franchise table structure
        print("\n=== FRANCHISE TABLE STRUCTURE ===")
        cursor.execute("DESCRIBE TABLE franchise;")
        columns = cursor.fetchall()
        for col in columns:
            print(f"Column: {col[0]} - Type: {col[1]}")
        
        # Check sample franchise data
        print("\n=== SAMPLE FRANCHISE DATA ===")
        cursor.execute("SELECT * FROM franchise LIMIT 5;")
        franchise_data = cursor.fetchall()
        for i, row in enumerate(franchise_data):
            print(f"Row {i+1}: {row}")
        
        # Find Sara Nicholson
        print("\n=== FINDING SARA NICHOLSON ===")
        cursor.execute("SELECT * FROM franchise WHERE first_name = 'Sara' AND last_name = 'Nicholson';")
        sara_data = cursor.fetchall()
        if sara_data:
            print(f"Sara Nicholson found: {sara_data[0]}")
            sara_franchise_id = sara_data[0][0]
            print(f"Sara's franchise_id: {sara_franchise_id}")
            
            # Find Sara's truck
            cursor.execute(f"SELECT * FROM truck WHERE franchise_id = {sara_franchise_id};")
            sara_truck = cursor.fetchall()
            if sara_truck:
                print(f"Sara's truck: {sara_truck[0]}")
            else:
                print("No truck found for Sara")
        else:
            print("Sara Nicholson not found")
        
        # Check all franchise names to see if there's a similar name
        print("\n=== CHECKING SIMILAR NAMES ===")
        cursor.execute("SELECT first_name, last_name FROM franchise WHERE first_name LIKE '%Sara%' OR last_name LIKE '%Nicholson%';")
        similar_names = cursor.fetchall()
        for name in similar_names:
            print(f"Similar name: {name}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        if conn:
            conn.close()

if __name__ == "__main__":
    debug_truck_data()
