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

def execute_query(conn, query, description):
    """
    Execute a query and return results
    """
    try:
        cursor = conn.cursor()
        print(f"\n--- {description} ---")
        print(f"Query: {query}")
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        if results:
            print(f"Results: {results}")
            return results
        else:
            print("No results found")
            return []
        
        cursor.close()
        
    except Exception as e:
        print(f"Error executing query: {e}")
        return []

def answer_warehouse_questions():
    """
    Answer the warehouse management questions by executing SQL commands
    """
    conn = connect_to_snowflake()
    if not conn:
        return
    
    print("="*70)
    print("ANSWERING WAREHOUSE MANAGEMENT QUESTIONS")
    print("="*70)
    
    # Question 1: Create warehouse_one and check its size
    print("\n" + "="*50)
    print("QUESTION 1: Create warehouse_one and check size")
    print("="*50)
    
    # Create warehouse_one
    create_warehouse1 = "CREATE WAREHOUSE warehouse_one;"
    execute_query(conn, create_warehouse1, "Creating warehouse_one")
    
    # Show warehouses to see metadata
    show_warehouses1 = "SHOW WAREHOUSES;"
    results1 = execute_query(conn, show_warehouses1, "Showing warehouses after creating warehouse_one")
    
    if results1:
        # Find warehouse_one in the results
        for row in results1:
            if 'warehouse_one' in str(row).lower():
                print(f"Warehouse_one details: {row}")
                # The size is typically in a specific column position
                # Let's get the column names first
                cursor = conn.cursor()
                cursor.execute("SHOW WAREHOUSES;")
                columns = [desc[0] for desc in cursor.description]
                print(f"Column names: {columns}")
                cursor.close()
                break
    
    # Question 2: Create warehouse_two, switch to it, and check is_current
    print("\n" + "="*50)
    print("QUESTION 2: Create warehouse_two and check is_current status")
    print("="*50)
    
    # Create warehouse_two
    create_warehouse2 = "CREATE WAREHOUSE warehouse_two;"
    execute_query(conn, create_warehouse2, "Creating warehouse_two")
    
    # Switch to warehouse_two
    use_warehouse2 = "USE WAREHOUSE warehouse_two;"
    execute_query(conn, use_warehouse2, "Switching to warehouse_two")
    
    # Show warehouses to see is_current status
    show_warehouses2 = "SHOW WAREHOUSES;"
    results2 = execute_query(conn, show_warehouses2, "Showing warehouses after switching to warehouse_two")
    
    if results2:
        print("Analyzing is_current status...")
        for row in results2:
            print(f"Warehouse details: {row}")
    
    # Question 3: Drop warehouse_two
    print("\n" + "="*50)
    print("QUESTION 3: Drop warehouse_two")
    print("="*50)
    
    drop_warehouse2 = "DROP WAREHOUSE warehouse_two;"
    results3 = execute_query(conn, drop_warehouse2, "Dropping warehouse_two")
    
    # Question 4: Alter warehouse_one to SMALL size
    print("\n" + "="*50)
    print("QUESTION 4: Alter warehouse_one to SMALL size")
    print("="*50)
    
    alter_warehouse1_size = "ALTER WAREHOUSE warehouse_one SET warehouse_size = 'SMALL';"
    execute_query(conn, alter_warehouse1_size, "Altering warehouse_one to SMALL size")
    
    # Show warehouses to see the size
    show_warehouses4 = "SHOW WAREHOUSES;"
    results4 = execute_query(conn, show_warehouses4, "Showing warehouses after altering warehouse_one size")
    
    if results4:
        print("Analyzing size column...")
        for row in results4:
            print(f"Warehouse details: {row}")
    
    # Question 5: Set auto_suspend to 2 minutes
    print("\n" + "="*50)
    print("QUESTION 5: Set auto_suspend to 2 minutes")
    print("="*50)
    
    alter_auto_suspend = "ALTER WAREHOUSE warehouse_one SET auto_suspend = 120;"
    execute_query(conn, alter_auto_suspend, "Setting auto_suspend to 120 seconds (2 minutes)")
    
    # Show warehouses to see auto_suspend
    show_warehouses5 = "SHOW WAREHOUSES;"
    results5 = execute_query(conn, show_warehouses5, "Showing warehouses after setting auto_suspend")
    
    if results5:
        print("Analyzing auto_suspend column...")
        for row in results5:
            print(f"Warehouse details: {row}")
    
    # Question 6: This is a knowledge question about warehouse sizes
    print("\n" + "="*50)
    print("QUESTION 6: Warehouse size comparison (Knowledge question)")
    print("="*50)
    print("This is a knowledge question about Snowflake warehouse sizes.")
    print("In Snowflake, warehouse sizes follow this pattern:")
    print("- X-Small: 1 cluster")
    print("- Small: 1 cluster") 
    print("- Medium: 2 clusters (2X)")
    print("- Large: 4 clusters (4X)")
    print("- X-Large: 8 clusters (8X)")
    print("- 2X-Large: 16 clusters (16X)")
    print("- 3X-Large: 32 clusters (32X)")
    print("- 4X-Large: 64 clusters (64X)")
    print("- 5X-Large: 128 clusters (128X)")
    print("\nSo a LARGE warehouse is 4X larger than a SMALL warehouse.")
    
    # Clean up - drop warehouse_one
    print("\n" + "="*50)
    print("CLEANUP: Dropping warehouse_one")
    print("="*50)
    
    drop_warehouse1 = "DROP WAREHOUSE warehouse_one;"
    execute_query(conn, drop_warehouse1, "Dropping warehouse_one for cleanup")
    
    conn.close()
    print("\n" + "="*70)
    print("WAREHOUSE QUESTIONS ANALYSIS COMPLETE")
    print("="*70)

if __name__ == "__main__":
    answer_warehouse_questions()
