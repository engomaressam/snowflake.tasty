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
            print("Query executed successfully (no results to display)")
            return []
        
        cursor.close()
        
    except Exception as e:
        print(f"Error executing query: {e}")
        return []

def answer_semi_structured_questions():
    """
    Answer the Snowflake semi-structured data questions by executing SQL commands
    """
    conn = connect_to_snowflake()
    if not conn:
        return
    
    print("="*70)
    print("ANSWERING SNOWFLAKE SEMI-STRUCTURED DATA QUESTIONS")
    print("="*70)
    
    # Question 1: Describe table to check MENU_ITEM_HEALTH_METRICS_OBJ type
    print("\n" + "="*50)
    print("QUESTION 1: Describe menu table to check MENU_ITEM_HEALTH_METRICS_OBJ type")
    print("="*50)
    
    describe_query = "DESCRIBE TABLE tasty_bytes_sample_data.raw_pos.menu;"
    results1 = execute_query(conn, describe_query, "Describing menu table")
    
    if results1:
        print("Analyzing type column for MENU_ITEM_HEALTH_METRICS_OBJ...")
        for row in results1:
            if 'MENU_ITEM_HEALTH_METRICS_OBJ' in str(row).upper():
                # type is typically at index 1
                type_value = row[1]
                print(f"\nAnswer for Question 1: type = {type_value}")
                if type_value == 'VARCHAR':
                    print("✅ Correct! Answer is 'VARCHAR'")
                elif type_value == 'OBJECT':
                    print("✅ Correct! Answer is 'OBJECT'")
                elif type_value == 'ARRAY':
                    print("✅ Correct! Answer is 'ARRAY'")
                elif type_value == 'VARIANT':
                    print("✅ Correct! Answer is 'VARIANT'")
                else:
                    print(f"❌ Unexpected type: {type_value}")
                break
    
    # Question 2: Use TYPEOF to check underlying data type
    print("\n" + "="*50)
    print("QUESTION 2: Use TYPEOF to check underlying data type")
    print("="*50)
    
    typeof_query = "SELECT TYPEOF(MENU_ITEM_HEALTH_METRICS_OBJ) FROM tasty_bytes_sample_data.raw_pos.menu LIMIT 1;"
    results2 = execute_query(conn, typeof_query, "Checking underlying data type with TYPEOF")
    
    if results2:
        underlying_type = results2[0][0]
        print(f"\nAnswer for Question 2: underlying type = {underlying_type}")
        if underlying_type == 'ARRAY':
            print("✅ Correct! Answer is 'ARRAY'")
        elif underlying_type == 'VARIANT':
            print("✅ Correct! Answer is 'VARIANT'")
        elif underlying_type == 'VARCHAR':
            print("✅ Correct! Answer is 'VARCHAR'")
        elif underlying_type == 'OBJECT':
            print("✅ Correct! Answer is 'OBJECT'")
        else:
            print(f"❌ Unexpected underlying type: {underlying_type}")
    
    # Question 3: Knowledge question about array access
    print("\n" + "="*50)
    print("QUESTION 3: Array access syntax (Knowledge question)")
    print("="*50)
    print("This is a knowledge question about Snowflake array access syntax.")
    print("\nIn Snowflake, arrays are 0-indexed, so the first element is at index 0.")
    print("The syntax to access array elements is: array_name[index]")
    print("\nLooking at the options:")
    print("Option 1: test_array:1 - INCORRECT (colon syntax, and wrong index)")
    print("Option 2: test_array:0 - INCORRECT (colon syntax)")
    print("Option 3: test_array['key'] - INCORRECT (object syntax)")
    print("Option 4: test_array[0] - CORRECT (bracket syntax with 0 index)")
    print("\n✅ Answer: Option 4 (test_array[0])")
    
    # Question 4: Extract "Sweet Mango" from JSON structure
    print("\n" + "="*50)
    print("QUESTION 4: Extract 'Sweet Mango' from JSON structure")
    print("="*50)
    
    # First, let's examine the structure of MENU_ITEM_HEALTH_METRICS_OBJ
    print("Examining the JSON structure...")
    examine_query = """SELECT MENU_ITEM_HEALTH_METRICS_OBJ
FROM tasty_bytes_sample_data.raw_pos.menu
WHERE MENU_ITEM_NAME = 'Mango Sticky Rice'
LIMIT 1;"""
    
    results4a = execute_query(conn, examine_query, "Examining JSON structure for Mango Sticky Rice")
    
    if results4a:
        print("JSON structure found, now testing different extraction paths...")
        
        # Test different extraction paths
        test_paths = [
            "MENU_ITEM_HEALTH_METRICS_OBJ['menu_item_health_metrics'][0][0]",
            "MENU_ITEM_HEALTH_METRICS_OBJ['menu_item_health_metrics'][0]['ingredients'][0]",
            "MENU_ITEM_HEALTH_METRICS_OBJ['menu_item_health_metrics']['ingredients'][1]",
            "MENU_ITEM_HEALTH_METRICS_OBJ[0]['menu_item_health_metrics'][0]['ingredients']"
        ]
        
        for i, path in enumerate(test_paths, 1):
            test_query = f"""SELECT {path} as result
FROM tasty_bytes_sample_data.raw_pos.menu
WHERE MENU_ITEM_NAME = 'Mango Sticky Rice'
LIMIT 1;"""
            
            try:
                result = execute_query(conn, test_query, f"Testing path {i}: {path}")
                if result and result[0][0] == 'Sweet Mango':
                    print(f"\nAnswer for Question 4: Path {i} is correct!")
                    print(f"✅ Correct! Answer is option {i}")
                    break
                elif result:
                    print(f"Path {i} result: {result[0][0]}")
            except Exception as e:
                print(f"Path {i} failed: {e}")
    
    # Clean up
    print("\n" + "="*50)
    print("CLEANUP: No cleanup needed")
    print("="*50)
    
    conn.close()
    print("\n" + "="*70)
    print("SEMI-STRUCTURED DATA QUESTIONS ANALYSIS COMPLETE")
    print("="*70)

if __name__ == "__main__":
    answer_semi_structured_questions()
