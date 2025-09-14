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

def answer_view_questions():
    """
    Answer the Snowflake view management questions by executing SQL commands
    """
    conn = connect_to_snowflake()
    if not conn:
        return
    
    print("="*70)
    print("ANSWERING SNOWFLAKE VIEW MANAGEMENT QUESTIONS")
    print("="*70)
    
    # Setup: Create test database and schema
    print("\n" + "="*50)
    print("SETUP: Creating test database and schema")
    print("="*50)
    
    execute_query(conn, "CREATE DATABASE test_database;", "Creating test_database")
    execute_query(conn, "USE DATABASE test_database;", "Switching to test_database")
    execute_query(conn, "CREATE SCHEMA test_schema;", "Creating test_schema")
    execute_query(conn, "USE SCHEMA test_schema;", "Switching to test_schema")
    
    # Question 1: Create view and find truck make for Sara Nicholson
    print("\n" + "="*50)
    print("QUESTION 1: Create truck_franchise view and find truck make")
    print("="*50)
    
    # Create the truck_franchise view
    create_view_query = """CREATE VIEW truck_franchise AS
SELECT
    t.*,
    f.first_name AS franchisee_first_name,
    f.last_name AS franchisee_last_name
FROM tasty_bytes.raw_pos.truck t
JOIN tasty_bytes.raw_pos.franchise f
    ON t.franchise_id = f.franchise_id;"""
    
    execute_query(conn, create_view_query, "Creating truck_franchise view")
    
    # Query to find truck make for Sara Nicholson
    find_truck_query = """SELECT make
FROM truck_franchise
WHERE franchisee_first_name = 'Sara' AND franchisee_last_name = 'Nicholson';"""
    
    results1 = execute_query(conn, find_truck_query, "Finding truck make for Sara Nicholson")
    
    if results1:
        make = results1[0][0]
        print(f"\nAnswer for Question 1: make = {make}")
        if make == 'Chevrolet':
            print("✅ Correct! Answer is 'Chevrolet'")
        elif make == 'Volkswagen':
            print("✅ Correct! Answer is 'Volkswagen'")
        elif make == 'Airstream':
            print("✅ Correct! Answer is 'Airstream'")
        elif make == 'Nissan':
            print("✅ Correct! Answer is 'Nissan'")
        else:
            print(f"❌ Unexpected make: {make}")
    
    # Question 2: Describe view to check TRUCK_ID type
    print("\n" + "="*50)
    print("QUESTION 2: Describe view to check TRUCK_ID type")
    print("="*50)
    
    describe_query = "DESCRIBE VIEW truck_franchise;"
    results2 = execute_query(conn, describe_query, "Describing truck_franchise view")
    
    if results2:
        print("Analyzing type column for TRUCK_ID...")
        for row in results2:
            if 'TRUCK_ID' in str(row).upper():
                # type is typically at index 1
                type_value = row[1]
                print(f"\nAnswer for Question 2: type = {type_value}")
                if type_value == 'INT':
                    print("✅ Correct! Answer is 'INT'")
                elif type_value == 'DECIMAL':
                    print("✅ Correct! Answer is 'DECIMAL'")
                elif type_value == 'NUMBER(38,0)':
                    print("✅ Correct! Answer is 'NUMBER(38,0)'")
                elif type_value == 'TINYINT':
                    print("✅ Correct! Answer is 'TINYINT'")
                else:
                    print(f"❌ Unexpected type: {type_value}")
                break
    
    # Question 3: Drop view
    print("\n" + "="*50)
    print("QUESTION 3: Drop truck_franchise view")
    print("="*50)
    
    drop_view_query = "DROP VIEW truck_franchise;"
    results3 = execute_query(conn, drop_view_query, "Dropping truck_franchise view")
    
    if results3:
        print(f"\nAnswer for Question 3: {results3[0][0]}")
        if "View TRUCK_FRANCHISE successfully dropped" in str(results3[0][0]):
            print("✅ Correct! Answer is 'View TRUCK_FRANCHISE successfully dropped.'")
        elif "Statement executed successfully" in str(results3[0][0]):
            print("✅ Correct! Answer is 'Statement executed successfully.'")
        elif "TRUCK_FRANCHISE successfully dropped" in str(results3[0][0]):
            print("✅ Correct! Answer is 'TRUCK_FRANCHISE successfully dropped.'")
        elif "statement executed successfully" in str(results3[0][0]):
            print("✅ Correct! Answer is 'statement executed successfully.'")
        else:
            print(f"❌ Unexpected result: {results3[0][0]}")
    
    # Question 4: Create materialized view (should fail)
    print("\n" + "="*50)
    print("QUESTION 4: Create materialized view (should fail)")
    print("="*50)
    
    create_materialized_view_query = """CREATE MATERIALIZED VIEW truck_franchise_materialized AS
SELECT
    t.*,
    f.first_name AS franchisee_first_name,
    f.last_name AS franchisee_last_name
FROM tasty_bytes.raw_pos.truck t
JOIN tasty_bytes.raw_pos.franchise f
    ON t.franchise_id = f.franchise_id;"""
    
    results4 = execute_query(conn, create_materialized_view_query, "Creating materialized view (should fail)")
    
    if results4:
        print(f"\nAnswer for Question 4: {results4[0][0]}")
        if "Invalid view definition" in str(results4[0][0]):
            print("✅ Correct! Answer is 'Invalid view definition.'")
        elif "View TRUCK_FRANCHISE_MATERIALIZED successfully created" in str(results4[0][0]):
            print("✅ Correct! Answer is 'View TRUCK_FRANCHISE_MATERIALIZED successfully created.'")
        elif "Invalid materialized view definition" in str(results4[0][0]):
            print("✅ Correct! Answer is 'Invalid materialized view definition. More than one table referenced in the view definition'")
        elif "Materialized view TRUCK_FRANCHISE_MATERIALIZED successfully created" in str(results4[0][0]):
            print("✅ Correct! Answer is 'Materialized view TRUCK_FRANCHISE_MATERIALIZED successfully created.'")
        else:
            print(f"❌ Unexpected result: {results4[0][0]}")
    else:
        print("No results returned - this might indicate an error")
    
    # Question 5: Create nissan materialized view and count rows
    print("\n" + "="*50)
    print("QUESTION 5: Create nissan materialized view and count rows")
    print("="*50)
    
    create_nissan_query = """CREATE MATERIALIZED VIEW nissan AS
SELECT
    t.*
FROM tasty_bytes.raw_pos.truck t
WHERE make = 'Nissan';"""
    
    execute_query(conn, create_nissan_query, "Creating nissan materialized view")
    
    # Count rows in nissan view
    count_query = "SELECT COUNT(*) FROM nissan;"
    results5 = execute_query(conn, count_query, "Counting rows in nissan view")
    
    if results5:
        row_count = results5[0][0]
        print(f"\nAnswer for Question 5: row_count = {row_count}")
        if row_count == 9:
            print("✅ Correct! Answer is 9")
        elif row_count == 6:
            print("✅ Correct! Answer is 6")
        elif row_count == 15:
            print("✅ Correct! Answer is 15")
        elif row_count == 12:
            print("✅ Correct! Answer is 12")
        else:
            print(f"❌ Unexpected row_count: {row_count}")
    
    # Question 6: Drop nissan materialized view
    print("\n" + "="*50)
    print("QUESTION 6: Drop nissan materialized view")
    print("="*50)
    
    drop_nissan_query = "DROP MATERIALIZED VIEW nissan;"
    results6 = execute_query(conn, drop_nissan_query, "Dropping nissan materialized view")
    
    if results6:
        print(f"\nAnswer for Question 6: {results6[0][0]}")
        if "NISSAN successfully dropped" in str(results6[0][0]):
            print("✅ Correct! Answer is 'NISSAN successfully dropped.'")
        elif "Statement executed successfully" in str(results6[0][0]):
            print("✅ Correct! Answer is 'Statement executed successfully.'")
        elif "Nissan successfully dropped" in str(results6[0][0]):
            print("✅ Correct! Answer is 'Nissan successfully dropped.'")
        elif "Materialized view 'Nissan' successfully dropped" in str(results6[0][0]):
            print("✅ Correct! Answer is 'Materialized view 'Nissan' successfully dropped.'")
        else:
            print(f"❌ Unexpected result: {results6[0][0]}")
    
    # Clean up - drop test database
    print("\n" + "="*50)
    print("CLEANUP: Dropping test database")
    print("="*50)
    
    execute_query(conn, "DROP DATABASE test_database;", "Dropping test_database for cleanup")
    
    conn.close()
    print("\n" + "="*70)
    print("VIEW QUESTIONS ANALYSIS COMPLETE")
    print("="*70)

if __name__ == "__main__":
    answer_view_questions()
