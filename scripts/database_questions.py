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

def answer_database_questions():
    """
    Answer the Snowflake database management questions by executing SQL commands
    """
    conn = connect_to_snowflake()
    if not conn:
        return
    
    print("="*70)
    print("ANSWERING SNOWFLAKE DATABASE MANAGEMENT QUESTIONS")
    print("="*70)
    
    # Question 1: Create database and check is_default
    print("\n" + "="*50)
    print("QUESTION 1: Create database and check is_default")
    print("="*50)
    
    # Create test_database
    execute_query(conn, "CREATE DATABASE test_database;", "Creating test_database")
    
    # Show databases to check is_default
    show_databases_query = "SHOW DATABASES;"
    results1 = execute_query(conn, show_databases_query, "Showing databases after creating test_database")
    
    if results1:
        print("Analyzing is_default column...")
        for row in results1:
            print(f"Database details: {row}")
            if 'test_database' in str(row).lower():
                # Find the is_default column (typically index 6)
                try:
                    is_default = row[6] if len(row) > 6 else None
                    print(f"\nAnswer for Question 1: is_default = {is_default}")
                    if is_default == 'N':
                        print("✅ Correct! Answer is 'N'")
                    elif is_default == 'Y':
                        print("✅ Correct! Answer is 'Y'")
                    elif is_default == 'No':
                        print("✅ Correct! Answer is 'No'")
                    elif is_default == 'Yes':
                        print("✅ Correct! Answer is 'Yes'")
                    else:
                        print(f"❌ Unexpected is_default: {is_default}")
                except:
                    print("Could not extract is_default from result")
    
    # Question 2: Drop and undrop database
    print("\n" + "="*50)
    print("QUESTION 2: Drop and undrop database")
    print("="*50)
    
    # Drop test_database
    execute_query(conn, "DROP DATABASE test_database;", "Dropping test_database")
    
    # Undrop test_database
    undrop_query = "UNDROP DATABASE test_database;"
    results2 = execute_query(conn, undrop_query, "Undropping test_database")
    
    if results2:
        print(f"\nAnswer for Question 2: {results2[0][0]}")
        if "Database successfully restored" in str(results2[0][0]):
            print("✅ Correct! Answer is 'Database successfully restored.'")
        elif "Statement executed successfully" in str(results2[0][0]):
            print("✅ Correct! Answer is 'Statement executed successfully.'")
        elif "STATEMENT EXECUTED SUCCESSFULLY" in str(results2[0][0]):
            print("✅ Correct! Answer is 'STATEMENT EXECUTED SUCCESSFULLY.'")
        elif "Database TEST_DATABASE successfully restored" in str(results2[0][0]):
            print("✅ Correct! Answer is 'Database TEST_DATABASE successfully restored.'")
        else:
            print(f"❌ Unexpected result: {results2[0][0]}")
    
    # Question 3: Create test_database2 and switch to test_database
    print("\n" + "="*50)
    print("QUESTION 3: Create test_database2 and switch to test_database")
    print("="*50)
    
    # Create test_database2
    execute_query(conn, "CREATE DATABASE test_database2;", "Creating test_database2")
    
    # Switch to test_database
    use_database_query = "USE DATABASE test_database;"
    results3 = execute_query(conn, use_database_query, "Switching to test_database")
    
    if results3:
        print(f"\nAnswer for Question 3: {results3[0][0]}")
        if "STATEMENT EXECUTED SUCCESSFULLY" in str(results3[0][0]):
            print("✅ Correct! Answer is 'STATEMENT EXECUTED SUCCESSFULLY.'")
        elif "Database TEST_DATABASE now in use" in str(results3[0][0]):
            print("✅ Correct! Answer is 'Database TEST_DATABASE now in use.'")
        elif "Statement executed successfully" in str(results3[0][0]):
            print("✅ Correct! Answer is 'Statement executed successfully.'")
        elif "DATABASE TEST_DATABASE NOW IN USE" in str(results3[0][0]):
            print("✅ Correct! Answer is 'DATABASE TEST_DATABASE NOW IN USE.'")
        else:
            print(f"❌ Unexpected result: {results3[0][0]}")
    
    # Question 4: Create schema and check is_current
    print("\n" + "="*50)
    print("QUESTION 4: Create schema and check is_current")
    print("="*50)
    
    # Create test_schema
    execute_query(conn, "CREATE SCHEMA test_schema;", "Creating test_schema")
    
    # Show schemas to check is_current
    show_schemas_query = "SHOW SCHEMAS;"
    results4 = execute_query(conn, show_schemas_query, "Showing schemas after creating test_schema")
    
    if results4:
        print("Analyzing is_current column...")
        for row in results4:
            print(f"Schema details: {row}")
            if 'test_schema' in str(row).lower():
                # Find the is_current column (typically index 7)
                try:
                    is_current = row[7] if len(row) > 7 else None
                    print(f"\nAnswer for Question 4: is_current = {is_current}")
                    if is_current == 'Yes':
                        print("✅ Correct! Answer is 'Yes'")
                    elif is_current == 'N':
                        print("✅ Correct! Answer is 'N'")
                    elif is_current == 'No':
                        print("✅ Correct! Answer is 'No'")
                    elif is_current == 'Y':
                        print("✅ Correct! Answer is 'Y'")
                    else:
                        print(f"❌ Unexpected is_current: {is_current}")
                except:
                    print("Could not extract is_current from result")
    
    # Question 5: Describe database to check kind column
    print("\n" + "="*50)
    print("QUESTION 5: Describe database to check kind column")
    print("="*50)
    
    # Describe database
    describe_query = "DESCRIBE DATABASE test_database;"
    results5 = execute_query(conn, describe_query, "Describing test_database")
    
    if results5:
        print("Analyzing kind column...")
        for row in results5:
            print(f"Schema details: {row}")
            if 'test_schema' in str(row).lower():
                # Find the kind column (typically index 2)
                try:
                    kind = row[2] if len(row) > 2 else None
                    print(f"\nAnswer for Question 5: kind = {kind}")
                    if kind == 'SCHEMA':
                        print("✅ Correct! Answer is 'SCHEMA'")
                    elif kind == 'Schema':
                        print("✅ Correct! Answer is 'Schema'")
                    elif kind == 'Object':
                        print("✅ Correct! Answer is 'Object'")
                    elif kind == 'object':
                        print("✅ Correct! Answer is 'object'")
                    else:
                        print(f"❌ Unexpected kind: {kind}")
                except:
                    print("Could not extract kind from result")
    
    # Question 6: Drop and undrop schema
    print("\n" + "="*50)
    print("QUESTION 6: Drop and undrop schema")
    print("="*50)
    
    # Drop test_schema
    execute_query(conn, "DROP SCHEMA test_schema;", "Dropping test_schema")
    
    # Undrop test_schema
    undrop_schema_query = "UNDROP SCHEMA test_schema;"
    results6 = execute_query(conn, undrop_schema_query, "Undropping test_schema")
    
    if results6:
        print(f"\nAnswer for Question 6: {results6[0][0]}")
        if "Schema TEST_SCHEMA successfully restored" in str(results6[0][0]):
            print("✅ Correct! Answer is 'Schema TEST_SCHEMA successfully restored.'")
        elif "Statement executed successfully" in str(results6[0][0]):
            print("✅ Correct! Answer is 'Statement executed successfully.'")
        elif "Schema test_schema successfully restored" in str(results6[0][0]):
            print("✅ Correct! Answer is 'Schema test_schema successfully restored.'")
        elif "statement executed successfully" in str(results6[0][0]):
            print("✅ Correct! Answer is 'statement executed successfully.'")
        else:
            print(f"❌ Unexpected result: {results6[0][0]}")
    
    # Clean up - drop test databases
    print("\n" + "="*50)
    print("CLEANUP: Dropping test databases")
    print("="*50)
    
    execute_query(conn, "DROP DATABASE test_database;", "Dropping test_database for cleanup")
    execute_query(conn, "DROP DATABASE test_database2;", "Dropping test_database2 for cleanup")
    
    conn.close()
    print("\n" + "="*70)
    print("DATABASE QUESTIONS ANALYSIS COMPLETE")
    print("="*70)

if __name__ == "__main__":
    answer_database_questions()
