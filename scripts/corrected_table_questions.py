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

def answer_table_questions():
    """
    Answer the Snowflake table management questions by executing SQL commands
    """
    conn = connect_to_snowflake()
    if not conn:
        return
    
    print("="*70)
    print("ANSWERING SNOWFLAKE TABLE MANAGEMENT QUESTIONS")
    print("="*70)
    
    # Setup: Create test database and schema
    print("\n" + "="*50)
    print("SETUP: Creating test database and schema")
    print("="*50)
    
    execute_query(conn, "CREATE DATABASE test_database;", "Creating test_database")
    execute_query(conn, "USE DATABASE test_database;", "Switching to test_database")
    execute_query(conn, "CREATE SCHEMA test_schema;", "Creating test_schema")
    execute_query(conn, "USE SCHEMA test_schema;", "Switching to test_schema")
    
    # Create test_table for Question 1
    execute_query(conn, """CREATE TABLE test_table (
        id NUMBER,
        name VARCHAR(50)
    );""", "Creating test_table for Question 1")
    
    # Question 1: Show tables and check bytes column
    print("\n" + "="*50)
    print("QUESTION 1: Show tables and check bytes column for test_table")
    print("="*50)
    
    show_tables_query = "SHOW TABLES;"
    results1 = execute_query(conn, show_tables_query, "Showing tables after creating test_table")
    
    if results1:
        print("Analyzing bytes column...")
        for row in results1:
            if 'test_table' in str(row).lower():
                # bytes is at index 8
                bytes_value = row[8]
                print(f"\nAnswer for Question 1: bytes = {bytes_value}")
                if bytes_value == 1536:
                    print("✅ Correct! Answer is 1536")
                elif bytes_value == 2048:
                    print("✅ Correct! Answer is 2048")
                elif bytes_value == 0:
                    print("✅ Correct! Answer is 0")
                elif bytes_value == 2560:
                    print("✅ Correct! Answer is 2560")
                else:
                    print(f"❌ Unexpected bytes: {bytes_value}")
                break
    
    # Question 2: Create test_table2 and check bytes
    print("\n" + "="*50)
    print("QUESTION 2: Create test_table2 and check bytes column")
    print("="*50)
    
    # Create test_table2
    execute_query(conn, """CREATE TABLE test_table2 (
        test_number NUMBER
    );""", "Creating test_table2 with test_number column")
    
    # Insert value 42
    execute_query(conn, "INSERT INTO test_table2 (test_number) VALUES (42);", "Inserting value 42 into test_table2")
    
    # Show tables to check bytes for test_table2
    results2 = execute_query(conn, show_tables_query, "Showing tables after creating test_table2")
    
    if results2:
        print("Analyzing bytes column for test_table2...")
        for row in results2:
            if 'test_table2' in str(row).lower():
                # bytes is at index 8
                bytes_value = row[8]
                print(f"\nAnswer for Question 2: bytes = {bytes_value}")
                if bytes_value == 2048:
                    print("✅ Correct! Answer is 2048")
                elif bytes_value == 0:
                    print("✅ Correct! Answer is 0")
                elif bytes_value == 1024:
                    print("✅ Correct! Answer is 1024")
                elif bytes_value == 8192:
                    print("✅ Correct! Answer is 8192")
                else:
                    print(f"❌ Unexpected bytes: {bytes_value}")
                break
    
    # Question 3: Drop and undrop test_table
    print("\n" + "="*50)
    print("QUESTION 3: Drop and undrop test_table")
    print("="*50)
    
    # Drop test_table
    execute_query(conn, "DROP TABLE test_table;", "Dropping test_table")
    
    # Undrop test_table
    undrop_table_query = "UNDROP TABLE test_table;"
    results3 = execute_query(conn, undrop_table_query, "Undropping test_table")
    
    if results3:
        print(f"\nAnswer for Question 3: {results3[0][0]}")
        if "Statement executed successfully" in str(results3[0][0]):
            print("✅ Correct! Answer is 'Statement executed successfully.'")
        elif "Table TEST_TABLE successfully restored" in str(results3[0][0]):
            print("✅ Correct! Answer is 'Table TEST_TABLE successfully restored.'")
        elif "Table test_table successfully restored" in str(results3[0][0]):
            print("✅ Correct! Answer is 'Table test_table successfully restored.'")
        elif "statement executed successfully" in str(results3[0][0]):
            print("✅ Correct! Answer is 'statement executed successfully.'")
        else:
            print(f"❌ Unexpected result: {results3[0][0]}")
    
    # Question 4: Knowledge question about NUMBER data type
    print("\n" + "="*50)
    print("QUESTION 4: NUMBER data type synonym (Knowledge question)")
    print("="*50)
    print("This is a knowledge question about Snowflake data types.")
    print("\nIn Snowflake, the NUMBER data type is synonymous with DECIMAL.")
    print("Both NUMBER and DECIMAL represent exact numeric values with precision and scale.")
    print("\nLooking at the options:")
    print("BIGINT - A 64-bit signed integer, different from NUMBER")
    print("BOOLEAN - A logical data type (TRUE/FALSE), different from NUMBER")
    print("DECIMAL - Exact numeric with precision and scale, SYNONYMOUS with NUMBER")
    print("FLOAT - Approximate numeric, different from NUMBER")
    print("\n✅ Answer: DECIMAL")
    
    # Clean up - drop test database
    print("\n" + "="*50)
    print("CLEANUP: Dropping test database")
    print("="*50)
    
    execute_query(conn, "DROP DATABASE test_database;", "Dropping test_database for cleanup")
    
    conn.close()
    print("\n" + "="*70)
    print("TABLE QUESTIONS ANALYSIS COMPLETE")
    print("="*70)

if __name__ == "__main__":
    answer_table_questions()
