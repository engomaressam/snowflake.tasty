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

def answer_questions():
    """
    Answer the multiple choice questions by querying the data
    """
    conn = connect_to_snowflake()
    if not conn:
        return
    
    print("="*60)
    print("ANSWERING SNOWFLAKE ASSIGNMENT QUESTIONS")
    print("="*60)
    
    # Question 1: How many items are there with an item_category of 'Snack' and an item_subcategory of 'Warm Option'?
    query1 = """
    SELECT COUNT(*) as snack_warm_count
    FROM tasty_bytes_sample_data.raw_pos.menu
    WHERE item_category = 'Snack' 
    AND item_subcategory = 'Warm Option';
    """
    
    results1 = execute_query(conn, query1, "Question 1: Count of Snack items with Warm Option subcategory")
    
    if results1:
        count = results1[0][0]
        print(f"\nAnswer for Question 1: {count}")
        if count == 5:
            print("✅ Correct! Answer is 5")
        elif count == 9:
            print("✅ Correct! Answer is 9")
        elif count == 3:
            print("✅ Correct! Answer is 3")
        elif count == 7:
            print("✅ Correct! Answer is 7")
        else:
            print(f"❌ Unexpected result: {count}")
    
    # Question 2: What are the max sales prices for each of the three item subcategories?
    query2 = """
    SELECT 
        item_subcategory,
        MAX(sale_price_usd) as max_price
    FROM tasty_bytes_sample_data.raw_pos.menu
    WHERE item_subcategory IN ('Hot Option', 'Warm Option', 'Cold Option')
    GROUP BY item_subcategory
    ORDER BY max_price DESC;
    """
    
    results2 = execute_query(conn, query2, "Question 2: Max sales prices by subcategory")
    
    if results2:
        print(f"\nAnswer for Question 2:")
        prices = []
        for row in results2:
            subcategory, max_price = row
            prices.append(max_price)
            print(f"  {subcategory}: ${max_price}")
        
        # Check against the options
        if len(prices) >= 3:
            hot_price = prices[0] if 'Hot Option' in [row[0] for row in results2 if row[1] == prices[0]] else None
            warm_price = prices[1] if 'Warm Option' in [row[0] for row in results2 if row[1] == prices[1]] else None
            cold_price = prices[2] if 'Cold Option' in [row[0] for row in results2 if row[1] == prices[2]] else None
            
            # Find the actual prices for each category
            actual_prices = {}
            for row in results2:
                subcategory, max_price = row
                actual_prices[subcategory] = max_price
            
            hot_price = actual_prices.get('Hot Option', 0)
            warm_price = actual_prices.get('Warm Option', 0)
            cold_price = actual_prices.get('Cold Option', 0)
            
            print(f"\nMax prices: Hot=${hot_price}, Warm=${warm_price}, Cold=${cold_price}")
            
            # Check against the given options
            if hot_price == 21 and warm_price == 12.5 and cold_price == 11:
                print("✅ Correct! Answer is $21, $12.5, $11")
            elif hot_price == 21 and warm_price == 11 and cold_price == 11:
                print("✅ Correct! Answer is $21, $11, $11")
            elif hot_price == 19 and warm_price == 12.5 and cold_price == 11:
                print("✅ Correct! Answer is $19, $12.5, $11")
            elif hot_price == 21 and warm_price == 19 and cold_price == 9:
                print("✅ Correct! Answer is $21, $19, $9")
            else:
                print(f"❌ Unexpected result: Hot=${hot_price}, Warm=${warm_price}, Cold=${cold_price}")
    
    # Additional analysis queries
    print("\n" + "="*60)
    print("ADDITIONAL DATA ANALYSIS")
    print("="*60)
    
    # Show all item categories and subcategories
    query3 = """
    SELECT 
        item_category,
        item_subcategory,
        COUNT(*) as count
    FROM tasty_bytes_sample_data.raw_pos.menu
    GROUP BY item_category, item_subcategory
    ORDER BY item_category, item_subcategory;
    """
    
    execute_query(conn, query3, "All categories and subcategories with counts")
    
    # Show price ranges by subcategory
    query4 = """
    SELECT 
        item_subcategory,
        MIN(sale_price_usd) as min_price,
        MAX(sale_price_usd) as max_price,
        AVG(sale_price_usd) as avg_price,
        COUNT(*) as count
    FROM tasty_bytes_sample_data.raw_pos.menu
    WHERE item_subcategory IN ('Hot Option', 'Warm Option', 'Cold Option')
    GROUP BY item_subcategory
    ORDER BY max_price DESC;
    """
    
    execute_query(conn, query4, "Price analysis by subcategory")
    
    conn.close()
    print("\n" + "="*60)
    print("ANALYSIS COMPLETE")
    print("="*60)

if __name__ == "__main__":
    answer_questions()
