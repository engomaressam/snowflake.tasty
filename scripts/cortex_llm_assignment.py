"""
Snowflake Cortex LLM Functions Assignment
Demonstrates various Cortex LLM capabilities including COMPLETE and SUMMARIZE functions
"""

import snowflake.connector
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def connect_to_snowflake():
    """
    Connect to Snowflake using environment variables
    """
    try:
        user = os.getenv('SNOWFLAKE_USER')
        account = os.getenv('SNOWFLAKE_ACCOUNT')
        password = os.getenv('SNOWFLAKE_PASSWORD')
        role = os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')
        warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
        
        if not all([user, account, password]):
            raise ValueError("Missing required environment variables. Please check your .env file.")
        
        conn = snowflake.connector.connect(
            user=user,
            account=account,
            password=password,
            role=role,
            warehouse=warehouse
        )
        
        print("Successfully connected to Snowflake!")
        return conn
        
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        return None

def find_menu_table(conn):
    """
    Try to find the correct menu table database
    """
    cursor = conn.cursor()
    try:
        # Try FROSTBYTE_TASTY_BYTES first (from example)
        cursor.execute("SELECT COUNT(*) FROM FROSTBYTE_TASTY_BYTES.RAW_POS.MENU LIMIT 1")
        return "FROSTBYTE_TASTY_BYTES.RAW_POS.MENU"
    except:
        try:
            # Try TASTY_BYTES_SAMPLE_DATA (our database)
            cursor.execute("SELECT COUNT(*) FROM TASTY_BYTES_SAMPLE_DATA.RAW_POS.MENU LIMIT 1")
            return "TASTY_BYTES_SAMPLE_DATA.RAW_POS.MENU"
        except:
            try:
                # Try TASTY_BYTES
                cursor.execute("SELECT COUNT(*) FROM TASTY_BYTES.RAW_POS.MENU LIMIT 1")
                return "TASTY_BYTES.RAW_POS.MENU"
            except:
                return None

def execute_cortex_llm_assignment():
    """
    Execute all Cortex LLM function examples
    """
    conn = connect_to_snowflake()
    if not conn:
        return
    
    cursor = conn.cursor()
    
    try:
        print("\n" + "="*60)
        print("SNOWFLAKE CORTEX LLM FUNCTIONS - STARTING")
        print("="*60)
        
        # Find the correct menu table
        print("\n[0] Finding menu table...")
        menu_table = find_menu_table(conn)
        if menu_table:
            print(f"[OK] Found menu table: {menu_table}")
        else:
            print("[WARN] Could not find menu table. Will try example queries anyway.")
            menu_table = "FROSTBYTE_TASTY_BYTES.RAW_POS.MENU"
        
        # Example 1: Use mistral-7b model and Snowflake Cortex Complete to ask a question
        print("\n[1] Using Cortex COMPLETE with mistral-7b model...")
        try:
            query1 = """
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'mistral-7b', 'What are three reasons that Snowflake is positioned to become the go-to data platform?')
            """
            cursor.execute(query1)
            result1 = cursor.fetchone()
            print("\nResponse:")
            print(result1[0] if result1 else "No response")
        except Exception as e:
            print(f"[ERROR] Failed: {e}")
        
        # Example 2: Send result to Cortex Summarize function
        print("\n[2] Using Cortex SUMMARIZE on COMPLETE result...")
        try:
            query2 = """
            SELECT SNOWFLAKE.CORTEX.SUMMARIZE(SNOWFLAKE.CORTEX.COMPLETE(
                'mistral-7b', 'What are three reasons that Snowflake is positioned to become the go-to data platform?'))
            """
            cursor.execute(query2)
            result2 = cursor.fetchone()
            print("\nSummary:")
            print(result2[0] if result2 else "No summary")
        except Exception as e:
            print(f"[ERROR] Failed: {e}")
        
        # Example 3: Run Cortex Complete on multiple rows at once
        print("\n[3] Running Cortex COMPLETE on multiple rows...")
        try:
            query3 = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'mistral-7b',
                CONCAT('Tell me why this food is tasty: ', menu_item_name)
            ) FROM {menu_table} LIMIT 5
            """
            cursor.execute(query3)
            results3 = cursor.fetchall()
            print(f"\nResponses for {len(results3)} menu items:")
            for i, row in enumerate(results3, 1):
                print(f"\n  Item {i}:")
                print(f"    {row[0][:200] if row[0] else 'No response'}...")
        except Exception as e:
            print(f"[ERROR] Failed: {e}")
            print("   Note: This might fail if the menu table doesn't exist or Cortex LLM is not enabled")
        
        # Example 4: Check what the prompts look like
        print("\n[4] Checking the prompts we're feeding to COMPLETE...")
        try:
            query4 = f"""
            SELECT CONCAT('Tell me why this food is tasty: ', menu_item_name)
            FROM {menu_table} LIMIT 5
            """
            cursor.execute(query4)
            results4 = cursor.fetchall()
            print("\nSample prompts:")
            for i, row in enumerate(results4, 1):
                print(f"  {i}. {row[0]}")
        except Exception as e:
            print(f"[ERROR] Failed: {e}")
        
        # Example 5: Cortex Complete with prompt history
        print("\n[5] Using Cortex COMPLETE with prompt history...")
        try:
            query5 = """
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'mistral-7b',
                [
                    {'role': 'system', 
                    'content': 'Analyze this Snowflake review and determine the overall sentiment. Answer with just "Positive", "Negative", or "Neutral"' },
                    {'role': 'user',
                    'content': 'I love Snowflake because it is so simple to use.'}
                ],
                {}
            ) AS response
            """
            cursor.execute(query5)
            result5 = cursor.fetchone()
            print("\nResponse:")
            print(result5[0] if result5 else "No response")
        except Exception as e:
            print(f"[ERROR] Failed: {e}")
        
        # Example 6: Cortex Complete with lengthier history
        print("\n[6] Using Cortex COMPLETE with lengthier prompt history...")
        try:
            query6 = """
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'mistral-7b',
                [
                    {'role': 'system', 
                    'content': 'Analyze this Snowflake review and determine the overall sentiment. Answer with just "Positive", "Negative", or "Neutral"' },
                    {'role': 'user',
                    'content': 'I love Snowflake because it is so simple to use.'},
                    {'role': 'assistant',
                    'content': 'Positive. The review expresses a positive sentiment towards Snowflake, specifically mentioning that it is "so simple to use."'},
                    {'role': 'user',
                    'content': 'Based on other information you know about Snowflake, explain why the reviewer might feel they way they do.'}
                ],
                {}
            ) AS response
            """
            cursor.execute(query6)
            result6 = cursor.fetchone()
            print("\nResponse:")
            print(result6[0] if result6 else "No response")
        except Exception as e:
            print(f"[ERROR] Failed: {e}")
        
        print("\n" + "="*60)
        print("SNOWFLAKE CORTEX LLM FUNCTIONS - COMPLETED")
        print("="*60)
        
    except Exception as e:
        print(f"\n[ERROR] Error during assignment: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()
        print("\n[OK] Connection closed")

if __name__ == "__main__":
    print("Starting Snowflake Cortex LLM Functions Assignment...")
    print("Note: Some functions may fail if Cortex LLM is not enabled in your account.")
    execute_cortex_llm_assignment()

