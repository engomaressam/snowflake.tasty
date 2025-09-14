import snowflake.connector
from snowflake.connector import DictCursor
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_connection():
    """
    Test different connection methods to find the correct account format
    """
    
    # Get credentials from environment variables
    user = os.getenv('SNOWFLAKE_USER')
    password = os.getenv('SNOWFLAKE_PASSWORD')
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    role = os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
    
    if not all([user, password, account]):
        print("Missing required environment variables. Please check your .env file.")
        return None
    
    # Different account formats to try
    account_formats = [
        account,  # Full account identifier
        account.split('-')[0] if '-' in account else account,  # Organization name only
        account.split('-')[1] if '-' in account else account,  # Account name only
        account.replace('-', '.'),  # With dot separator
    ]
    
    for account_format in account_formats:
        print(f"\nTrying account format: {account_format}")
        try:
            conn = snowflake.connector.connect(
                user=user,
                password=password,
                account=account_format,
                role=role,
                warehouse=warehouse
            )
            print(f"✅ SUCCESS with account format: {account_format}")
            
            # Test a simple query
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            result = cursor.fetchone()
            print(f"Snowflake version: {result[0]}")
            
            cursor.close()
            conn.close()
            return account_format
            
        except Exception as e:
            print(f"❌ Failed with {account_format}: {e}")
    
    return None

if __name__ == "__main__":
    print("Testing Snowflake connection with different account formats...")
    successful_format = test_connection()
    
    if successful_format:
        print(f"\n🎉 Connection successful! Use account format: {successful_format}")
    else:
        print("\n❌ All connection attempts failed. Please check your credentials.")
