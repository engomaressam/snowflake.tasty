#!/usr/bin/env python3
"""
Setup environment variables for Snowflake connection
"""

def create_env_file():
    """
    Create .env file with actual Snowflake credentials
    """
    env_content = """# Snowflake Connection Configuration
# DO NOT COMMIT THIS FILE TO GIT - Contains sensitive credentials

# Account details
SNOWFLAKE_ACCOUNT=<your-account-identifier>
SNOWFLAKE_USER=<your-username>
SNOWFLAKE_PASSWORD=<your-password>
SNOWFLAKE_EMAIL=<your-email>
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=tasty_bytes_sample_data
SNOWFLAKE_SCHEMA=raw_pos

# Connection strings (for reference)
SNOWFLAKE_ODBC_STRING=Driver={SnowflakeDSIIDriver};Server=<your-account-identifier>.snowflakecomputing.com;Database=<none selected>;uid=<your-username>;pwd=<your-password>
SNOWFLAKE_JDBC_STRING=jdbc:snowflake://<your-account-identifier>.snowflakecomputing.com/?user=<your-username>&warehouse=<none selected>&db=<none selected>&schema=<none selected>&password=<your-password>
"""
    
    try:
        with open('.env', 'w') as f:
            f.write(env_content)
        print("✅ .env file created successfully with Snowflake credentials")
        return True
    except Exception as e:
        print(f"❌ Error creating .env file: {e}")
        return False

if __name__ == "__main__":
    create_env_file()
