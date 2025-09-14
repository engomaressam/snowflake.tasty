# Snowflake Configuration
# Configuration using environment variables for security

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get configuration from environment variables
SNOWFLAKE_CONFIG = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'role': os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
    'database': os.getenv('SNOWFLAKE_DATABASE', 'SNOWFLAKE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA', 'INFORMATION_SCHEMA')
}

# Account URL for reference (constructed from environment)
ACCOUNT_URL = f"{os.getenv('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com" if os.getenv('SNOWFLAKE_ACCOUNT') else None

# Additional account information (non-sensitive)
ACCOUNT_INFO = {
    'cloud_platform': 'AWS',
    'edition': 'Standard'
}
