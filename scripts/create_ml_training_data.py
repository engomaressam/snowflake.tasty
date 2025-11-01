"""
Create ML Training Data Table
Generates the df_clean table with MONTH, DAY, and NEIGHBORHOOD columns
based on the truck's visiting pattern
"""

import snowflake.connector
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def connect_to_snowflake():
    """Connect to Snowflake using environment variables"""
    try:
        user = os.getenv('SNOWFLAKE_USER')
        account = os.getenv('SNOWFLAKE_ACCOUNT')
        password = os.getenv('SNOWFLAKE_PASSWORD')
        role = os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')
        warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
        
        if not all([user, account, password]):
            raise ValueError("Missing required environment variables.")
        
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

def create_neighborhood_pattern():
    """Create the neighborhood visiting pattern dictionary"""
    month_days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    pre = {}
    
    for i, month_length in enumerate(month_days):
        month = i + 1
        for day in range(1, month_length + 1):
            if month == 1:
                if day % 7 == 1:
                    pre[(month, day)] = 1
                else:
                    pre[(month, day)] = 2
            elif month <= 11:
                pre[(month, day)] = ((day - 1) % 7) + 1
            elif month == 12:
                pre[(month, day)] = 8
    
    return pre

def create_training_data_table():
    """Create the df_clean table in Snowflake"""
    conn = connect_to_snowflake()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        print("\n[1] Creating database and schema if they don't exist...")
        cursor.execute("CREATE DATABASE IF NOT EXISTS test_database")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS test_database.test_schema")
        print("[OK] Database and schema ready")
        
        print("\n[2] Creating df_clean table...")
        cursor.execute("""
            CREATE OR REPLACE TABLE test_database.test_schema.df_clean (
                MONTH INT,
                DAY INT,
                NEIGHBORHOOD INT
            )
        """)
        print("[OK] Table created")
        
        print("\n[3] Generating neighborhood visiting pattern...")
        pre = create_neighborhood_pattern()
        print(f"[OK] Pattern created with {len(pre)} entries")
        
        print("\n[4] Inserting data into table...")
        # Prepare insert statements
        values = []
        for (month, day), neighborhood in pre.items():
            values.append(f"({month}, {day}, {neighborhood})")
        
        # Insert in batches
        batch_size = 100
        for i in range(0, len(values), batch_size):
            batch = values[i:i+batch_size]
            insert_sql = f"""
                INSERT INTO test_database.test_schema.df_clean (MONTH, DAY, NEIGHBORHOOD)
                VALUES {', '.join(batch)}
            """
            cursor.execute(insert_sql)
        
        print(f"[OK] Inserted {len(values)} rows")
        
        print("\n[5] Verifying data...")
        cursor.execute("SELECT COUNT(*) FROM test_database.test_schema.df_clean")
        count = cursor.fetchone()[0]
        print(f"[OK] Total rows in table: {count}")
        
        print("\n[6] Showing sample data...")
        cursor.execute("SELECT * FROM test_database.test_schema.df_clean LIMIT 10")
        rows = cursor.fetchall()
        print("Sample rows:")
        for row in rows:
            print(f"  Month: {row[0]}, Day: {row[1]}, Neighborhood: {row[2]}")
        
        print("\n[7] Verifying neighborhood distribution...")
        cursor.execute("""
            SELECT NEIGHBORHOOD, COUNT(*) as count
            FROM test_database.test_schema.df_clean
            GROUP BY NEIGHBORHOOD
            ORDER BY NEIGHBORHOOD
        """)
        distribution = cursor.fetchall()
        print("Neighborhood distribution:")
        for row in distribution:
            print(f"  Neighborhood {row[0]}: {row[1]} days")
        
        print("\n[OK] Training data table created successfully!")
        return True
        
    except Exception as e:
        print(f"[ERROR] Error creating table: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    print("Creating ML Training Data Table...")
    print("This will create test_database.test_schema.df_clean with MONTH, DAY, NEIGHBORHOOD columns")
    create_training_data_table()

