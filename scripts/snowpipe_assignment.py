"""
Snowpipe Assignment Script
Creates a storage integration, database, table, stage, and Snowpipe for automatic data ingestion from S3
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

def execute_snowpipe_assignment(aws_role_arn=None):
    """
    Execute the complete Snowpipe assignment
    """
    conn = connect_to_snowflake()
    if not conn:
        return
    
    cursor = conn.cursor()
    
    try:
        print("\n" + "="*60)
        print("SNOWPIPE ASSIGNMENT - STARTING")
        print("="*60)
        
        # Step 1: Create storage integration
        print("\n[1] Creating storage integration...")
        storage_integration_exists = False
        
        if aws_role_arn and aws_role_arn != "REMOVED":
            try:
                storage_integration_sql = f"""
                CREATE OR REPLACE STORAGE INTEGRATION S3_role_integration
                  TYPE = EXTERNAL_STAGE
                  STORAGE_PROVIDER = S3
                  ENABLED = TRUE
                  STORAGE_AWS_ROLE_ARN = "{aws_role_arn}"
                  STORAGE_ALLOWED_LOCATIONS = ("s3://intro-to-snowflake-snowpipe/");
                """
                cursor.execute(storage_integration_sql)
                print("[OK] Storage integration created")
                storage_integration_exists = True
            except Exception as e:
                print(f"[ERROR] Failed to create storage integration: {e}")
                print("   Attempting to check if it already exists...")
                try:
                    cursor.execute("DESCRIBE INTEGRATION S3_role_integration")
                    storage_integration_exists = True
                    print("   Storage integration already exists, continuing...")
                except:
                    print("   Storage integration does not exist and cannot be created without valid ARN")
                    print("   Please provide a valid AWS Role ARN to continue")
        else:
            print("[WARN] AWS Role ARN not provided or is 'REMOVED'.")
            print("   Attempting to use existing storage integration (if it exists)...")
            try:
                cursor.execute("DESCRIBE INTEGRATION S3_role_integration")
                storage_integration_exists = True
                print("   Using existing storage integration")
            except:
                print("   Storage integration does not exist.")
                print("   You'll need to create it manually with a valid AWS Role ARN:")
                print("   Format: arn:aws:iam::ACCOUNT_ID:role/ROLE_NAME")
                print("\n   SQL to create manually:")
                print('   CREATE OR REPLACE STORAGE INTEGRATION S3_role_integration')
                print('     TYPE = EXTERNAL_STAGE')
                print('     STORAGE_PROVIDER = S3')
                print('     ENABLED = TRUE')
                print('     STORAGE_AWS_ROLE_ARN = "YOUR_AWS_ROLE_ARN_HERE"')
                print('     STORAGE_ALLOWED_LOCATIONS = ("s3://intro-to-snowflake-snowpipe/");')
        
        # Step 2: Describe the storage integration
        if storage_integration_exists:
            print("\n[2] Describing storage integration...")
            cursor.execute("DESCRIBE INTEGRATION S3_role_integration")
            results = cursor.fetchall()
            print("\nStorage Integration Details:")
            for row in results:
                print(f"  {row[0]}: {row[1]}")
        else:
            print("\n[2] Skipping storage integration description (integration does not exist)")
        
        # Step 3: Create database
        print("\n[3] Creating database S3_db...")
        cursor.execute("CREATE OR REPLACE DATABASE S3_db")
        print("[OK] Database created")
        
        # Step 4: Create table
        print("\n[4] Creating table S3_table...")
        cursor.execute("CREATE OR REPLACE TABLE S3_db.public.S3_table(food STRING, taste INT)")
        print("[OK] Table created")
        
        # Step 5: Use schema
        print("\n[5] Setting schema context...")
        cursor.execute("USE SCHEMA S3_db.public")
        print("[OK] Schema context set")
        
        # Step 6: Create stage
        print("\n[6] Creating stage...")
        if storage_integration_exists:
            try:
                cursor.execute("""
                    CREATE OR REPLACE STAGE S3_stage
                      url = ('s3://intro-to-snowflake-snowpipe/')
                      storage_integration = S3_role_integration
                """)
                print("[OK] Stage created with storage integration")
            except Exception as e:
                print(f"[ERROR] Failed to create stage with storage integration: {e}")
                print("   Attempting to create without storage integration (may not work for S3)...")
                try:
                    cursor.execute("""
                        CREATE OR REPLACE STAGE S3_stage
                          url = ('s3://intro-to-snowflake-snowpipe/')
                    """)
                    print("[OK] Stage created without storage integration (may have limited access)")
                except Exception as e2:
                    print(f"[ERROR] Failed to create stage: {e2}")
        else:
            print("[WARN] Storage integration does not exist, creating stage without it")
            print("   Note: This may not work for S3 access without proper credentials")
            try:
                cursor.execute("""
                    CREATE OR REPLACE STAGE S3_stage
                      url = ('s3://intro-to-snowflake-snowpipe/')
                """)
                print("[OK] Stage created (may have limited access)")
            except Exception as e:
                print(f"[ERROR] Failed to create stage: {e}")
        
        # Step 7: Show stages
        print("\n[7] Showing stages...")
        cursor.execute("SHOW STAGES")
        stages = cursor.fetchall()
        print("\nStages:")
        if stages:
            columns = [desc[0] for desc in cursor.description]
            print(f"  Columns: {', '.join(columns)}")
            for stage in stages[:5]:  # Show first 5
                print(f"  {stage}")
        else:
            print("  No stages found")
        
        # Step 8: List files in stage
        print("\n[8] Listing files in stage...")
        try:
            cursor.execute("LIST @S3_stage")
            files = cursor.fetchall()
            print(f"\nFiles in stage: {len(files)} files found")
            for file in files[:10]:  # Show first 10 files
                print(f"  {file[0] if len(file) > 0 else file}")
        except Exception as e:
            print(f"[WARN]  Error listing files: {e}")
            print("   This might be expected if the storage integration isn't fully configured")
        
        # Step 9: Select from stage
        print("\n[9] Selecting first two columns from stage...")
        try:
            cursor.execute("SELECT $1, $2 FROM @S3_stage LIMIT 10")
            rows = cursor.fetchall()
            print(f"\nSample data from stage (first 10 rows):")
            for row in rows:
                print(f"  {row}")
        except Exception as e:
            print(f"[WARN]  Error selecting from stage: {e}")
        
        # Step 10: Set warehouse
        print("\n[10] Setting warehouse...")
        cursor.execute("USE WAREHOUSE COMPUTE_WH")
        print("[OK] Warehouse set")
        
        # Step 11: Create Snowpipe
        print("\n[11] Creating Snowpipe...")
        cursor.execute("""
            CREATE PIPE S3_db.public.S3_pipe AUTO_INGEST=TRUE as
              COPY INTO S3_db.public.S3_table
              FROM @S3_db.public.S3_stage
        """)
        print("[OK] Snowpipe created")
        
        # Step 12: Select from table
        print("\n[12] Selecting from S3_table...")
        cursor.execute("SELECT * FROM S3_db.public.S3_table")
        table_data = cursor.fetchall()
        print(f"\nData in table: {len(table_data)} rows")
        for row in table_data[:10]:  # Show first 10 rows
            print(f"  {row}")
        
        # Step 13: Show pipes
        print("\n[13] Showing all pipes...")
        cursor.execute("SHOW PIPES")
        pipes = cursor.fetchall()
        print("\nPipes:")
        if pipes:
            columns = [desc[0] for desc in cursor.description]
            print(f"  Columns: {', '.join(columns)}")
            for pipe in pipes:
                print(f"  {pipe}")
        else:
            print("  No pipes found")
        
        # Step 14: Describe pipe
        print("\n[14] Describing S3_pipe...")
        cursor.execute("DESCRIBE PIPE S3_db.public.S3_pipe")
        pipe_details = cursor.fetchall()
        print("\nPipe Details:")
        for row in pipe_details:
            print(f"  {row[0]}: {row[1]}")
        
        # Step 15: Pause pipe
        print("\n[15] Pausing pipe...")
        cursor.execute("ALTER PIPE S3_db.public.S3_pipe SET PIPE_EXECUTION_PAUSED = TRUE")
        print("[OK] Pipe paused")
        
        # Step 16: Drop pipe
        print("\n[16] Dropping pipe...")
        cursor.execute("DROP PIPE S3_db.public.S3_pipe")
        print("[OK] Pipe dropped")
        
        # Step 17: Show pipes again (should be empty)
        print("\n[17] Showing pipes after drop...")
        cursor.execute("SHOW PIPES")
        pipes_after = cursor.fetchall()
        print(f"\nRemaining pipes: {len(pipes_after)}")
        
        print("\n" + "="*60)
        print("SNOWPIPE ASSIGNMENT - COMPLETED")
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
    # You can provide the AWS Role ARN as an environment variable or pass it here
    aws_role_arn = os.getenv('AWS_ROLE_ARN')  # Optional: set this in .env if needed
    
    print("Starting Snowpipe Assignment...")
    print("Note: If AWS Role ARN is 'REMOVED', you'll need to configure it properly.")
    print("      Check the storage integration details to get the ARN to use in AWS.")
    
    execute_snowpipe_assignment(aws_role_arn)

