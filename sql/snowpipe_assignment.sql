-- Snowpipe Assignment
-- This script creates a storage integration, database, table, stage, and Snowpipe for automatic data ingestion from S3

-- IMPORTANT: Replace "REMOVED" with your actual AWS IAM Role ARN
-- Format: arn:aws:iam::ACCOUNT_ID:role/ROLE_NAME
-- You'll get the ARN details after creating the integration and running DESCRIBE INTEGRATION
-- The integration will show you what ARN to configure in AWS IAM

-- Step 1: Create the storage integration
CREATE OR REPLACE STORAGE INTEGRATION S3_role_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = "REMOVED"
  STORAGE_ALLOWED_LOCATIONS = ("s3://intro-to-snowflake-snowpipe/");

-- Step 2: Describe the storage integration to see the info you need to copy over to AWS
DESCRIBE INTEGRATION S3_role_integration;

-- Step 3: Create the database
CREATE OR REPLACE DATABASE S3_db;

-- Step 4: Create the table (automatically in the public schema, because we didn't specify)
CREATE OR REPLACE TABLE S3_db.public.S3_table(food STRING, taste INT);

-- Step 5: Use schema
USE SCHEMA S3_db.public;

-- Step 6: Create stage with the link to the S3 bucket and info on the associated storage integration
CREATE OR REPLACE STAGE S3_stage
  url = ('s3://intro-to-snowflake-snowpipe/')
  storage_integration = S3_role_integration;

-- Step 7: Show stages
SHOW STAGES;

-- Step 8: See the files in the stage
LIST @S3_stage;

-- Step 9: Select the first two columns from the stage
SELECT $1, $2 FROM @S3_stage LIMIT 10;

-- Step 10: Use warehouse
USE WAREHOUSE COMPUTE_WH;

-- Step 11: Create the snowpipe, copying from S3_stage into S3_table
CREATE PIPE S3_db.public.S3_pipe AUTO_INGEST=TRUE as
  COPY INTO S3_db.public.S3_table
  FROM @S3_db.public.S3_stage;

-- Step 12: Check the data in the table
SELECT * FROM S3_db.public.S3_table;

-- Step 13: See a list of all the pipes
SHOW PIPES;

-- Step 14: Describe the pipe
DESCRIBE PIPE S3_db.public.S3_pipe;

-- Step 15: Pause the pipe
ALTER PIPE S3_db.public.S3_pipe SET PIPE_EXECUTION_PAUSED = TRUE;

-- Step 16: Drop the pipe
DROP PIPE S3_db.public.S3_pipe;

-- Step 17: Show pipes again (should be empty)
SHOW PIPES;

