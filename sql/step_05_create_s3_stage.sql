-- Step 5: Create S3 Stage
-- Creates a stage that points to the S3 bucket containing our sample data

-- Create the Stage referencing the Blob location and CSV File Format
CREATE OR REPLACE STAGE tasty_bytes_sample_data.public.blob_stage
url = 's3://sfquickstarts/tastybytes/'
file_format = (type = csv);