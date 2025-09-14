-- Step 7: Load Data into Table
-- Loads the CSV data from S3 into our menu table

-- Copy the Menu file into the Menu table
COPY INTO tasty_bytes_sample_data.raw_pos.menu
FROM @tasty_bytes_sample_data.public.blob_stage/raw_pos/menu/;