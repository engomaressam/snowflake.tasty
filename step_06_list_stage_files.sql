-- Step 6: List Stage Files
-- Lists the files available in the stage to verify data is accessible

-- Query the Stage to find the Menu CSV file
LIST @tasty_bytes_sample_data.public.blob_stage/raw_pos/menu/;