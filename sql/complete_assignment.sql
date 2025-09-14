-- Tasty Bytes Sample Data Setup - Complete Assignment
-- Copy and paste this entire block into Snowflake Worksheets
-- Make sure to click "Run All" (down arrow next to run button)

-- Step 1: Set Role and Warehouse
-- Set the Role
USE ROLE accountadmin;

-- Set the Warehouse  
USE WAREHOUSE compute_wh;

-- Step 2: Create Database and Schema
-- Create the Tasty Bytes Database
CREATE OR REPLACE DATABASE tasty_bytes_sample_data;

-- Create the Raw POS (Point-of-Sale) Schema
CREATE OR REPLACE SCHEMA tasty_bytes_sample_data.raw_pos;

-- Step 3: Create Menu Table
-- Create the Raw Menu Table
CREATE OR REPLACE TABLE tasty_bytes_sample_data.raw_pos.menu
(
    menu_id NUMBER(19,0),
    menu_type_id NUMBER(38,0),
    menu_type VARCHAR(16777216),
    truck_brand_name VARCHAR(16777216),
    menu_item_id NUMBER(38,0),
    menu_item_name VARCHAR(16777216),
    item_category VARCHAR(16777216),
    item_subcategory VARCHAR(16777216),
    cost_of_goods_usd NUMBER(38,4),
    sale_price_usd NUMBER(38,4),
    menu_item_health_metrics_obj VARIANT
);

-- Step 4: Verify Empty Table
-- Confirm the empty Menu table exists
SELECT * FROM tasty_bytes_sample_data.raw_pos.menu;

-- Step 5: Create S3 Stage
-- Create the Stage referencing the Blob location and CSV File Format
CREATE OR REPLACE STAGE tasty_bytes_sample_data.public.blob_stage
url = 's3://sfquickstarts/tastybytes/'
file_format = (type = csv);

-- Step 6: List Stage Files
-- Query the Stage to find the Menu CSV file
LIST @tasty_bytes_sample_data.public.blob_stage/raw_pos/menu/;

-- Step 7: Load Data into Table
-- Copy the Menu file into the Menu table
COPY INTO tasty_bytes_sample_data.raw_pos.menu
FROM @tasty_bytes_sample_data.public.blob_stage/raw_pos/menu/;

-- Step 8: Count Total Rows
-- How many rows are in the table?
SELECT COUNT(*) AS row_count FROM tasty_bytes_sample_data.raw_pos.menu;

-- Step 9: Show Sample Data
-- What do the top 10 rows look like?
SELECT TOP 10 * FROM tasty_bytes_sample_data.raw_pos.menu;

-- Step 10: Group by Truck Brand
-- Group by truck brand name
SELECT TRUCK_BRAND_NAME, COUNT(*)
FROM tasty_bytes_sample_data.raw_pos.menu
GROUP BY 1
ORDER BY 2 DESC;

-- Step 11: Group by Truck Brand and Menu Type
-- Group by truck brand name and menu type
SELECT
    TRUCK_BRAND_NAME,
    MENU_TYPE,
    COUNT(*)
FROM tasty_bytes_sample_data.raw_pos.menu
GROUP BY 1,2
ORDER BY 3 DESC;

