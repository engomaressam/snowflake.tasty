-- Tasty Bytes Quick Setup - Copy and paste this entire block into Snowflake Worksheets
-- Make sure to click "Run All" (down arrow next to run button)

-- Set the Role
USE ROLE accountadmin;

-- Set the Warehouse
USE WAREHOUSE compute_wh;

-- Create the Tasty Bytes Database
CREATE OR REPLACE DATABASE tasty_bytes_sample_data;

-- Create the Raw POS (Point-of-Sale) Schema
CREATE OR REPLACE SCHEMA tasty_bytes_sample_data.raw_pos;

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

-- Confirm the empty Menu table exists
SELECT * FROM tasty_bytes_sample_data.raw_pos.menu;

-- Create the Stage referencing the Blob location and CSV File Format
CREATE OR REPLACE STAGE tasty_bytes_sample_data.public.blob_stage
url = 's3://sfquickstarts/tastybytes/'
file_format = (type = csv);

-- Query the Stage to find the Menu CSV file
LIST @tasty_bytes_sample_data.public.blob_stage/raw_pos/menu/;

-- Copy the Menu file into the Menu table
COPY INTO tasty_bytes_sample_data.raw_pos.menu
FROM @tasty_bytes_sample_data.public.blob_stage/raw_pos/menu/;

-- How many rows are in the table?
SELECT COUNT(*) AS row_count FROM tasty_bytes_sample_data.raw_pos.menu;

-- What do the top 10 rows look like?
SELECT TOP 10 * FROM tasty_bytes_sample_data.raw_pos.menu;

-- Group by truck brand name
SELECT TRUCK_BRAND_NAME, COUNT(*)
FROM tasty_bytes_sample_data.raw_pos.menu
GROUP BY 1
ORDER BY 2 DESC;

-- Group by truck brand name and menu type
SELECT
    TRUCK_BRAND_NAME,
    MENU_TYPE,
    COUNT(*)
FROM tasty_bytes_sample_data.raw_pos.menu
GROUP BY 1,2
ORDER BY 3 DESC;
