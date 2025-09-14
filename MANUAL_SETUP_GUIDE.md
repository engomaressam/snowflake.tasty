# Tasty Bytes Sample Data Setup - Manual Guide

Since the programmatic connection is having authentication issues, here's a step-by-step guide to complete the assignment manually in the Snowflake web interface.

## Step 1: Access Snowflake Web Interface

1. Open your web browser and go to your Snowflake account URL
2. Log in with your credentials
3. Make sure you're in the ACCOUNTADMIN role

## Step 2: Open a New Worksheet

1. Click on "Worksheets" in the left navigation
2. Click "New Worksheet" or the "+" button
3. Name it "Tasty Bytes Setup"

## Step 3: Execute the SQL Commands

Copy and paste the following SQL commands one by one, or all at once and click "Run All":

### Step 3.1: Set Role and Warehouse
```sql
-- Set the Role
USE ROLE accountadmin;

-- Set the Warehouse
USE WAREHOUSE compute_wh;
```

### Step 3.2: Create Database and Schema
```sql
-- Create the Tasty Bytes Database
CREATE OR REPLACE DATABASE tasty_bytes_sample_data;

-- Create the Raw POS (Point-of-Sale) Schema
CREATE OR REPLACE SCHEMA tasty_bytes_sample_data.raw_pos;
```

### Step 3.3: Create the Menu Table
```sql
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
```

### Step 3.4: Verify Empty Table
```sql
-- Confirm the empty Menu table exists
SELECT * FROM tasty_bytes_sample_data.raw_pos.menu;
```

### Step 3.5: Create Stage and Load Data
```sql
-- Create the Stage referencing the Blob location and CSV File Format
CREATE OR REPLACE STAGE tasty_bytes_sample_data.public.blob_stage
url = 's3://sfquickstarts/tastybytes/'
file_format = (type = csv);

-- Query the Stage to find the Menu CSV file
LIST @tasty_bytes_sample_data.public.blob_stage/raw_pos/menu/;

-- Copy the Menu file into the Menu table
COPY INTO tasty_bytes_sample_data.raw_pos.menu
FROM @tasty_bytes_sample_data.public.blob_stage/raw_pos/menu/;
```

### Step 3.6: Verification Queries
```sql
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
```

## Expected Results

After completing all steps, you should see:

1. **Empty table initially**: 0 rows
2. **After data load**: Several hundred rows of menu data
3. **Verification queries**: Should show:
   - Total row count (likely 100+ rows)
   - Sample data with menu items, truck brands, prices
   - Grouped data showing truck brand popularity

## Troubleshooting

- If you get permission errors, make sure you're using the ACCOUNTADMIN role
- If the warehouse doesn't exist, try using a different warehouse or create one
- If the stage creation fails, check that the S3 URL is accessible
- If the COPY command fails, verify the file path in the stage

## Next Steps

Once this setup is complete, you'll have:
- A working database with sample data
- Understanding of Snowflake's data loading process
- A foundation for future assignments

The data will be ready for analysis, transformations, and other Snowflake operations in subsequent assignments.
