# Tasty Bytes Assignment - Step-by-Step Guide

## Prerequisites
- Access to Snowflake web interface: https://app.snowflake.com/cfzfjcw/rnb12276/#/homepage
- Logged in as OMARESSAM with ACCOUNTADMIN role
- COMPUTE_WH warehouse available

## Instructions

### Method 1: Quick Setup (Recommended)
1. Open Snowflake Worksheets
2. Copy the entire contents of `complete_assignment.sql`
3. Paste into a new worksheet
4. Click "Run All" (down arrow next to run button)

### Method 2: Step-by-Step Execution

#### Step 1: Set Role and Warehouse
**Description:** This sets your session to use the ACCOUNTADMIN role and COMPUTE_WH warehouse

**SQL Command:**
```sql
-- Set the Role
USE ROLE accountadmin;

-- Set the Warehouse  
USE WAREHOUSE compute_wh;
```

**Expected Result:** Role and warehouse set successfully

---

#### Step 2: Create Database and Schema
**Description:** Creates the main database and schema for our sample data

**SQL Command:**
```sql
-- Create the Tasty Bytes Database
CREATE OR REPLACE DATABASE tasty_bytes_sample_data;

-- Create the Raw POS (Point-of-Sale) Schema
CREATE OR REPLACE SCHEMA tasty_bytes_sample_data.raw_pos;
```

**Expected Result:** Database and schema created successfully

---

#### Step 3: Create Menu Table
**Description:** Creates the menu table with all required columns and data types

**SQL Command:**
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

**Expected Result:** Table created with proper structure

---

#### Step 4: Verify Empty Table
**Description:** Verifies the table was created (should return 0 rows initially)

**SQL Command:**
```sql
-- Confirm the empty Menu table exists
SELECT * FROM tasty_bytes_sample_data.raw_pos.menu;
```

**Expected Result:** Empty table (0 rows) - this is expected

---

#### Step 5: Create S3 Stage
**Description:** Creates a stage that points to the S3 bucket containing our sample data

**SQL Command:**
```sql
-- Create the Stage referencing the Blob location and CSV File Format
CREATE OR REPLACE STAGE tasty_bytes_sample_data.public.blob_stage
url = 's3://sfquickstarts/tastybytes/'
file_format = (type = csv);
```

**Expected Result:** Stage created successfully

---

#### Step 6: List Stage Files
**Description:** Lists the files available in the stage to verify data is accessible

**SQL Command:**
```sql
-- Query the Stage to find the Menu CSV file
LIST @tasty_bytes_sample_data.public.blob_stage/raw_pos/menu/;
```

**Expected Result:** List of CSV files in the stage

---

#### Step 7: Load Data into Table
**Description:** Loads the CSV data from S3 into our menu table

**SQL Command:**
```sql
-- Copy the Menu file into the Menu table
COPY INTO tasty_bytes_sample_data.raw_pos.menu
FROM @tasty_bytes_sample_data.public.blob_stage/raw_pos/menu/;
```

**Expected Result:** Data loaded successfully (100+ rows)

---

#### Step 8: Count Total Rows
**Description:** Shows the total number of menu items loaded

**SQL Command:**
```sql
-- How many rows are in the table?
SELECT COUNT(*) AS row_count FROM tasty_bytes_sample_data.raw_pos.menu;
```

**Expected Result:** Total row count (should be 100+)

---

#### Step 9: Show Sample Data
**Description:** Displays the first 10 rows to verify data structure

**SQL Command:**
```sql
-- What do the top 10 rows look like?
SELECT TOP 10 * FROM tasty_bytes_sample_data.raw_pos.menu;
```

**Expected Result:** Sample menu data with truck brands and prices

---

#### Step 10: Group by Truck Brand
**Description:** Shows which truck brands have the most menu items

**SQL Command:**
```sql
-- Group by truck brand name
SELECT TRUCK_BRAND_NAME, COUNT(*)
FROM tasty_bytes_sample_data.raw_pos.menu
GROUP BY 1
ORDER BY 2 DESC;
```

**Expected Result:** Truck brands ranked by menu item count

---

#### Step 11: Group by Truck Brand and Menu Type
**Description:** Shows menu item counts by truck brand and menu type

**SQL Command:**
```sql
-- Group by truck brand name and menu type
SELECT
    TRUCK_BRAND_NAME,
    MENU_TYPE,
    COUNT(*)
FROM tasty_bytes_sample_data.raw_pos.menu
GROUP BY 1,2
ORDER BY 3 DESC;
```

**Expected Result:** Truck brands and menu types with counts

---


## Troubleshooting

- **Permission Error**: Ensure you're using ACCOUNTADMIN role
- **Warehouse Error**: Check that COMPUTE_WH warehouse exists and is running
- **Stage Error**: Verify S3 URL is accessible from your Snowflake account
- **Copy Error**: Check that the file path in the stage is correct

## Success Criteria

After completing all steps, you should have:
- ✅ Database: tasty_bytes_sample_data
- ✅ Schema: raw_pos  
- ✅ Table: menu (with 100+ rows of data)
- ✅ Stage: blob_stage (pointing to S3)
- ✅ Verification queries showing sample data

## Next Steps

Once this assignment is complete, you'll be ready for:
- Data analysis and transformations
- Creating additional tables and schemas
- Working with Snowflake's advanced features
- Subsequent course assignments
