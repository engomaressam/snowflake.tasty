#!/usr/bin/env python3
"""
Automated SQL Generator for Snowflake Tasty Bytes Assignment
This script generates the exact SQL commands and provides step-by-step execution
"""

def generate_sql_commands():
    """
    Generate all SQL commands for the Tasty Bytes assignment
    """
    
    sql_commands = {
        "setup": [
            {
                "step": 1,
                "title": "Set Role and Warehouse",
                "sql": """-- Set the Role
USE ROLE accountadmin;

-- Set the Warehouse  
USE WAREHOUSE compute_wh;""",
                "description": "This sets your session to use the ACCOUNTADMIN role and COMPUTE_WH warehouse"
            },
            {
                "step": 2,
                "title": "Create Database and Schema",
                "sql": """-- Create the Tasty Bytes Database
CREATE OR REPLACE DATABASE tasty_bytes_sample_data;

-- Create the Raw POS (Point-of-Sale) Schema
CREATE OR REPLACE SCHEMA tasty_bytes_sample_data.raw_pos;""",
                "description": "Creates the main database and schema for our sample data"
            },
            {
                "step": 3,
                "title": "Create Menu Table",
                "sql": """-- Create the Raw Menu Table
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
);""",
                "description": "Creates the menu table with all required columns and data types"
            },
            {
                "step": 4,
                "title": "Verify Empty Table",
                "sql": """-- Confirm the empty Menu table exists
SELECT * FROM tasty_bytes_sample_data.raw_pos.menu;""",
                "description": "Verifies the table was created (should return 0 rows initially)"
            },
            {
                "step": 5,
                "title": "Create S3 Stage",
                "sql": """-- Create the Stage referencing the Blob location and CSV File Format
CREATE OR REPLACE STAGE tasty_bytes_sample_data.public.blob_stage
url = 's3://sfquickstarts/tastybytes/'
file_format = (type = csv);""",
                "description": "Creates a stage that points to the S3 bucket containing our sample data"
            },
            {
                "step": 6,
                "title": "List Stage Files",
                "sql": """-- Query the Stage to find the Menu CSV file
LIST @tasty_bytes_sample_data.public.blob_stage/raw_pos/menu/;""",
                "description": "Lists the files available in the stage to verify data is accessible"
            },
            {
                "step": 7,
                "title": "Load Data into Table",
                "sql": """-- Copy the Menu file into the Menu table
COPY INTO tasty_bytes_sample_data.raw_pos.menu
FROM @tasty_bytes_sample_data.public.blob_stage/raw_pos/menu/;""",
                "description": "Loads the CSV data from S3 into our menu table"
            }
        ],
        "verification": [
            {
                "step": 8,
                "title": "Count Total Rows",
                "sql": """-- How many rows are in the table?
SELECT COUNT(*) AS row_count FROM tasty_bytes_sample_data.raw_pos.menu;""",
                "description": "Shows the total number of menu items loaded"
            },
            {
                "step": 9,
                "title": "Show Sample Data",
                "sql": """-- What do the top 10 rows look like?
SELECT TOP 10 * FROM tasty_bytes_sample_data.raw_pos.menu;""",
                "description": "Displays the first 10 rows to verify data structure"
            },
            {
                "step": 10,
                "title": "Group by Truck Brand",
                "sql": """-- Group by truck brand name
SELECT TRUCK_BRAND_NAME, COUNT(*)
FROM tasty_bytes_sample_data.raw_pos.menu
GROUP BY 1
ORDER BY 2 DESC;""",
                "description": "Shows which truck brands have the most menu items"
            },
            {
                "step": 11,
                "title": "Group by Truck Brand and Menu Type",
                "sql": """-- Group by truck brand name and menu type
SELECT
    TRUCK_BRAND_NAME,
    MENU_TYPE,
    COUNT(*)
FROM tasty_bytes_sample_data.raw_pos.menu
GROUP BY 1,2
ORDER BY 3 DESC;""",
                "description": "Shows menu item counts by truck brand and menu type"
            }
        ]
    }
    
    return sql_commands

def generate_complete_sql_file():
    """
    Generate a complete SQL file with all commands
    """
    commands = generate_sql_commands()
    
    complete_sql = """-- Tasty Bytes Sample Data Setup - Complete Assignment
-- Copy and paste this entire block into Snowflake Worksheets
-- Make sure to click "Run All" (down arrow next to run button)

"""
    
    # Add setup commands
    for cmd in commands["setup"]:
        complete_sql += f"-- Step {cmd['step']}: {cmd['title']}\n"
        complete_sql += cmd["sql"] + "\n\n"
    
    # Add verification commands
    for cmd in commands["verification"]:
        complete_sql += f"-- Step {cmd['step']}: {cmd['title']}\n"
        complete_sql += cmd["sql"] + "\n\n"
    
    return complete_sql

def generate_step_by_step_guide():
    """
    Generate a detailed step-by-step guide
    """
    commands = generate_sql_commands()
    
    guide = """# Tasty Bytes Assignment - Step-by-Step Guide

## Prerequisites
- Access to Snowflake web interface
- Logged in with ACCOUNTADMIN role
- COMPUTE_WH warehouse available

## Instructions

### Method 1: Quick Setup (Recommended)
1. Open Snowflake Worksheets
2. Copy the entire contents of `complete_assignment.sql`
3. Paste into a new worksheet
4. Click "Run All" (down arrow next to run button)

### Method 2: Step-by-Step Execution

"""
    
    # Add setup steps
    for cmd in commands["setup"]:
        guide += f"""#### Step {cmd['step']}: {cmd['title']}
**Description:** {cmd['description']}

**SQL Command:**
```sql
{cmd['sql']}
```

**Expected Result:** {get_expected_result(cmd['step'])}

---

"""
    
    # Add verification steps
    for cmd in commands["verification"]:
        guide += f"""#### Step {cmd['step']}: {cmd['title']}
**Description:** {cmd['description']}

**SQL Command:**
```sql
{cmd['sql']}
```

**Expected Result:** {get_expected_result(cmd['step'])}

---

"""
    
    guide += """
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
"""
    
    return guide

def get_expected_result(step):
    """
    Get expected result for each step
    """
    expected_results = {
        1: "Role and warehouse set successfully",
        2: "Database and schema created successfully", 
        3: "Table created with proper structure",
        4: "Empty table (0 rows) - this is expected",
        5: "Stage created successfully",
        6: "List of CSV files in the stage",
        7: "Data loaded successfully (100+ rows)",
        8: "Total row count (should be 100+)",
        9: "Sample menu data with truck brands and prices",
        10: "Truck brands ranked by menu item count",
        11: "Truck brands and menu types with counts"
    }
    return expected_results.get(step, "Command executed successfully")

def main():
    """
    Main function to generate all files
    """
    print("Generating Tasty Bytes Assignment Files...")
    
    # Generate complete SQL file
    complete_sql = generate_complete_sql_file()
    with open('complete_assignment.sql', 'w', encoding='utf-8') as f:
        f.write(complete_sql)
    print("Generated: complete_assignment.sql")
    
    # Generate step-by-step guide
    guide = generate_step_by_step_guide()
    with open('STEP_BY_STEP_GUIDE.md', 'w', encoding='utf-8') as f:
        f.write(guide)
    print("Generated: STEP_BY_STEP_GUIDE.md")
    
    # Generate individual step files
    commands = generate_sql_commands()
    
    for category in ['setup', 'verification']:
        for cmd in commands[category]:
            filename = f"step_{cmd['step']:02d}_{cmd['title'].lower().replace(' ', '_')}.sql"
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(f"-- Step {cmd['step']}: {cmd['title']}\n")
                f.write(f"-- {cmd['description']}\n\n")
                f.write(cmd['sql'])
            print(f"Generated: {filename}")
    
    print("\nAll files generated successfully!")
    print("\nNext Steps:")
    print("1. Open Snowflake Worksheets")
    print("2. Copy complete_assignment.sql content")
    print("3. Paste and click 'Run All'")
    print("4. Follow STEP_BY_STEP_GUIDE.md if you need detailed instructions")

if __name__ == "__main__":
    main()
