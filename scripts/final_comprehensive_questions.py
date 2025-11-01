"""
Final Comprehensive Snowflake Multiple Choice Questions - Answers

25 questions covering Snowsight, databases, warehouses, views, Snowpark, 
procedures, roles, tables, observability, Cortex, and more
"""

def answer_final_questions():
    """
    Answer all 25 final comprehensive Snowflake questions
    """
    print("="*70)
    print("FINAL COMPREHENSIVE SNOWFLAKE QUESTIONS - 25 QUESTIONS - ANSWERS")
    print("="*70)
    
    print("\nQuestion 1: What is the name of the primary browser-based interface of Snowflake?")
    print("\nOptions:")
    print("A. Snowsight")
    print("B. Snowgrid")
    print("C. SnowPro")
    print("D. Snowpark")
    print("\nAnswer: A")
    print("Explanation: Snowsight is Snowflake's primary web-based user interface.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 2: How do you delete and then restore a database in Snowflake?")
    print("\nOptions:")
    print("A. Delete from the UI and restore from the 'Deleted items.'")
    print("B. Run DELETE DATABASE and then RECOVER DATABASE.")
    print("C. Use the DROP DATABASE command to delete and UNDROP DATABASE to restore.")
    print("D. Use the REMOVE DATABASE command and then RESTORE DATABASE.")
    print("\nAnswer: C")
    print("Explanation: DROP DATABASE removes the database, and UNDROP DATABASE restores it.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 3: How do you resize a warehouse to a larger size using an SQL command in Snowflake?")
    print("\nOptions:")
    print("A. Use the 'UPDATE WAREHOUSE' command followed by the desired size.")
    print("B. Type 'ALTER WAREHOUSE' followed by the warehouse name, then 'SET warehouse_size = MEDIUM'")
    print("C. Right-click on the warehouse and select 'Resize.'")
    print("D. Click on the warehouse settings and drag the size slider to the desired position.")
    print("\nAnswer: B")
    print("Explanation: ALTER WAREHOUSE <name> SET warehouse_size = <SIZE> is the SQL command.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 4: What is the purpose of the INFORMATION_SCHEMA in Snowflake?")
    print("\nOptions:")
    print("A. It contains views that can be used to query metadata about the objects in the database.")
    print("B. It initiates changes and updates within the database.")
    print("C. It is used for setting database and schema configurations.")
    print("D. It stores raw data and information for database operations.")
    print("\nAnswer: A")
    print("Explanation: INFORMATION_SCHEMA provides metadata views about database objects.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 5: What is one way to find out when a materialized view was last refreshed in Snowflake?")
    print("\nOptions:")
    print("A. Use the SHOW MATERIALIZED VIEWS command.")
    print("B. Run the QUERY LAST REFRESHED command on the materialized view.")
    print("C. Check the 'Last Refreshed' column in the materialized view's properties in the UI.")
    print("D. Look up the refresh date in the materialized view's settings menu.")
    print("\nAnswer: A")
    print("Explanation: SHOW MATERIALIZED VIEWS displays metadata including refresh information.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 6: What is a primary difference between a non-materialized view and a materialized view in Snowflake?")
    print("\nOptions:")
    print("A. Non-materialized views cannot include joins, while materialized views can.")
    print("B. Non-materialized views automatically refresh data, while materialized views do not.")
    print("C. Non-materialized views store the query, while materialized views store the query's results.")
    print("D. Non-materialized views require you to set a refresh, while materialized views have a default refresh cadence.")
    print("\nAnswer: C")
    print("Explanation: Standard views store the query definition, materialized views store pre-computed results.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 7: What is the primary function of a virtual warehouse in Snowflake?")
    print("\nOptions:")
    print("A. Executing SQL queries and DML commands such as INSERT and DELETE.")
    print("B. Organizing and displaying data visualization tools.")
    print("C. Managing user access and security settings.")
    print("D. Storing permanent data such as tables and databases.")
    print("\nAnswer: A")
    print("Explanation: Virtual warehouses are compute resources that execute SQL queries and DML operations.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 8: How do you query the INFORMATION_SCHEMA to find a list of all tables in a database in Snowflake?")
    print("\nOptions:")
    print("A. Execute a GET INFO command on INFORMATION_SCHEMA.")
    print("B. Use a SELECT statement from the INFORMATION_SCHEMA.TABLES view.")
    print("C. Use the QUERY SCHEMA function within INFORMATION_SCHEMA.")
    print("D. Look up the table details in the database properties in the UI.")
    print("\nAnswer: B")
    print("Explanation: SELECT * FROM INFORMATION_SCHEMA.TABLES queries table metadata.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 9: What is one way to view metadata about a materialized view in Snowflake?")
    print("\nOptions:")
    print("A. Run the METADATA FOR VIEW command with the materialized view's name.")
    print("B. Select 'View Metadata' from the options menu for the materialized view in the UI.")
    print("C. Use the DESCRIBE MATERIALIZED VIEW command followed by the materialized view name.")
    print("D. Access the metadata from the materialized view's details page in the account settings UI.")
    print("\nAnswer: C")
    print("Explanation: DESCRIBE MATERIALIZED VIEW <name> displays metadata about the view.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 10: What is the primary purpose of using resource monitors in Snowflake?")
    print("\nOptions:")
    print("A. To track the number of users accessing the Snowflake environment.")
    print("B. To manually monitor and adjust credit usage on a daily basis.")
    print("C. To increase the number of credits available to an account or warehouse.")
    print("D. To manage credit usage efficiently and prevent excessive consumption by setting automatic actions based on credit usage thresholds.")
    print("\nAnswer: D")
    print("Explanation: Resource monitors track credit usage and trigger actions (notify/suspend) at thresholds.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 11: What is a way to load a table as a Snowpark DataFrame using the session object in Snowflake?")
    print("\nOptions:")
    print("A. Utilize the IMPORT DATA command to load data into a Snowpark DataFrame.")
    print("B. Use the CREATE TABLE SQL command within a Snowpark session.")
    print("C. Use session.table('table_name') or session.sql('SELECT * FROM table_name').")
    print("D. Directly input the table name into the Snowflake UI without using the session object.")
    print("\nAnswer: C")
    print("Explanation: session.table() and session.sql() are the methods to load data as DataFrames.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 12: What is a primary purpose of using the CREATE PROCEDURE command in Snowflake?")
    print("\nOptions:")
    print("A. To modify the settings of the Snowflake environment.")
    print("B. To retrieve data from the database without changing any data.")
    print("C. To generate a new table in the database for data storage.")
    print("D. To define a stored procedure that can perform complex operations like deleting data based on specific conditions.")
    print("\nAnswer: D")
    print("Explanation: Stored procedures encapsulate complex logic and can perform DDL/DML operations.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 13: What is one way to view the privileges granted to a specific role in Snowflake?")
    print("\nOptions:")
    print("A. Use the SHOW GRANTS TO ROLE command followed by the role name.")
    print("B. Use the CREATE ROLE command to view privileges.")
    print("C. Use the USE ROLE command to list privileges.")
    print("D. Use the GRANT ROLE command to check privileges.")
    print("\nAnswer: A")
    print("Explanation: SHOW GRANTS TO ROLE <role_name> displays all privileges granted to that role.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 14: What are some benefits of using the VS Code Snowflake extension when working in VS Code?")
    print("\nOptions:")
    print("A. It provides a secure network connection to Snowflake servers.")
    print("B. It enhances the Snowflake user experience by providing features like autocomplete, integrated documentation, and SQL execution within VS Code.")
    print("C. It increases the storage capacity of Snowflake databases.")
    print("D. It allows for the creation of new Snowflake accounts directly within VS Code.")
    print("\nAnswer: B")
    print("Explanation: VS Code extension provides IDE features like autocomplete, docs, and SQL execution.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 15: What is one purpose of user-defined functions (UDF) in Snowflake?")
    print("\nOptions:")
    print("A. To simplify SQL queries by encapsulating frequently used code into a function.")
    print("B. To reduce the number of databases in Snowflake.")
    print("C. To correct built-in functions.")
    print("D. To make it easy to run commands like inserts or updates.")
    print("\nAnswer: A")
    print("Explanation: UDFs encapsulate reusable logic to simplify and standardize SQL queries.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 16: Which table type in Snowflake can have a retention period of up to 90 days if you're using the enterprise edition?")
    print("\nOptions:")
    print("A. Permanent table")
    print("B. Transient table")
    print("C. Temporary table")
    print("D. All of the above")
    print("\nAnswer: A")
    print("Explanation: Only permanent tables can have retention periods up to 90 days (Enterprise edition).")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 17: Which of the following statements is true regarding the fail-safe period in Snowflake?")
    print("\nOptions:")
    print("A. Both transient and temporary tables have a fail-safe period.")
    print("B. All table types have a fail-safe period of seven days.")
    print("C. Only permanent tables have a fail-safe period of seven days.")
    print("D. Temporary tables have a longer fail-safe period than transient tables.")
    print("\nAnswer: C")
    print("Explanation: Only permanent tables have a 7-day fail-safe period after Time Travel retention.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 18: How do observability features in Snowflake help manage data pipelines?")
    print("\nOptions:")
    print("A. By increasing the speed of data ingestion.")
    print("B. By automating the deployment of data pipelines.")
    print("C. By reducing the cost of data storage.")
    print("D. By providing tools for monitoring, alerting, and debugging issues.")
    print("\nAnswer: D")
    print("Explanation: Observability features provide monitoring, alerting, and debugging capabilities.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 19: Which one of the following is NOT a command you can run with pipes?")
    print("\nOptions:")
    print("A. EXPLAIN PIPE")
    print("B. SHOW PIPES")
    print("C. CREATE PIPE")
    print("D. DESCRIBE PIPE")
    print("\nAnswer: A")
    print("Explanation: EXPLAIN PIPE is not a valid command. Valid commands are CREATE, SHOW, DESCRIBE, ALTER, DROP.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 20: How does Snowflake Copilot assist in query generation?")
    print("\nOptions:")
    print("A. It generates natural language from SQL queries.")
    print("B. It automatically optimizes database performance.")
    print("C. It creates visual representations of data models.")
    print("D. It generates SQL queries from natural language questions.")
    print("\nAnswer: D")
    print("Explanation: Snowflake Copilot is an AI assistant that generates SQL from natural language.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 21: How do you use a Snowflake Cortex ML function in a SQL worksheet?")
    print("\nOptions:")
    print("A. Some Snowflake Cortex ML functions can be called directly within a SELECT statement, and others through a CALL command, both in a SQL workflow.")
    print("B. You use a CALL command.")
    print("C. You call Snowflake Cortex ML functions directly within a SELECT statement.")
    print("D. You use SQL to generate predictions that are then analyzed in a separate ML software.")
    print("\nAnswer: A")
    print("Explanation: Some Cortex functions (like COMPLETE) work in SELECT, others (like some model training) use CALL.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 22: Which line of code would load the 'test_database.test_schema.df_clean' table in Snowpark?")
    print("\nOptions:")
    print("A. snowpark_df = session.load_table('test_database.test_schema.df_clean')")
    print("B. snowpark_df = session.table('test_database.test_schema.df_clean')")
    print("C. snowpark_df = session.sql('test_database.test_schema.df_clean')")
    print("D. snowpark_df = session.load('test_database.test_schema.df_clean')")
    print("\nAnswer: B")
    print("Explanation: session.table('table_name') is the method to load a table as a Snowpark DataFrame.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 23: How can Snowflake's Hybrid Tables feature benefit application development?")
    print("\nOptions:")
    print("A. They provide capabilities for both transactional and analytical workloads within a single table.")
    print("B. They enhance the security settings of the application.")
    print("C. They automatically generate user interfaces for applications.")
    print("D. They serve as a flexible structure for generating tables that have both temporary and transient features.")
    print("\nAnswer: A")
    print("Explanation: Hybrid Tables support both OLTP (transactional) and OLAP (analytical) workloads.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 24: What is one way you can generate a table of responses using Snowflake Cortex Complete?")
    print("\nOptions:")
    print("A. By downloading pre-generated responses from an external database.")
    print("B. By feeding a column of prompts to Complete in a SELECT statement.")
    print("C. By sending individual emails with prompts to the system.")
    print("D. By manually entering each response into a spreadsheet.")
    print("\nAnswer: B")
    print("Explanation: You can use CORTEX.COMPLETE in a SELECT statement with a column of prompts to generate responses.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 25: Which of the following is an example of an LLM model available out-of-the-box for use with the Snowflake Cortex Complete function?")
    print("\nOptions:")
    print("A. Mistral-7b")
    print("B. Peak-9z")
    print("C. Crest-5y")
    print("D. Summit-10x")
    print("\nAnswer: A")
    print("Explanation: Mistral-7b is a pre-built LLM model available in Snowflake Cortex.")
    
    print("\n" + "="*70)
    print("SUMMARY - ALL 25 ANSWERS:")
    print("="*70)
    print("Q1:  A - Snowsight")
    print("Q2:  C - DROP DATABASE and UNDROP DATABASE")
    print("Q3:  B - ALTER WAREHOUSE ... SET warehouse_size = MEDIUM")
    print("Q4:  A - Contains views for querying metadata")
    print("Q5:  A - SHOW MATERIALIZED VIEWS")
    print("Q6:  C - Non-materialized stores query, materialized stores results")
    print("Q7:  A - Executing SQL queries and DML commands")
    print("Q8:  B - SELECT from INFORMATION_SCHEMA.TABLES")
    print("Q9:  C - DESCRIBE MATERIALIZED VIEW")
    print("Q10: D - Manage credit usage with automatic actions")
    print("Q11: C - session.table() or session.sql()")
    print("Q12: D - Define stored procedure for complex operations")
    print("Q13: A - SHOW GRANTS TO ROLE")
    print("Q14: B - Autocomplete, documentation, SQL execution in VS Code")
    print("Q15: A - Simplify SQL queries by encapsulating code")
    print("Q16: A - Permanent table")
    print("Q17: C - Only permanent tables have 7-day fail-safe")
    print("Q18: D - Monitoring, alerting, and debugging tools")
    print("Q19: A - EXPLAIN PIPE (not valid)")
    print("Q20: D - Generates SQL from natural language")
    print("Q21: A - Some in SELECT, others via CALL command")
    print("Q22: B - session.table('table_name')")
    print("Q23: A - Both transactional and analytical workloads")
    print("Q24: B - Feed column of prompts to Complete in SELECT")
    print("Q25: A - Mistral-7b")
    print("="*70)

if __name__ == "__main__":
    answer_final_questions()

