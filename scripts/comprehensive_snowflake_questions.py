"""
Comprehensive Snowflake Multiple Choice Questions - Answers

15 questions covering Cortex LLM, Streamlit, Snowpipe, Document AI, Feature Store, 
Snowpark ML, and Container Services
"""

def answer_comprehensive_questions():
    """
    Answer all 15 comprehensive Snowflake questions
    """
    print("="*70)
    print("COMPREHENSIVE SNOWFLAKE MULTIPLE CHOICE QUESTIONS - ANSWERS")
    print("="*70)
    
    print("\nQuestion 1: What is one way to use the Snowflake Cortex Complete function in a SELECT statement?")
    print("\nOptions:")
    print("A. To update security settings within Snowflake.")
    print("B. To create a new database schema.")
    print("C. To manage user access permissions.")
    print("D. To get a response from an LLM model based on a given prompt.")
    print("\nAnswer: D")
    print("Explanation: SNOWFLAKE.CORTEX.COMPLETE is used to get responses from LLM models like mistral-7b based on prompts.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 2: How can you run a SQL query within a Streamlit app in Snowflake?")
    print("\nOptions:")
    print("A. By configuring the app's database connection in Streamlit's UI.")
    print("B. By using the 'session.sql' function, which allows you to execute queries directly.")
    print("C. By directly inserting SQL code into the Streamlit app settings.")
    print("D. By writing a SQL command into the 'st.write' function.")
    print("\nAnswer: B")
    print("Explanation: In Streamlit in Snowflake, you use session = get_active_session() and then session.sql('query') to run SQL queries.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 3: What is one way you can use the 'st.title' function in a Streamlit app?")
    print("\nOptions:")
    print("A. To organize data into multiple columns.")
    print("B. To generate a SQL query.")
    print("C. To create a session object.")
    print("D. To create a markdown-style title in the app.")
    print("\nAnswer: D")
    print("Explanation: st.title() creates a large title heading in Streamlit apps, similar to markdown # heading.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 4: What is one benefit of using Streamlit in Snowflake?")
    print("\nOptions:")
    print("A. Increase the physical storage capacity available to apps.")
    print("B. Directly manage user access permissions for databases.")
    print("C. Easily connect to datasets within your warehouse and deploy securely.")
    print("D. Automatically translate application code into multiple languages.")
    print("\nAnswer: C")
    print("Explanation: Streamlit in Snowflake allows secure, native connection to Snowflake data and easy deployment within Snowflake's environment.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 5: What is one way you can use the 'st.select_slider' function in a Streamlit app within Snowflake?")
    print("\nOptions:")
    print("A. To create a secure connection to external web services.")
    print("B. To create a slider that users can interact with to select a range of values.")
    print("C. To display static images from a dataset.")
    print("D. To generate real-time updates to the database schema.")
    print("\nAnswer: B")
    print("Explanation: st.select_slider creates an interactive slider widget for users to select a range of values (e.g., date ranges).")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 6: How can you use the Snowflake Cortex Complete function to incorporate a history of prompts and responses?")
    print("\nOptions:")
    print("A. By recording each session manually for future reference.")
    print("B. By appending each new prompt to a text file.")
    print("C. By including an array of objects, each specifying a role and content, in the function call.")
    print("D. By creating a new database for each prompt and response pair.")
    print("\nAnswer: C")
    print("Explanation: Cortex COMPLETE accepts an array of message objects with 'role' (system/user/assistant) and 'content' fields for conversation history.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 7: What is one way streaming and batch ingestion methods differ?")
    print("\nOptions:")
    print("A. Streaming ingestion is primarily used for unstructured data, while batch is used for structured data.")
    print("B. Batch ingestion allows for real-time data updates, whereas streaming does not.")
    print("C. Batch ingestion is more secure than streaming ingestion.")
    print("D. Streaming ingestion provides updates from external sources very quickly, while batch ingestion happens less frequently.")
    print("\nAnswer: D")
    print("Explanation: Streaming provides near real-time updates continuously, while batch processes data in scheduled intervals.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 8: What are the two pieces of information you need from AWS to create a STORAGE INTEGRATION object?")
    print("\nOptions:")
    print("A. 'AWS_ROLE', 'STORAGE_ALLOWED_LOCATIONS'")
    print("B. 'STORAGE_AWS_ROLE_ARN', 'STORAGE_ALLOWED_LOCATIONS'")
    print("C. 'AWS_ROLE', 'S3_URL'")
    print("D. 'ARN', 'S3_URL'")
    print("\nAnswer: B")
    print("Explanation: STORAGE_AWS_ROLE_ARN (the AWS IAM Role ARN) and STORAGE_ALLOWED_LOCATIONS (S3 bucket paths) are the required parameters.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 9: What does a Snowpipe do?")
    print("\nOptions:")
    print("A. It automatically checks to see if there are new files in a stage, and if there are, it copies them from the stage to a table.")
    print("B. It pairs with a task, and when that task says there are new rows in an external stage, it pulls those rows automatically.")
    print("C. It pairs with a stream, and if the stream has new rows, it pulls those rows automatically.")
    print("D. It checks to see if there are new files in a stage at a set cadence (between 5 minutes and 1 hour), and if there, it copies them from the stage to a table.")
    print("\nAnswer: A")
    print("Explanation: Snowpipe automatically detects new files in a stage (via S3 notifications with AUTO_INGEST=TRUE) and copies them to a table - it's event-driven, not scheduled.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 10: How can Document AI in Snowflake be utilized to enhance data extraction from documents?")
    print("\nOptions:")
    print("A. To manually scan documents and upload them to the cloud.")
    print("B. To create backup copies of physical documents.")
    print("C. To convert PDFs into editable text files.")
    print("D. To automatically extract data from PDFs and store it in Snowflake tables for analysis.")
    print("\nAnswer: D")
    print("Explanation: Document AI uses ML to automatically extract structured data from unstructured documents (PDFs) and store it in Snowflake for analysis.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 11: What is a primary benefit of the Snowflake Feature Store?")
    print("\nOptions:")
    print("A. It provides a centralized repository to manage and serve features consistently across ML models.")
    print("B. It reduces the cost of data storage within Snowflake.")
    print("C. It automatically converts SQL queries into machine learning algorithms.")
    print("D. It enhances the graphical visualization of ML modeling results.")
    print("\nAnswer: A")
    print("Explanation: Feature Store provides centralized feature management, versioning, and serving for consistent ML model training and inference.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 12: What is one way you can leverage Streamlit in Snowflake to enhance your ML projects?")
    print("\nOptions:")
    print("A. Use Streamlit in Snowflake to directly modify the underlying ML models in Snowflake.")
    print("B. Use Streamlit in Snowflake to replace the need for SQL in Snowflake.")
    print("C. Use Streamlit in Snowflake to develop interactive web applications based on ML models from directly within Snowflake.")
    print("D. Use Streamlit in Snowflake to automate your model registry.")
    print("\nAnswer: C")
    print("Explanation: Streamlit enables building interactive web apps to visualize and interact with ML model results directly within Snowflake.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 13: How would you randomly allocate 90% of data to training set and 10% to test set?")
    print("\nOptions:")
    print("A. train_df, test_df = snowpark_df.randomSplit([0.9, 0.1])")
    print("B. train_df, test_df = snowpark_df.random([0.9, 0.1])")
    print("C. train_df, test_df = snowpark_df.train_test([0.9, 0.1])")
    print("D. train_df, test_df = snowpark_df.split([0.9, 0.1])")
    print("\nAnswer: A")
    print("Explanation: The randomSplit() method in Snowpark splits a DataFrame into multiple DataFrames based on the provided weights.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 14: Which line of code would train and fit an XGBClassifier model?")
    print("\nOptions:")
    print("A. XGBClassifier.train_and_fit(train_df, FEATURE_COLS, LABEL_COLS)")
    print("B. XGBClassifier(input_cols=FEATURE_COLS, label_cols=LABEL_COLS).fit(train_df)")
    print("C. XGBClassifier.train_df(FEATURE_COLS, LABEL_COLS).fit()")
    print("D. train_df.XGBClassifier.fit(FEATURE_COLS, LABEL_COLS)")
    print("\nAnswer: B")
    print("Explanation: XGBClassifier is instantiated with input_cols and label_cols parameters, then fit() is called with the training DataFrame.")
    
    print("\n" + "-"*70)
    
    print("\nQuestion 15: What is a unique capability that Snowpark Container Services offers for application hosting in Snowflake?")
    print("\nOptions:")
    print("A. It eliminates the need for SQL within applications.")
    print("B. It provides automatic conversion of non-Python applications to Python.")
    print("C. It reduces the need for any backend development.")
    print("D. It allows the entire application architecture, including the frontend, to be hosted within Snowflake's environment.")
    print("\nAnswer: D")
    print("Explanation: Snowpark Container Services allows hosting full-stack applications (frontend, backend, APIs) entirely within Snowflake's secure environment.")
    
    print("\n" + "="*70)
    print("SUMMARY:")
    print("Question 1:  D - To get a response from an LLM model based on a given prompt")
    print("Question 2:  B - By using the 'session.sql' function")
    print("Question 3:  D - To create a markdown-style title in the app")
    print("Question 4:  C - Easily connect to datasets within your warehouse and deploy securely")
    print("Question 5:  B - To create a slider that users can interact with to select a range of values")
    print("Question 6:  C - By including an array of objects, each specifying a role and content")
    print("Question 7:  D - Streaming provides updates very quickly, while batch happens less frequently")
    print("Question 8:  B - 'STORAGE_AWS_ROLE_ARN', 'STORAGE_ALLOWED_LOCATIONS'")
    print("Question 9:  A - It automatically checks for new files in a stage and copies them to a table")
    print("Question 10: D - To automatically extract data from PDFs and store it in Snowflake tables")
    print("Question 11: A - Provides a centralized repository to manage and serve features consistently")
    print("Question 12: C - To develop interactive web applications based on ML models")
    print("Question 13: A - train_df, test_df = snowpark_df.randomSplit([0.9, 0.1])")
    print("Question 14: B - XGBClassifier(input_cols=FEATURE_COLS, label_cols=LABEL_COLS).fit(train_df)")
    print("Question 15: D - Allows the entire application architecture, including frontend, to be hosted within Snowflake")
    print("="*70)

if __name__ == "__main__":
    answer_comprehensive_questions()

