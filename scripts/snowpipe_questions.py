"""
Snowpipe Multiple Choice Questions - Answers

Based on Snowflake documentation and the assignment we just completed.
"""

def answer_snowpipe_questions():
    """
    Answer the Snowpipe multiple choice questions
    """
    print("="*60)
    print("SNOWPIPE MULTIPLE CHOICE QUESTIONS - ANSWERS")
    print("="*60)
    
    print("\nQuestion 1: What does a Snowpipe do?")
    print("\nOptions:")
    print("A. It automatically checks to see if there are new files in a stage, and if there are, it copies them from the stage to a table.")
    print("B. It pairs with a task, and when that task says there are new rows in an external stage, it pulls those rows automatically.")
    print("C. It checks to see if there are new files in a stage at a set cadence (between 5 minutes and 1 hour), and if there, it copies them from the stage to a table.")
    print("D. It pairs with a stream, and if the stream has new rows, it pulls those rows automatically.")
    
    print("\nAnswer: A")
    print("Explanation: Snowpipe automatically detects new files in a stage (via S3 event notifications when AUTO_INGEST=TRUE) and copies them to a table. It's event-driven, not schedule-based, and doesn't use tasks or streams.")
    
    print("\n" + "-"*60)
    
    print("\nQuestion 2: If you want to create a Snowpipe object that automatically refreshes when files are added to your AWS S3 bucket, you first need to create a role in AWS and give it the necessary permissions.")
    print("Then you need to go back to Snowflake and create a 'STORAGE INTEGRATION' object. What are the two pieces of information you need to get from AWS so that when you can create this Snowflake integration object, Snowflake has all the information it needs to make the connection?")
    
    print("\nOptions:")
    print("A. 'ARN', 'S3_URL'")
    print("B. 'AWS_ROLE', 'STORAGE_ALLOWED_LOCATIONS'")
    print("C. 'AWS_ROLE', 'S3_URL'")
    print("D. 'STORAGE_AWS_ROLE_ARN', 'STORAGE_ALLOWED_LOCATIONS'")
    
    print("\nAnswer: D")
    print("Explanation: When creating a STORAGE_INTEGRATION object in Snowflake, you need:")
    print("  - STORAGE_AWS_ROLE_ARN: The AWS IAM Role ARN you created in AWS")
    print("  - STORAGE_ALLOWED_LOCATIONS: The S3 bucket path(s) you want to allow access to")
    print("These are the exact parameter names used in the CREATE STORAGE INTEGRATION command.")
    
    print("\n" + "-"*60)
    
    print("\nQuestion 3: Which one of the following is NOT a command you can run with pipes?")
    
    print("\nOptions:")
    print("A. CREATE PIPE")
    print("B. EXPLAIN PIPE")
    print("C. SHOW PIPES")
    print("D. DESCRIBE PIPE")
    
    print("\nAnswer: B")
    print("Explanation: EXPLAIN PIPE is not a valid Snowflake command. The valid pipe commands are:")
    print("  - CREATE PIPE: Create a new pipe")
    print("  - SHOW PIPES: List all pipes")
    print("  - DESCRIBE PIPE: Get details about a specific pipe")
    print("  - ALTER PIPE: Modify pipe settings")
    print("  - DROP PIPE: Remove a pipe")
    print("EXPLAIN is used for queries (EXPLAIN SELECT...), not for pipes.")
    
    print("\n" + "="*60)
    print("SUMMARY:")
    print("Question 1: A")
    print("Question 2: D")
    print("Question 3: B")
    print("="*60)

if __name__ == "__main__":
    answer_snowpipe_questions()

