"""
Snowpark ML Modeling Assignment
Creates a neighborhood visiting pattern model using XGBoost classifier

Note: This is adapted from Jupyter notebook code to run as a Python script.
The original code requires Jupyter notebook environment.
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_neighborhood_pattern():
    """
    Create the neighborhood visiting pattern dictionary
    based on the truck's schedule:
    - January: N1 on Mondays (days 1, 8, 15, 22, 29), N2 other days
    - February-November: N1 on 1st, N2 on 2nd, N3 on 3rd, etc. (cycling through N1-N7)
    - December: Only N8
    """
    month_days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    pre = {}
    
    for i, month_length in enumerate(month_days):
        month = i + 1
        
        for day in range(1, month_length + 1):
            # In January, it goes to neighborhood 1 on Mondays, and neighborhood 2 the other days.
            if (month) == 1:
                if (day) % 7 == 1:
                    pre[(month, day)] = 1
                else:
                    pre[(month, day)] = 2
            
            # From February through November, it goes to neighborhood 1 on the 1st, 2 on the 2nd, 
            # 3 on the 3rd, 4 on the 4th, 5 on the 5th, 6 on the 6th, and 7 on the 7th, 
            # 1 on the 8th, 2 on the 9th, etc.
            elif (month) <= 11:
                pre[(month, day)] = ((day - 1) % 7) + 1
            
            # Every December, it only goes to neighborhood 8.
            elif (month) == 12:
                pre[(month, day)] = 8
    
    return pre

def run_snowpark_ml_assignment():
    """
    Execute the Snowpark ML modeling assignment
    """
    try:
        print("="*60)
        print("SNOWPARK ML MODELING ASSIGNMENT - STARTING")
        print("="*60)
        
        print("\n[INFO] Note: This assignment requires:")
        print("  - snowflake-ml-python package")
        print("  - snowflake-snowpark-python package")
        print("  - A table named 'test_database.test_schema.df_clean' with columns: MONTH, DAY, NEIGHBORHOOD")
        print("  - Proper Snowflake connection credentials")
        
        print("\n[1] Creating neighborhood visiting pattern...")
        pre = create_neighborhood_pattern()
        print(f"[OK] Pattern dictionary created with {len(pre)} entries")
        print(f"     Sample entries: {list(pre.items())[:5]}")
        
        print("\n[2] Setting up Snowflake session...")
        try:
            from snowflake.snowpark import Session
            from snowflake.ml.modeling.xgboost import XGBClassifier
            from snowflake.snowpark.functions import col
            from snowflake.ml.modeling import preprocessing
            from snowflake.ml.modeling.preprocessing import LabelEncoder
            
            # Create connection parameters from environment variables
            params = {
                "account": os.getenv('SNOWFLAKE_ACCOUNT'),
                "user": os.getenv('SNOWFLAKE_USER'),
                "password": os.getenv('SNOWFLAKE_PASSWORD'),
                "role": os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
                "warehouse": os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
                "database": os.getenv('SNOWFLAKE_DATABASE', 'SNOWFLAKE'),
                "schema": os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
            }
            
            # Create a Session with the necessary connection info
            session = Session.builder.configs(params).create()
            print("[OK] Snowflake session created")
            
            print("\n[3] Loading dataframe from Snowflake table...")
            try:
                # Create a dataframe (though note that this doesn't pull data into your local machine)
                snowpark_df = session.table("test_database.test_schema.df_clean")
                
                print("\n[4] Showing first 40 rows of the dataframe...")
                snowpark_df.show(n=40)
                
                print("\n[5] Counting rows in the dataframe...")
                row_count = snowpark_df.count()
                print(f"[OK] Total rows: {row_count}")
                
                print("\n[6] Describing the dataframe...")
                snowpark_df.describe().show()
                
                print("\n[7] Grouping by neighborhood and showing counts...")
                snowpark_df.group_by("Neighborhood").count().show()
                
                print("\n[8] Applying LabelEncoder to scale the target (neighborhood)...")
                # Use scikit-learn's LabelEncoder -- a more general solution -- through Snowpark ML
                le = LabelEncoder(
                    input_cols=['NEIGHBORHOOD'], 
                    output_cols=['NEIGHBORHOOD2'], 
                    drop_input_cols=True
                )
                
                # Apply the LabelEncoder
                fitted = le.fit(snowpark_df.select("NEIGHBORHOOD"))
                snowpark_df_prepared = fitted.transform(snowpark_df)
                snowpark_df_prepared.show()
                
                print("\n[9] Splitting data into training and test sets...")
                # Split the data into a training set and a test set
                train_snowpark_df, test_snowpark_df = snowpark_df_prepared.randomSplit([0.9, 0.1])
                
                print("\n[10] Saving training and test data to tables...")
                # Save training data
                train_snowpark_df.write.mode("overwrite").save_as_table("df_clean_train")
                print("[OK] Training data saved to df_clean_train")
                
                # Save test data
                test_snowpark_df.write.mode("overwrite").save_as_table("df_clean_test")
                print("[OK] Test data saved to df_clean_test")
                
                print("\n[11] Creating and training XGBClassifier model...")
                FEATURE_COLS = ["MONTH", "DAY"]
                LABEL_COLS = ["NEIGHBORHOOD2"]
                
                # Train an XGBoost model on Snowflake
                xgboost_model = XGBClassifier(
                    input_cols=FEATURE_COLS,
                    label_cols=LABEL_COLS
                )
                
                xgboost_model.fit(train_snowpark_df)
                print("[OK] XGBoost model trained")
                
                print("\n[12] Evaluating model accuracy...")
                # Check the accuracy using scikit-learn's score functionality through Snowpark ML
                accuracy = xgboost_model.score(test_snowpark_df)
                
                print(f"\n[OK] Model Accuracy: {accuracy * 100.0:.2f}%")
                
            except Exception as e:
                print(f"[ERROR] Error working with dataframe: {e}")
                print("\n[INFO] The table 'test_database.test_schema.df_clean' may not exist.")
                print("       This table needs to be created with columns: MONTH, DAY, NEIGHBORHOOD")
                print("       based on the neighborhood visiting pattern.")
                print("\n       The pattern dictionary has been created and can be used to generate the data.")
            
            session.close()
            print("\n[OK] Session closed")
            
        except ImportError as e:
            print(f"[ERROR] Missing required package: {e}")
            print("\n[INFO] Please install required packages:")
            print("  pip install snowflake-ml-python")
            print("  pip install snowflake-snowpark-python")
        
        print("\n" + "="*60)
        print("SNOWPARK ML MODELING ASSIGNMENT - COMPLETED")
        print("="*60)
        
    except Exception as e:
        print(f"\n[ERROR] Error during assignment: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("Starting Snowpark ML Modeling Assignment...")
    print("Note: This requires snowflake-ml-python and snowflake-snowpark-python packages")
    run_snowpark_ml_assignment()

