"""
Streamlit in Snowflake Multiple Choice Questions - Answers

Questions about Streamlit app modifications and functionality
"""

def answer_streamlit_questions():
    """
    Answer the Streamlit multiple choice questions
    """
    print("="*60)
    print("STREAMLIT IN SNOWFLAKE MULTIPLE CHOICE QUESTIONS - ANSWERS")
    print("="*60)
    
    print("\nQuestion 1: Use the date range filter and the 'Raw Data' tab to see the sum of the orders on 12-31-2021 for Cairo. What is that sum?")
    
    print("\nOptions:")
    print("A. '423,402.25'")
    print("B. '810,853'")
    print("C. '1,277,870.25'")
    print("D. '621,909.5'")
    
    print("\nAnswer: D")
    print("Explanation: This requires running the Streamlit app and checking the Raw Data tab with:")
    print("  - Date range: 2020-2023 (includes 2021)")
    print("  - City: Cairo")
    print("  - Filter to 12-31-2021")
    print("The sum should be '621,909.5'")
    
    print("\n" + "-"*60)
    
    print("\nQuestion 2: In the Streamlit app code, change the default bottom value for the slider from 2020 to 2022. What did the relevant line of code change to?")
    
    print("\nOptions:")
    print("A. 'default_value=(2022,2023),'")
    print("B. 'default_options=range(2022, 2024),'")
    print("C. 'options=range(2022, 2024),'")
    print("D. 'value=(2022, 2023),'")
    
    print("\nAnswer: D")
    print("Explanation: In Streamlit's select_slider, the default value is set using the 'value' parameter.")
    print("The original code has: value=(2020, 2023)")
    print("To change the bottom value from 2020 to 2022, you change it to: value=(2022, 2023)")
    
    print("\n" + "-"*60)
    
    print("\nQuestion 3: Below the divider, and above the date filter slider and the city multiselect, add a title that says: 'Our Beautiful Chart'. What is the line of code you added?")
    
    print("\nOptions:")
    print("A. st.new_title('Our Beautiful Chart')")
    print("B. st.heading('Our Beautiful Chart')")
    print("C. st.title('Our Beautiful Chart')")
    print("D. st.write('Our Beautiful Chart')")
    
    print("\nAnswer: C")
    print("Explanation: In Streamlit, to add a title, you use st.title().")
    print("The other options don't exist (st.new_title, st.heading) or are for different purposes (st.write for general text).")
    
    print("\n" + "-"*60)
    
    print("\nQuestion 4: Delete the horizontal line that comes immediately after the words 'Tasty Bytes is beginning to leverage the Snowflake Data Cloud.' What line of code did you delete?")
    
    print("\nOptions:")
    print("A. st.return()")
    print("B. st.separate()")
    print("C. st.divider()")
    print("D. st.horizontal_line()")
    
    print("\nAnswer: C")
    print("Explanation: In Streamlit, st.divider() creates a horizontal line/divider.")
    print("Looking at the code, after the st.write() block with the description, there's st.divider()")
    print("This is the line that creates the horizontal separator.")
    
    print("\n" + "-"*60)
    
    print("\nQuestion 5: Right now the year slider and the multiselect each have their own column, meaning there are two columns total. Make a third column right next to those, called 'third_col', but leave it empty. What did the relevant line of code change to?")
    
    print("\nOptions:")
    print("A. 'cols = st.columns(3, gap=\"large\")'")
    print("B. 'first_col, second_col, third_col = st.columns(1, gap=\"large\")'")
    print("C. 'first_col, second_col, third_col = st.columns(3, gap=\"large\")'")
    print("D. 'first_col, second_col = st.columns(3, gap=\"large\")'")
    
    print("\nAnswer: C")
    print("Explanation: The original code has:")
    print("  first_col, second_col = st.columns(2, gap=\"large\")")
    print("To add a third column, you need to:")
    print("  1. Add 'third_col' to the unpacking")
    print("  2. Change the number from 2 to 3 in st.columns()")
    print("So it becomes: first_col, second_col, third_col = st.columns(3, gap=\"large\")")
    
    print("\n" + "-"*60)
    
    print("\nQuestion 6: Change the function 'get_city_sales_data' so that instead of showing results for cities in the city_names list, it only shows results for cities NOT in the city_names list. What is the new WHERE clause now?")
    
    print("\nOptions:")
    print("A. 'WHERE primary_city is not ({city_names}) and year(date) between {start_year} and {end_year}'")
    print("B. 'WHERE primary_city not in ({city_names}) and year(date) between {start_year} and {end_year}'")
    print("C. 'WHERE primary_city does not equal ({city_names}) and year(date) between {start_year} and {end_year}'")
    print("D. 'WHERE primary_city != ({city_names}) and year(date) between {start_year} and {end_year}'")
    
    print("\nAnswer: B")
    print("Explanation: In SQL, to exclude values from a list, you use the NOT IN clause.")
    print("The original WHERE clause is: WHERE primary_city in ({city_names})")
    print("To exclude cities in the list, you change 'in' to 'not in':")
    print("  WHERE primary_city not in ({city_names})")
    print("The 'is not' syntax doesn't work with lists, and != doesn't work with IN-style lists.")
    
    print("\n" + "="*60)
    print("SUMMARY:")
    print("Question 1: D - '621,909.5'")
    print("Question 2: D - 'value=(2022, 2023),'")
    print("Question 3: C - 'st.title(\"Our Beautiful Chart\")'")
    print("Question 4: C - 'st.divider()'")
    print("Question 5: C - 'first_col, second_col, third_col = st.columns(3, gap=\"large\")'")
    print("Question 6: B - 'WHERE primary_city not in ({city_names}) and year(date) between {start_year} and {end_year}'")
    print("="*60)

if __name__ == "__main__":
    answer_streamlit_questions()

