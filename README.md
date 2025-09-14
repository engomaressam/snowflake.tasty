# Snowflake Assignments Repository

This repository contains completed Snowflake assignments and exercises, including data setup, analysis, and warehouse management tasks.

## 🔐 Security First

**IMPORTANT**: This repository is configured for security. All sensitive credentials are stored in environment variables and are NOT committed to Git.

## 🚀 Quick Start

1. **Clone and Setup**
   ```bash
   git clone <your-repo-url>
   cd snowflake
   pip install -r requirements.txt
   ```

2. **Configure Environment Variables**
   ```bash
   # Copy the template
   cp env_template.txt .env
   
   # Edit .env with your actual Snowflake credentials
   # Never commit the .env file to Git!
   ```

3. **Run Assignments**
   ```bash
   python tasty_bytes_setup.py      # Data setup
   python answer_questions.py       # Analysis questions
   python warehouse_questions.py    # Warehouse management
   ```

## 📁 Project Structure

- **`complete_assignment.sql`** - Complete SQL assignment ready for Snowflake Worksheets
- **`snowflake_connection.py`** - Secure connection utility using environment variables
- **`tasty_bytes_setup.py`** - Tasty Bytes sample data setup
- **`answer_questions.py`** - Data analysis questions solver
- **`warehouse_questions.py`** - Warehouse management exercises
- **`SETUP_GUIDE.md`** - Detailed setup and security guide

## 🎯 Assignments Completed

### ✅ Tasty Bytes Sample Data Setup
- Database and schema creation
- Table structure with proper data types
- S3 stage setup and data loading
- 100+ menu items loaded successfully

### ✅ Data Analysis Questions
- Item category and subcategory analysis
- Price analysis by subcategory
- Truck brand popularity analysis

### ✅ Warehouse Management
- Warehouse creation and configuration
- Size modification and auto-suspend settings
- Warehouse switching and management

## 🛡️ Security Features

- ✅ **No hardcoded credentials** in any code files
- ✅ **Environment variables** for all sensitive data
- ✅ **`.env` file ignored** by Git
- ✅ **Comprehensive `.gitignore`** for security
- ✅ **Error handling** for missing environment variables

## 📋 Requirements

- Python 3.7+
- Snowflake account with ACCOUNTADMIN role
- Required Python packages (see `requirements.txt`)

## 📖 Documentation

- **`SETUP_GUIDE.md`** - Complete setup instructions
- **`env_template.txt`** - Environment variables template
- **Individual step files** - Granular SQL execution

## 🤝 Contributing

1. **Never commit credentials** to the repository
2. **Use environment variables** for all sensitive data
3. **Test with your own credentials** before submitting
4. **Follow the security guidelines** in `SETUP_GUIDE.md`

## 🎉 Success!

This repository demonstrates:
- Secure Snowflake connectivity
- Data loading and analysis
- Warehouse management
- SQL query execution
- Best practices for credential management

Ready for GitHub publication! 🚀
