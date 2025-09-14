# Snowflake Assignment Setup Guide

This repository contains Snowflake assignments and exercises completed using Python and the Snowflake connector.

## 🔐 Security Setup

**IMPORTANT**: This repository is configured for security. All sensitive credentials are stored in environment variables and are NOT committed to Git.

### 1. Environment Variables Setup

1. **Create a `.env` file** in the root directory:
   ```bash
   cp .env.example .env
   ```

2. **Edit the `.env` file** with your actual Snowflake credentials:
   ```env
   # Snowflake Connection Configuration
   SNOWFLAKE_ACCOUNT=your_account_here
   SNOWFLAKE_USER=your_username_here
   SNOWFLAKE_PASSWORD=your_password_here
   SNOWFLAKE_EMAIL=your_email_here
   SNOWFLAKE_ROLE=ACCOUNTADMIN
   SNOWFLAKE_WAREHOUSE=COMPUTE_WH
   SNOWFLAKE_DATABASE=tasty_bytes_sample_data
   SNOWFLAKE_SCHEMA=raw_pos
   ```

3. **Verify `.env` is in `.gitignore`** (already configured):
   - The `.env` file is automatically ignored by Git
   - Never commit sensitive credentials to version control

## 🚀 Installation

1. **Clone the repository**:
   ```bash
   git clone <your-repo-url>
   cd snowflake
   ```

2. **Install Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables** (see Security Setup above)

## 📁 Project Structure

```
snowflake/
├── .env.example              # Environment variables template
├── .gitignore               # Git ignore file (includes .env)
├── requirements.txt         # Python dependencies
├── README.md               # Project documentation
├── SETUP_GUIDE.md          # This setup guide
├── 
├── # Assignment Files
├── complete_assignment.sql  # Complete SQL assignment
├── quick_setup.sql         # Quick SQL setup
├── 
├── # Python Scripts
├── snowflake_connection.py  # Basic connection utility
├── snowflake_config.py     # Configuration management
├── tasty_bytes_setup.py    # Tasty Bytes data setup
├── answer_questions.py     # Assignment questions solver
├── warehouse_questions.py  # Warehouse management questions
├── 
├── # Generated Files
├── step_01_*.sql           # Individual step SQL files
├── step_02_*.sql
├── ...
└── STEP_BY_STEP_GUIDE.md   # Detailed step-by-step guide
```

## 🎯 Assignments Completed

### 1. Tasty Bytes Sample Data Setup
- **File**: `tasty_bytes_setup.py`
- **Description**: Creates database, schema, table, and loads sample data
- **Run**: `python tasty_bytes_setup.py`

### 2. Data Analysis Questions
- **File**: `answer_questions.py`
- **Description**: Answers multiple choice questions about the loaded data
- **Run**: `python answer_questions.py`

### 3. Warehouse Management
- **File**: `warehouse_questions.py`
- **Description**: Demonstrates warehouse creation, modification, and management
- **Run**: `python warehouse_questions.py`

## 🔧 Usage

### Running Individual Scripts

1. **Set up environment variables** (see Security Setup)
2. **Run any script**:
   ```bash
   python script_name.py
   ```

### Manual SQL Execution

1. **Copy SQL from files**:
   - `complete_assignment.sql` - Complete assignment
   - `quick_setup.sql` - Quick setup
   - Individual step files for granular control

2. **Paste into Snowflake Worksheets**:
   - Open Snowflake web interface
   - Create new worksheet
   - Paste and run SQL

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

## 🤝 Contributing

1. **Never commit credentials** to the repository
2. **Use environment variables** for all sensitive data
3. **Test with your own credentials** before submitting
4. **Follow the security guidelines** outlined in this guide

## 📞 Support

If you encounter issues:
1. Check that your `.env` file is properly configured
2. Verify your Snowflake credentials are correct
3. Ensure you have the required permissions (ACCOUNTADMIN role)
4. Check the error messages for specific guidance

## 🎉 Success!

Once set up correctly, you should be able to:
- Connect to Snowflake securely
- Run all assignment scripts
- Execute SQL commands manually
- Complete all Snowflake exercises

Happy coding! 🚀
