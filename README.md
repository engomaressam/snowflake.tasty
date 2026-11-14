# Snowflake Assignments Repository

This repository contains completed Snowflake assignments and exercises, including data setup, analysis, warehouse management, and database operations.

## Security First

**IMPORTANT**: This repository is configured for security. All sensitive credentials are stored in environment variables and are NOT committed to Git.

## Repository Structure

```
snowflake/
├── assignments/           # Assignment screenshots and materials
│   └── *.jpg
├── docs/                 # Documentation and guides
│   ├── README.md
│   ├── SETUP_GUIDE.md
│   ├── MANUAL_SETUP_GUIDE.md
│   ├── STEP_BY_STEP_GUIDE.md
├── scripts/              # Python scripts for Snowflake operations
│   ├── snowflake_connection.py
│   ├── snowflake_config.py
│   └── tasty_bytes_setup.py
├── sql/                  # SQL scripts and queries
│   ├── complete_assignment.sql
│   ├── quick_setup.sql
│   ├── tasty_bytes_setup.sql
│   └── step_*.sql
├── security/             # Security configuration
│   ├── .gitignore
│   └── env_template.txt
├── requirements.txt      # Python dependencies
└── README.md            # This file
```

## Quick Start

1. **Clone and Setup**
   ```bash
   git clone <your-repo-url>
   cd snowflake
   pip install -r requirements.txt
   ```

2. **Configure Environment Variables**
   ```bash
   # Copy the template
   cp security/env_template.txt .env
   
   # Edit .env with your actual Snowflake credentials
   # Never commit the .env file to Git!
   ```

3. **Load the sample data**
   ```bash
   python scripts/tasty_bytes_setup.py
   ```

## Assignments Completed

### Tasty Bytes Sample Data Setup
- Database and schema creation
- Table structure with proper data types
- S3 stage setup and data loading
- 100+ menu items loaded successfully

### Data Analysis Questions
- Item category and subcategory analysis
- Price analysis by subcategory
- Truck brand popularity analysis

### Warehouse Management
- Warehouse creation and configuration
- Size modification and auto-suspend settings
- Warehouse switching and management

### Data Ingestion
- External stage creation
- File format configuration
- Data loading from S3
- 450+ truck records loaded

### Database Operations
- Database creation and management
- Schema operations
- Database switching and undrop operations

## Security Features

- **No hardcoded credentials** in any code files
- **Environment variables** for all sensitive data
- **`.env` file ignored** by Git
- **Comprehensive `.gitignore`** for security
- **Error handling** for missing environment variables

## Requirements

- Python 3.7+
- Snowflake account with ACCOUNTADMIN role
- Required Python packages (see `requirements.txt`)

## Documentation

- **`docs/SETUP_GUIDE.md`** - Complete setup instructions
- **`security/env_template.txt`** - Environment variables template
- **`sql/`** - SQL scripts for manual execution
- **`scripts/`** - Python automation scripts

## Contributing

1. **Never commit credentials** to the repository
2. **Use environment variables** for all sensitive data
3. **Test with your own credentials** before submitting
4. **Follow the security guidelines** in `docs/SETUP_GUIDE.md`

## Success!

This repository demonstrates:
- Secure Snowflake connectivity
- Data loading and analysis
- Warehouse management
- Database operations
- SQL query execution
- Best practices for credential management

Ready for GitHub publication!
