# Snowflake Project Handover

## Overview
This repository contains all code, scripts, SQL, and documentation related to Tasty Bytes Snowflake assignments, including data ingestion, transformation, analysis, and Snowflake platform management.

---

## 1. Environment Setup

### a. **Python & Dependencies**
- Python 3.8+ recommended.
- Install dependencies:
  ```bash
  pip install -r requirements.txt
  ```

### b. **Environment Variables ( [4mDo this before running anything! [0m)**
- All Snowflake credentials are managed via a `.env` file (NEVER commit this to Git!).
- Use the template in `security/env_template.txt`:
  ```
  SNOWFLAKE_ACCOUNT=your_account_here
  SNOWFLAKE_USER=your_username_here
  SNOWFLAKE_PASSWORD=your_password_here
  SNOWFLAKE_EMAIL=your_email_here
  SNOWFLAKE_ROLE=ACCOUNTADMIN
  SNOWFLAKE_WAREHOUSE=COMPUTE_WH
  SNOWFLAKE_DATABASE=tasty_bytes_sample_data
  SNOWFLAKE_SCHEMA=raw_pos
  ```
- Copy this template to a new file named `.env` in the root of the repo and fill in actual values (see below).
- The `.env` file is included in `.gitignore` for security.

### c. **Snowflake CLI (Optional)**
- To use Snowflake CLI, run:
  ```bash
  pip install snowflake-cli-labs
  ```

---

## 2. Project Structure

- `assignments/`: Assignment scripts and solutions (may not exist if moved to `scripts/`).
- `docs/`: Documentation, guides, and summaries.
- `scripts/`: All solution code/scripts for the assignments.
- `security/`: Templates for environment security (`env_template.txt`).
- `sql/`: Raw SQL files and stepwise assignment code.
- `requirements.txt`: Dependency list.
- `README.md`: Entry-level summary and setup guide.

---

## 3. Running Assignments
- Scripts are run as standard Python files, eg:
  ```bash
  python scripts/tasty_bytes_setup.py
  python scripts/table_questions.py
  # etc.
  ```
- Refer to the latest `README.md` for step-by-step usage. Outputs will often print directly to the console.

---

## 4. Security Principles
- **Never expose credentials!**
- All sensitive info is stored via env vars and NOT checked into version control.
- If the `.env` file appears in Git, remove (commit removal), add it to `.gitignore`, then copy contents **locally**.

---

## 5. Credentials (FOR HANDOVER ONLY: COPY THIS TO A PRIVATE LOCATION, NOT THE REPO!)

> The actual Snowflake connection data is for private transfer only. Copy this out before sharing repo.
> 
> Example:
> 
> SNOWFLAKE_ACCOUNT=CFZFJCW-RNB12276
> SNOWFLAKE_USER=OMARESSAM
> SNOWFLAKE_PASSWORD=Aboghaleb_54321
> SNOWFLAKE_EMAIL=omar.essam@rowad-rme.com
> SNOWFLAKE_ROLE=ACCOUNTADMIN
> SNOWFLAKE_WAREHOUSE=COMPUTE_WH
> SNOWFLAKE_DATABASE=tasty_bytes_sample_data
> SNOWFLAKE_SCHEMA=raw_pos

---

## 6. Help
- For repo-related issues, see `README.md` and `docs/SECURITY_SUMMARY.md`.
- For Snowflake questions, consult the official Snowflake documentation: https://docs.snowflake.com/

---

# End of Handover
