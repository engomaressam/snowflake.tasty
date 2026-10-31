#!/usr/bin/env python3
"""
Create a local .env file for the Snowflake connection.

Values come from the environment when set, otherwise you are prompted.
The .env file is git-ignored; never commit it.
"""

import os
from getpass import getpass

FIELDS = [
    ("SNOWFLAKE_ACCOUNT", "Account identifier (orgname-accountname)", False, None),
    ("SNOWFLAKE_USER", "User name", False, None),
    ("SNOWFLAKE_PASSWORD", "Password", True, None),
    ("SNOWFLAKE_ROLE", "Role", False, "ACCOUNTADMIN"),
    ("SNOWFLAKE_WAREHOUSE", "Warehouse", False, "COMPUTE_WH"),
    ("SNOWFLAKE_DATABASE", "Database", False, "tasty_bytes_sample_data"),
    ("SNOWFLAKE_SCHEMA", "Schema", False, "raw_pos"),
]


def create_env_file(path=".env"):
    """Write the Snowflake connection settings to a local .env file."""
    lines = ["# Snowflake connection settings (local only, git-ignored)"]
    for key, label, secret, default in FIELDS:
        value = os.environ.get(key)
        if not value:
            prompt = f"{label}" + (f" [{default}]" if default else "") + ": "
            value = (getpass(prompt) if secret else input(prompt)).strip() or (default or "")
        lines.append(f"{key}={value}")
    try:
        with open(path, "w") as f:
            f.write("\n".join(lines) + "\n")
        print(f"✅ {path} written")
        return True
    except OSError as e:
        print(f"❌ Error creating {path}: {e}")
        return False


if __name__ == "__main__":
    create_env_file()
