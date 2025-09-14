#!/usr/bin/env python3
"""
Security Check Script
Scans all Python files for potential hardcoded credentials
"""

import os
import re
import glob

def check_for_credentials():
    """
    Check all Python files for hardcoded credentials
    """
    print("🔍 Security Check: Scanning for hardcoded credentials...")
    print("="*60)
    
    # Patterns to look for
    credential_patterns = [
        r'password\s*=\s*[\'"][^\'"]+[\'"]',
        r'user\s*=\s*[\'"][^\'"]+[\'"]',
        r'account\s*=\s*[\'"][^\'"]+[\'"]',
        r'OMARESSAM',
        r'Aboghaleb_54321',
        r'CFZFJCW-RNB12276',
        r'omar\.essam@rowad-rme\.com'
    ]
    
    # Get all Python files
    python_files = glob.glob("*.py")
    
    issues_found = False
    
    for file_path in python_files:
        print(f"\n📁 Checking {file_path}...")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                lines = content.split('\n')
                
                file_issues = []
                
                for i, line in enumerate(lines, 1):
                    for pattern in credential_patterns:
                        if re.search(pattern, line, re.IGNORECASE):
                            file_issues.append(f"  Line {i}: {line.strip()}")
                
                if file_issues:
                    print(f"  ❌ Found {len(file_issues)} potential issues:")
                    for issue in file_issues:
                        print(issue)
                    issues_found = True
                else:
                    print(f"  ✅ No hardcoded credentials found")
                    
        except Exception as e:
            print(f"  ⚠️  Error reading file: {e}")
    
    print("\n" + "="*60)
    if issues_found:
        print("❌ SECURITY ISSUES FOUND!")
        print("Please review and remove any hardcoded credentials.")
        print("Use environment variables instead.")
    else:
        print("✅ SECURITY CHECK PASSED!")
        print("No hardcoded credentials found in Python files.")
    
    print("\n🔐 Security Recommendations:")
    print("1. All credentials should be in .env file")
    print("2. .env file should be in .gitignore")
    print("3. Use os.getenv() to read environment variables")
    print("4. Never commit .env files to Git")

if __name__ == "__main__":
    check_for_credentials()
