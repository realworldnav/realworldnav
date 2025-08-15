"""
Check GL data structure to understand LP identification.
"""

import pandas as pd
import sys
import os

# Add the main_app directory to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import S3 utilities
from main_app.s3_utils import load_GL_file

def check_gl_structure():
    """Check the structure of GL data."""
    
    print("Loading GL data from S3...")
    gl_df = load_GL_file()
    print(f"Loaded {len(gl_df)} GL entries")
    
    print("\nGL DataFrame columns:")
    print(gl_df.columns.tolist())
    
    print("\nFirst 5 rows of GL data:")
    print(gl_df.head())
    
    print("\nGL DataFrame dtypes:")
    print(gl_df.dtypes)
    
    print("\nUnique values in key columns:")
    
    # Check for columns that might contain LP information
    potential_lp_columns = ['limited_partner_ID', 'LP_ID', 'Entity', 'Partner', 'Counterparty', 
                           'Account_Entity', 'entity', 'partner', 'lp_id']
    
    for col in potential_lp_columns:
        if col in gl_df.columns:
            unique_values = gl_df[col].dropna().unique()
            print(f"\n{col}: {len(unique_values)} unique values")
            if len(unique_values) <= 10:
                print(f"  Values: {unique_values}")
            else:
                print(f"  First 10: {unique_values[:10]}")
    
    # Check account_name column for LP patterns
    if 'account_name' in gl_df.columns:
        print("\nUnique account_name values:")
        account_names = gl_df['account_name'].dropna().unique()
        for name in sorted(account_names):
            print(f"  - {name}")
    
    # Check if there are any columns with 'LP' pattern in values
    print("\nSearching for LP patterns in all columns...")
    for col in gl_df.columns:
        if gl_df[col].dtype == 'object':
            lp_pattern_found = gl_df[col].astype(str).str.contains('LP_', case=False, na=False).any()
            if lp_pattern_found:
                print(f"  Found LP pattern in column: {col}")
                sample_lps = gl_df[gl_df[col].astype(str).str.contains('LP_', case=False, na=False)][col].head(5)
                print(f"    Sample values: {sample_lps.tolist()}")

if __name__ == "__main__":
    check_gl_structure()