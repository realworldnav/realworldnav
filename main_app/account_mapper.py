"""
Account Mapper Module
Maps GL account names to COA GL account numbers using intelligent matching
"""

import pandas as pd
import re
from functools import lru_cache
from .s3_utils import load_COA_file


@lru_cache(maxsize=1)
def get_coa_mappings():
    """Load and prepare COA mappings with caching"""
    try:
        coa_df = load_COA_file()
        
        if coa_df.empty:
            print("ERROR - ACCOUNT_MAPPER: COA file is empty")
            return {}, pd.DataFrame()
        
        # Verify required columns exist
        if 'GL_Acct_Number' not in coa_df.columns or 'GL_Acct_Name' not in coa_df.columns:
            print(f"ERROR - ACCOUNT_MAPPER: COA missing required columns. Has: {coa_df.columns.tolist()}")
            return {}, coa_df
        
        # Create various mapping dictionaries for flexible matching
        mappings = {
            'exact': {},  # Exact matches (normalized)
            'keywords': {},  # Keyword-based matches
            'patterns': {}  # Pattern-based matches
        }
        
        for _, row in coa_df.iterrows():
            try:
                gl_acct_num = row['GL_Acct_Number'] 
                gl_acct_name = str(row['GL_Acct_Name']).strip()
                
                # Skip rows with invalid data
                if pd.isna(gl_acct_num) or pd.isna(gl_acct_name) or gl_acct_name == 'nan':
                    continue
                
                # Normalize for exact matching
                normalized_name = normalize_account_name(gl_acct_name)
                if normalized_name:  # Only add if normalization produced something
                    mappings['exact'][normalized_name] = gl_acct_num
                
                # Extract keywords for keyword matching
                keywords = extract_keywords(gl_acct_name)
                for keyword in keywords:
                    if keyword not in mappings['keywords']:
                        mappings['keywords'][keyword] = []
                    mappings['keywords'][keyword].append((gl_acct_num, gl_acct_name))
            except Exception as e:
                print(f"ERROR - ACCOUNT_MAPPER: Error processing COA row {row.get('GL_Acct_Number', 'Unknown')}: {e}")
                continue
        
        print(f"DEBUG - ACCOUNT_MAPPER: Loaded COA with {len(coa_df)} accounts")
        print(f"DEBUG - ACCOUNT_MAPPER: Created {len(mappings['exact'])} exact mappings")
        print(f"DEBUG - ACCOUNT_MAPPER: Created {len(mappings['keywords'])} keyword mappings")
        
        return mappings, coa_df
        
    except Exception as e:
        print(f"ERROR - ACCOUNT_MAPPER: Failed to load COA mappings: {e}")
        import traceback
        traceback.print_exc()
        return {}, pd.DataFrame()


def normalize_account_name(name):
    """Normalize account name for consistent matching"""
    if pd.isna(name):
        return ""
    
    # Convert to lowercase, replace underscores/dashes with spaces, remove extra spaces
    normalized = str(name).lower()
    normalized = re.sub(r'[_-]+', ' ', normalized)
    normalized = re.sub(r'\s+', ' ', normalized)
    normalized = normalized.strip()
    
    return normalized


def extract_keywords(account_name):
    """Extract meaningful keywords from account name"""
    if pd.isna(account_name):
        return []
    
    # Normalize first
    normalized = normalize_account_name(account_name)
    
    # Split into words and filter out common words
    words = normalized.split()
    
    # Remove common words that don't help with matching
    skip_words = {'the', 'of', 'and', 'or', 'in', 'on', 'at', 'to', 'for', 'with', 'by'}
    
    keywords = []
    for word in words:
        if len(word) > 2 and word not in skip_words:
            keywords.append(word)
    
    return keywords


def map_gl_account_to_coa(gl_account_name):
    """
    Map a GL account name to a COA GL account number using multiple strategies
    
    Args:
        gl_account_name: The account name from GL data
        
    Returns:
        tuple: (gl_acct_number, coa_account_name, match_type) or (None, None, None) if no match
    """
    if pd.isna(gl_account_name):
        return None, None, None
    
    mappings, coa_df = get_coa_mappings()
    
    # Strategy 1: Exact normalized match (only COA-based mapping allowed)
    normalized_gl = normalize_account_name(gl_account_name)
    if normalized_gl in mappings['exact']:
        gl_num = mappings['exact'][normalized_gl]
        matching_rows = coa_df[coa_df['GL_Acct_Number'] == gl_num]
        if not matching_rows.empty:
            coa_name = matching_rows['GL_Acct_Name'].iloc[0]
            return gl_num, coa_name, 'exact'
        else:
            print(f"DEBUG - ACCOUNT_MAPPER: GL number {gl_num} not found in COA for account '{gl_account_name}'")
            return None, None, None
    
    # Strategy 2: Pattern-based matching for specific cases (check BEFORE keyword matching)
    # This ensures provision accounts are correctly identified before keyword matching
    pattern_match = try_pattern_matching(gl_account_name, coa_df)
    if pattern_match:
        return pattern_match
    
    # Strategy 3: Keyword-based matching
    gl_keywords = extract_keywords(gl_account_name)
    
    # Try to find COA accounts that share keywords
    candidates = []
    for keyword in gl_keywords:
        if keyword in mappings['keywords']:
            for gl_num, coa_name in mappings['keywords'][keyword]:
                coa_keywords = extract_keywords(coa_name)
                # Calculate how many keywords match
                common_keywords = set(gl_keywords) & set(coa_keywords)
                if common_keywords:
                    score = len(common_keywords) / max(len(gl_keywords), len(coa_keywords))
                    candidates.append((gl_num, coa_name, score, 'keyword'))
    
    # Sort by score and return best match
    if candidates:
        candidates.sort(key=lambda x: x[2], reverse=True)
        best_match = candidates[0]
        if best_match[2] >= 0.5:  # At least 50% keyword overlap
            return best_match[0], best_match[1], best_match[3]
    
    return None, None, None


def get_manual_mappings():
    """Manual mappings for accounts that don't match well automatically - DISABLED per user request"""
    # User requested to remove all manual mappings and use only COA-based mapping via account_name
    return {}


def try_pattern_matching(gl_account_name, coa_df):
    """Try pattern-based matching for specific account types"""
    normalized = normalize_account_name(gl_account_name)
    
    # CRITICAL: Check for provision accounts FIRST (before loan matching)
    # This ensures provision accounts map to 13501/13511 not 13500/13510
    if 'provision' in normalized or 'bad debt' in normalized:
        # Determine which provision account based on currency/pool
        if 'blur' in normalized:
            # Map to Blur Pool provision account
            matches = coa_df[coa_df['GL_Acct_Number'] == 13501]
            if not matches.empty:
                return 13501, matches.iloc[0]['GL_Acct_Name'], 'pattern'
        elif 'weth' in normalized:
            # Map to WETH provision account
            matches = coa_df[coa_df['GL_Acct_Number'] == 13511]
            if not matches.empty:
                return 13511, matches.iloc[0]['GL_Acct_Name'], 'pattern'
    
    # Digital assets pattern
    if 'digital assets' in normalized:
        # Extract currency from name (eth, usdc, weth, etc.)
        currencies = ['eth', 'usdc', 'weth', 'usdt', 'btc']
        for currency in currencies:
            if currency in normalized:
                # Look for matching COA entry
                pattern = f"digital assets.*{currency}"
                matches = coa_df[coa_df['GL_Acct_Name'].str.contains(pattern, case=False, na=False)]
                if not matches.empty:
                    gl_num = matches.iloc[0]['GL_Acct_Number']
                    coa_name = matches.iloc[0]['GL_Acct_Name']
                    return gl_num, coa_name, 'pattern'
    
    # Interest income pattern
    if 'interest income' in normalized:
        try:
            matches = coa_df[coa_df['GL_Acct_Name'].str.contains('interest.*income', case=False, na=False)]
            if not matches.empty:
                gl_num = matches.iloc[0]['GL_Acct_Number']
                coa_name = matches.iloc[0]['GL_Acct_Name']
                return gl_num, coa_name, 'pattern'
        except Exception as e:
            print(f"DEBUG - ACCOUNT_MAPPER: Error in interest income pattern matching: {e}")
    
    # Realized gain/loss pattern
    if 'realized gain' in normalized or 'realized loss' in normalized:
        try:
            matches = coa_df[coa_df['GL_Acct_Name'].str.contains('realized.*gain', case=False, na=False)]
            if not matches.empty:
                gl_num = matches.iloc[0]['GL_Acct_Number']
                coa_name = matches.iloc[0]['GL_Acct_Name']  
                return gl_num, coa_name, 'pattern'
        except Exception as e:
            print(f"DEBUG - ACCOUNT_MAPPER: Error in realized gain pattern matching: {e}")
    
    return None


def enrich_gl_with_coa_mapping(gl_df):
    """
    Enrich GL DataFrame with COA mappings
    
    Args:
        gl_df: GL DataFrame with account_name column
        
    Returns:
        GL DataFrame with additional columns: GL_Acct_Number, GL_Acct_Name, match_type
    """
    print(f"DEBUG - ACCOUNT_MAPPER: Starting GL enrichment for {len(gl_df)} records")
    
    if 'account_name' not in gl_df.columns:
        print("ERROR - ACCOUNT_MAPPER: No account_name column in GL data")
        return gl_df
    
    try:
        # Initialize new columns
        gl_df = gl_df.copy()
        gl_df['GL_Acct_Number'] = None
        gl_df['GL_Acct_Name'] = None
        gl_df['mapping_match_type'] = None
        
        # Get unique account names to avoid duplicate processing
        unique_accounts = gl_df['account_name'].dropna().unique()
        print(f"DEBUG - ACCOUNT_MAPPER: Processing {len(unique_accounts)} unique account names")
        
        mapping_results = {}
        mapping_stats = {'exact': 0, 'keyword': 0, 'pattern': 0, 'unmapped': 0}
        
        for i, account_name in enumerate(unique_accounts):
            try:
                gl_num, coa_name, match_type = map_gl_account_to_coa(account_name)
                mapping_results[account_name] = (gl_num, coa_name, match_type)
                
                if match_type:
                    mapping_stats[match_type] += 1
                    if i < 5:  # Show first few successful mappings
                        print(f"DEBUG - ACCOUNT_MAPPER: '{account_name}' -> {gl_num} '{coa_name}' [{match_type}]")
                else:
                    mapping_stats['unmapped'] += 1
                    print(f"DEBUG - ACCOUNT_MAPPER: UNMAPPED account: '{account_name}'")
            except Exception as e:
                print(f"ERROR - ACCOUNT_MAPPER: Failed to map account '{account_name}': {e}")
                mapping_results[account_name] = (None, None, None)
                mapping_stats['unmapped'] += 1
        
        # Apply mappings to DataFrame
        for account_name, (gl_num, coa_name, match_type) in mapping_results.items():
            try:
                mask = gl_df['account_name'] == account_name
                if gl_num is not None and mask.any():
                    gl_df.loc[mask, 'GL_Acct_Number'] = gl_num
                    gl_df.loc[mask, 'GL_Acct_Name'] = coa_name
                    gl_df.loc[mask, 'mapping_match_type'] = match_type
            except Exception as e:
                print(f"ERROR - ACCOUNT_MAPPER: Failed to apply mapping for '{account_name}': {e}")
        
        # Convert GL_Acct_Number to proper numeric type
        gl_df['GL_Acct_Number'] = pd.to_numeric(gl_df['GL_Acct_Number'], errors='coerce')
        
        print(f"DEBUG - ACCOUNT_MAPPER: Mapping statistics: {mapping_stats}")
        
        mapped_count = gl_df['GL_Acct_Number'].notna().sum()
        print(f"DEBUG - ACCOUNT_MAPPER: Successfully mapped {mapped_count}/{len(gl_df)} GL records")
        
        return gl_df
        
    except Exception as e:
        print(f"ERROR - ACCOUNT_MAPPER: Critical error in GL enrichment: {e}")
        import traceback
        traceback.print_exc()
        # Return original DataFrame if enrichment fails
        return gl_df


def debug_account_mappings(gl_df, show_unmapped=True, show_mapped=False):
    """Debug helper to show mapping results"""
    if 'account_name' not in gl_df.columns:
        print("No account_name column for debugging")
        return
    
    unique_accounts = gl_df['account_name'].unique()
    
    print(f"\nDEBUG - ACCOUNT MAPPING ANALYSIS:")
    print(f"Total unique accounts: {len(unique_accounts)}")
    
    mapped_accounts = []
    unmapped_accounts = []
    
    for account_name in unique_accounts:
        gl_num, coa_name, match_type = map_gl_account_to_coa(account_name)
        if gl_num is not None:
            mapped_accounts.append((account_name, gl_num, coa_name, match_type))
        else:
            unmapped_accounts.append(account_name)
    
    if show_mapped and mapped_accounts:
        print(f"\nMAPPED ACCOUNTS ({len(mapped_accounts)}):")
        for account_name, gl_num, coa_name, match_type in mapped_accounts:
            print(f"  '{account_name}' -> {gl_num} '{coa_name}' [{match_type}]")
    
    if show_unmapped and unmapped_accounts:
        print(f"\nUNMAPPED ACCOUNTS ({len(unmapped_accounts)}):")
        for account_name in unmapped_accounts:
            print(f"  '{account_name}'")
    
    print(f"\nMapping success rate: {len(mapped_accounts)}/{len(unique_accounts)} ({len(mapped_accounts)/len(unique_accounts)*100:.1f}%)")