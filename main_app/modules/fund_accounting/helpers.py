from shiny import reactive
import pandas as pd
from ...s3_utils import load_GL_file, load_COA_file

def gl_data_for_fund(selected_fund=None):
    """Load GL data with proper columns, optionally filtered by fund"""
    gl_df = load_GL_file()
    coa_df = load_COA_file()
    
    
    # Fund fields are available for filtering
    
    # Filter by fund if available and fund selection exists
    if selected_fund and hasattr(selected_fund, '__call__'):
        fund_id = selected_fund()
        if fund_id:
            # Try different fund fields
            original_shape = gl_df.shape[0]
            
            if 'fund_id' in gl_df.columns and fund_id in gl_df['fund_id'].values:
                gl_df = gl_df[gl_df['fund_id'] == fund_id]
            elif 'counterparty_fund_id' in gl_df.columns and fund_id in gl_df['counterparty_fund_id'].values:
                gl_df = gl_df[gl_df['counterparty_fund_id'] == fund_id]
            # Fund not found in available columns, use all data
    
    # Create a mapping function to normalize account names
    def normalize_account_name(name):
        """Convert snake_case to title case format for matching"""
        if pd.isna(name):
            return name
        # Replace underscores with spaces and title case
        normalized = str(name).replace('_', ' ').title()
        
        # Handle specific mappings we can see from the data
        mappings = {
            'Deemed Cash Usd': 'Deemed cash - USD',
            'Digital Assets Usdc': 'Digital Assets - USDC',
            'Digital Assets Eth': 'Digital Assets - ETH', 
            'Digital Assets Weth': 'Digital Assets - WETH',
            'Digital Assets Usdt': 'Digital Assets - USDT'
        }
        
        return mappings.get(normalized, normalized)
    
    # Create normalized columns for matching
    gl_df['account_name_norm'] = gl_df['account_name'].apply(normalize_account_name)
    coa_df['GL_Acct_Name_norm'] = coa_df['GL_Acct_Name'].apply(lambda x: str(x) if pd.notna(x) else x)
    
    
    # Try merge with normalized names
    merged_df = gl_df.merge(
        coa_df[['GL_Acct_Number', 'GL_Acct_Name', 'GL_Acct_Name_norm']], 
        left_on='account_name_norm', 
        right_on='GL_Acct_Name_norm', 
        how='left'
    )
    
    # Debug merge results
    matched_accounts = merged_df['GL_Acct_Number'].notna().sum()
    unmatched_accounts = merged_df['GL_Acct_Number'].isna().sum()
    
    if unmatched_accounts > 0:
        unmatched_names = merged_df[merged_df['GL_Acct_Number'].isna()]['account_name'].unique()[:5]
        unmatched_norm = merged_df[merged_df['GL_Acct_Number'].isna()]['account_name_norm'].unique()[:5]
        
        # For unmatched accounts, let's try to create a mapping based on partial matches
        for name in unmatched_names[:3]:
            norm_name = normalize_account_name(name)
            similar_coa = coa_df[coa_df['GL_Acct_Name'].str.contains(norm_name.split()[0], case=False, na=False)]
            if not similar_coa.empty:
                # Found potential matches but not using them in fallback mode
                pass
    
    # Convert date columns properly
    if 'date' in merged_df.columns:
        merged_df['date'] = pd.to_datetime(merged_df['date'], errors='coerce')
    elif 'operating_date' in merged_df.columns:
        merged_df['date'] = pd.to_datetime(merged_df['operating_date'], errors='coerce')
    
    # Ensure debit/credit columns are numeric
    merged_df['debit_crypto'] = pd.to_numeric(merged_df['debit_crypto'], errors='coerce').fillna(0)
    merged_df['credit_crypto'] = pd.to_numeric(merged_df['credit_crypto'], errors='coerce').fillna(0)
    
    # If most accounts don't match COA, create fallback account numbers based on account name patterns
    if matched_accounts == 0 or matched_accounts < len(merged_df) * 0.5:
        merged_df['GL_Acct_Number'] = merged_df['account_name'].apply(assign_fallback_account_number)
        # Ensure proper dtype for GL_Acct_Number to avoid warnings
        merged_df['GL_Acct_Number'] = merged_df['GL_Acct_Number'].astype('Int64')
    
    
    # Show sample account mappings
    if merged_df['GL_Acct_Number'].notna().sum() > 0:
        sample_accounts = merged_df[merged_df['GL_Acct_Number'].notna()][['account_name', 'GL_Acct_Number']].drop_duplicates().head(5)
        # Sample account mappings available for debugging if needed
    
    return merged_df

def assign_fallback_account_number(account_name):
    """Assign account numbers based on account name patterns when COA matching fails"""
    if pd.isna(account_name):
        return None
        
    name = str(account_name).lower()
    
    # Asset accounts (1xxx)
    if any(keyword in name for keyword in ['digital_assets', 'cash', 'receivable', 'prepaid']):
        if 'eth' in name:
            return 1101  # Digital Assets - ETH
        elif 'usdc' in name:
            return 1102  # Digital Assets - USDC
        elif 'weth' in name:
            return 1103  # Digital Assets - WETH
        elif 'usdt' in name:
            return 1104  # Digital Assets - USDT
        elif 'cash' in name:
            return 1001  # Cash/Deemed Cash
        elif 'prepaid' in name:
            return 1201  # Prepaid Expenses
        else:
            return 1100  # Other Digital Assets
    
    # Liability accounts (2xxx)
    elif any(keyword in name for keyword in ['payable', 'due_to', 'accrued']):
        if 'management_fee' in name:
            return 2101  # Management Fee Payable
        elif 'due_to' in name:
            return 2001  # Due to Related Party
        else:
            return 2100  # Other Payables
    
    # Equity accounts (3xxx)
    elif any(keyword in name for keyword in ['capital', 'equity', 'contribution']):
        return 3001  # Capital/Equity
    
    # Income accounts (4xxx)
    elif any(keyword in name for keyword in ['income', 'revenue', 'gain']) and 'realized' in name:
        return 4001  # Realized Gains
    
    # Other Income (9xxx) 
    elif any(keyword in name for keyword in ['income', 'revenue', 'gain']):
        return 9001  # Other Income
    
    # Expense accounts (8xxx)
    elif any(keyword in name for keyword in ['expense', 'fee']) and 'management' in name:
        return 8001  # Management Fee Expense
    elif any(keyword in name for keyword in ['expense', 'cost']):
        return 8100  # Other Expenses
    
    # Default to asset if unknown
    else:
        return 1999  # Unknown Asset

@reactive.calc
def gl_data():
    """Legacy wrapper for gl_data_for_fund"""
    return gl_data_for_fund()

def calculate_nav_for_date(target_date=None):
    """Calculate NAV for a specific date by summing all GL entries up to that date"""
    df = gl_data()
    
    if df.empty:
        return 0, 0, 0  # assets, liabilities, nav
    
    # Use latest date if not specified
    if target_date is None:
        target_date = df['date'].max()
    else:
        target_date = pd.to_datetime(target_date, utc=True)
    
    # Filter to all transactions up to and including target date
    from ..financial_reporting.data_processor import safe_date_compare
    historical_df = df[safe_date_compare(df['date'], target_date)].copy()
    
    if historical_df.empty:
        return 0, 0, 0
    
    # Convert GL_Acct_Number to string for comparison
    historical_df['GL_Acct_Number'] = historical_df['GL_Acct_Number'].astype(str)
    
    # Group by account and calculate net balance for each account
    account_balances = historical_df.groupby('GL_Acct_Number').agg({
        'debit_crypto': 'sum',
        'credit_crypto': 'sum'
    }).reset_index()
    
    # Calculate net balance based on account type - use float64 to avoid dtype warnings
    account_balances['net_balance'] = 0.0
    
    # Assets (1xxx): Normal debit balance (debit - credit)
    asset_mask = account_balances['GL_Acct_Number'].str.startswith('1')
    account_balances.loc[asset_mask, 'net_balance'] = (
        account_balances.loc[asset_mask, 'debit_crypto'] - 
        account_balances.loc[asset_mask, 'credit_crypto']
    )
    
    # Liabilities (2xxx): Normal credit balance (credit - debit) 
    liability_mask = account_balances['GL_Acct_Number'].str.startswith('2')
    account_balances.loc[liability_mask, 'net_balance'] = (
        account_balances.loc[liability_mask, 'credit_crypto'] - 
        account_balances.loc[liability_mask, 'debit_crypto']
    )
    
    # Sum up assets and liabilities
    total_assets = account_balances[asset_mask]['net_balance'].sum()
    total_liabilities = account_balances[liability_mask]['net_balance'].sum()
    
    # NAV = Assets - Liabilities
    nav = total_assets - total_liabilities
    
    
    return total_assets, total_liabilities, nav

@reactive.calc  
def daily_nav_series():
    """Calculate NAV for each unique date in the GL"""
    df = gl_data()
    
    if df.empty:
        return pd.Series(dtype=float)
    
    # Get unique dates
    unique_dates = sorted(df['date'].dropna().unique())
    
    nav_data = []
    for i, date in enumerate(unique_dates):
        assets, liabilities, nav = calculate_nav_for_date(date)
        nav_data.append({
            'date': date,
            'assets': assets,
            'liabilities': liabilities,
            'nav': nav
        })
        if i < 3:  # Show first few calculations
            # Debug calculations available if needed
            pass
    
    nav_df = pd.DataFrame(nav_data)
    nav_series = pd.Series(nav_df['nav'].values, index=pd.DatetimeIndex(nav_df['date']))
    
    
    return nav_series

# Legacy compatibility functions
@reactive.calc
def daily_balances():
    """Legacy function - now returns NAV data in expected format"""
    df = gl_data()
    
    if df.empty:
        return pd.DataFrame()
    
    # Get unique dates
    unique_dates = sorted(df['date'].dropna().unique())
    
    daily_data = []
    for date in unique_dates:
        # Get all accounts with activity up to this date
        historical_df = df[safe_date_compare(df['date'], date)].copy()
        historical_df['GL_Acct_Number'] = historical_df['GL_Acct_Number'].astype(str)
        
        # Group by account
        account_balances = historical_df.groupby('GL_Acct_Number').agg({
            'debit_crypto': 'sum',
            'credit_crypto': 'sum'
        }).reset_index()
        
        for _, row in account_balances.iterrows():
            account = row['GL_Acct_Number']
            
            # Calculate balance based on account type
            # Chart of Accounts Structure:
            # 1xxx - Assets (debit normal balance)
            # 2xxx - Liabilities (credit normal balance)
            # 3xxx - Equity (credit normal balance)
            # 4xxx - Income/Revenue (credit normal balance)
            # 8xxx - Expenses (debit normal balance)
            # 9xxx - Other Income (credit normal balance)
            
            if str(account).startswith('1'):  # Assets
                balance = row['debit_crypto'] - row['credit_crypto']
            elif str(account).startswith('2'):  # Liabilities
                balance = row['credit_crypto'] - row['debit_crypto']
            elif str(account).startswith('3'):  # Equity
                balance = row['credit_crypto'] - row['debit_crypto']
            elif str(account).startswith('4'):  # Income/Revenue
                balance = row['credit_crypto'] - row['debit_crypto']
            elif str(account).startswith('8'):  # Expenses
                balance = row['debit_crypto'] - row['credit_crypto']
            elif str(account).startswith('9'):  # Other Income
                balance = row['credit_crypto'] - row['debit_crypto']
            else:  # Any other accounts
                balance = row['debit_crypto'] - row['credit_crypto']
            
            daily_data.append({
                'Report Date': pd.Timestamp(date),
                'GL_Acct_Number': account,
                'Balance': balance
            })
    
    return pd.DataFrame(daily_data)

# Backward compatibility aliases
@reactive.calc  
def trial_balance():
    """Legacy alias for daily_balances"""
    return daily_balances()

@reactive.calc
def melted_tb():
    """Legacy alias for daily_balances"""
    return daily_balances()

def apply_plotly_theme(fig, title="Chart"):
    """Apply unified theme to Plotly figures"""
    from ...theme_manager import theme_manager
    
    # Get theme colors directly
    theme_data = theme_manager.get_current_theme_data()
    text_color = theme_data.get("colors", {}).get("text_primary", "#f8fafc")
    
    # Apply layout settings
    fig.update_layout(
        title_text=title,
        title_font={"color": text_color, "size": 16},
        font={"color": text_color},
        paper_bgcolor=theme_data.get("colors", {}).get("surface_primary", "#1e293b"),
        plot_bgcolor=theme_data.get("colors", {}).get("surface_primary", "#1e293b")
    )
    
    # Apply axis styling with subtle, transparent grid lines
    fig.update_xaxes(
        title_font={"color": text_color, "size": 12},
        tickfont={"color": text_color, "size": 10},
        linecolor="rgba(148, 163, 184, 0.3)",
        gridcolor="rgba(148, 163, 184, 0.15)",
        tickcolor=text_color,
        zerolinecolor="rgba(148, 163, 184, 0.4)",
        showgrid=True,
        showline=True,
        zeroline=True,
        gridwidth=0.5
    )
    
    fig.update_yaxes(
        title_font={"color": text_color, "size": 12},
        tickfont={"color": text_color, "size": 10},
        linecolor="rgba(148, 163, 184, 0.3)",
        gridcolor="rgba(148, 163, 184, 0.15)",
        tickcolor=text_color,
        zerolinecolor="rgba(148, 163, 184, 0.4)",
        showgrid=True,
        showline=True,
        zeroline=True,
        gridwidth=0.5
    )
    
    return fig
