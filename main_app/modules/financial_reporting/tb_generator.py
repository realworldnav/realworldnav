"""
Trial Balance Generator from GL Data
Generates trial balance and period calculations from general ledger entries
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from typing import Dict, Tuple, Optional
from ...s3_utils import load_GL_file, load_COA_file
from .data_processor import safe_date_compare


def generate_trial_balance_from_gl(gl_df: pd.DataFrame, as_of_date: datetime) -> pd.DataFrame:
    """
    Generate a trial balance from GL entries up to a specific date.
    
    Args:
        gl_df: General ledger DataFrame
        as_of_date: Date to calculate balances up to
        
    Returns:
        DataFrame with columns: GL_Acct_Number, GL_Acct_Name, Category, Balance
    """
    if gl_df.empty:
        return pd.DataFrame()
    
    # Filter GL entries up to as_of_date using safe comparison
    filtered_gl = gl_df[safe_date_compare(gl_df['date'], as_of_date)].copy()
    
    if filtered_gl.empty:
        return pd.DataFrame()
    
    # Load COA for account names and categorization
    coa_df = load_COA_file()
    coa_dict = dict(zip(coa_df['GL_Acct_Number'], coa_df['GL_Acct_Name']))
    
    # Account mapping should already be done upstream by account_mapper
    if 'GL_Acct_Number' not in filtered_gl.columns:
        print("ERROR - No GL_Acct_Number column found in GL data - account mapping may have failed")
        return pd.DataFrame()
    
    # Ensure GL_Acct_Number is numeric for proper grouping
    filtered_gl['GL_Acct_Number'] = pd.to_numeric(filtered_gl['GL_Acct_Number'], errors='coerce')
    
    # Keep original account name for display
    filtered_gl['original_account_name'] = filtered_gl.get('account_name', filtered_gl['GL_Acct_Number'].apply(
        lambda x: coa_dict.get(int(x), f"Account {int(x)}") if pd.notna(x) else "Unknown"
    ))
    
    # Debug mapping status
    mapped_count = filtered_gl['GL_Acct_Number'].notna().sum()
    unmapped_count = filtered_gl['GL_Acct_Number'].isna().sum()
    print(f"DEBUG - TB_GENERATOR: GL records with account numbers: {mapped_count}, without: {unmapped_count}")
    
    # Remove rows with invalid account numbers
    filtered_gl = filtered_gl[pd.notna(filtered_gl['GL_Acct_Number'])]
    
    # Ensure debit/credit columns are numeric
    for col in ['debit_crypto', 'credit_crypto', 'debit_USD', 'credit_USD']:
        if col in filtered_gl.columns:
            filtered_gl[col] = pd.to_numeric(filtered_gl[col], errors='coerce').fillna(0)
    
    # Group by account number and original account name, sum debits/credits
    if 'original_account_name' in filtered_gl.columns:
        account_totals = filtered_gl.groupby(['GL_Acct_Number', 'original_account_name']).agg({
            'debit_crypto': 'sum',
            'credit_crypto': 'sum'
        }).reset_index()
    else:
        account_totals = filtered_gl.groupby('GL_Acct_Number').agg({
            'debit_crypto': 'sum',
            'credit_crypto': 'sum'
        }).reset_index()
        account_totals['original_account_name'] = account_totals['GL_Acct_Number'].apply(
            lambda x: coa_dict.get(int(x), f"Account {int(x)}")
        )
    
    # Calculate net balance as debit - credit (NO SIGN FLIPPING)
    account_totals['Balance'] = account_totals['debit_crypto'] - account_totals['credit_crypto']
    
    # Add formal account names from COA and categories
    account_totals['GL_Acct_Name'] = account_totals['GL_Acct_Number'].apply(
        lambda x: coa_dict.get(int(x), f"Account {int(x)}")
    )
    
    account_totals['Category'] = account_totals['GL_Acct_Number'].apply(categorize_account)
    account_totals['Sort_Order'] = account_totals['GL_Acct_Number'].apply(get_sort_order)
    
    # Filter out zero balances
    account_totals = account_totals[np.abs(account_totals['Balance']) > 0.000001]
    
    # Sort by category (1=Assets, 2=Liabilities, 3=Capital, 4=Income, 8=Expenses, 9=Income) then by account number
    account_totals = account_totals.sort_values(['Sort_Order', 'GL_Acct_Number'])
    
    return account_totals


def categorize_account(acct_num) -> str:
    """Categorize account based on account number (1=Assets, 2=Liabilities, 3=Capital, 4=Income, 8=Expenses, 9=Income)."""
    acct_str = str(int(acct_num))
    
    if acct_str.startswith('1'):
        return "Assets"
    elif acct_str.startswith('2'):
        return "Liabilities"  
    elif acct_str.startswith('3'):
        return "Capital"
    elif acct_str.startswith('4'):
        return "Income"
    elif acct_str.startswith('8'):
        return "Expenses"
    elif acct_str.startswith('9'):
        return "Income"
    else:
        return "Other"


def calculate_period_changes(
    gl_df: pd.DataFrame,
    current_date: datetime,
    selected_fund: Optional[str] = None
) -> Dict[str, pd.DataFrame]:
    """
    Calculate trial balance changes for different periods (MTD, QTD, YTD, ITD).
    
    Returns dict with keys: 'mtd', 'qtd', 'ytd', 'itd'
    Each value is a DataFrame with beginning balance, ending balance, and change.
    """
    if gl_df.empty:
        return {}
    
    # Ensure date column is datetime
    gl_df['date'] = pd.to_datetime(gl_df['date'])
    
    # Ensure current_date is a datetime object
    if hasattr(current_date, 'date') and not isinstance(current_date, datetime):
        current_date = datetime.combine(current_date, datetime.min.time())
    elif not isinstance(current_date, datetime):
        current_date = pd.Timestamp(current_date).to_pydatetime()
    
    # Calculate period start dates
    mtd_start = datetime(current_date.year, current_date.month, 1) - timedelta(days=1)
    
    quarter = ((current_date.month - 1) // 3) + 1
    qtd_start = datetime(current_date.year, (quarter - 1) * 3 + 1, 1) - timedelta(days=1)
    
    ytd_start = datetime(current_date.year, 1, 1) - timedelta(days=1)
    
    # Handle ITD start - convert from pandas timestamp to datetime
    itd_min = gl_df['date'].min()
    if hasattr(itd_min, 'to_pydatetime'):
        itd_start = itd_min.to_pydatetime() - timedelta(days=1)
    else:
        itd_start = pd.Timestamp(itd_min).to_pydatetime() - timedelta(days=1)
    
    periods = {
        'mtd': (mtd_start, current_date),
        'qtd': (qtd_start, current_date),
        'ytd': (ytd_start, current_date),
        'itd': (itd_start, current_date)
    }
    
    results = {}
    
    for period_name, (start_date, end_date) in periods.items():
        # Get beginning and ending balances
        begin_tb = generate_trial_balance_from_gl(gl_df, start_date)
        end_tb = generate_trial_balance_from_gl(gl_df, end_date)
        
        # Merge to calculate changes
        if not begin_tb.empty and not end_tb.empty:
            merged = pd.merge(
                begin_tb[['GL_Acct_Number', 'GL_Acct_Name', 'Category', 'Balance']],
                end_tb[['GL_Acct_Number', 'Balance']],
                on='GL_Acct_Number',
                how='outer',
                suffixes=('_begin', '_end')
            )
        elif not end_tb.empty:
            merged = end_tb.copy()
            merged['Balance_begin'] = 0
            merged['Balance_end'] = merged['Balance']
        else:
            merged = pd.DataFrame()
        
        if not merged.empty:
            # Fill missing values
            merged['Balance_begin'] = merged['Balance_begin'].fillna(0)
            merged['Balance_end'] = merged['Balance_end'].fillna(0)
            
            # Calculate change
            merged['Change'] = merged['Balance_end'] - merged['Balance_begin']
            
            # Ensure we have all required columns
            if 'GL_Acct_Name' not in merged.columns:
                coa_df = load_COA_file()
                coa_dict = dict(zip(coa_df['GL_Acct_Number'], coa_df['GL_Acct_Name']))
                merged['GL_Acct_Name'] = merged['GL_Acct_Number'].apply(
                    lambda x: coa_dict.get(int(x), f"Account {int(x)}")
                )
            
            if 'Category' not in merged.columns:
                merged['Category'] = merged['GL_Acct_Number'].apply(categorize_account)
            
            results[period_name] = merged
        else:
            results[period_name] = pd.DataFrame()
    
    return results


def get_income_expense_changes(
    gl_df: pd.DataFrame,
    current_date: datetime
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Get income and expense account changes using ORIGINAL EXCEL GENERATOR LOGIC.
    This creates a master trial balance approach and calculates period changes.
    
    For DISPLAY purposes:
    - Income accounts (4xxx, 9xxx): Show as POSITIVE values in statement
    - Expense accounts (8xxx): Show as POSITIVE values in statement
    
    Returns:
        Tuple of (income_df, expense_df) with MTD, QTD, YTD, ITD columns
    """
    print("DEBUG - TB_GENERATOR: Starting get_income_expense_changes (ORIGINAL LOGIC)")
    print(f"DEBUG - TB_GENERATOR: GL data shape: {gl_df.shape}")
    print(f"DEBUG - TB_GENERATOR: Current date: {current_date}")
    
    if gl_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    
    # Create a master trial balance-like structure from GL data
    # Group by account and get cumulative balances for each account
    print("DEBUG - TB_GENERATOR: Building master TB structure from GL...")
    
    # First, create trial balance for the current date to get all accounts
    current_tb = generate_trial_balance_from_gl(gl_df, current_date)
    if current_tb.empty:
        print("DEBUG - TB_GENERATOR: No trial balance data for current date")
        return pd.DataFrame(), pd.DataFrame()
    
    # Filter for income and expenses only
    income_expense_accounts = current_tb[
        current_tb['GL_Acct_Number'].astype(str).str[0].isin(['4', '8', '9'])
    ].copy()
    
    print(f"DEBUG - TB_GENERATOR: Found {len(income_expense_accounts)} income/expense accounts")
    
    # Load COA for account names
    coa_df = load_COA_file()
    coa_dict = dict(zip(coa_df['GL_Acct_Number'], coa_df['GL_Acct_Name']))
    
    # Calculate period start dates (matching original logic)
    current_period1 = current_date.date() if hasattr(current_date, 'date') else current_date
    
    # Month to Date (MTD) - start is last day of previous month
    start_mtd = current_period1.replace(day=1) - timedelta(days=1)
    
    # Quarter to Date (QTD) 
    quarter = ((current_period1.month - 1) // 3) + 1
    start_qtd = datetime(current_period1.year, (quarter - 1) * 3 + 1, 1).date() - timedelta(days=1)
    
    # Year to Date (YTD)
    start_ytd = datetime(current_period1.year, 1, 1).date() - timedelta(days=1)
    
    # Inception to Date (ITD) - find earliest date in GL
    if 'date' in gl_df.columns:
        start_itd = gl_df['date'].min()
        if hasattr(start_itd, 'date'):
            start_itd = start_itd.date()
    else:
        start_itd = start_ytd
    
    periods = {
        'MTD': (start_mtd, current_period1),
        'QTD': (start_qtd, current_period1), 
        'YTD': (start_ytd, current_period1),
        'ITD': (start_itd, current_period1)
    }
    
    print(f"DEBUG - TB_GENERATOR: Period dates: {periods}")
    
    # Build change data for each account
    change_data = []
    for _, account_row in income_expense_accounts.iterrows():
        account_num = int(account_row['GL_Acct_Number'])
        account_name = coa_dict.get(account_num, f"Account {account_num}")
        
        # Determine category based on account number
        category = "Income" if str(account_num)[0] in ['4', '9'] else "Expenses"
        
        row_data = {
            'GL_Acct_Number': account_num,
            'GL_Acct_Name': account_name, 
            'Category': category
        }
        
        # Calculate change for each period
        for period_name, (start_date, end_date) in periods.items():
            # Get trial balance at start and end of period
            start_tb = generate_trial_balance_from_gl(gl_df, datetime.combine(start_date, datetime.min.time()) if isinstance(start_date, date) else start_date)
            end_tb = generate_trial_balance_from_gl(gl_df, datetime.combine(end_date, datetime.min.time()) if isinstance(end_date, date) else end_date)
            
            # Get balances for this account
            start_val = 0.0
            if not start_tb.empty:
                start_row = start_tb[start_tb['GL_Acct_Number'] == account_num]
                if not start_row.empty:
                    start_val = start_row['Balance'].iloc[0]
            
            end_val = 0.0 
            if not end_tb.empty:
                end_row = end_tb[end_tb['GL_Acct_Number'] == account_num]
                if not end_row.empty:
                    end_val = end_row['Balance'].iloc[0]
            
            # Calculate change
            change = end_val - start_val
            
            # IMPORTANT: For DISPLAY in income statement, present all values as positive
            # The original Excel generator applies sign flips in calculation but shows positive in statement
            if str(account_num).startswith(("9", "4")):
                # Income accounts: flip sign to show positive in statement
                change *= -1
                print(f"DEBUG - TB_GENERATOR: Income account {account_num} {period_name}: {end_val - start_val} -> {change} (display positive)")
            else:
                # Expense accounts: keep as positive for statement display
                print(f"DEBUG - TB_GENERATOR: Expense account {account_num} {period_name}: {change} (display positive)")
            
            # Ensure display values are positive for income statement presentation
            row_data[period_name] = round(abs(change), 6)
        
        # Only include accounts with non-zero values
        if any(abs(row_data[p]) > 0.000001 for p in ['MTD', 'QTD', 'YTD', 'ITD']):
            change_data.append(row_data)
        else:
            print(f"DEBUG - TB_GENERATOR: Skipping {account_name} - all periods zero")
    
    # Convert to DataFrame and separate income/expenses
    if not change_data:
        return pd.DataFrame(), pd.DataFrame()
    
    change_df = pd.DataFrame(change_data)
    
    income_df = change_df[change_df['Category'] == 'Income'].copy()
    expense_df = change_df[change_df['Category'] == 'Expenses'].copy()
    
    # Sort by account name
    if not income_df.empty:
        income_df = income_df.sort_values('GL_Acct_Name')
        print(f"DEBUG - TB_GENERATOR: Final income accounts: {income_df['GL_Acct_Name'].tolist()}")
    
    if not expense_df.empty:
        expense_df = expense_df.sort_values('GL_Acct_Name')
        print(f"DEBUG - TB_GENERATOR: Final expense accounts: {expense_df['GL_Acct_Name'].tolist()}")
    
    print(f"DEBUG - TB_GENERATOR: Final results - Income: {len(income_df)}, Expenses: {len(expense_df)}")
    
    return income_df, expense_df


def calculate_nav_changes(
    gl_df: pd.DataFrame,
    current_date: datetime
) -> pd.DataFrame:
    """
    Calculate NAV changes using ORIGINAL EXCEL GENERATOR LOGIC.
    This matches the exact approach from the original Excel generator.
    
    Key NAV presentation rules:
    - Beginning balance: Calculated from all accounts with sign flips at start period
    - Capital contributions (30110): Use ABS value (positive)
    - Distributions (30210): Use NEGATIVE value (distributions reduce NAV)
    - Net income: Income accounts (4xxx, 9xxx) POSITIVE, minus expenses (8xxx)
    
    Returns DataFrame with NAV waterfall: Beginning, Contributions, Distributions, Net Income, Ending
    """
    print("DEBUG - TB_GENERATOR: Starting NAV calculation (ORIGINAL LOGIC)")
    
    if gl_df.empty:
        return pd.DataFrame()
    
    # Calculate period dates (matching original)
    current_period1 = current_date.date() if hasattr(current_date, 'date') else current_date
    
    start_mtd = current_period1.replace(day=1) - timedelta(days=1)
    quarter = ((current_period1.month - 1) // 3) + 1
    start_qtd = datetime(current_period1.year, (quarter - 1) * 3 + 1, 1).date() - timedelta(days=1)
    start_ytd = datetime(current_period1.year, 1, 1).date() - timedelta(days=1)
    
    # Find minimum date in GL for ITD
    if 'date' in gl_df.columns:
        start_itd = gl_df['date'].min()
        if hasattr(start_itd, 'date'):
            start_itd = start_itd.date()
    else:
        start_itd = start_ytd
    
    periods = [
        ("MTD", start_mtd, current_period1),
        ("QTD", start_qtd, current_period1),
        ("YTD", start_ytd, current_period1), 
        ("ITD", start_itd, current_period1),
    ]
    
    nav_data = {
        "Beginning balance": [],
        "Capital contributions": [],
        "Distributions": [],
        "Net income (loss)": [],
        "Ending balance": [],
    }
    
    for label, start_date, end_date in periods:
        print(f"DEBUG - TB_GENERATOR: Calculating NAV for {label}: {start_date} to {end_date}")
        
        # Get trial balances for start and end periods
        start_tb = generate_trial_balance_from_gl(gl_df, datetime.combine(start_date, datetime.min.time()) if isinstance(start_date, date) else start_date)
        end_tb = generate_trial_balance_from_gl(gl_df, datetime.combine(end_date, datetime.min.time()) if isinstance(end_date, date) else end_date)
        
        if end_tb.empty:
            # Fill with zeros if no end data
            for key in nav_data:
                nav_data[key].append(0.0)
            continue
        
        # Calculate beginning balance using original logic
        # Beginning balance = sum of all equity (including income/expenses) at start
        if start_tb.empty:
            # If no start data, beginning balance is 0
            beginning = 0.0
            print(f"DEBUG - TB_GENERATOR: {label} beginning calculation: No start data, beginning = 0")
        else:
            df_begin = start_tb.copy()
            df_begin["AcctStr"] = df_begin["GL_Acct_Number"].astype(str)
            df_begin["Category"] = df_begin["AcctStr"].str[0].map({"9": "Income", "4": "Income", "8": "Expenses"}).fillna("Other")
            df_begin["value"] = df_begin["Balance"]
            
            # CRITICAL: Apply original sign flipping logic for beginning balance calculation
            df_begin.loc[df_begin["Category"] == "Income", "value"] *= -1  # Flip income signs
            df_begin.loc[df_begin["GL_Acct_Number"].isin([30110, 30210]), "value"] *= -1  # Flip specific capital accounts
            
            contrib_prior = df_begin.loc[df_begin["GL_Acct_Number"] == 30110, "value"].sum()
            dist_prior = df_begin.loc[df_begin["GL_Acct_Number"] == 30210, "value"].sum()
            income_prior = df_begin.loc[df_begin["Category"] == "Income", "value"].sum()
            expenses_prior = df_begin.loc[df_begin["Category"] == "Expenses", "value"].sum()
            net_prior = income_prior - expenses_prior
            beginning = contrib_prior + dist_prior + net_prior
            
            print(f"DEBUG - TB_GENERATOR: {label} beginning calculation:")
            print(f"  Contrib prior: {contrib_prior:.6f}")
            print(f"  Dist prior: {dist_prior:.6f}")
            print(f"  Income prior: {income_prior:.6f}")
            print(f"  Expenses prior: {expenses_prior:.6f}")
            print(f"  Beginning balance: {beginning:.6f}")
        
        # Calculate period changes (deltas)
        # Handle case when start_tb is empty
        if start_tb.empty:
            # If no start data, delta = ending balance
            delta_df = end_tb[['GL_Acct_Number', 'Balance']].copy()
            delta_df['Balance_end'] = delta_df['Balance']
            delta_df['Balance_start'] = 0
            delta_df['delta'] = delta_df['Balance_end']
        else:
            # Merge start and end to calculate changes
            delta_df = end_tb[['GL_Acct_Number', 'Balance']].merge(
                start_tb[['GL_Acct_Number', 'Balance']], 
                on='GL_Acct_Number', 
                how='outer', 
                suffixes=('_end', '_start')
            )
            delta_df['Balance_end'] = delta_df['Balance_end'].fillna(0)
            delta_df['Balance_start'] = delta_df['Balance_start'].fillna(0)
            delta_df['delta'] = delta_df['Balance_end'] - delta_df['Balance_start']
        
        # Apply categorization to deltas
        delta_df["AcctStr"] = delta_df["GL_Acct_Number"].astype(str)
        delta_df["Category"] = delta_df["AcctStr"].str[0].map({"9": "Income", "4": "Income", "8": "Expenses"}).fillna("Other")
        
        # Apply sign flipping to deltas (matching original logic)
        delta_df.loc[delta_df["Category"] == "Income", "delta"] *= -1  # Flip income deltas
        delta_df.loc[delta_df["GL_Acct_Number"].isin([30110, 30210]), "delta"] *= -1  # Flip capital account deltas
        
        # CRITICAL: Calculate NAV components with correct presentation logic
        # Capital contributions (30110): Should be POSITIVE in NAV statement
        contrib_delta = delta_df.loc[delta_df["GL_Acct_Number"] == 30110, "delta"].sum()
        contrib = abs(contrib_delta)  # Always show contributions as positive
        
        # Distributions (30210): Should be NEGATIVE in NAV statement  
        dist_delta = delta_df.loc[delta_df["GL_Acct_Number"] == 30210, "delta"].sum()
        dist = -abs(dist_delta) if dist_delta != 0 else 0  # Always show distributions as negative
        
        # Net Income: Income (positive) minus expenses (positive) 
        income_delta = delta_df.loc[delta_df["Category"] == "Income", "delta"].sum()
        expense_delta = delta_df.loc[delta_df["Category"] == "Expenses", "delta"].sum()
        # After sign flipping, income should be positive, expenses should be positive for subtraction
        net_income = abs(income_delta) - abs(expense_delta)
        
        ending = beginning + contrib + dist + net_income
        
        print(f"DEBUG - TB_GENERATOR: {label} NAV components:")
        print(f"  Beginning: {beginning:.6f}")
        print(f"  Contributions (30110): {contrib_delta:.6f} -> {contrib:.6f} (abs)")
        print(f"  Distributions (30210): {dist_delta:.6f} -> {dist:.6f} (negative)")
        print(f"  Income: {income_delta:.6f} -> {abs(income_delta):.6f}")
        print(f"  Expenses: {expense_delta:.6f} -> {abs(expense_delta):.6f}")
        print(f"  Net Income: {net_income:.6f}")
        print(f"  Ending: {ending:.6f}")
        
        nav_data["Beginning balance"].append(round(beginning, 6))
        nav_data["Capital contributions"].append(round(contrib, 6))
        nav_data["Distributions"].append(round(dist, 6))
        nav_data["Net income (loss)"].append(round(net_income, 6))
        nav_data["Ending balance"].append(round(ending, 6))
    
    # Convert to DataFrame matching original format
    nav_df = pd.DataFrame(nav_data, index=["Month to Date", "Quarter to Date", "Year to Date", "Inception to Date"]).T
    nav_df.reset_index(inplace=True)
    nav_df.rename(columns={"index": "Period"}, inplace=True)
    
    # Convert from wide to long format to match expected output
    result_data = []
    for period in ["Month to Date", "Quarter to Date", "Year to Date", "Inception to Date"]:
        result_data.append({
            'Period': period,
            'Beginning Balance': nav_df.loc[nav_df['Period'] == 'Beginning balance', period].iloc[0] if not nav_df.empty else 0,
            'Capital Contributions': nav_df.loc[nav_df['Period'] == 'Capital contributions', period].iloc[0] if not nav_df.empty else 0,
            'Distributions': nav_df.loc[nav_df['Period'] == 'Distributions', period].iloc[0] if not nav_df.empty else 0,
            'Net Income (Loss)': nav_df.loc[nav_df['Period'] == 'Net income (loss)', period].iloc[0] if not nav_df.empty else 0,
            'Ending Balance': nav_df.loc[nav_df['Period'] == 'Ending balance', period].iloc[0] if not nav_df.empty else 0
        })
    
    return pd.DataFrame(result_data)



def get_sort_order(acct_num) -> int:
    """Get sort order based on account number for proper financial statement ordering"""
    acct_str = str(int(acct_num))
    
    if acct_str.startswith('1'):  # Assets
        return 1
    elif acct_str.startswith('2'):  # Liabilities  
        return 2
    elif acct_str.startswith('3'):  # Capital
        return 3
    elif acct_str.startswith('4'):  # Income
        return 4
    elif acct_str.startswith('8'):  # Expenses
        return 8
    elif acct_str.startswith('9'):  # Other Income
        return 9
    else:
        return 99  # Other