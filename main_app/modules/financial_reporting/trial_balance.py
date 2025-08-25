"""
Trial Balance Module
Generates trial balance with current and comparison periods
"""

from shiny import ui, render, reactive
import pandas as pd
import numpy as np
from datetime import datetime
from .tb_generator import generate_trial_balance_from_gl
from .data_processor import format_currency, get_previous_period_date


def trial_balance_ui():
    """Create UI for trial balance"""
    return ui.card(
        ui.card_header("Trial Balance"),
        ui.output_table("trial_balance_table"),
        ui.hr(),
        ui.download_button("download_trial_balance", "Download as CSV")
    )


def register_outputs(output, input, gl_data, selected_date):
    """Register server outputs for trial balance"""
    
    @reactive.calc
    def trial_balance_data():
        """Calculate trial balance data using COA-first approach to ensure ALL non-zero accounts are captured"""
        df = gl_data()
        if df.empty:
            return pd.DataFrame()
        
        # Get the selected reporting date
        report_date = selected_date() if selected_date else datetime.now()
        
        # Get comparison date (previous month end)
        comp_date = get_previous_period_date(report_date, 'month')
        
        # CRITICAL: Load COA first as master account list (like Fund Accounting does)
        from ...s3_utils import load_COA_file
        from .tb_generator import categorize_account
        
        coa_df = load_COA_file()
        if coa_df.empty:
            return pd.DataFrame()
        
        # Ensure GL data has proper account mapping
        if 'GL_Acct_Number' not in df.columns:
            print("ERROR - Financial Reporting TB: No GL_Acct_Number column found in GL data")
            return pd.DataFrame()
        
        # Build trial balance using COA-first approach
        print(f"DEBUG - Financial Reporting TB: Building trial balance for {len(coa_df)} COA accounts")
        
        tb_accounts = []
        coa_dict = dict(zip(coa_df['GL_Acct_Number'], coa_df['GL_Acct_Name']))
        
        # For each account in COA, check if it has activity in GL data
        for _, coa_row in coa_df.iterrows():
            gl_acct_number = coa_row['GL_Acct_Number']
            gl_acct_name = coa_row['GL_Acct_Name']
            
            # Get all transactions for this account (regardless of date filtering for now)
            account_gl_data = df[df['GL_Acct_Number'] == gl_acct_number].copy()
            
            if account_gl_data.empty:
                continue  # Skip accounts with no transactions
            
            # Ensure proper timezone handling for date comparisons
            account_dates = pd.to_datetime(account_gl_data['date'], errors='coerce')
            
            # Convert comparison dates to match GL data timezone if needed
            report_dt = pd.to_datetime(report_date)
            comp_dt = pd.to_datetime(comp_date)
            
            # If GL dates are timezone-aware, make comparison dates timezone-aware too
            if account_dates.dt.tz is not None:
                if report_dt.tz is None:
                    report_dt = pd.Timestamp(report_dt, tz='UTC')
                if comp_dt.tz is None:
                    comp_dt = pd.Timestamp(comp_dt, tz='UTC')
            
            # Calculate balance for current period (up to report_date)
            current_data = account_gl_data[account_dates <= report_dt]
            
            if not current_data.empty:
                current_debits = pd.to_numeric(current_data['debit_crypto'], errors='coerce').fillna(0).sum()
                current_credits = pd.to_numeric(current_data['credit_crypto'], errors='coerce').fillna(0).sum()
                current_balance = current_debits - current_credits
            else:
                current_balance = 0.0
            
            # Calculate balance for comparison period (up to comp_date)
            comp_data = account_gl_data[account_dates <= comp_dt]
            
            if not comp_data.empty:
                comp_debits = pd.to_numeric(comp_data['debit_crypto'], errors='coerce').fillna(0).sum()
                comp_credits = pd.to_numeric(comp_data['credit_crypto'], errors='coerce').fillna(0).sum()
                comp_balance = comp_debits - comp_credits
            else:
                comp_balance = 0.0
            
            # Include account if either period has non-zero balance
            if abs(current_balance) > 0.000001 or abs(comp_balance) > 0.000001:
                # Get original account name for display (first occurrence in GL data)
                original_name = account_gl_data['account_name'].iloc[0] if 'account_name' in account_gl_data.columns else gl_acct_name
                
                tb_accounts.append({
                    'GL_Acct_Number': gl_acct_number,
                    'GL_Acct_Name': gl_acct_name,
                    'original_account_name': original_name,
                    'Category': categorize_account(gl_acct_number),
                    'Balance_current': round(current_balance, 6),
                    'Balance_comparison': round(comp_balance, 6),
                    'Change': round(current_balance - comp_balance, 6)
                })
        
        if not tb_accounts:
            print("DEBUG - Financial Reporting TB: No accounts with non-zero balances found")
            return pd.DataFrame()
        
        # Convert to DataFrame
        merged_tb = pd.DataFrame(tb_accounts)
        
        # Sort by account number
        merged_tb['GL_Acct_Number'] = pd.to_numeric(merged_tb['GL_Acct_Number'], errors='coerce')
        merged_tb = merged_tb.sort_values('GL_Acct_Number')
        
        print(f"DEBUG - Financial Reporting TB: Final trial balance has {len(merged_tb)} accounts")
        
        return merged_tb, report_date, comp_date
    
    @output
    @render.table
    def trial_balance_table():
        """Render the trial balance table"""
        result = trial_balance_data()
        if isinstance(result, tuple):
            tb_df, report_date, comp_date = result
        else:
            return pd.DataFrame({'Message': ['No data available']})
        
        if tb_df.empty:
            return pd.DataFrame({'Message': ['No trial balance data available']})
        
        # Prepare display DataFrame with both original and formal account names
        display_df = pd.DataFrame({
            'Category': tb_df['Category'],
            'GL#': tb_df['GL_Acct_Number'].astype(int),
            'Account Name': tb_df.get('original_account_name', tb_df['GL_Acct_Name']),
            'GL Account Name': tb_df['GL_Acct_Name'],
            f'{report_date.strftime("%m/%d/%Y")}': tb_df['Balance_current'].apply(
                lambda x: format_currency(x, 'ETH')
            ),
            f'{comp_date.strftime("%m/%d/%Y")}': tb_df['Balance_comparison'].apply(
                lambda x: format_currency(x, 'ETH')
            ),
            'Change': tb_df['Change'].apply(lambda x: format_currency(x, 'ETH'))
        })
        
        # Add totals row
        totals_row = pd.DataFrame({
            'Category': ['TOTAL'],
            'GL#': [''],
            'Account Name': [''],
            'GL Account Name': [''],
            f'{report_date.strftime("%m/%d/%Y")}': [
                format_currency(tb_df['Balance_current'].sum(), 'ETH')
            ],
            f'{comp_date.strftime("%m/%d/%Y")}': [
                format_currency(tb_df['Balance_comparison'].sum(), 'ETH')
            ],
            'Change': [format_currency(tb_df['Change'].sum(), 'ETH')]
        })
        
        display_df = pd.concat([display_df, totals_row], ignore_index=True)
        
        return display_df
    
    @output
    @render.download(filename=lambda: f"trial_balance_{datetime.now().strftime('%Y%m%d')}.csv")
    def download_trial_balance():
        """Download trial balance as CSV"""
        import io
        result = trial_balance_data()
        if isinstance(result, tuple):
            tb_df, report_date, comp_date = result
        else:
            csv_buffer = io.StringIO()
            csv_buffer.write("No trial balance data available")
            csv_buffer.seek(0)
            return csv_buffer
        
        if tb_df.empty:
            csv_buffer = io.StringIO()
            csv_buffer.write("No trial balance data available")
            csv_buffer.seek(0)
            return csv_buffer
        
        # Prepare download DataFrame
        download_df = tb_df[['Category', 'GL_Acct_Number', 'GL_Acct_Name', 
                            'Balance_current', 'Balance_comparison', 'Change']].copy()
        download_df.columns = ['Category', 'GL_Number', 'Account', 
                              f'Balance_{report_date.strftime("%Y%m%d")}',
                              f'Balance_{comp_date.strftime("%Y%m%d")}',
                              'Change']
        
        csv_buffer = io.StringIO()
        download_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        return csv_buffer