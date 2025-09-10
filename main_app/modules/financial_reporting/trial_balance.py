"""
Trial Balance Module
Generates trial balance with current and comparison periods
"""

from shiny import ui, render, reactive
import pandas as pd
import numpy as np
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pandas.tseries.offsets import MonthEnd
import re
from .tb_generator import generate_trial_balance_from_gl
from .data_processor import format_currency, get_previous_period_date


def clean_date_utc(date_val):
    """Clean and convert dates to UTC timezone"""
    if pd.isna(date_val):
        return pd.NaT

    date_str = str(date_val).strip()

    # Try direct conversion first
    try:
        parsed_date = pd.to_datetime(date_str, errors='raise')
        # Ensure UTC timezone
        if parsed_date.tz is None:
            return parsed_date.tz_localize('UTC')
        else:
            return parsed_date.tz_convert('UTC')
    except:
        # If that fails, try extracting just the date part
        if len(date_str) >= 10:
            date_part = date_str[:10]  # YYYY-MM-DD
            try:
                parsed_date = pd.to_datetime(date_part, errors='raise')
                # Localize to UTC (assume midnight UTC)
                return parsed_date.tz_localize('UTC')
            except:
                pass
        return pd.NaT


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
        """Calculate trial balance data using enhanced pivot table approach with comparison periods"""
        print(f"DEBUG - Financial Reporting TB: Starting trial_balance_data()")
        
        df = gl_data()
        if df.empty:
            print(f"DEBUG - Financial Reporting TB: GL data is empty")
            return pd.DataFrame()
        
        # Get the selected reporting date
        report_date = selected_date() if selected_date else datetime.now()
        
        # Get comparison date (previous month end)
        comp_date = get_previous_period_date(report_date, 'month')
        
        print(f"DEBUG - Financial Reporting TB: Report date: {report_date}, Comparison date: {comp_date}")
        
        # Load COA for account mapping
        from ...s3_utils import load_COA_file
        from .tb_generator import categorize_account
        
        coa_df = load_COA_file()
        if coa_df.empty:
            print(f"DEBUG - Financial Reporting TB: COA data is empty")
            return pd.DataFrame()
        
        try:
            # Make working copy and apply enhanced date cleaning
            gl_df = df.copy()
            print(f"DEBUG - Financial Reporting TB: GL data shape: {gl_df.shape}")
            
            # Apply enhanced date cleaning with UTC
            gl_df['date'] = gl_df['date'].apply(clean_date_utc)
            
            # Check for any remaining NaT dates
            nat_count = gl_df['date'].isna().sum()
            print(f"DEBUG - Financial Reporting TB: NaT dates after cleaning: {nat_count}")
            
            if nat_count > 0:
                # Remove NaT records
                gl_df = gl_df[gl_df['date'].notna()]
            
            # Check available columns and handle missing ones
            print(f"DEBUG - Financial Reporting TB: Available columns: {list(gl_df.columns)}")
            
            # Ensure GL_Acct_Number exists - if not, try to derive it from COA
            if 'GL_Acct_Number' not in gl_df.columns:
                print(f"DEBUG - Financial Reporting TB: GL_Acct_Number not found, using COA to map account_name")
                try:
                    gl_df = gl_df.merge(
                        coa_df[['account_name', 'GL_Acct_Number']].dropna(),
                        on='account_name',
                        how='left'
                    )
                    print(f"DEBUG - Financial Reporting TB: After COA merge, GL_Acct_Number available for {gl_df['GL_Acct_Number'].notna().sum()} records")
                except Exception as e:
                    print(f"DEBUG - Financial Reporting TB: COA merge failed: {e}")
                    return pd.DataFrame()
            
            # Convert debit/credit to Decimal for precision
            print(f"DEBUG - Financial Reporting TB: Converting amounts to Decimal...")
            for col in ['debit_crypto', 'credit_crypto']:
                if col in gl_df.columns:
                    gl_df[col] = (
                        gl_df[col]
                        .astype(str)
                        .apply(lambda x: Decimal(x) if (x.replace('.', '', 1).replace('-', '', 1).replace('e', '', 1).replace('+', '', 1).isdigit()) else Decimal(0))
                    )
            
            # Compute net_debit_credit at 18-decimal precision
            gl_df['net_debit_credit_crypto'] = (gl_df['debit_crypto'] - gl_df['credit_crypto']).round(18)
            
            # Extract "day" (midnight UTC) for grouping
            gl_df['day'] = gl_df['date'].dt.normalize()
            
            # Convert dates for comparison
            report_dt = pd.to_datetime(report_date)
            comp_dt = pd.to_datetime(comp_date)
            
            # Make dates UTC aware to match GL data
            report_dt = pd.Timestamp(report_dt, tz='UTC')
            comp_dt = pd.Timestamp(comp_dt, tz='UTC')
            
            print(f"DEBUG - Financial Reporting TB: UTC dates - report_dt: {report_dt}, comp_dt: {comp_dt}")
            
            # Function to generate trial balance for a specific date
            def generate_tb_for_date(end_date, period_name):
                print(f"DEBUG - Financial Reporting TB: Generating TB for {period_name} up to {end_date}")
                
                # Filter data up to the specified date
                period_df = gl_df[gl_df['date'] <= end_date].copy()
                print(f"DEBUG - Financial Reporting TB: {period_name} data shape: {period_df.shape}")
                
                if period_df.empty:
                    return pd.DataFrame()
                
                # Use only columns that exist for the pivot table
                if 'GL_Acct_Number' in period_df.columns:
                    pivot_index = ['GL_Acct_Number', 'account_name']
                else:
                    pivot_index = ['account_name']
                
                # Create pivot table with cumulative balances
                acct_by_day = (
                    period_df
                    .pivot_table(
                        index=pivot_index,
                        columns='day',
                        values='net_debit_credit_crypto',
                        aggfunc='sum',
                        fill_value=Decimal(0),
                    )
                )
                
                if acct_by_day.empty:
                    return pd.DataFrame()
                
                # Take cumulative sum to get running balances
                trial_balance = acct_by_day.cumsum(axis=1)
                
                # Get the final balance (last column) for each account
                final_balances = trial_balance.iloc[:, -1].to_frame('Balance')
                final_balances = final_balances.reset_index()
                
                # Ensure GL_Acct_Number column exists
                if 'GL_Acct_Number' not in final_balances.columns:
                    final_balances['GL_Acct_Number'] = final_balances.index.astype(str)
                
                # Convert to float for display
                final_balances['Balance'] = final_balances['Balance'].apply(float)
                
                # Filter out near-zero balances
                final_balances = final_balances[abs(final_balances['Balance']) > 0.000001]
                
                print(f"DEBUG - Financial Reporting TB: {period_name} final accounts: {len(final_balances)}")
                return final_balances
            
            # Generate trial balances for both periods
            current_tb = generate_tb_for_date(report_dt, "Current")
            comp_tb = generate_tb_for_date(comp_dt, "Comparison")
            
            if current_tb.empty and comp_tb.empty:
                print("DEBUG - Financial Reporting TB: No trial balance data for either period")
                return pd.DataFrame()
            
            # Merge current and comparison balances
            if not current_tb.empty and not comp_tb.empty:
                merged_tb = pd.merge(
                    current_tb[['GL_Acct_Number', 'account_name', 'Balance']],
                    comp_tb[['GL_Acct_Number', 'Balance']],
                    on='GL_Acct_Number',
                    how='outer',
                    suffixes=('_current', '_comparison')
                )
            elif not current_tb.empty:
                merged_tb = current_tb.copy()
                merged_tb['Balance_current'] = merged_tb['Balance']
                merged_tb['Balance_comparison'] = 0.0
            else:  # only comp_tb has data
                merged_tb = comp_tb.copy()
                merged_tb['Balance_current'] = 0.0
                merged_tb['Balance_comparison'] = merged_tb['Balance']
            
            # Fill missing values
            merged_tb['Balance_current'] = merged_tb.get('Balance_current', merged_tb.get('Balance', 0.0)).fillna(0.0)
            merged_tb['Balance_comparison'] = merged_tb.get('Balance_comparison', 0.0).fillna(0.0)
            
            # Calculate change
            merged_tb['Change'] = merged_tb['Balance_current'] - merged_tb['Balance_comparison']
            
            # Add account information from COA
            coa_dict = dict(zip(coa_df['GL_Acct_Number'], coa_df['GL_Acct_Name']))
            
            # Ensure GL_Acct_Number is numeric for proper mapping and sorting
            merged_tb['GL_Acct_Number'] = pd.to_numeric(merged_tb['GL_Acct_Number'], errors='coerce')
            merged_tb = merged_tb[pd.notna(merged_tb['GL_Acct_Number'])]
            
            # Add formal account names from COA
            merged_tb['GL_Acct_Name'] = merged_tb['GL_Acct_Number'].apply(
                lambda x: coa_dict.get(int(x), f"Account {int(x)}")
            )
            
            # Add categories
            merged_tb['Category'] = merged_tb['GL_Acct_Number'].apply(categorize_account)
            
            # Keep original account name for display
            if 'account_name' not in merged_tb.columns:
                merged_tb['account_name'] = merged_tb['GL_Acct_Name']
            merged_tb['original_account_name'] = merged_tb['account_name']
            
            # Sort by account number
            merged_tb = merged_tb.sort_values('GL_Acct_Number')
            
            # Round values for display
            for col in ['Balance_current', 'Balance_comparison', 'Change']:
                merged_tb[col] = merged_tb[col].round(6)
            
            print(f"DEBUG - Financial Reporting TB: Final trial balance has {len(merged_tb)} accounts")
            
            return merged_tb, report_date, comp_date
            
        except Exception as e:
            print(f"ERROR - Financial Reporting TB: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
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