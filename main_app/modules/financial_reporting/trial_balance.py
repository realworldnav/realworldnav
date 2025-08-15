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
        """Calculate trial balance data"""
        df = gl_data()
        if df.empty:
            return pd.DataFrame()
        
        # Get the selected reporting date
        report_date = selected_date() if selected_date else datetime.now()
        
        # Get comparison date (previous month end)
        comp_date = get_previous_period_date(report_date, 'month')
        
        # Generate trial balances for both dates
        current_tb = generate_trial_balance_from_gl(df, report_date)
        comparison_tb = generate_trial_balance_from_gl(df, comp_date)
        
        if current_tb.empty:
            return pd.DataFrame()
        
        # Merge the two trial balances
        if not comparison_tb.empty:
            merged_tb = pd.merge(
                current_tb,
                comparison_tb[['GL_Acct_Number', 'Balance']],
                on='GL_Acct_Number',
                how='outer',
                suffixes=('_current', '_comparison')
            )
            
            # Fill missing values
            merged_tb['Balance_current'] = merged_tb['Balance_current'].fillna(0)
            merged_tb['Balance_comparison'] = merged_tb['Balance_comparison'].fillna(0)
            
            # Ensure we have GL_Acct_Name and Category
            if 'GL_Acct_Name' not in merged_tb.columns:
                from ...s3_utils import load_COA_file
                coa_df = load_COA_file()
                coa_dict = dict(zip(coa_df['GL_Acct_Number'], coa_df['GL_Acct_Name']))
                merged_tb['GL_Acct_Name'] = merged_tb['GL_Acct_Number'].apply(
                    lambda x: coa_dict.get(int(x), f"Account {int(x)}")
                )
            
            if 'Category' not in merged_tb.columns:
                from .tb_generator import categorize_account
                merged_tb['Category'] = merged_tb['GL_Acct_Number'].apply(categorize_account)
        else:
            # No comparison data
            merged_tb = current_tb.copy()
            merged_tb['Balance_current'] = merged_tb['Balance']
            merged_tb['Balance_comparison'] = 0
        
        # Calculate change
        merged_tb['Change'] = merged_tb['Balance_current'] - merged_tb['Balance_comparison']
        
        # Sort by account number
        merged_tb['GL_Acct_Number'] = pd.to_numeric(merged_tb['GL_Acct_Number'], errors='coerce')
        merged_tb = merged_tb.sort_values('GL_Acct_Number')
        
        # Filter out rows where all balances are zero
        merged_tb = merged_tb[
            (np.abs(merged_tb['Balance_current']) > 0.000001) |
            (np.abs(merged_tb['Balance_comparison']) > 0.000001) |
            (np.abs(merged_tb['Change']) > 0.000001)
        ]
        
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