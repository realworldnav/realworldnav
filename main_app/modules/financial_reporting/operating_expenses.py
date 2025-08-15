"""
Operating Expenses Module
Schedule of Operating Expenses with accrual and payment details
"""

from shiny import ui, render, reactive
import pandas as pd
from datetime import datetime
from .tb_generator import calculate_period_changes
from .data_processor import format_currency, get_previous_period_date


def operating_expenses_ui():
    """Create UI for operating expenses schedule"""
    return ui.card(
        ui.card_header("Schedule of Operating Expenses"),
        ui.output_table("operating_expenses_table"),
        ui.hr(),
        ui.download_button("download_operating_expenses", "Download as CSV")
    )


def register_outputs(output, input, gl_data, selected_date):
    """Register server outputs for operating expenses"""
    
    @reactive.calc
    def operating_expenses_data():
        """Calculate operating expenses data"""
        df = gl_data()
        if df.empty:
            return pd.DataFrame()
        
        # Get the selected reporting date
        report_date = selected_date() if selected_date else datetime.now()
        
        # Get comparison date (previous month end)
        comp_date = get_previous_period_date(report_date, 'month')
        
        # Get period changes
        period_changes = calculate_period_changes(df, report_date)
        mtd_changes = period_changes.get('mtd', pd.DataFrame())
        
        if mtd_changes.empty:
            return pd.DataFrame()
        
        # Filter for expense accounts only
        expense_df = mtd_changes[mtd_changes['Category'] == 'Expenses'].copy()
        
        if expense_df.empty:
            return pd.DataFrame()
        
        # Load COA to identify bad debt accounts (provision-related)
        from ...s3_utils import load_COA_file
        coa_df = load_COA_file()
        bad_debt_accounts = coa_df[
            coa_df['GL_Acct_Name'].str.contains('fair value', case=False, na=False)
        ]['GL_Acct_Number'].tolist()
        
        # Exclude bad debt accounts
        expense_df = expense_df[~expense_df['GL_Acct_Number'].isin(bad_debt_accounts)]
        
        # Calculate expense schedule columns
        expense_schedule = []
        
        for _, row in expense_df.iterrows():
            acct_num = int(row['GL_Acct_Number'])
            acct_name = row['GL_Acct_Name']
            
            beginning_balance = row.get('Balance_begin', 0)
            accrual_amount = row.get('Change', 0)
            ending_balance = row.get('Balance_end', 0)
            
            # Calculate paid amount: Beginning + Accrual - Ending
            paid_amount = beginning_balance + accrual_amount - ending_balance
            
            # Skip if all amounts are zero
            if all(abs(val) < 0.000001 for val in [beginning_balance, accrual_amount, ending_balance, paid_amount]):
                continue
            
            expense_schedule.append({
                'Category': 'Operating Expenses',
                'GL_Number': acct_num,
                'Accrual_Description': acct_name,
                'Beginning_Balance': beginning_balance,
                'Accrual_Period': accrual_amount,
                'Paid_Period': paid_amount,
                'Ending_Balance': ending_balance,
                'YTD_Accrual': accrual_amount,  # For now, same as period
                'YTD_Paid': paid_amount  # For now, same as period
            })
        
        return pd.DataFrame(expense_schedule)
    
    @output
    @render.table
    def operating_expenses_table():
        """Render the operating expenses table"""
        expense_df = operating_expenses_data()
        
        if expense_df.empty:
            return pd.DataFrame({'Message': ['No operating expenses data available']})
        
        # Format for display
        display_data = []
        
        # Header row
        display_data.append({
            'Category': 'Operating Expense Detail',
            'GL Number': '',
            'Accrual Description': '',
            'Beginning Balance': '',
            'Accrual for Period': '',
            'Paid During Period': '',
            'Ending Balance': '',
            'YTD Accrual': '',
            'YTD Paid': ''
        })
        
        # Data rows
        totals = {
            'beginning': 0,
            'accrual': 0,
            'paid': 0,
            'ending': 0,
            'ytd_accrual': 0,
            'ytd_paid': 0
        }
        
        for _, row in expense_df.iterrows():
            display_data.append({
                'Category': '',
                'GL Number': str(row['GL_Number']),
                'Accrual Description': row['Accrual_Description'],
                'Beginning Balance': format_currency(row['Beginning_Balance'], 'ETH'),
                'Accrual for Period': format_currency(row['Accrual_Period'], 'ETH'),
                'Paid During Period': format_currency(row['Paid_Period'], 'ETH'),
                'Ending Balance': format_currency(row['Ending_Balance'], 'ETH'),
                'YTD Accrual': format_currency(row['YTD_Accrual'], 'ETH'),
                'YTD Paid': format_currency(row['YTD_Paid'], 'ETH')
            })
            
            # Add to totals
            totals['beginning'] += row['Beginning_Balance']
            totals['accrual'] += row['Accrual_Period']
            totals['paid'] += row['Paid_Period']
            totals['ending'] += row['Ending_Balance']
            totals['ytd_accrual'] += row['YTD_Accrual']
            totals['ytd_paid'] += row['YTD_Paid']
        
        # Totals row
        display_data.append({
            'Category': '',
            'GL Number': '',
            'Accrual Description': 'TOTAL',
            'Beginning Balance': format_currency(totals['beginning'], 'ETH'),
            'Accrual for Period': format_currency(totals['accrual'], 'ETH'),
            'Paid During Period': format_currency(totals['paid'], 'ETH'),
            'Ending Balance': format_currency(totals['ending'], 'ETH'),
            'YTD Accrual': format_currency(totals['ytd_accrual'], 'ETH'),
            'YTD Paid': format_currency(totals['ytd_paid'], 'ETH')
        })
        
        return pd.DataFrame(display_data)
    
    @output
    @render.download(filename=lambda: f"operating_expenses_{datetime.now().strftime('%Y%m%d')}.csv")
    def download_operating_expenses():
        """Download operating expenses as CSV"""
        import io
        expense_df = operating_expenses_data()
        
        if expense_df.empty:
            csv_buffer = io.StringIO()
            csv_buffer.write("No operating expenses data available")
            csv_buffer.seek(0)
            return csv_buffer
        
        csv_buffer = io.StringIO()
        expense_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        return csv_buffer