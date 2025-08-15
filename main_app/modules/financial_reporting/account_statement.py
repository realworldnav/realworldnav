"""
Account Statement Module (Statement of Income)
Generates income statement with MTD, QTD, YTD, ITD columns
"""

from shiny import ui, render, reactive
import pandas as pd
from datetime import datetime
from .tb_generator import get_income_expense_changes
from .data_processor import format_currency, filter_zero_balances


def account_statement_ui():
    """Create UI for account statement (income statement)"""
    return ui.card(
        ui.card_header("Statement of Income"),
        ui.output_table("income_statement_table"),
        ui.hr(),
        ui.download_button("download_income_statement", "Download as CSV")
    )


def register_outputs(output, input, gl_data, selected_date):
    """Register server outputs for account statement"""
    
    @reactive.calc
    def income_statement_data():
        """Calculate income statement data"""
        print("DEBUG - ACCOUNT STATEMENT: Starting income statement calculation")
        
        df = gl_data()
        if df.empty:
            print("DEBUG - ACCOUNT STATEMENT: GL data is empty")
            return pd.DataFrame(), pd.DataFrame(), {}
        
        print(f"DEBUG - ACCOUNT STATEMENT: GL data shape: {df.shape}")
        print(f"DEBUG - ACCOUNT STATEMENT: GL columns: {df.columns.tolist()}")
        
        # Get the selected reporting date
        report_date = selected_date() if selected_date else datetime.now()
        print(f"DEBUG - ACCOUNT STATEMENT: Report date: {report_date}")
        
        # Debug account names and patterns
        if 'account_name' in df.columns:
            unique_accounts = df['account_name'].unique()
            print(f"DEBUG - ACCOUNT STATEMENT: Found {len(unique_accounts)} unique account names")
            print(f"DEBUG - ACCOUNT STATEMENT: Sample accounts: {unique_accounts[:10].tolist()}")
            
            # Look for potential income accounts
            income_keywords = ['income', 'revenue', 'gain', 'interest']
            potential_income = []
            for account in unique_accounts:
                account_lower = str(account).lower()
                for keyword in income_keywords:
                    if keyword in account_lower:
                        potential_income.append(account)
                        break
            
            print(f"DEBUG - ACCOUNT STATEMENT: Potential income accounts: {potential_income}")
            
            # Look for accounts with 4xxx or 9xxx patterns
            income_4x_9x = []
            for account in unique_accounts:
                if any(char.isdigit() and account.startswith(char) for char in ['4', '9']):
                    income_4x_9x.append(account)
            
            print(f"DEBUG - ACCOUNT STATEMENT: Accounts starting with 4/9: {income_4x_9x}")
        
        # Debug date filtering
        if 'date' in df.columns:
            print(f"DEBUG - ACCOUNT STATEMENT: Date range in GL: {df['date'].min()} to {df['date'].max()}")
            from .data_processor import safe_date_compare
            filtered_records = df[safe_date_compare(df['date'], report_date)]
            print(f"DEBUG - ACCOUNT STATEMENT: Records up to report date: {len(filtered_records)} (from {len(df)} total)")
        
        # Get income and expense changes
        print("DEBUG - ACCOUNT STATEMENT: Calling get_income_expense_changes")
        income_df, expense_df = get_income_expense_changes(df, report_date)
        
        print(f"DEBUG - ACCOUNT STATEMENT: Income DF shape: {income_df.shape}")
        print(f"DEBUG - ACCOUNT STATEMENT: Income DF columns: {income_df.columns.tolist() if not income_df.empty else 'Empty'}")
        if not income_df.empty:
            print(f"DEBUG - ACCOUNT STATEMENT: Income accounts: {income_df['GL_Acct_Name'].tolist() if 'GL_Acct_Name' in income_df.columns else 'No GL_Acct_Name column'}")
            print(f"DEBUG - ACCOUNT STATEMENT: Income categories: {income_df['Category'].unique().tolist() if 'Category' in income_df.columns else 'No Category column'}")
        
        print(f"DEBUG - ACCOUNT STATEMENT: Expense DF shape: {expense_df.shape}")
        print(f"DEBUG - ACCOUNT STATEMENT: Expense DF columns: {expense_df.columns.tolist() if not expense_df.empty else 'Empty'}")
        if not expense_df.empty:
            print(f"DEBUG - ACCOUNT STATEMENT: Expense accounts: {expense_df['GL_Acct_Name'].tolist() if 'GL_Acct_Name' in expense_df.columns else 'No GL_Acct_Name column'}")
            print(f"DEBUG - ACCOUNT STATEMENT: Expense categories: {expense_df['Category'].unique().tolist() if 'Category' in expense_df.columns else 'No Category column'}")
        
        # Calculate totals
        totals = {}
        for period in ['MTD', 'QTD', 'YTD', 'ITD']:
            income_total = income_df[period].sum() if not income_df.empty and period in income_df.columns else 0
            expense_total = expense_df[period].sum() if not expense_df.empty and period in expense_df.columns else 0
            net_income = income_total - expense_total
            
            print(f"DEBUG - ACCOUNT STATEMENT: {period} - Income: {income_total}, Expenses: {expense_total}, Net: {net_income}")
            
            totals[period] = {
                'income': income_total,
                'expenses': expense_total,
                'net_income': net_income
            }
        
        print("DEBUG - ACCOUNT STATEMENT: Completed income statement calculation")
        return income_df, expense_df, totals
    
    @output
    @render.table
    def income_statement_table():
        """Render the income statement table"""
        income_df, expense_df, totals = income_statement_data()
        
        # Create formatted table
        table_data = []
        
        # Add header row
        table_data.append({
            'Account': 'INCOME',
            'MTD': '',
            'QTD': '',
            'YTD': '',
            'ITD': ''
        })
        
        # Add income rows
        if not income_df.empty:
            for _, row in income_df.iterrows():
                table_data.append({
                    'Account': f"  {row['GL_Acct_Name']}",
                    'MTD': format_currency(row.get('MTD', 0), 'ETH'),
                    'QTD': format_currency(row.get('QTD', 0), 'ETH'),
                    'YTD': format_currency(row.get('YTD', 0), 'ETH'),
                    'ITD': format_currency(row.get('ITD', 0), 'ETH')
                })
        
        # Add income total
        table_data.append({
            'Account': 'Total Income',
            'MTD': format_currency(totals['MTD']['income'], 'ETH'),
            'QTD': format_currency(totals['QTD']['income'], 'ETH'),
            'YTD': format_currency(totals['YTD']['income'], 'ETH'),
            'ITD': format_currency(totals['ITD']['income'], 'ETH')
        })
        
        # Add blank row
        table_data.append({
            'Account': '',
            'MTD': '',
            'QTD': '',
            'YTD': '',
            'ITD': ''
        })
        
        # Add expenses header
        table_data.append({
            'Account': 'EXPENSES',
            'MTD': '',
            'QTD': '',
            'YTD': '',
            'ITD': ''
        })
        
        # Add expense rows
        if not expense_df.empty:
            for _, row in expense_df.iterrows():
                table_data.append({
                    'Account': f"  {row['GL_Acct_Name']}",
                    'MTD': format_currency(row.get('MTD', 0), 'ETH'),
                    'QTD': format_currency(row.get('QTD', 0), 'ETH'),
                    'YTD': format_currency(row.get('YTD', 0), 'ETH'),
                    'ITD': format_currency(row.get('ITD', 0), 'ETH')
                })
        
        # Add expense total
        table_data.append({
            'Account': 'Total Expenses',
            'MTD': format_currency(totals['MTD']['expenses'], 'ETH'),
            'QTD': format_currency(totals['QTD']['expenses'], 'ETH'),
            'YTD': format_currency(totals['YTD']['expenses'], 'ETH'),
            'ITD': format_currency(totals['ITD']['expenses'], 'ETH')
        })
        
        # Add blank row
        table_data.append({
            'Account': '',
            'MTD': '',
            'QTD': '',
            'YTD': '',
            'ITD': ''
        })
        
        # Add net income
        table_data.append({
            'Account': 'NET INCOME',
            'MTD': format_currency(totals['MTD']['net_income'], 'ETH'),
            'QTD': format_currency(totals['QTD']['net_income'], 'ETH'),
            'YTD': format_currency(totals['YTD']['net_income'], 'ETH'),
            'ITD': format_currency(totals['ITD']['net_income'], 'ETH')
        })
        
        return pd.DataFrame(table_data)
    
    @output
    @render.download(filename=lambda: f"income_statement_{datetime.now().strftime('%Y%m%d')}.csv")
    def download_income_statement():
        """Download income statement as CSV"""
        income_df, expense_df, totals = income_statement_data()
        
        # Combine into single DataFrame for download
        combined = []
        
        # Add income section
        if not income_df.empty:
            income_section = income_df.copy()
            income_section['Section'] = 'Income'
            combined.append(income_section)
        
        # Add expense section
        if not expense_df.empty:
            expense_section = expense_df.copy()
            expense_section['Section'] = 'Expenses'
            combined.append(expense_section)
        
        if combined:
            result = pd.concat(combined, ignore_index=True)
            
            # Add totals row
            totals_row = {
                'GL_Acct_Number': '',
                'GL_Acct_Name': 'Net Income',
                'Category': 'Total',
                'Section': 'Summary',
                'MTD': totals['MTD']['net_income'],
                'QTD': totals['QTD']['net_income'],
                'YTD': totals['YTD']['net_income'],
                'ITD': totals['ITD']['net_income']
            }
            result = pd.concat([result, pd.DataFrame([totals_row])], ignore_index=True)
            
            import io
            csv_buffer = io.StringIO()
            result.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            return csv_buffer
        else:
            import io
            csv_buffer = io.StringIO()
            csv_buffer.write("No income statement data available")
            csv_buffer.seek(0)
            return csv_buffer