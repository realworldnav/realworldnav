"""
Assets and Liabilities Module (Balance Sheet)
Statement of Assets and Liabilities
"""

from shiny import ui, render, reactive
import pandas as pd
import numpy as np
from datetime import datetime
from .tb_generator import generate_trial_balance_from_gl
from .data_processor import format_currency, consolidate_accounts


def apply_bad_debt_netting(tb_df, provision_accounts):
    """
    Apply provision netting to loan receivables (sister account netting).
    
    Logic based on user requirements:
    - Find accounts with "provision" in the name
    - Net them against their sister loan receivable accounts
    - Sister account is the account number minus 1 (e.g., 13501 provision nets with 13500 loan)
    - Creates "Net Loan Receivable" assets
    
    Args:
        tb_df: Trial balance DataFrame
        provision_accounts: DataFrame of provision accounts to net
        
    Returns:
        Updated trial balance with netted loan receivables
    """
    if provision_accounts.empty:
        return tb_df
    
    print(f"DEBUG - ASSETS_LIABILITIES: Applying provision netting for {len(provision_accounts)} provision accounts")
    
    tb_df = tb_df.copy()
    
    for _, provision_row in provision_accounts.iterrows():
        provision_account_num = provision_row['GL_Acct_Number']
        provision_balance = provision_row['Balance']
        
        # Sister account is typically the account number minus 1
        # E.g., 13501 provision nets with 13500 loan receivable
        sister_account_num = provision_account_num - 1
        
        # Find the sister loan receivable account
        loan_mask = tb_df['GL_Acct_Number'] == sister_account_num
        if not loan_mask.any():
            print(f"DEBUG - ASSETS_LIABILITIES: No sister account {sister_account_num} found for provision {provision_account_num}")
            continue
            
        # Get loan balance
        loan_balance = tb_df.loc[loan_mask, 'Balance'].iloc[0]
        
        # Net the provision against the loan 
        # Provision reduces the loan receivable (contra-asset)
        netted_balance = loan_balance + provision_balance  # Provision is typically negative, reducing the asset
        
        print(f"DEBUG - ASSETS_LIABILITIES: Netting loan {sister_account_num}: {loan_balance:.6f} + provision {provision_account_num}: {provision_balance:.6f} = {netted_balance:.6f}")
        
        # Update the loan account balance and name to reflect netting
        tb_df.loc[loan_mask, 'Balance'] = netted_balance
        original_name = tb_df.loc[loan_mask, 'GL_Acct_Name'].iloc[0]
        tb_df.loc[loan_mask, 'GL_Acct_Name'] = original_name.replace('Loan receivable', 'Net Loan Receivable')
    
    return tb_df


def assets_liabilities_ui():
    """Create UI for assets and liabilities statement"""
    return ui.card(
        ui.card_header("Statement of Assets and Liabilities"),
        ui.output_table("balance_sheet_table"),
        ui.hr(),
        ui.download_button("download_balance_sheet", "Download as CSV")
    )


def register_outputs(output, input, gl_data, selected_date):
    """Register server outputs for balance sheet"""
    
    @reactive.calc
    def balance_sheet_data():
        """Calculate balance sheet data"""
        df = gl_data()
        if df.empty:
            return pd.DataFrame(), pd.DataFrame(), 0
        
        # Get the selected reporting date
        report_date = selected_date() if selected_date else datetime.now()
        
        # Generate trial balance
        tb_df = generate_trial_balance_from_gl(df, report_date)
        
        if tb_df.empty:
            return pd.DataFrame(), pd.DataFrame(), 0
        
        # Filter out provision accounts for netting with sister loan accounts
        # Find accounts with "provision" in the name (these are contra-assets)
        provision_mask = tb_df['GL_Acct_Name'].str.contains('provision', case=False, na=False)
        provision_accounts = tb_df[provision_mask].copy() if provision_mask.any() else pd.DataFrame()
        
        # Remove provision accounts from main trial balance (they'll be netted)
        tb_df = tb_df[~provision_mask]
        
        # Apply provision netting to loan receivables (sister account netting)
        tb_df = apply_bad_debt_netting(tb_df, provision_accounts)
        
        # Separate assets and liabilities
        assets_df = tb_df[tb_df['Category'] == 'Assets'].copy()
        liabilities_df = tb_df[tb_df['Category'] == 'Liabilities'].copy()
        
        # Consolidate Note Payable accounts (25000-25099) if they exist
        note_payable_mask = (tb_df['GL_Acct_Number'] >= 25000) & (tb_df['GL_Acct_Number'] <= 25099)
        if note_payable_mask.any():
            note_payable_total = tb_df[note_payable_mask]['Balance'].sum()
            
            # Remove individual note payable accounts
            liabilities_df = liabilities_df[
                ~((liabilities_df['GL_Acct_Number'] >= 25000) & 
                  (liabilities_df['GL_Acct_Number'] <= 25099))
            ]
            
            # Add consolidated note payable
            consolidated_note = pd.DataFrame([{
                'GL_Acct_Number': 25000,
                'GL_Acct_Name': 'Note payable - net',
                'Category': 'Liabilities',
                'Balance': note_payable_total
            }])
            liabilities_df = pd.concat([liabilities_df, consolidated_note], ignore_index=True)
        
        # Calculate totals
        total_assets = assets_df['Balance'].sum() if not assets_df.empty else 0
        total_liabilities = liabilities_df['Balance'].sum() if not liabilities_df.empty else 0  # Keep as negative
        
        # Partners' Capital = sum of assets and liabilities (per user request)
        partners_capital = total_assets + total_liabilities
        
        # Sort by account name
        if not assets_df.empty:
            assets_df = assets_df.sort_values('GL_Acct_Name')
        if not liabilities_df.empty:
            liabilities_df = liabilities_df.sort_values('GL_Acct_Name')
        
        return assets_df, liabilities_df, partners_capital
    
    @output
    @render.table
    def balance_sheet_table():
        """Render the balance sheet table"""
        assets_df, liabilities_df, partners_capital = balance_sheet_data()
        
        # Create formatted table
        table_data = []
        
        # Assets section
        table_data.append({
            'Category': 'ASSETS',
            'Account': '',
            'Amount': ''
        })
        
        if not assets_df.empty:
            for _, row in assets_df.iterrows():
                table_data.append({
                    'Category': '',
                    'Account': row['GL_Acct_Name'],
                    'Amount': format_currency(row['Balance'], 'ETH')
                })
        
        # Total Assets
        total_assets = assets_df['Balance'].sum() if not assets_df.empty else 0
        table_data.append({
            'Category': '',
            'Account': 'Total Assets',
            'Amount': format_currency(total_assets, 'ETH')
        })
        
        # Blank row
        table_data.append({
            'Category': '',
            'Account': '',
            'Amount': ''
        })
        
        # Liabilities section
        table_data.append({
            'Category': 'LIABILITIES',
            'Account': '',
            'Amount': ''
        })
        
        if not liabilities_df.empty:
            for _, row in liabilities_df.iterrows():
                # Show liabilities as negative values (natural credit balances)
                table_data.append({
                    'Category': '',
                    'Account': row['GL_Acct_Name'],
                    'Amount': format_currency(row['Balance'], 'ETH')  # Keep natural negative balance
                })
        
        # Total Liabilities (keep as negative)
        total_liabilities = liabilities_df['Balance'].sum() if not liabilities_df.empty else 0
        table_data.append({
            'Category': '',
            'Account': 'Total Liabilities',
            'Amount': format_currency(total_liabilities, 'ETH')
        })
        
        # Blank row
        table_data.append({
            'Category': '',
            'Account': '',
            'Amount': ''
        })
        
        # Partners' Capital - formatted as TOTAL to bring the page together
        table_data.append({
            'Category': '',
            'Account': "PARTNERS' CAPITAL",
            'Amount': format_currency(partners_capital, 'ETH')
        })
        
        return pd.DataFrame(table_data)
    
    @output
    @render.download(filename=lambda: f"balance_sheet_{datetime.now().strftime('%Y%m%d')}.csv")
    def download_balance_sheet():
        """Download balance sheet as CSV"""
        import io
        assets_df, liabilities_df, partners_capital = balance_sheet_data()
        
        # Create structured CSV output like the reference implementation
        csv_buffer = io.StringIO()
        
        # Write header
        csv_buffer.write("Statement of Assets and Liabilities\n")
        csv_buffer.write("Account,Amount\n")
        
        # Assets section
        csv_buffer.write("ASSETS,\n")
        if not assets_df.empty:
            total_assets = 0
            for _, row in assets_df.iterrows():
                csv_buffer.write(f"{row['GL_Acct_Name']},{row['Balance']:.6f}\n")
                total_assets += row['Balance']
            csv_buffer.write(f"Total Assets,{total_assets:.6f}\n")
        else:
            csv_buffer.write("Total Assets,0.000000\n")
        
        csv_buffer.write("\n")
        
        # Liabilities section
        csv_buffer.write("LIABILITIES,\n")
        if not liabilities_df.empty:
            total_liabilities = 0
            for _, row in liabilities_df.iterrows():
                # Show liabilities as negative (natural credit balance)
                csv_buffer.write(f"{row['GL_Acct_Name']},{row['Balance']:.6f}\n")
                total_liabilities += row['Balance']
            csv_buffer.write(f"Total Liabilities,{total_liabilities:.6f}\n")
        else:
            csv_buffer.write("Total Liabilities,0.000000\n")
        
        csv_buffer.write("\n")
        
        # Partners' Capital section
        csv_buffer.write("EQUITY,\n")
        csv_buffer.write(f"Partners' Capital,{partners_capital:.6f}\n")
        
        csv_buffer.seek(0)
        return csv_buffer