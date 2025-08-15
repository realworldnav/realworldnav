from shiny import ui as shiny_ui, render, reactive
from datetime import datetime
import pandas as pd
from ...s3_utils import load_GL_file
from ..fund_accounting.helpers import gl_data_for_fund
from .account_statement import account_statement_ui, register_outputs as as_register_outputs
from .nav_changes import nav_changes_ui, register_outputs as nc_register_outputs
from .trial_balance import trial_balance_ui, register_outputs as tb_register_outputs
from .assets_liabilities import assets_liabilities_ui, register_outputs as al_register_outputs
from .operating_expenses import operating_expenses_ui, register_outputs as oe_register_outputs
from .management_fee import management_fee_ui, register_outputs as mf_register_outputs
from .excel_export_new import generate_excel_report

def register_outputs(output, input, selected_fund, selected_report_date):
    """Register all financial reporting outputs"""
    
    @reactive.calc
    def gl_data():
        """Load GL data directly from S3 with proper processing and COA mapping"""
        fund_id = selected_fund() if selected_fund and hasattr(selected_fund, '__call__') else None
        print(f"DEBUG - Financial Reporting loading GL data for fund: {fund_id}")
        
        # Load GL data directly from S3
        gl_df = load_GL_file()
        
        if gl_df.empty:
            print("DEBUG - No GL data loaded from S3")
            return pd.DataFrame()
        
        print(f"DEBUG - Loaded GL data with shape: {gl_df.shape}")
        print(f"DEBUG - GL columns: {gl_df.columns.tolist()}")
        
        # Filter by fund if specified
        if fund_id:
            original_shape = gl_df.shape[0]
            if 'fund_id' in gl_df.columns:
                gl_df = gl_df[gl_df['fund_id'] == fund_id]
                print(f"DEBUG - Filtered by fund_id {fund_id}: {gl_df.shape[0]} rows (was {original_shape})")
        
        # Ensure date column is properly formatted
        if 'date' in gl_df.columns:
            gl_df['date'] = pd.to_datetime(gl_df['date'], utc=True, errors='coerce')
        elif 'operating_date' in gl_df.columns:
            gl_df['date'] = pd.to_datetime(gl_df['operating_date'], utc=True, errors='coerce')
        
        # Ensure numeric columns are properly formatted
        for col in ['debit_crypto', 'credit_crypto', 'debit_USD', 'credit_USD']:
            if col in gl_df.columns:
                gl_df[col] = pd.to_numeric(gl_df[col], errors='coerce').fillna(0)
        
        # CRITICAL: Use account mapper to connect GL account names to COA
        if 'account_name' in gl_df.columns:
            print("DEBUG - Enriching GL data with COA mappings...")
            from ...account_mapper import enrich_gl_with_coa_mapping, debug_account_mappings
            
            # Debug current mappings
            debug_account_mappings(gl_df, show_unmapped=True, show_mapped=True)
            
            # Enrich with COA mappings
            gl_df = enrich_gl_with_coa_mapping(gl_df)
        
        # Debug sample data
        if not gl_df.empty:
            print(f"DEBUG - Date range: {gl_df['date'].min()} to {gl_df['date'].max()}")
            print(f"DEBUG - Sample account names: {gl_df['account_name'].unique()[:5].tolist() if 'account_name' in gl_df.columns else 'No account_name column'}")
            
            # Debug mapping results
            if 'GL_Acct_Number' in gl_df.columns:
                mapped_count = gl_df['GL_Acct_Number'].notna().sum()
                print(f"DEBUG - GL records with mapped account numbers: {mapped_count}/{len(gl_df)}")
                
                # Show sample of mapped income accounts
                income_mask = gl_df['GL_Acct_Number'].astype(str).str.startswith('4', na=False) | gl_df['GL_Acct_Number'].astype(str).str.startswith('9', na=False)
                income_accounts = gl_df[income_mask]
                if not income_accounts.empty:
                    print(f"DEBUG - Found {len(income_accounts)} income account records")
                    sample_income = income_accounts[['account_name', 'GL_Acct_Number', 'GL_Acct_Name']].drop_duplicates().head(3)
                    print(f"DEBUG - Sample income mappings: {sample_income.to_dict('records')}")
        
        return gl_df
    
    @reactive.calc
    def report_date():
        """Get selected report date from financial reporting date input"""
        # Use the financial reporting specific date input
        if hasattr(input, 'fr_report_date') and input.fr_report_date():
            from datetime import datetime
            date_val = input.fr_report_date()
            if isinstance(date_val, str):
                return datetime.strptime(date_val, '%Y-%m-%d')
            return date_val
        # Fallback to main report date
        elif selected_report_date and hasattr(selected_report_date, '__call__'):
            return selected_report_date()
        return datetime(2024, 7, 31)  # Default to end of available data
    
    @reactive.calc
    def report_currency():
        """Get selected currency"""
        if hasattr(input, 'fr_currency') and input.fr_currency():
            return input.fr_currency()
        return "ETH"
    
    # Register outputs for each module
    as_register_outputs(output, input, gl_data, report_date)
    nc_register_outputs(output, input, gl_data, report_date)
    tb_register_outputs(output, input, gl_data, report_date)
    al_register_outputs(output, input, gl_data, report_date)
    oe_register_outputs(output, input, gl_data, report_date)
    mf_register_outputs(output, input, gl_data, report_date, selected_fund)
    
    @output
    @render.download(filename=lambda: f"financial_report_{report_date().strftime('%Y%m%d')}_{selected_fund() if selected_fund and hasattr(selected_fund, '__call__') else 'fund_i'}.xlsx")
    def download_excel_report():
        """Download complete Excel report"""
        df = gl_data()
        date = report_date()
        currency = report_currency()
        
        # Get fund information
        fund_id = selected_fund() if selected_fund and hasattr(selected_fund, '__call__') else "fund_i_class_B_ETH"
        
        # Map fund ID to proper display name (matching reference implementation)
        fund_name_lookup = {
            "fund_i_class_B_ETH": "ETH Lending Fund I, LP",
            "fund_ii_class_B_ETH": "ETH Lending Fund II, LP",
            "holdings_class_B_ETH": "Drip Capital Holdings, LLC"
        }
        fund_name = fund_name_lookup.get(fund_id, "ETH Lending Fund I, LP")
        
        print(f"DEBUG - Excel download: Fund ID: {fund_id}, Fund Name: {fund_name}")
        print(f"DEBUG - Excel download: GL data shape: {df.shape if not df.empty else 'empty'}")
        print(f"DEBUG - Excel download: Date: {date}, Currency: {currency}")
        
        if df.empty:
            print("DEBUG - Excel download: No GL data, creating empty workbook")
            # Return empty workbook if no data
            from openpyxl import Workbook
            from io import BytesIO
            wb = Workbook()
            wb.active.title = "No Data"
            wb.active["A1"] = f"No GL data available for {fund_name} on {date.strftime('%Y-%m-%d')}"
            output = BytesIO()
            wb.save(output)
            output.seek(0)
            print(f"DEBUG - Excel download: No data case returning BytesIO object: {type(output)}")
            return output
        
        try:
            print("DEBUG - Excel download: Generating Excel report...")
            print(f"DEBUG - Excel download: About to call generate_excel_report with parameters:")
            print(f"DEBUG - Excel download: - fund_name: {fund_name} (type: {type(fund_name)})")
            print(f"DEBUG - Excel download: - date: {date} (type: {type(date)})")
            print(f"DEBUG - Excel download: - currency: {currency} (type: {type(currency)})")
            print(f"DEBUG - Excel download: - fund_id: {fund_id} (type: {type(fund_id)})")
            
            excel_file = generate_excel_report(df, fund_name, date, currency, fund_id)
            
            print(f"DEBUG - Excel download: generate_excel_report returned: {type(excel_file)}")
            print(f"DEBUG - Excel download: excel_file has getvalue: {hasattr(excel_file, 'getvalue')}")
            
            # Try returning the BytesIO object directly like CSV downloads do
            print("DEBUG - Excel download: Returning BytesIO object directly")
            return excel_file
                
        except Exception as e:
            print(f"ERROR - Excel download: Exception in try block: {e}")
            print(f"ERROR - Excel download: Exception type: {type(e)}")
            import traceback
            traceback.print_exc()
            
            # Create error workbook and return BytesIO object directly
            from openpyxl import Workbook
            from io import BytesIO
            wb = Workbook()
            wb.active.title = "Error"
            wb.active["A1"] = f"Error generating report: {str(e)}"
            error_output = BytesIO()
            wb.save(error_output)
            error_output.seek(0)
            
            print(f"DEBUG - Excel download: Error fallback returning BytesIO object: {type(error_output)}")
            
            return error_output


def financial_reporting_ui():
    """Financial Reporting Interface"""
    return shiny_ui.page_fluid(
        shiny_ui.card(
            shiny_ui.card_header(
                shiny_ui.row(
                    shiny_ui.column(6, shiny_ui.h2("Financial Reporting")),
                    shiny_ui.column(6, 
                        # Controls in a simple row layout
                        shiny_ui.row(
                            shiny_ui.column(4, 
                                shiny_ui.input_date(
                                    "fr_report_date",
                                    "Report Date",
                                    value="2024-07-31",
                                    min="2024-07-01",
                                    max="2024-12-31"
                                )
                            ),
                            shiny_ui.column(3, 
                                shiny_ui.input_select(
                                    "fr_currency",
                                    "Currency",
                                    choices={"ETH": "ETH (Ethereum)", "USD": "USD (US Dollar)"},
                                    selected="ETH"
                                )
                            ),
                            shiny_ui.column(5, 
                                shiny_ui.download_button(
                                    "download_excel_report", 
                                    "Download Excel Report"
                                )
                            )
                        )
                    )
                )
            ),
            
            # Simple navigation tabs 
            shiny_ui.input_radio_buttons(
                "report_view",
                label=None,
                choices={
                    "account_stmt": "Account Statement",
                    "nav_changes": "Changes in NAV", 
                    "trial_balance": "Trial Balance",
                    "balance_sheet": "Assets & Liabilities",
                    "operating_exp": "Operating Expenses",
                    "mgmt_fee": "Management Fee"
                },
                selected="account_stmt",
                inline=True
            ),
            
            # Content areas
            shiny_ui.panel_conditional(
                "input.report_view === 'account_stmt'",
                shiny_ui.div(
                    shiny_ui.h4("Statement of Operations", class_="mb-2"),
                    shiny_ui.p("Comprehensive view of revenues and expenses with period comparisons.", 
                             class_="text-muted mb-4"),
                    account_statement_ui()
                )
            ),
            
            shiny_ui.panel_conditional(
                "input.report_view === 'nav_changes'",
                shiny_ui.div(
                    shiny_ui.h4("Statement of Changes in Net Asset Value", class_="mb-2"),
                    shiny_ui.p("Detailed breakdown of capital contributions, distributions, and net income impact on fund NAV.", 
                             class_="text-muted mb-4"),
                    nav_changes_ui()
                )
            ),
            
            shiny_ui.panel_conditional(
                "input.report_view === 'trial_balance'",
                shiny_ui.div(
                    shiny_ui.h4("Trial Balance Report", class_="mb-2"),
                    shiny_ui.p("Complete listing of all general ledger account balances with period-over-period comparisons.", 
                             class_="text-muted mb-4"),
                    trial_balance_ui()
                )
            ),
            
            shiny_ui.panel_conditional(
                "input.report_view === 'balance_sheet'",
                shiny_ui.div(
                    shiny_ui.h4("Statement of Assets and Liabilities", class_="mb-2"),
                    shiny_ui.p("Balance sheet presentation with provision netting and partners' capital calculations.", 
                             class_="text-muted mb-4"),
                    assets_liabilities_ui()
                )
            ),
            
            shiny_ui.panel_conditional(
                "input.report_view === 'operating_exp'",
                shiny_ui.div(
                    shiny_ui.h4("Operating Expense Schedule", class_="mb-2"),
                    shiny_ui.p("Detailed expense tracking with accrual accounting, payments, and year-to-date summaries.", 
                             class_="text-muted mb-4"),
                    operating_expenses_ui()
                )
            ),
            
            shiny_ui.panel_conditional(
                "input.report_view === 'mgmt_fee'",
                shiny_ui.div(
                    shiny_ui.h4("Management Fee Calculation", class_="mb-2"),
                    shiny_ui.p("Limited partner management fee calculations based on commitment amounts and fee rates.", 
                             class_="text-muted mb-4"),
                    management_fee_ui()
                )
            )
        )
    )
