from shiny import ui as shiny_ui, render, reactive
import pandas as pd
from datetime import datetime, timedelta
from ...s3_utils import load_GL_file, load_COA_file
from shiny.render import DataGrid, data_frame
import json
import os
import tempfile
from jinja2 import Environment, BaseLoader
try:
    from weasyprint import HTML
    HAS_WEASYPRINT = True
except ImportError:
    HAS_WEASYPRINT = False
import io

def trial_balance_ui():
    return shiny_ui.page_fluid(
        # Enhanced CSS for better table styling with working frozen columns
        shiny_ui.tags.style("""
            .trial-balance-container {
                position: relative;
                width: 100%;
                overflow: auto;
                border: 1px solid #dee2e6;
                border-radius: 0.375rem;
                max-height: 600px;
            }
            
            /* DataGrid specific styling for frozen columns */
            .trial-balance-container .shiny-data-grid {
                width: 100%;
                min-width: 100%;
            }
            
            .trial-balance-container .shiny-data-grid table {
                width: 100%;
                border-collapse: separate;
                border-spacing: 0;
            }
            
            /* Freeze first two columns with improved approach */
            .trial-balance-container .shiny-data-grid table thead th:nth-child(1),
            .trial-balance-container .shiny-data-grid table thead th:nth-child(2),
            .trial-balance-container .shiny-data-grid table tbody td:nth-child(1),
            .trial-balance-container .shiny-data-grid table tbody td:nth-child(2) {
                position: sticky;
                background-color: #f8f9fa;
                z-index: 10;
                border-right: 2px solid #dee2e6;
                box-shadow: 2px 0 5px rgba(0,0,0,0.1);
            }
            
            .trial-balance-container .shiny-data-grid table thead th:nth-child(1),
            .trial-balance-container .shiny-data-grid table tbody td:nth-child(1) {
                left: 0;
                min-width: 120px;
                max-width: 120px;
                width: 120px;
            }
            
            .trial-balance-container .shiny-data-grid table thead th:nth-child(2),
            .trial-balance-container .shiny-data-grid table tbody td:nth-child(2) {
                left: 120px;
                min-width: 200px;
                max-width: 200px;
                width: 200px;
            }
            
            /* Header styling */
            .trial-balance-container .shiny-data-grid table thead th {
                position: sticky;
                top: 0;
                background-color: #f8f9fa;
                z-index: 9;
                font-weight: bold;
                text-align: center;
                padding: 8px;
                border-bottom: 2px solid #dee2e6;
            }
            
            /* Ensure frozen column headers have higher z-index */
            .trial-balance-container .shiny-data-grid table thead th:nth-child(1),
            .trial-balance-container .shiny-data-grid table thead th:nth-child(2) {
                z-index: 11;
            }
            
            /* Date column styling */
            .trial-balance-container .shiny-data-grid table thead th:nth-child(n+3),
            .trial-balance-container .shiny-data-grid table tbody td:nth-child(n+3) {
                min-width: 90px;
                text-align: right;
                padding: 4px 8px;
            }
            
            /* Row hover effects */
            .trial-balance-container .shiny-data-grid table tbody tr:hover {
                background-color: #e3f2fd;
            }
            
            /* Selected row styling */
            .trial-balance-container .shiny-data-grid table tbody tr.selected {
                background-color: #2196f3 !important;
                color: white !important;
            }
            
            /* NET row styling - make it stand out */
            .trial-balance-container .shiny-data-grid table tbody tr:last-child {
                background-color: #fff3cd !important;
                border-top: 3px solid #ffc107 !important;
                font-weight: bold !important;
            }
            
            .trial-balance-container .shiny-data-grid table tbody tr:last-child td {
                background-color: #fff3cd !important;
                font-weight: bold !important;
                border-top: 3px solid #ffc107 !important;
            }
            
            /* NET row frozen columns */
            .trial-balance-container .shiny-data-grid table tbody tr:last-child td:nth-child(1),
            .trial-balance-container .shiny-data-grid table tbody tr:last-child td:nth-child(2) {
                background-color: #fff3cd !important;
                z-index: 10;
                border-right: 2px solid #dee2e6;
                box-shadow: 2px 0 5px rgba(0,0,0,0.1);
            }
        """),
        
        shiny_ui.card(
            shiny_ui.card_header("📊 Trial Balance Generator"),
            
            shiny_ui.card_body(
                shiny_ui.layout_columns(
                    shiny_ui.div(
                        shiny_ui.h6("Fund Selection", class_="mb-3"),
                        shiny_ui.output_ui("tb_fund_selector"),
                    ),
                    shiny_ui.div(
                        shiny_ui.h6("Date Range Selection", class_="mb-3"),
                        shiny_ui.input_date(
                            "tb_start_date",
                            "Start Date:",
                            value=datetime(2024, 7, 1),
                            min="2024-01-01",
                            max="2024-12-31",
                            width="100%"
                        ),
                    ),
                    shiny_ui.div(
                        shiny_ui.br(),
                        shiny_ui.input_date(
                            "tb_end_date", 
                            "End Date:",
                            value=datetime(2024, 7, 31),
                            min="2024-01-01", 
                            max="2024-12-31",
                            width="100%"
                        ),
                    ),
                    shiny_ui.div(
                        shiny_ui.br(),
                        shiny_ui.input_action_button(
                            "generate_tb",
                            "Generate Trial Balance",
                            class_="btn-primary w-100"
                        ),
                    ),
                    col_widths=[3, 3, 3, 3]
                ),
                shiny_ui.hr(),
                
                # Unbalanced Days Alert (appears if any days don't balance)
                shiny_ui.output_ui("unbalanced_days_alert"),
                
                # Trial Balance Table with frozen columns
                shiny_ui.div(
                    shiny_ui.output_data_frame("fund_trial_balance_table"),
                    class_="trial-balance-container"
                ),
                
                shiny_ui.hr(),
                
                # Account Detail Section (appears when row selected)
                shiny_ui.output_ui("account_detail_section"),
                
                shiny_ui.hr(),
                
                # Export Options
                shiny_ui.card(
                    shiny_ui.card_header("📄 Export Trial Balance"),
                    shiny_ui.card_body(
                        shiny_ui.layout_columns(
                            shiny_ui.div(
                                shiny_ui.download_button(
                                    "download_tb_csv",
                                    "📊 Download CSV",
                                    class_="btn-success w-100"
                                ),
                            ),
                            shiny_ui.div(
                                shiny_ui.download_button(
                                    "download_tb_pdf",
                                    "📋 Download PDF",
                                    class_="btn-primary w-100"
                                ),
                            ),
                            col_widths=[6, 6]
                        ),
                        shiny_ui.div(
                            shiny_ui.output_ui("tb_export_status"),
                            class_="mt-3"
                        )
                    )
                )
            )
        )
    )

def register_outputs(output, input, selected_fund):
    
    # Reactive values for selected account
    selected_account = reactive.value(None)
    
    @output
    @render.ui
    def tb_fund_selector():
        """Render fund selector for trial balance"""
        print(f"DEBUG - TRIAL BALANCE FUND SELECTOR: Loading fund selector...")
        # Get available funds from GL data
        try:
            gl_df = load_GL_file()
            print(f"DEBUG - TRIAL BALANCE FUND SELECTOR: GL data shape: {gl_df.shape if not gl_df.empty else 'EMPTY'}")
            
            if gl_df.empty:
                print(f"DEBUG - TRIAL BALANCE FUND SELECTOR: GL data is empty!")
                return shiny_ui.input_select(
                    "tb_selected_fund",
                    "Fund:",
                    choices={"": "No funds available"},
                    selected="",
                    width="100%"
                )
            
            print(f"DEBUG - TRIAL BALANCE FUND SELECTOR: GL columns: {list(gl_df.columns)}")
            
            # Get unique funds from GL data - prioritize fund_id over fund
            if 'fund_id' in gl_df.columns:
                print(f"DEBUG - TRIAL BALANCE FUND SELECTOR: Found 'fund_id' column")
                unique_fund_ids = sorted(gl_df['fund_id'].dropna().unique())
                print(f"DEBUG - TRIAL BALANCE FUND SELECTOR: Unique fund_ids: {unique_fund_ids}")
                fund_choices = {fund_id: fund_id for fund_id in unique_fund_ids}
                if not fund_choices:
                    fund_choices = {"": "No fund_ids found"}
            elif 'fund' in gl_df.columns:
                print(f"DEBUG - TRIAL BALANCE FUND SELECTOR: Found 'fund' column")
                unique_funds = sorted(gl_df['fund'].dropna().unique())
                print(f"DEBUG - TRIAL BALANCE FUND SELECTOR: Unique funds: {unique_funds}")
                fund_choices = {fund: fund for fund in unique_funds}
                if not fund_choices:
                    fund_choices = {"": "No funds found"}
            else:
                print(f"DEBUG - TRIAL BALANCE FUND SELECTOR: No 'fund' or 'fund_id' column found")
                fund_choices = {"ALL": "All Funds"}
            
            print(f"DEBUG - TRIAL BALANCE FUND SELECTOR: Final fund choices: {fund_choices}")
            
            # Set default selection
            default_fund = list(fund_choices.keys())[0] if fund_choices else ""
            print(f"DEBUG - TRIAL BALANCE FUND SELECTOR: Default fund: {default_fund}")
            
            return shiny_ui.input_select(
                "tb_selected_fund",
                "Fund:",
                choices=fund_choices,
                selected=default_fund,
                width="100%"
            )
            
        except Exception as e:
            print(f"Error in tb_fund_selector: {e}")
            return shiny_ui.input_select(
                "tb_selected_fund",
                "Fund:",
                choices={"": "Error loading funds"},
                selected="",
                width="100%"
            )
    
    @reactive.calc
    def get_trial_balance_data():
        """Generate trial balance data from GL transactions"""
        print(f"DEBUG - TRIAL BALANCE: Starting get_trial_balance_data()")
        try:
            print(f"DEBUG - TRIAL BALANCE: Loading GL and COA data...")
            # Load data
            gl_df = load_GL_file()
            coa_df = load_COA_file()
            
            print(f"DEBUG - TRIAL BALANCE: GL data shape: {gl_df.shape if not gl_df.empty else 'EMPTY'}")
            print(f"DEBUG - TRIAL BALANCE: COA data shape: {coa_df.shape if not coa_df.empty else 'EMPTY'}")
            
            if gl_df.empty or coa_df.empty:
                print(f"DEBUG - TRIAL BALANCE: ERROR - GL or COA data is empty!")
                return {"data": pd.DataFrame({"Error": ["No data available"]}), "unbalanced_days": []}
            
            # Get inputs
            print(f"DEBUG - TRIAL BALANCE: Getting input values...")
            try:
                start_date = input.tb_start_date()
                print(f"DEBUG - TRIAL BALANCE: Start date: {start_date}")
            except Exception as e:
                print(f"DEBUG - TRIAL BALANCE: Error getting start date: {e}")
                start_date = None
                
            try:
                end_date = input.tb_end_date()
                print(f"DEBUG - TRIAL BALANCE: End date: {end_date}")
            except Exception as e:
                print(f"DEBUG - TRIAL BALANCE: Error getting end date: {e}")
                end_date = None
                
            try:
                selected_fund = input.tb_selected_fund()
                print(f"DEBUG - TRIAL BALANCE: Selected fund: {selected_fund}")
            except Exception as e:
                print(f"DEBUG - TRIAL BALANCE: Error getting selected fund: {e}")
                selected_fund = None
            
            if start_date is None or end_date is None:
                print(f"DEBUG - TRIAL BALANCE: Missing date inputs - start: {start_date}, end: {end_date}")
                return {"data": pd.DataFrame({"Message": ["Please select date range and click Generate"]}), "unbalanced_days": []}
            
            # Convert to datetime for processing
            print(f"DEBUG - TRIAL BALANCE: Converting dates to datetime...")
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)
            print(f"DEBUG - TRIAL BALANCE: Converted dates - start_dt: {start_dt}, end_dt: {end_dt}")
            
            if start_dt > end_dt:
                print(f"DEBUG - TRIAL BALANCE: ERROR - Start date after end date!")
                return {"data": pd.DataFrame({"Error": ["Start date must be before end date"]}), "unbalanced_days": []}
            
            # Filter GL data for date range with timezone-aware handling
            print(f"DEBUG - TRIAL BALANCE: Processing GL date column...")
            gl_df['date'] = pd.to_datetime(gl_df['date'], errors='coerce')
            print(f"DEBUG - TRIAL BALANCE: GL date range: {gl_df['date'].min()} to {gl_df['date'].max()}")
            
            # Convert comparison dates to match GL data timezone
            if gl_df['date'].dt.tz is not None:
                # GL data is timezone-aware (UTC), convert our dates to UTC
                print(f"DEBUG - TRIAL BALANCE: Converting dates to UTC to match GL data timezone")
                start_dt = pd.Timestamp(start_dt, tz='UTC')
                end_dt = pd.Timestamp(end_dt, tz='UTC')
                print(f"DEBUG - TRIAL BALANCE: UTC dates - start_dt: {start_dt}, end_dt: {end_dt}")
            
            # Filter by date range
            print(f"DEBUG - TRIAL BALANCE: Filtering GL data by date range...")
            print(f"DEBUG - TRIAL BALANCE: GL data shape before date filter: {gl_df.shape}")
            gl_filtered = gl_df[
                (gl_df['date'] >= start_dt) & 
                (gl_df['date'] <= end_dt)
            ].copy()
            print(f"DEBUG - TRIAL BALANCE: GL data shape after date filter: {gl_filtered.shape}")
            
            # Filter by fund if specified
            print(f"DEBUG - TRIAL BALANCE: Checking fund filter - selected_fund: '{selected_fund}'")
            print(f"DEBUG - TRIAL BALANCE: GL columns: {list(gl_filtered.columns)}")
            if selected_fund and selected_fund != "ALL" and selected_fund != "":
                if 'fund' in gl_filtered.columns:
                    print(f"DEBUG - TRIAL BALANCE: Filtering by fund: {selected_fund}")
                    unique_funds = gl_filtered['fund'].unique()
                    print(f"DEBUG - TRIAL BALANCE: Available funds in GL data: {unique_funds}")
                    gl_filtered = gl_filtered[gl_filtered['fund'] == selected_fund]
                    print(f"DEBUG - TRIAL BALANCE: GL data shape after fund filter: {gl_filtered.shape}")
                elif 'fund_id' in gl_filtered.columns:
                    print(f"DEBUG - TRIAL BALANCE: Using 'fund_id' column for filtering")
                    unique_fund_ids = gl_filtered['fund_id'].unique()
                    print(f"DEBUG - TRIAL BALANCE: Available fund_ids: {unique_fund_ids}")
                    print(f"DEBUG - TRIAL BALANCE: Filtering by fund_id: {selected_fund}")
                    gl_filtered = gl_filtered[gl_filtered['fund_id'] == selected_fund]
                    print(f"DEBUG - TRIAL BALANCE: GL data shape after fund_id filter: {gl_filtered.shape}")
                else:
                    print(f"DEBUG - TRIAL BALANCE: WARNING - No 'fund' or 'fund_id' column in GL data")
            else:
                print(f"DEBUG - TRIAL BALANCE: No fund filter applied (using all funds)")
            
            if gl_filtered.empty:
                print(f"DEBUG - TRIAL BALANCE: ERROR - No GL transactions found after filtering!")
                return {"data": pd.DataFrame({"Message": ["No GL transactions found in selected date range"]}), "unbalanced_days": []}
            
            # Generate date range columns
            print(f"DEBUG - TRIAL BALANCE: Generating date range columns...")
            date_range = pd.date_range(start=start_dt, end=end_dt, freq='D')
            date_columns = [dt.strftime('%Y-%m-%d') for dt in date_range]
            print(f"DEBUG - TRIAL BALANCE: Date columns ({len(date_columns)}): {date_columns[:5]}...{date_columns[-5:] if len(date_columns) > 5 else ''}")
            
            # Get all accounts from COA that have activity
            print(f"DEBUG - TRIAL BALANCE: Getting active accounts from COA...")
            active_accounts = set(gl_filtered['account_name'].unique())
            print(f"DEBUG - TRIAL BALANCE: Active accounts ({len(active_accounts)}): {list(active_accounts)[:5]}...")
            
            print(f"DEBUG - TRIAL BALANCE: Checking COA for matching accounts...")
            print(f"DEBUG - TRIAL BALANCE: COA account_name column available: {'account_name' in coa_df.columns}")
            if 'account_name' in coa_df.columns:
                print(f"DEBUG - TRIAL BALANCE: COA account names: {list(coa_df['account_name'].unique())[:5]}...")
            
            trial_balance_accounts = coa_df[coa_df['account_name'].isin(active_accounts)].copy()
            print(f"DEBUG - TRIAL BALANCE: Trial balance accounts found in COA: {len(trial_balance_accounts)}")
            
            if trial_balance_accounts.empty:
                print(f"DEBUG - TRIAL BALANCE: ERROR - No matching accounts found in COA!")
                return {"data": pd.DataFrame({"Message": ["No matching accounts found in COA"]}), "unbalanced_days": []}
            
            # Initialize trial balance structure
            print(f"DEBUG - TRIAL BALANCE: Initializing trial balance structure...")
            tb_data = []
            
            # Track daily totals for balance checking
            daily_debits = {date_str: 0.0 for date_str in date_columns}
            daily_credits = {date_str: 0.0 for date_str in date_columns}
            print(f"DEBUG - TRIAL BALANCE: Processing {len(trial_balance_accounts)} accounts...")
            
            account_count = 0
            for _, coa_row in trial_balance_accounts.iterrows():
                account_count += 1
                account_name = coa_row['account_name']
                gl_acct_number = coa_row['GL_Acct_Number']
                
                if account_count <= 3:  # Only debug first 3 accounts to avoid spam
                    print(f"DEBUG - TRIAL BALANCE: Processing account {account_count}: '{account_name}' ({gl_acct_number})")
                
                # Get transactions for this account
                account_transactions = gl_filtered[gl_filtered['account_name'] == account_name]
                
                if account_count <= 3:
                    print(f"DEBUG - TRIAL BALANCE: Account '{account_name}' has {len(account_transactions)} transactions")
                
                if account_transactions.empty:
                    if account_count <= 3:
                        print(f"DEBUG - TRIAL BALANCE: Skipping account '{account_name}' - no transactions")
                    continue
                
                # Calculate running balance for each date
                row_data = {
                    'GL_Acct_Number': gl_acct_number,
                    'account_name': account_name
                }
                
                running_balance = 0
                has_activity = False
                
                for date_str in date_columns:
                    date_dt = pd.to_datetime(date_str)
                    
                    # Convert date to match GL data timezone
                    if account_transactions['date'].dt.tz is not None:
                        date_dt = pd.Timestamp(date_dt, tz='UTC')
                    
                    # Get transactions up to this date
                    transactions_to_date = account_transactions[
                        account_transactions['date'] <= date_dt
                    ]
                    
                    # Get transactions just for this specific date for daily totals
                    transactions_this_date = account_transactions[
                        account_transactions['date'].dt.date == date_dt.date()
                    ]
                    
                    if transactions_to_date.empty:
                        row_data[date_str] = 0.0
                    else:
                        # Calculate cumulative balance using proper accounting principles
                        total_debits = pd.to_numeric(transactions_to_date['debit_crypto'], errors='coerce').fillna(0).sum()
                        total_credits = pd.to_numeric(transactions_to_date['credit_crypto'], errors='coerce').fillna(0).sum()
                        
                        # Determine normal balance based on account type
                        first_digit = str(gl_acct_number)[0]
                        if first_digit in ['1', '8']:  # Assets, Expenses
                            balance = total_debits - total_credits
                        else:  # Liabilities, Equity, Income
                            balance = total_credits - total_debits
                        
                        row_data[date_str] = round(balance, 6)
                        
                        if abs(balance) > 0.0001:
                            has_activity = True
                    
                    # Track daily debits and credits for this specific date
                    if not transactions_this_date.empty:
                        day_debits = pd.to_numeric(transactions_this_date['debit_crypto'], errors='coerce').fillna(0).sum()
                        day_credits = pd.to_numeric(transactions_this_date['credit_crypto'], errors='coerce').fillna(0).sum()
                        daily_debits[date_str] += day_debits
                        daily_credits[date_str] += day_credits
                
                # Only include accounts with meaningful activity
                if has_activity:
                    tb_data.append(row_data)
            
            print(f"DEBUG - TRIAL BALANCE: Finished processing accounts. Total accounts with activity: {len(tb_data)}")
            
            if not tb_data:
                print(f"DEBUG - TRIAL BALANCE: ERROR - No accounts with activity in selected period!")
                return {"data": pd.DataFrame({"Message": ["No accounts with activity in selected period"]}), "unbalanced_days": []}
            
            # Create DataFrame and sort by account number
            print(f"DEBUG - TRIAL BALANCE: Creating final DataFrame...")
            tb_df = pd.DataFrame(tb_data)
            print(f"DEBUG - TRIAL BALANCE: DataFrame created with shape: {tb_df.shape}")
            tb_df = tb_df.sort_values('GL_Acct_Number')
            print(f"DEBUG - TRIAL BALANCE: DataFrame sorted by GL_Acct_Number")
            
            # Calculate net debit/credit row and check for unbalanced days
            net_row = {
                'GL_Acct_Number': '*** NET ***',
                'account_name': 'Daily Net (Debits - Credits)'
            }
            
            unbalanced_days = []
            BALANCE_THRESHOLD = 0.0001
            
            for date_str in date_columns:
                daily_net = daily_debits[date_str] - daily_credits[date_str]
                net_row[date_str] = round(daily_net, 6)
                
                # Check if day is balanced within threshold
                if abs(daily_net) > BALANCE_THRESHOLD:
                    unbalanced_days.append({
                        'date': date_str,
                        'net_amount': daily_net,
                        'debits': daily_debits[date_str],
                        'credits': daily_credits[date_str]
                    })
            
            # Add net row to DataFrame
            print(f"DEBUG - TRIAL BALANCE: Adding NET row to DataFrame...")
            tb_df = pd.concat([tb_df, pd.DataFrame([net_row])], ignore_index=True)
            print(f"DEBUG - TRIAL BALANCE: Final DataFrame shape with NET row: {tb_df.shape}")
            print(f"DEBUG - TRIAL BALANCE: Final DataFrame columns: {list(tb_df.columns)}")
            print(f"DEBUG - TRIAL BALANCE: Unbalanced days: {len(unbalanced_days)}")
            
            print(f"DEBUG - TRIAL BALANCE: Returning final result...")
            return {"data": tb_df, "unbalanced_days": unbalanced_days}
            
        except Exception as e:
            print(f"Error in get_trial_balance_data: {e}")
            import traceback
            traceback.print_exc()
            return {"data": pd.DataFrame({"Error": [f"Error generating trial balance: {str(e)}"]}), "unbalanced_days": []}
    
    @output
    @data_frame
    def fund_trial_balance_table():
        """Render the trial balance table with frozen columns using DataGrid"""
        
        print(f"DEBUG - TRIAL BALANCE TABLE: ========== STARTING RENDER FUNCTION ==========")
        print(f"DEBUG - TRIAL BALANCE TABLE: Function called successfully")
        
        try:
            # Only generate when button is clicked
            print(f"DEBUG - TRIAL BALANCE TABLE: Getting button click count...")
            button_count = input.generate_tb()
            print(f"DEBUG - TRIAL BALANCE TABLE: Button click count: {button_count}")
            
            if button_count == 0:
                print(f"DEBUG - TRIAL BALANCE TABLE: Button not clicked yet, returning message DataFrame")
                return pd.DataFrame({"Message": ["Click 'Generate Trial Balance' to create report"]})
        except Exception as e:
            print(f"DEBUG - TRIAL BALANCE TABLE: ERROR getting button count: {e}")
            return pd.DataFrame({"Error": [f"Error getting button state: {str(e)}"]})
        
        print(f"DEBUG - TRIAL BALANCE TABLE: Button clicked, getting trial balance data...")
        try:
            tb_result = get_trial_balance_data()
            print(f"DEBUG - TRIAL BALANCE TABLE: Got tb_result: {type(tb_result)}")
            print(f"DEBUG - TRIAL BALANCE TABLE: tb_result keys: {list(tb_result.keys()) if isinstance(tb_result, dict) else 'Not a dict'}")
            
            tb_df = tb_result["data"]
            print(f"DEBUG - TRIAL BALANCE TABLE: tb_df shape: {tb_df.shape if not tb_df.empty else 'EMPTY'}")
            print(f"DEBUG - TRIAL BALANCE TABLE: tb_df columns: {list(tb_df.columns) if not tb_df.empty else 'No columns'}")
            
            if tb_df.empty or 'Error' in tb_df.columns or 'Message' in tb_df.columns:
                print(f"DEBUG - TRIAL BALANCE TABLE: Returning df as-is (empty/error/message)")
                return tb_df
            
            print(f"DEBUG - TRIAL BALANCE TABLE: Creating DataGrid...")
            # Return DataGrid with enhanced styling (frozen columns handled by CSS)
            data_grid = DataGrid(
                tb_df,
                width="100%",
                height="600px",
                filters=True,
                summary=False,
                selection_mode="row"
            )
            print(f"DEBUG - TRIAL BALANCE TABLE: DataGrid created successfully")
            return data_grid
            
        except Exception as e:
            print(f"DEBUG - TRIAL BALANCE TABLE: ERROR in trial_balance_table: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame({"Error": [f"Error rendering table: {str(e)}"]})
        
    
    @reactive.effect
    def handle_row_selection():
        """Handle row selection in trial balance table"""
        try:
            # Check if generate_tb input exists and is ready before accessing
            if not hasattr(input, 'generate_tb'):
                return
            
            try:
                tb_counter = input.generate_tb()
                if tb_counter == 0:
                    return
            except Exception:
                # generate_tb input not ready yet
                return
            
            # Get selected rows from DataGrid - check if input exists first
            if not hasattr(input, 'fund_trial_balance_table_selected_rows'):
                return
                
            try:
                selected_rows = input.fund_trial_balance_table_selected_rows()
            except (AttributeError, Exception):
                # Selected rows input doesn't exist yet or reactive dependency not ready
                return
            
            if selected_rows is None or len(selected_rows) == 0:
                selected_account.set(None)
                return
            
            # Get the trial balance data
            tb_result = get_trial_balance_data()
            tb_df = tb_result["data"]
            
            if tb_df.empty or 'Error' in tb_df.columns or 'Message' in tb_df.columns:
                return
            
            # Get the selected account (skip NET row)
            if len(selected_rows) > 0:
                selected_row_index = selected_rows[0]
                if selected_row_index < len(tb_df):
                    row_data = tb_df.iloc[selected_row_index]
                    # Skip NET row selection
                    if row_data['GL_Acct_Number'] != '*** NET ***':
                        account_info = {
                            'account_name': row_data['account_name'],
                            'gl_acct_number': row_data['GL_Acct_Number']
                        }
                        selected_account.set(account_info)
                    else:
                        selected_account.set(None)
                    
        except Exception as e:
            # Silently handle row selection errors - these are usually reactive dependency issues
            pass
    
    @output
    @render.ui
    def unbalanced_days_alert():
        """Display alert for unbalanced days"""
        # Only show when trial balance is generated
        if input.generate_tb() == 0:
            return shiny_ui.div()
        
        try:
            tb_result = get_trial_balance_data()
            unbalanced_days = tb_result["unbalanced_days"]
            
            if not unbalanced_days:
                return shiny_ui.div(
                    shiny_ui.div(
                        shiny_ui.span("✅ All days are balanced!", class_="text-success fw-bold"),
                        shiny_ui.span(" (All daily debits equal credits within 0.0001 threshold)", class_="text-muted"),
                        class_="alert alert-success"
                    ),
                    class_="mb-3"
                )
            
            # Create unbalanced days display
            unbalanced_items = []
            for day in unbalanced_days:
                unbalanced_items.append(
                    shiny_ui.li(
                        f"{day['date']}: Net {day['net_amount']:,.6f} ETH "
                        f"(Debits: {day['debits']:,.6f}, Credits: {day['credits']:,.6f})",
                        class_="mb-1"
                    )
                )
            
            return shiny_ui.div(
                shiny_ui.div(
                    shiny_ui.h6("⚠️ Unbalanced Days Detected", class_="alert-heading text-warning mb-2"),
                    shiny_ui.p(
                        f"Found {len(unbalanced_days)} day(s) where debits don't equal credits (threshold: 0.0001 ETH):",
                        class_="mb-2"
                    ),
                    shiny_ui.ul(unbalanced_items, class_="mb-0"),
                    class_="alert alert-warning"
                ),
                class_="mb-3"
            )
            
        except Exception as e:
            print(f"Error in unbalanced_days_alert: {e}")
            return shiny_ui.div()
    
    @output
    @render.ui
    def account_detail_section():
        """Render account detail section when account is selected"""
        account_info = selected_account.get()
        
        if account_info is None:
            return shiny_ui.div(
                shiny_ui.p("Select an account row above to view detailed GL entries", 
                          class_="text-muted text-center"),
                style="padding: 20px;"
            )
        
        return shiny_ui.card(
            shiny_ui.card_header(
                f"📋 GL Entries for {account_info['account_name']} (#{account_info['gl_acct_number']})"
            ),
            shiny_ui.card_body(
                shiny_ui.layout_columns(
                    shiny_ui.input_text(
                        "search_entries",
                        "Search entries:",
                        placeholder="Search by description, amount, etc...",
                        width="100%"
                    ),
                    shiny_ui.input_action_button(
                        "clear_search",
                        "Clear",
                        class_="btn-outline-secondary"
                    ),
                    col_widths=[10, 2]
                ),
                shiny_ui.br(),
                shiny_ui.output_data_frame("account_gl_entries")
            )
        )
    
    @output
    @data_frame  
    def account_gl_entries():
        """Show GL entries for selected account with search functionality"""
        account_info = selected_account.get()
        
        if account_info is None:
            return pd.DataFrame()
        
        try:
            gl_df = load_GL_file()
            
            if gl_df.empty:
                return pd.DataFrame({"Message": ["No GL data available"]})
            
            # Filter for selected account
            account_entries = gl_df[
                gl_df['account_name'] == account_info['account_name']
            ].copy()
            
            if account_entries.empty:
                return pd.DataFrame({"Message": ["No entries found for this account"]})
            
            # Apply search filter if provided
            search_term = input.search_entries()
            if search_term and search_term.strip():
                search_mask = (
                    account_entries.astype(str).apply(
                        lambda x: x.str.contains(search_term, case=False, na=False)
                    ).any(axis=1)
                )
                account_entries = account_entries[search_mask]
            
            # Clear search when button clicked
            if input.clear_search() > 0:
                # This will trigger a re-render without search term
                pass
            
            # Show all available columns
            entries_display = account_entries.copy()
            
            # Sort by date descending
            if 'date' in entries_display.columns:
                entries_display['date'] = pd.to_datetime(entries_display['date'], errors='coerce')
                entries_display = entries_display.sort_values('date', ascending=False)
                entries_display['date'] = entries_display['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Round numeric columns that commonly contain crypto amounts
            numeric_columns = ['debit_crypto', 'credit_crypto', 'debit_usd', 'credit_usd', 'amount', 'balance']
            for col in numeric_columns:
                if col in entries_display.columns:
                    entries_display[col] = pd.to_numeric(entries_display[col], errors='coerce').fillna(0).round(6)
            
            # Format any remaining numeric columns
            for col in entries_display.columns:
                if entries_display[col].dtype in ['float64', 'int64'] and col not in numeric_columns:
                    try:
                        entries_display[col] = pd.to_numeric(entries_display[col], errors='coerce').fillna(0).round(6)
                    except:
                        pass  # Skip if conversion fails
            
            return DataGrid(
                entries_display,
                width="100%",
                height="400px",
                filters=True,
                summary=True,
                selection_mode="none"
            )
            
        except Exception as e:
            print(f"Error in account_gl_entries: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame({"Error": [f"Error loading entries: {str(e)}"]})
    
    @output
    @render.ui
    def tb_export_status():
        """Display export status messages"""
        # Check if trial balance data is available
        if input.generate_tb() == 0:
            return shiny_ui.div(
                shiny_ui.p("⚠️ Please generate trial balance first before downloading", class_="text-warning"),
                class_="alert alert-warning"
            )
        
        tb_result = get_trial_balance_data()
        tb_df = tb_result["data"]
        
        if tb_df.empty or 'Error' in tb_df.columns or 'Message' in tb_df.columns:
            return shiny_ui.div(
                shiny_ui.p("⚠️ No trial balance data available for download", class_="text-warning"),
                class_="alert alert-warning"
            )
        
        return shiny_ui.div(
            shiny_ui.p("✅ Trial balance data ready for download", class_="text-success"),
            class_="alert alert-success"
        )
    
    @render.download(
        filename=lambda: f"Trial_Balance_{input.tb_selected_fund() or 'ALL'}_{input.tb_start_date()}_{input.tb_end_date()}.csv"
    )
    def download_tb_csv():
        """Download trial balance as CSV"""
        try:
            # Check if trial balance data is available
            if input.generate_tb() == 0:
                return io.StringIO("Error: Please generate trial balance first")
            
            tb_result = get_trial_balance_data()
            tb_df = tb_result["data"]
            
            if tb_df.empty or 'Error' in tb_df.columns or 'Message' in tb_df.columns:
                return io.StringIO("Error: No trial balance data available")
            
            # Convert to CSV
            csv_buffer = io.StringIO()
            tb_df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            
            return csv_buffer
            
        except Exception as e:
            error_buffer = io.StringIO()
            error_buffer.write(f"Error generating CSV: {str(e)}")
            error_buffer.seek(0)
            return error_buffer
    
    @render.download(
        filename=lambda: f"Trial_Balance_{input.tb_selected_fund() or 'ALL'}_{input.tb_start_date().strftime('%Y%m%d')}_{input.tb_end_date().strftime('%Y%m%d')}.pdf"
    )
    def download_tb_pdf():
        """Download trial balance as PDF"""
        try:
            # Check if trial balance data is available
            if input.generate_tb() == 0:
                # Return a simple error PDF
                error_html = "<html><body><h1>Error: Please generate trial balance first</h1></body></html>"
                if HAS_WEASYPRINT:
                    pdf_buffer = io.BytesIO()
                    HTML(string=error_html).write_pdf(pdf_buffer)
                    pdf_buffer.seek(0)
                    return pdf_buffer
                else:
                    return io.BytesIO(error_html.encode())
            
            tb_result = get_trial_balance_data()
            tb_df = tb_result["data"]
            unbalanced_days = tb_result["unbalanced_days"]
            
            if tb_df.empty or 'Error' in tb_df.columns or 'Message' in tb_df.columns:
                error_html = "<html><body><h1>Error: No trial balance data available</h1></body></html>"
                if HAS_WEASYPRINT:
                    pdf_buffer = io.BytesIO()
                    HTML(string=error_html).write_pdf(pdf_buffer)
                    pdf_buffer.seek(0)
                    return pdf_buffer
                else:
                    return io.BytesIO(error_html.encode())
            
            # Generate HTML content
            start_date = input.tb_start_date()
            end_date = input.tb_end_date()
            selected_fund = input.tb_selected_fund() or "ALL"
            
            html_content = generate_trial_balance_html(tb_df, unbalanced_days, {
                "fund_name": selected_fund,
                "start_date": start_date.strftime("%B %d, %Y"),
                "end_date": end_date.strftime("%B %d, %Y"),
                "generated_on": datetime.now().strftime("%B %d, %Y at %I:%M %p")
            })
            
            # Generate PDF if weasyprint is available, otherwise return HTML
            if HAS_WEASYPRINT:
                pdf_buffer = io.BytesIO()
                HTML(string=html_content).write_pdf(pdf_buffer)
                pdf_buffer.seek(0)
                return pdf_buffer
            else:
                # Fallback to HTML download if weasyprint not available
                html_buffer = io.BytesIO()
                html_buffer.write(html_content.encode('utf-8'))
                html_buffer.seek(0)
                return html_buffer
            
        except Exception as e:
            error_html = f"<html><body><h1>Error generating PDF: {str(e)}</h1></body></html>"
            if HAS_WEASYPRINT:
                try:
                    pdf_buffer = io.BytesIO()
                    HTML(string=error_html).write_pdf(pdf_buffer)
                    pdf_buffer.seek(0)
                    return pdf_buffer
                except:
                    pass
            
            # Fallback to text error
            error_buffer = io.BytesIO()
            error_buffer.write(error_html.encode())
            error_buffer.seek(0)
            return error_buffer
    


def generate_trial_balance_html(tb_df, unbalanced_days, report_info):
    """Generate beautiful HTML for trial balance PDF with pagination for wide tables"""
    
    # Get date columns
    date_columns = [col for col in tb_df.columns if col not in ['GL_Acct_Number', 'account_name']]
    
    # Split date columns into chunks of 8 for pagination (gives more room for text wrapping)
    DAYS_PER_PAGE = 8
    date_chunks = [date_columns[i:i + DAYS_PER_PAGE] for i in range(0, len(date_columns), DAYS_PER_PAGE)]
    
    # CSS styling based on the PCAP PDF
    css_style = """
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
        
        :root {
            --font-main: 'Inter', sans-serif;
            --text-color: #222;
            --accent-color: #004aad;
            --border-color: #dee2e6;
            --success-color: #28a745;
            --warning-color: #ffc107;
        }
        
        body {
            font-family: var(--font-main);
            color: var(--text-color);
            margin: 0;
            padding: 1.5rem;
            font-size: 8pt;
            background-color: #fff;
            line-height: 1.4;
        }
        
        .container {
            max-width: 100%;
            margin: 0 auto;
        }
        
        header {
            margin-bottom: 1.5rem;
            border-bottom: 2px solid var(--accent-color);
            padding-bottom: 1rem;
        }
        
        .header-container {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
        }
        
        .fund-name {
            font-weight: 700;
            font-size: 18pt;
            color: var(--accent-color);
            margin: 0 0 0.5rem 0;
        }
        
        .report-title {
            font-size: 14pt;
            font-weight: 600;
            margin: 0 0 0.5rem 0;
        }
        
        .report-date {
            font-size: 10pt;
            color: #666;
            margin: 0;
        }
        
        .alert {
            padding: 0.75rem 1rem;
            margin: 1rem 0;
            border-radius: 0.375rem;
            border: 1px solid;
        }
        
        .alert-success {
            background-color: #d4edda;
            border-color: #c3e6cb;
            color: #155724;
        }
        
        .alert-warning {
            background-color: #fff3cd;
            border-color: #ffeaa7;
            color: #856404;
        }
        
        .summary-info {
            background-color: #f8f9fa;
            padding: 1rem;
            border-radius: 0.375rem;
            margin: 1rem 0;
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
        }
        
        .summary-item {
            text-align: center;
        }
        
        .summary-label {
            font-size: 8pt;
            color: #666;
            margin-bottom: 0.25rem;
        }
        
        .summary-value {
            font-size: 12pt;
            font-weight: 600;
            color: var(--accent-color);
        }
        
        .table-wrapper {
            overflow-x: auto;
            margin: 1rem 0;
            border: 1px solid var(--border-color);
            border-radius: 0.375rem;
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 7pt;
            table-layout: fixed;
        }
        
        th, td {
            padding: 0.3rem 0.2rem;
            text-align: left;
            border-bottom: 1px solid var(--border-color);
            vertical-align: top;
        }
        
        th {
            background-color: #f8f9fa;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .account-number {
            font-family: 'Courier New', monospace;
            font-weight: 500;
            width: 80px;
            min-width: 80px;
            max-width: 80px;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        
        .account-name {
            font-weight: 500;
            width: 180px;
            min-width: 180px;
            max-width: 180px;
            white-space: normal;
            word-wrap: break-word;
            overflow-wrap: break-word;
            hyphens: auto;
            line-height: 1.2;
            padding: 0.3rem 0.2rem;
        }
        
        .balance {
            text-align: right;
            font-family: 'Courier New', monospace;
            font-weight: 400;
            width: 70px;
            min-width: 70px;
            max-width: 70px;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            font-size: 6.5pt;
        }
        
        .net-row {
            background-color: #fff3cd !important;
            font-weight: 700 !important;
            border-top: 2px solid var(--warning-color) !important;
        }
        
        .net-row td {
            background-color: #fff3cd !important;
            font-weight: 700 !important;
        }
        
        .date-header {
            text-align: center;
            font-size: 6.5pt;
            width: 70px;
            min-width: 70px;
            max-width: 70px;
            white-space: nowrap;
        }
        
        footer {
            margin-top: 2rem;
            padding-top: 1rem;
            border-top: 1px solid var(--border-color);
            text-align: center;
            font-size: 7pt;
            color: #999;
        }
        
        @page {
            size: A4 landscape;
            margin: 0.5in;
        }
        
        .page-break {
            page-break-before: always;
        }
        
        .page-header {
            margin-bottom: 1rem;
        }
        
        .page-info {
            text-align: right;
            font-size: 8pt;
            color: #666;
            margin-bottom: 1rem;
        }
    </style>
    """
    
    # Generate pages for each date chunk
    pages_html = ""
    
    for page_num, date_chunk in enumerate(date_chunks, 1):
        # Add page break for ALL data pages (since title page is separate)
        page_break = '<div class="page-break"></div>'
        
        # Generate table rows for this page
        table_rows = ""
        for _, row in tb_df.iterrows():
            is_net_row = row['GL_Acct_Number'] == '*** NET ***'
            row_class = ' class="net-row"' if is_net_row else ''
            
            table_rows += f'        <tr{row_class}>\n'
            table_rows += f'            <td class="account-number">{row["GL_Acct_Number"]}</td>\n'
            table_rows += f'            <td class="account-name">{row["account_name"]}</td>\n'
            
            for date_col in date_chunk:
                balance = row[date_col]
                formatted_balance = f"{balance:,.6f}" if isinstance(balance, (int, float)) else str(balance)
                table_rows += f'            <td class="balance">{formatted_balance}</td>\n'
            
            table_rows += '        </tr>\n'
        
        # Generate date headers for this page
        date_headers = ""
        for date_col in date_chunk:
            try:
                date_obj = datetime.strptime(date_col, '%Y-%m-%d')
                formatted_date = date_obj.strftime('%m/%d')
            except:
                formatted_date = date_col
            date_headers += f'            <th class="date-header">{formatted_date}</th>\n'
        
        # Generate page header info
        date_range_str = f"{date_chunk[0]} to {date_chunk[-1]}" if len(date_chunk) > 1 else date_chunk[0]
        
        # Create page HTML
        page_html = f"""
        {page_break}
        <div class="page-info">
            Page {page_num} of {len(date_chunks)} • Date Range: {date_range_str}
        </div>
        
        <div class="table-wrapper">
            <table>
                <thead>
                    <tr>
                        <th class="account-number">Account #</th>
                        <th class="account-name">Account Name</th>
{date_headers}
                    </tr>
                </thead>
                <tbody>
{table_rows}
                </tbody>
            </table>
        </div>
        """
        
        pages_html += page_html
    
    # Generate unbalanced days alert
    if unbalanced_days:
        unbalanced_list = ""
        for day in unbalanced_days:
            unbalanced_list += f"<li>{day['date']}: Net {day['net_amount']:,.6f} ETH</li>"
        
        balance_alert = f"""
        <div class="alert alert-warning">
            <strong>⚠️ Unbalanced Days Detected</strong><br>
            Found {len(unbalanced_days)} day(s) where debits don't equal credits:
            <ul style="margin: 0.5rem 0 0 1rem; padding: 0;">
                {unbalanced_list}
            </ul>
        </div>
        """
    else:
        balance_alert = """
        <div class="alert alert-success">
            <strong>✅ All Days Balanced</strong><br>
            All daily debits equal credits within 0.0001 threshold.
        </div>
        """
    
    # Generate summary info
    total_accounts = len(tb_df) - 1  # Exclude NET row
    date_range_days = len(date_columns)
    total_pages = len(date_chunks)
    
    html_template = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Trial Balance Report</title>
        {css_style}
    </head>
    <body>
        <div class="container">
            <!-- TITLE PAGE -->
            <header>
                <div class="header-container">
                    <div class="text-section">
                        <h1 class="fund-name">{report_info['fund_name']}</h1>
                        <h2 class="report-title">Trial Balance Report</h2>
                        <p class="report-date">Period: {report_info['start_date']} to {report_info['end_date']}</p>
                    </div>
                    <div class="logo-section">
                        <div style="color: var(--accent-color); font-weight: 600; font-size: 12pt;">RealWorldNAV</div>
                        <div style="font-size: 8pt; color: #666;">Financial Reporting</div>
                    </div>
                </div>
            </header>
            
            <div class="summary-info">
                <div class="summary-item">
                    <div class="summary-label">Total Accounts</div>
                    <div class="summary-value">{total_accounts}</div>
                </div>
                <div class="summary-item">
                    <div class="summary-label">Date Range</div>
                    <div class="summary-value">{date_range_days} Days</div>
                </div>
                <div class="summary-item">
                    <div class="summary-label">Data Pages</div>
                    <div class="summary-value">{total_pages}</div>
                </div>
                <div class="summary-item">
                    <div class="summary-label">Generated</div>
                    <div class="summary-value" style="font-size: 8pt;">{report_info['generated_on']}</div>
                </div>
            </div>
            
            {balance_alert}
            
            <!-- Title page description -->
            <div style="margin: 2rem 0; padding: 1.5rem; background-color: #f8f9fa; border-radius: 0.375rem;">
                <h3 style="color: var(--accent-color); margin-bottom: 1rem;">Report Overview</h3>
                <p style="margin: 0.5rem 0;">This trial balance shows running account balances for each day in the selected period.</p>
                <p style="margin: 0.5rem 0;">The report spans {total_pages} data page(s) with up to 8 days per page for optimal readability.</p>
                <p style="margin: 0.5rem 0;">Each account row shows cumulative balance from the start of the period through each date.</p>
                <p style="margin: 0.5rem 0; font-weight: 600;">The "*** NET ***" row at the bottom shows daily net balance (Debits - Credits).</p>
            </div>
            
            <!-- DATA PAGES START HERE -->
            {pages_html}
            
            <footer style="margin-top: 2rem; padding-top: 1rem; border-top: 1px solid var(--border-color); text-align: center; font-size: 7pt; color: #999;">
                <p>Generated by RealWorldNAV • {report_info['generated_on']}</p>
            </footer>
        </div>
    </body>
    </html>
    """
    
    return html_template