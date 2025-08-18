from shiny import reactive, render, ui
import pandas as pd
from datetime import datetime, timedelta

from .modules.fund_accounting import register_outputs as register_fund_accounting_outputs
from .modules.financial_reporting.financial_reporting import register_outputs as register_financial_reporting_outputs
from .modules.general_ledger.general_ledger import register_outputs as register_gl_outputs
from .modules.general_ledger.chart_of_accounts import register_chart_of_accounts_outputs
from .modules.general_ledger.crypto_tracker import register_crypto_tracker_outputs
from .modules.general_ledger.gl_analytics import register_gl_analytics_outputs

from .modules.investments.loan_portfolio import register_outputs as register_loan_portfolio_outputs
from .modules.investments.nft_portfolio import register_nft_portfolio_outputs
from .modules.investments.cryptocurrency_portfolio import register_cryptocurrency_portfolio_outputs
from .modules.investments.dashboard_calculations import create_investment_dashboard_calculations

# Import the enhanced UI functions
from .ui import (
    enhanced_fund_accounting_ui, 
    enhanced_investments_ui, 
    enhanced_general_ledger_ui, 
    enhanced_financial_reporting_ui
)

# Import theme manager
from .theme_manager import theme_manager


def server(input, output, session):

    # Get default values from environment (set by launcher) or use fallbacks
    import os
    default_client = os.environ.get('REALWORLDNAV_CLIENT', 'drip_capital')
    default_fund = os.environ.get('REALWORLDNAV_FUND', 'fund_i_class_B_ETH')
    
    # Client and fund selection reactives
    selected_client = reactive.calc(lambda: input.client() if hasattr(input, 'client') else default_client)
    @reactive.calc
    def selected_fund():
        """Get selected fund with error handling"""
        try:
            if hasattr(input, 'fund'):
                return input.fund()
        except Exception:
            # Input not ready or SilentException
            pass
        return default_fund
    selected_report_date = reactive.calc(lambda: input.report_date() if hasattr(input, 'report_date') else None)
    
    # Navigation state
    current_section = reactive.value("fund_accounting")
    
    # Smart helper function that uses TB when available, falls back to GL
    def calculate_financial_metrics(account_number_ranges, selected_date=None):
        """Calculate financial metrics using TB data when available, GL data as fallback"""
        from .s3_utils import load_tb_file, load_COA_file, load_GL_file
        
        df_coa = load_COA_file()
        if df_coa.empty:
            return {}
        
        # Determine target date
        if selected_date is None:
            target_date = datetime.now().date()
        else:
            target_date = pd.to_datetime(selected_date).date()
        
        # Try TB approach first
        df_tb = load_tb_file()
        use_tb = False
        
        if not df_tb.empty:
            # Check if TB has data for our target date
            date_mapping = {}
            for col in df_tb.columns[3:]:
                try:
                    date_obj = pd.to_datetime(col, errors="raise").date()
                    date_mapping[date_obj] = col
                except Exception:
                    continue
            
            parsed_dates = sorted(date_mapping.keys())
            if parsed_dates and min(parsed_dates) <= target_date <= max(parsed_dates):
                use_tb = True
                print(f"Using TB data for calculations (covers {min(parsed_dates)} to {max(parsed_dates)})")
        
        if use_tb:
            # Use TB-based calculation
            start_of_month = target_date.replace(day=1)
            start_mtd = start_of_month - timedelta(days=1)
            
            def get_nearest_or_first(date_obj):
                eligible = [d for d in parsed_dates if d <= date_obj]
                return eligible[-1] if eligible else min(parsed_dates)
            
            def get_mtd_change(row):
                start_dt = get_nearest_or_first(start_mtd)
                end_dt = get_nearest_or_first(target_date)
                start_val = pd.to_numeric(row.get(date_mapping[start_dt], 0.0), errors='coerce')
                end_val = pd.to_numeric(row.get(date_mapping[end_dt], 0.0), errors='coerce')
                change = (end_val or 0) - (start_val or 0)
                
                # For income accounts (9xxxx), flip the sign
                gl_acct = str(pd.to_numeric(row["GL_Acct_Number"], errors="coerce"))
                if gl_acct.startswith("9"):
                    change *= -1
                
                return round(change, 6)
            
            total_changes = {}
            for range_name, (start_range, end_range) in account_number_ranges.items():
                category_accounts = df_coa[
                    (df_coa['GL_Acct_Number'] >= start_range) & 
                    (df_coa['GL_Acct_Number'] < end_range)
                ]['account_name'].tolist()
                
                range_total = 0
                for _, row in df_tb.iterrows():
                    if row['account_name'] in category_accounts:
                        change = get_mtd_change(row)
                        range_total += change
                
                total_changes[range_name] = range_total
            
            return total_changes
        
        else:
            # Fall back to GL-based calculation
            df_gl = load_GL_file()
            if df_gl.empty:
                return {}
            
            
            # Convert GL dates and filter for current month
            df_gl['date'] = pd.to_datetime(df_gl['date'], errors='coerce')
            start_of_month = target_date.replace(day=1)
            
            # Filter to current month transactions
            month_transactions = df_gl[
                (df_gl['date'].dt.date >= start_of_month) & 
                (df_gl['date'].dt.date <= target_date)
            ]
            
            total_changes = {}
            for range_name, (start_range, end_range) in account_number_ranges.items():
                category_accounts = df_coa[
                    (df_coa['GL_Acct_Number'] >= start_range) & 
                    (df_coa['GL_Acct_Number'] < end_range)
                ]['account_name'].tolist()
                
                category_transactions = month_transactions[
                    month_transactions['account_name'].isin(category_accounts)
                ]
                
                if category_transactions.empty:
                    total_changes[range_name] = 0
                    continue
                
                # Calculate net change based on account type using proper accounting principles
                debits = pd.to_numeric(category_transactions['debit_crypto'], errors='coerce').fillna(0).sum()
                credits = pd.to_numeric(category_transactions['credit_crypto'], errors='coerce').fillna(0).sum()
                
                # Accounting principles for normal balances:
                # Assets (1xxxx): Debit increases, Credit decreases -> Debit - Credit
                # Liabilities (2xxxx): Credit increases, Debit decreases -> Credit - Debit  
                # Capital/Equity (3xxxx): Credit increases, Debit decreases -> Credit - Debit
                # Income/Revenue (4xxxx, 9xxxx): Credit increases, Debit decreases -> Credit - Debit
                # Expenses (8xxxx): Debit increases, Credit decreases -> Debit - Credit
                
                if range_name in ["assets", "expenses"]:
                    net_change = debits - credits  # Normal debit balance
                else:  # liabilities, capital, income, revenue
                    net_change = credits - debits  # Normal credit balance
                
                total_changes[range_name] = round(net_change, 6)
            
            return total_changes
    
    def calculate_tb_monthly_trends(account_number_ranges):
        """Calculate monthly trends using Trial Balance data"""
        from .s3_utils import load_tb_file, load_COA_file
        
        df_tb = load_tb_file()
        df_coa = load_COA_file()
        
        if df_tb.empty or df_coa.empty:
            return {}
        
        # Build date mapping from TB columns
        date_mapping = {}
        for col in df_tb.columns[3:]:  # Skip account info columns
            try:
                date_obj = pd.to_datetime(col, errors="raise").date()
                date_mapping[date_obj] = col
            except Exception:
                continue
        
        parsed_dates = sorted(date_mapping.keys())
        if not parsed_dates:
            return {}
        
        # Group dates by month
        monthly_dates = {}
        for date_obj in parsed_dates:
            month_key = date_obj.replace(day=1)
            if month_key not in monthly_dates:
                monthly_dates[month_key] = []
            monthly_dates[month_key].append(date_obj)
        
        def get_month_change(row, start_month, end_month):
            # Get last day of each month for comparison
            start_date = max(monthly_dates.get(start_month, [start_month]))
            end_date = max(monthly_dates.get(end_month, [end_month]))
            
            start_val = pd.to_numeric(row.get(date_mapping.get(start_date, None), 0.0), errors='coerce')
            end_val = pd.to_numeric(row.get(date_mapping.get(end_date, None), 0.0), errors='coerce')
            change = (end_val or 0) - (start_val or 0)
            
            # For income accounts (9xxxx), flip the sign since they're credit accounts
            gl_acct = str(pd.to_numeric(row["GL_Acct_Number"], errors="coerce"))
            if gl_acct.startswith("9"):
                change *= -1
                
            return round(change, 6)
        
        # Calculate monthly trends for each account range
        monthly_trends = {}
        sorted_months = sorted(monthly_dates.keys())
        
        for month in sorted_months:
            month_str = month.strftime('%Y-%m')
            monthly_trends[month_str] = {}
            
            # Calculate previous month for comparison
            prev_month = None
            if sorted_months.index(month) > 0:
                prev_month = sorted_months[sorted_months.index(month) - 1]
            
            for range_name, (start_range, end_range) in account_number_ranges.items():
                category_accounts = df_coa[
                    (df_coa['GL_Acct_Number'] >= start_range) & 
                    (df_coa['GL_Acct_Number'] < end_range)
                ]['account_name'].tolist()
                
                month_total = 0
                if prev_month:
                    for _, row in df_tb.iterrows():
                        if row['account_name'] in category_accounts:
                            change = get_month_change(row, prev_month, month)
                            month_total += change
                
                monthly_trends[month_str][range_name] = month_total
        
        return monthly_trends
    
    # Client data mapping
    client_data = {
        "drip_capital": {
            "name": "Drip Capital",
            "logo": "clients/drip_capital/drip.png",
            "funds": {
                "fund_i_class_B_ETH": "Fund I - Class B",
                "fund_ii_class_B_ETH": "Fund II - Class B", 
                "holdings_class_B_ETH": "Holdings - Class B"
            }
        }
    }
    
    # Dynamic client display
    @output
    @render.ui
    def client_display():
        """Display selected client logo and name"""
        client_id = selected_client()
        if client_id in client_data:
            client = client_data[client_id]
            return ui.div(
                ui.img(
                    src=client["logo"], 
                    height="32px", 
                    class_="client-logo"
                ),
                ui.h4(client["name"], class_="client-name"),
                class_="client-display-content"
            )
        return ui.div()
    
    # Dynamic fund selector
    @output
    @render.ui 
    def dynamic_fund_selector():
        """Generate fund selector based on selected client"""
        client_id = selected_client()
        if client_id in client_data:
            funds = client_data[client_id]["funds"]
            return ui.div(
                ui.input_select(
                    "fund", 
                    "", 
                    choices=funds, 
                    selected=default_fund,  # Use the environment variable default
                    width="100%"
                ),
                class_="dropdown-with-arrow"
            )
        return ui.div("No funds available")
    
    # Navigation links
    @output
    @render.ui
    def nav_links():
        """Generate clickable navigation links"""
        sections = {
            "fund_accounting": "Fund Accounting",
            "investments": "Investments", 
            "general_ledger": "General Ledger",
            "financial_reporting": "Financial Reporting"
        }
        
        links = []
        for section_id, section_name in sections.items():
            is_active = current_section.get() == section_id
            css_classes = ["nav-link"]
            if is_active:
                css_classes.append("nav-link-active")
            
            links.append(
                ui.div(
                    section_name,
                    onclick=f"Shiny.setInputValue('nav_click', '{section_id}', {{priority: 'event'}});",
                    class_=" ".join(css_classes)
                )
            )
        
        return ui.div(*links)
    
    # Handle navigation clicks
    @reactive.effect
    @reactive.event(input.nav_click, ignore_none=True)
    def handle_nav_click():
        section = input.nav_click()
        if section:
            current_section.set(section)
    
    # Reactive theme styles output
    @output
    @render.ui
    @reactive.event(input.theme_selector, ignore_none=False)
    def theme_styles():
        """Generate reactive theme styles based on selected theme"""
        try:
            # Handle theme selector dropdown
            if hasattr(input, 'theme_selector') and input.theme_selector():
                theme_id = input.theme_selector()
                theme_manager.set_current_theme(theme_id)
            else:
                # Use default theme
                theme_id = theme_manager.current_theme
        except Exception as e:
            # Theme selector error, use default
            theme_id = theme_manager.current_theme
        
        # Always return the current theme styles
        return theme_manager.get_theme_ui_element()

    # Create investment dashboard calculations
    dashboard_calcs = create_investment_dashboard_calculations(selected_fund)

    register_fund_accounting_outputs(output, input, selected_fund, selected_report_date, session)

    register_financial_reporting_outputs(output, input, selected_fund, selected_report_date)

    register_gl_outputs(output, input, session, selected_fund)

    register_chart_of_accounts_outputs(output, input, session, selected_fund)
    
    # Crypto tracker now includes token fetcher outputs
    register_crypto_tracker_outputs(output, input, session)
    
    register_gl_analytics_outputs(output, input, session, selected_fund)
    

    register_loan_portfolio_outputs(output, input, session, selected_fund)

    register_nft_portfolio_outputs(output, input, session, selected_fund)

    register_cryptocurrency_portfolio_outputs(output, input, session, selected_fund)

    # Dynamic content area based on navigation selection
    @output
    @render.ui
    def main_content_area():
        """Render main content based on navigation selection"""
        section = current_section.get()
        
        
        if section == "fund_accounting":
            return enhanced_fund_accounting_ui()
        elif section == "investments":
            return enhanced_investments_ui()
        elif section == "general_ledger":
            return enhanced_general_ledger_ui()
        elif section == "financial_reporting":
            return enhanced_financial_reporting_ui()
        else:
            return enhanced_fund_accounting_ui()  # Default fallback

    # Dashboard placeholder outputs - these will need to be implemented with actual data
    @output
    @render.ui
    def dashboard_nav_current():
        return "Loading..."
    
    @output
    @render.ui  
    def dashboard_nav_performance():
        return "Loading..."
        
    @output
    @render.ui
    def dashboard_total_assets():
        return "Loading..."
        
    @output
    @render.ui
    def dashboard_active_investments():
        return "Loading..."
        
    @output
    @render.ui
    def dashboard_nav_chart():
        return "Chart loading..."
        
    @output
    @render.ui
    def dashboard_recent_activity():
        return "Recent activity loading..."

    # Investment dashboard with real data from GL
    @output
    @render.ui
    def dashboard_portfolio_value():
        try:
            from .s3_utils import load_GL_file, load_COA_file
            import pandas as pd
            gl_df = load_GL_file()
            coa_df = load_COA_file()
            
            
            # Get all asset account names (1000-1999) 
            investment_coa = coa_df[
                (coa_df['GL_Acct_Number'] >= 1000) & 
                (coa_df['GL_Acct_Number'] < 2000)
            ]
            
            if len(investment_coa) > 0:
                investment_account_names = investment_coa['account_name'].tolist()
            else:
                investment_account_names = []
            
            # Filter GL entries for investment accounts
            investment_entries = gl_df[gl_df['account_name'].isin(investment_account_names)]
            
            
            if investment_entries.empty:
                # Fallback: try to find any investment-related entries
                investment_entries = gl_df[gl_df['account_name'].str.contains('investment|loan|nft|crypto|asset', case=False, na=False)]
            
            # Calculate net balance (debits - credits for assets)
            # Convert to numeric to handle string values
            debits = pd.to_numeric(investment_entries['debit_crypto'], errors='coerce').fillna(0).sum()
            credits = pd.to_numeric(investment_entries['credit_crypto'], errors='coerce').fillna(0).sum()
            total_value = debits - credits
            
            
            return f"{total_value:,.4f} ETH"
        except Exception as e:
            print(f"Error in dashboard_portfolio_value: {e}")
            import traceback
            traceback.print_exc()
            return "Error loading"
        
    @output
    @render.ui
    def dashboard_active_loans():
        try:
            from .s3_utils import load_GL_file, load_COA_file
            gl_df = load_GL_file()
            coa_df = load_COA_file()
            
            # Get loan account names (1100-1199)
            loan_coa = coa_df[(coa_df['GL_Acct_Number'] >= 1100) & (coa_df['GL_Acct_Number'] < 1200)]
            loan_account_names = loan_coa['account_name'].tolist()
            
            # Count unique loan accounts with non-zero balances
            loan_entries = gl_df[gl_df['account_name'].isin(loan_account_names)]
            
            # Group by account and check if balance is non-zero
            loan_balances = loan_entries.groupby('account_name').agg({
                'debit_crypto': 'sum',
                'credit_crypto': 'sum'
            }).fillna(0)
            
            loan_balances['net_balance'] = loan_balances['debit_crypto'] - loan_balances['credit_crypto']
            active_loans = len(loan_balances[loan_balances['net_balance'].abs() > 0.001])
            
            return f"{active_loans:,}"
        except Exception as e:
            print(f"Error in dashboard_active_loans: {e}")
            return "Data loading..."
        
    @output
    @render.ui
    def dashboard_nft_count():
        try:
            from .s3_utils import load_GL_file, load_COA_file
            gl_df = load_GL_file()
            coa_df = load_COA_file()
            
            # Get NFT account names (1200-1299)
            nft_coa = coa_df[(coa_df['GL_Acct_Number'] >= 1200) & (coa_df['GL_Acct_Number'] < 1300)]
            nft_account_names = nft_coa['account_name'].tolist()
            
            # Count unique NFT accounts with activity
            nft_entries = gl_df[gl_df['account_name'].isin(nft_account_names)]
            nft_count = len(nft_entries['account_name'].unique()) if not nft_entries.empty else 0
            
            return f"{nft_count:,}"
        except Exception as e:
            print(f"Error in dashboard_nft_count: {e}")
            return "Data loading..."
        
    @output
    @render.ui
    def dashboard_crypto_count():
        try:
            from .s3_utils import load_GL_file, load_COA_file
            gl_df = load_GL_file()
            coa_df = load_COA_file()
            
            # Get crypto account names (1300-1399)
            crypto_coa = coa_df[(coa_df['GL_Acct_Number'] >= 1300) & (coa_df['GL_Acct_Number'] < 1400)]
            crypto_account_names = crypto_coa['account_name'].tolist()
            
            # Count unique crypto accounts with activity
            crypto_entries = gl_df[gl_df['account_name'].isin(crypto_account_names)]
            crypto_count = len(crypto_entries['account_name'].unique()) if not crypto_entries.empty else 0
            
            return f"{crypto_count:,}"
        except Exception as e:
            print(f"Error in dashboard_crypto_count: {e}")
            return "Data loading..."
        
    @output
    @render.ui
    def dashboard_portfolio_allocation():
        try:
            allocation = dashboard_calcs["portfolio_allocation"]()
            
            if not allocation:
                return ui.div(
                    ui.p("No investment data available", class_="text-muted text-center"),
                    class_="empty-state"
                )
            
            # Create a simple breakdown display
            allocation_items = []
            for item in allocation:
                allocation_items.append(
                    ui.div(
                        ui.div(
                            ui.span(
                                item["category"],
                                style=f"color: {item['color']}; font-weight: bold;"
                            ),
                            ui.span(
                                f"{item['percentage']:.1f}% ({item['value']:.2f} ETH)",
                                class_="text-muted",
                                style="float: right;"
                            ),
                            class_="allocation-item"
                        )
                    )
                )
            
            return ui.div(
                *allocation_items,
                class_="portfolio-allocation-content"
            )
            
        except Exception as e:
            print(f"Error in dashboard_portfolio_allocation: {e}")
            return ui.div(
                ui.p("Error loading portfolio allocation", class_="text-danger text-center"),
                class_="error-state"
            )
        
    @output
    @render.ui
    def dashboard_top_assets():
        try:
            top_assets = dashboard_calcs["top_assets"]()
            
            if not top_assets:
                return ui.div(
                    ui.p("No asset data available", class_="text-muted text-center"),
                    class_="empty-state"
                )
            
            asset_items = []
            for i, asset in enumerate(top_assets):
                try:
                    asset_items.append(
                        ui.div(
                            ui.span(asset["name"], style=f"color: {asset['color']}; font-weight: bold;"),
                            ui.br(),
                            ui.span(f"{asset['type']} - {asset['value']:.4f} ETH", 
                                   class_="text-muted", 
                                   style="font-size: 0.875rem;"),
                            class_="asset-item"
                        )
                    )
                except Exception as asset_error:
                    print(f"Error processing asset {i}: {asset_error}")
                    continue
            
            if not asset_items:
                return ui.div(
                    ui.p("No valid assets to display", class_="text-muted text-center"),
                    class_="empty-state"
                )
            
            return ui.div(
                ui.h6("Top Assets", class_="mb-3"),
                *asset_items,
                class_="top-assets-content"
            )
            
        except Exception as e:
            print(f"Error in dashboard_top_assets: {e}")
            import traceback
            traceback.print_exc()
            return ui.div(
                ui.p(f"Error: {str(e)}", class_="text-danger text-center"),
                class_="error-state"
            )

    # General Ledger dashboard with real data
    @output
    @render.ui
    def dashboard_total_entries():
        try:
            from .s3_utils import load_GL_file
            gl_df = load_GL_file()
            total_entries = len(gl_df)
            return f"{total_entries:,}"
        except Exception as e:
            print(f"Error in dashboard_total_entries: {e}")
            return "Data loading..."
        
    @output
    @render.ui
    def dashboard_month_entries():
        try:
            from .s3_utils import load_GL_file
            import pandas as pd
            from datetime import datetime
            
            gl_df = load_GL_file()
            
            if gl_df.empty or 'date' not in gl_df.columns:
                return "0"
            
            # Convert date column to datetime if it's not already
            gl_df['date'] = pd.to_datetime(gl_df['date'], errors='coerce')
            
            # Get current month entries
            current_month = pd.Timestamp.now(tz='UTC').replace(day=1)
            month_entries = gl_df[gl_df['date'] >= current_month]
            
            return f"{len(month_entries):,}"
        except Exception as e:
            print(f"Error in dashboard_month_entries: {e}")
            import traceback
            traceback.print_exc()
            return "Error"
        
    @output
    @render.ui
    def dashboard_account_balance():
        try:
            from .s3_utils import load_GL_file
            gl_df = load_GL_file()
            
            if gl_df.empty:
                return "No Data"
            
            # Calculate trial balance using crypto amounts (debits minus credits should equal zero)
            if 'debit_crypto' in gl_df.columns and 'credit_crypto' in gl_df.columns:
                total_debits = pd.to_numeric(gl_df['debit_crypto'], errors='coerce').fillna(0).sum()
                total_credits = pd.to_numeric(gl_df['credit_crypto'], errors='coerce').fillna(0).sum()
                balance = total_debits - total_credits
                
                if abs(balance) < 0.001:  # Use smaller threshold for crypto amounts
                    return "Balanced"
                else:
                    return f"Imbalance: {balance:,.4f} ETH"
            else:
                return "No Balance Data"
                
        except Exception as e:
            print(f"Error in dashboard_account_balance: {e}")
            import traceback
            traceback.print_exc()
            return "Error"
        
    @output
    @render.ui
    def dashboard_pending_items():
        try:
            from .s3_utils import load_GL_file
            import pandas as pd
            
            gl_df = load_GL_file()
            
            if gl_df.empty or 'date' not in gl_df.columns:
                return "0"
            
            # Count recent entries (last 7 days) as "pending review"
            gl_df['date'] = pd.to_datetime(gl_df['date'], errors='coerce')
            recent_entries = gl_df[gl_df['date'] >= pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=7)]
            
            return f"{len(recent_entries):,}"
        except Exception as e:
            print(f"Error in dashboard_pending_items: {e}")
            import traceback
            traceback.print_exc()
            return "Error"
        
    @output
    @render.ui
    def dashboard_recent_transactions():
        try:
            from .s3_utils import load_GL_file
            import pandas as pd
            
            gl_df = load_GL_file()
            
            if gl_df.empty:
                return ui.div(
                    ui.p("No transactions available", class_="text-muted text-center"),
                    class_="empty-state"
                )
            
            # Get recent transactions (last 10)
            if 'date' in gl_df.columns:
                gl_df['date'] = pd.to_datetime(gl_df['date'], errors='coerce')
                recent_df = gl_df.sort_values('date', ascending=False).head(10)
            else:
                recent_df = gl_df.head(10)
            
            transaction_items = []
            for _, row in recent_df.iterrows():
                date_str = row.get('date', '').strftime('%m/%d/%Y') if pd.notna(row.get('date')) else 'Unknown'
                account = str(row.get('account_name', 'Unknown'))[:30] + ('...' if len(str(row.get('account_name', ''))) > 30 else '')
                debit = float(row.get('debit_crypto', 0)) if pd.notna(row.get('debit_crypto')) else 0
                credit = float(row.get('credit_crypto', 0)) if pd.notna(row.get('credit_crypto')) else 0
                amount = debit if debit > 0 else -credit
                
                transaction_items.append(
                    ui.div(
                        ui.div(
                            ui.span(account, style="font-size: 0.9rem; font-weight: 500;"),
                            ui.span(date_str, style="font-size: 0.8rem; color: var(--bs-secondary); float: right;")
                        ),
                        ui.div(
                            ui.span(f"{amount:+,.4f} ETH", 
                                   style=f"font-size: 0.85rem; color: {'var(--bs-success)' if amount >= 0 else 'var(--bs-danger)'};")
                        ),
                        style="padding: 0.5rem 0; border-bottom: 1px solid var(--bs-border-color-translucent);"
                    )
                )
            
            return ui.div(
                *transaction_items,
                style="max-height: 300px; overflow-y: auto;"
            )
            
        except Exception as e:
            print(f"Error in dashboard_recent_transactions: {e}")
            import traceback
            traceback.print_exc()
            return ui.div(
                ui.p("Error loading transactions", class_="text-danger text-center"),
                class_="error-state"
            )
        
    @output
    @render.ui
    def dashboard_account_summary():
        try:
            from .s3_utils import load_GL_file, load_COA_file
            import pandas as pd
            
            gl_df = load_GL_file()
            coa_df = load_COA_file()
            
            if gl_df.empty or coa_df.empty:
                return ui.div(
                    ui.p("No account data available", class_="text-muted text-center"),
                    class_="empty-state"
                )
            
            # Calculate account category summaries
            account_summaries = []
            
            categories = [
                ("Assets", 1000, 2000, "var(--bs-primary)"),
                ("Liabilities", 2000, 3000, "var(--bs-warning)"), 
                ("Equity", 3000, 4000, "var(--bs-info)"),
                ("Revenue", 4000, 5000, "var(--bs-success)"),
                ("Expenses", 5000, 10000, "var(--bs-danger)")
            ]
            
            for category_name, start_range, end_range, color in categories:
                # Get accounts in this range
                category_coa = coa_df[
                    (coa_df['GL_Acct_Number'] >= start_range) & 
                    (coa_df['GL_Acct_Number'] < end_range)
                ]
                
                if category_coa.empty:
                    continue
                
                account_names = category_coa['account_name'].tolist()
                category_entries = gl_df[gl_df['account_name'].isin(account_names)]
                
                if not category_entries.empty:
                    debits = pd.to_numeric(category_entries['debit_crypto'], errors='coerce').fillna(0).sum()
                    credits = pd.to_numeric(category_entries['credit_crypto'], errors='coerce').fillna(0).sum()
                    
                    # For assets and expenses, positive balance = debit balance
                    # For liabilities, equity, and revenue, positive balance = credit balance
                    if category_name in ["Assets", "Expenses"]:
                        balance = debits - credits
                    else:
                        balance = credits - debits
                    
                    account_summaries.append(
                        ui.div(
                            ui.div(
                                ui.span(category_name, 
                                       style=f"font-weight: 600; color: {color}; font-size: 0.95rem;"),
                                ui.span(f"{balance:,.4f} ETH", 
                                       style=f"font-size: 0.9rem; color: {color}; float: right;")
                            ),
                            ui.div(
                                ui.span(f"{len(account_names)} accounts", 
                                       style="font-size: 0.8rem; color: var(--bs-secondary);")
                            ),
                            style="padding: 0.75rem 0; border-bottom: 1px solid var(--bs-border-color-translucent);"
                        )
                    )
            
            return ui.div(
                *account_summaries,
                style="max-height: 400px; overflow-y: auto;"
            )
            
        except Exception as e:
            print(f"Error in dashboard_account_summary: {e}")
            import traceback
            traceback.print_exc()
            return ui.div(
                ui.p("Error loading account summary", class_="text-danger text-center"),
                class_="error-state"
            )

    # Financial Reporting dashboard with real data
    @output
    @render.ui
    def dashboard_total_revenue():
        try:
            from .modules.financial_reporting.tb_generator import get_income_expense_changes, generate_trial_balance_from_gl
            from .s3_utils import load_GL_file
            from datetime import datetime
            
            # Load GL data and filter by selected fund (same as financial reporting)
            gl_df = load_GL_file()
            
            if gl_df.empty:
                return "No Data"
            
            # Filter by selected fund if specified
            fund_id = selected_fund() if selected_fund and hasattr(selected_fund, '__call__') else None
            if fund_id and 'fund_id' in gl_df.columns:
                gl_df = gl_df[gl_df['fund_id'] == fund_id]
                
            if gl_df.empty:
                return "No Data"
                
            # Ensure date column is properly formatted
            if 'date' in gl_df.columns:
                gl_df['date'] = pd.to_datetime(gl_df['date'], utc=True, errors='coerce')
            elif 'operating_date' in gl_df.columns:
                gl_df['date'] = pd.to_datetime(gl_df['operating_date'], utc=True, errors='coerce')
            
            # Enrich with COA mappings
            from .account_mapper import enrich_gl_with_coa_mapping
            gl_df = enrich_gl_with_coa_mapping(gl_df)
            
            # Use current date for MTD calculation
            current_date = datetime(2024, 7, 31)  # Use end of available data
            
            # Get income and expense changes using same logic as financial reporting
            income_df, expense_df = get_income_expense_changes(gl_df, current_date)
            
            if income_df.empty:
                return "0.0000 ETH"
            
            # Sum MTD values from income accounts
            total_revenue = income_df['MTD'].sum() if 'MTD' in income_df.columns else 0
            
            return f"{total_revenue:,.4f} ETH"
        except Exception as e:
            print(f"Error in dashboard_total_revenue: {e}")
            import traceback
            traceback.print_exc()
            return "Error"
        
    @output
    @render.ui
    def dashboard_net_income():
        try:
            from .modules.financial_reporting.tb_generator import get_income_expense_changes
            from .s3_utils import load_GL_file
            from datetime import datetime
            
            # Load GL data and filter by selected fund (same as financial reporting)
            gl_df = load_GL_file()
            
            if gl_df.empty:
                return "No Data"
            
            # Filter by selected fund if specified
            fund_id = selected_fund() if selected_fund and hasattr(selected_fund, '__call__') else None
            if fund_id and 'fund_id' in gl_df.columns:
                gl_df = gl_df[gl_df['fund_id'] == fund_id]
                
            if gl_df.empty:
                return "No Data"
                
            # Ensure date column is properly formatted
            if 'date' in gl_df.columns:
                gl_df['date'] = pd.to_datetime(gl_df['date'], utc=True, errors='coerce')
            elif 'operating_date' in gl_df.columns:
                gl_df['date'] = pd.to_datetime(gl_df['operating_date'], utc=True, errors='coerce')
            
            # Enrich with COA mappings
            from .account_mapper import enrich_gl_with_coa_mapping
            gl_df = enrich_gl_with_coa_mapping(gl_df)
            
            # Use current date for MTD calculation
            current_date = datetime(2024, 7, 31)  # Use end of available data
            
            # Get income and expense changes using same logic as financial reporting
            income_df, expense_df = get_income_expense_changes(gl_df, current_date)
            
            # Calculate net income: revenue - expenses
            total_revenue = income_df['MTD'].sum() if not income_df.empty and 'MTD' in income_df.columns else 0
            total_expenses = expense_df['MTD'].sum() if not expense_df.empty and 'MTD' in expense_df.columns else 0
            net_income = total_revenue - total_expenses
            
            return f"{net_income:,.4f} ETH"
        except Exception as e:
            print(f"Error in dashboard_net_income: {e}")
            import traceback
            traceback.print_exc()
            return "Error"
        
    @output
    @render.ui
    def dashboard_assets():
        try:
            from .modules.financial_reporting.tb_generator import generate_trial_balance_from_gl
            from .s3_utils import load_GL_file
            from datetime import datetime
            
            # Load GL data and filter by selected fund (same as financial reporting)
            gl_df = load_GL_file()
            
            if gl_df.empty:
                return "No Data"
            
            # Filter by selected fund if specified
            fund_id = selected_fund() if selected_fund and hasattr(selected_fund, '__call__') else None
            if fund_id and 'fund_id' in gl_df.columns:
                gl_df = gl_df[gl_df['fund_id'] == fund_id]
                
            if gl_df.empty:
                return "No Data"
                
            # Ensure date column is properly formatted
            if 'date' in gl_df.columns:
                gl_df['date'] = pd.to_datetime(gl_df['date'], utc=True, errors='coerce')
            elif 'operating_date' in gl_df.columns:
                gl_df['date'] = pd.to_datetime(gl_df['operating_date'], utc=True, errors='coerce')
            
            # Enrich with COA mappings
            from .account_mapper import enrich_gl_with_coa_mapping
            gl_df = enrich_gl_with_coa_mapping(gl_df)
            
            # Use current date for trial balance
            current_date = datetime(2024, 7, 31)  # Use end of available data
            
            # Generate trial balance using same logic as financial reporting
            tb_df = generate_trial_balance_from_gl(gl_df, current_date)
            
            if tb_df.empty:
                return "0.0000 ETH"
            
            # Filter for asset accounts (1xxxx)
            asset_accounts = tb_df[tb_df['GL_Acct_Number'].astype(str).str.startswith('1')]
            
            if asset_accounts.empty:
                return "0.0000 ETH"
            
            # Sum asset balances
            total_assets = asset_accounts['Balance'].sum()
            
            return f"{total_assets:,.4f} ETH"
        except Exception as e:
            print(f"Error in dashboard_assets: {e}")
            import traceback
            traceback.print_exc()
            return "Error"
        
    @output
    @render.ui
    def dashboard_roi():
        try:
            from .modules.financial_reporting.tb_generator import get_income_expense_changes, generate_trial_balance_from_gl
            from .s3_utils import load_GL_file
            from datetime import datetime
            
            # Load GL data and filter by selected fund (same as financial reporting)
            gl_df = load_GL_file()
            
            if gl_df.empty:
                return "No Data"
            
            # Filter by selected fund if specified
            fund_id = selected_fund() if selected_fund and hasattr(selected_fund, '__call__') else None
            if fund_id and 'fund_id' in gl_df.columns:
                gl_df = gl_df[gl_df['fund_id'] == fund_id]
                
            if gl_df.empty:
                return "No Data"
                
            # Ensure date column is properly formatted
            if 'date' in gl_df.columns:
                gl_df['date'] = pd.to_datetime(gl_df['date'], utc=True, errors='coerce')
            elif 'operating_date' in gl_df.columns:
                gl_df['date'] = pd.to_datetime(gl_df['operating_date'], utc=True, errors='coerce')
            
            # Enrich with COA mappings
            from .account_mapper import enrich_gl_with_coa_mapping
            gl_df = enrich_gl_with_coa_mapping(gl_df)
            
            # Use current date
            current_date = datetime(2024, 7, 31)  # Use end of available data
            
            # Get income and expense changes for MTD
            income_df, expense_df = get_income_expense_changes(gl_df, current_date)
            
            # Get asset balances from trial balance
            tb_df = generate_trial_balance_from_gl(gl_df, current_date)
            
            # Calculate components
            revenue = income_df['MTD'].sum() if not income_df.empty and 'MTD' in income_df.columns else 0
            expenses = expense_df['MTD'].sum() if not expense_df.empty and 'MTD' in expense_df.columns else 0
            
            # Get total assets from trial balance
            assets = 0
            if not tb_df.empty:
                asset_accounts = tb_df[tb_df['GL_Acct_Number'].astype(str).str.startswith('1')]
                assets = asset_accounts['Balance'].sum() if not asset_accounts.empty else 0
            
            # Calculate ROI
            net_income = revenue - expenses
            roi = (net_income / assets * 100) if assets != 0 else 0
            
            return f"{roi:.2f}%"
        except Exception as e:
            print(f"Error in dashboard_roi: {e}")
            import traceback
            traceback.print_exc()
            return "Error"
        
    @output
    @render.ui
    def dashboard_income_trend():
        try:
            import plotly.graph_objects as go
            from shiny import ui
            from .s3_utils import load_GL_file, load_COA_file
            
            # For trend, use GL data directly to show actual activity by month
            gl_df = load_GL_file()
            coa_df = load_COA_file()
            
            if gl_df.empty or coa_df.empty:
                return ui.div(
                    ui.p("No data available for income trend", class_="text-muted text-center"),
                    class_="empty-state"
                )
            
            # Get income and expense account names (4=income, 9=income, 8=expenses)
            income_4_accounts = coa_df[(coa_df['GL_Acct_Number'] >= 40000) & (coa_df['GL_Acct_Number'] < 50000)]['account_name'].tolist()
            income_9_accounts = coa_df[(coa_df['GL_Acct_Number'] >= 90000) & (coa_df['GL_Acct_Number'] < 100000)]['account_name'].tolist()
            expense_accounts = coa_df[(coa_df['GL_Acct_Number'] >= 80000) & (coa_df['GL_Acct_Number'] < 90000)]['account_name'].tolist()
            
            # Combine all income accounts
            revenue_accounts = income_4_accounts + income_9_accounts
            
            # Convert GL dates and group by month
            gl_df['date'] = pd.to_datetime(gl_df['date'], errors='coerce')
            gl_df['month'] = gl_df['date'].dt.to_period('M').astype(str)
            
            # Calculate monthly totals
            monthly_data = []
            for month in sorted(gl_df['month'].dropna().unique()):
                month_data = gl_df[gl_df['month'] == month]
                
                # Revenue (credit balance)
                revenue_entries = month_data[month_data['account_name'].isin(revenue_accounts)]
                monthly_revenue = pd.to_numeric(revenue_entries['credit_crypto'], errors='coerce').fillna(0).sum() - pd.to_numeric(revenue_entries['debit_crypto'], errors='coerce').fillna(0).sum()
                
                # Expenses (debit balance)
                expense_entries = month_data[month_data['account_name'].isin(expense_accounts)]
                monthly_expenses = pd.to_numeric(expense_entries['debit_crypto'], errors='coerce').fillna(0).sum() - pd.to_numeric(expense_entries['credit_crypto'], errors='coerce').fillna(0).sum()
                
                monthly_data.append({
                    'month': month,
                    'revenue': monthly_revenue,
                    'expenses': monthly_expenses,
                    'net_income': monthly_revenue - monthly_expenses
                })
            
            if not monthly_data:
                return ui.div(
                    ui.p("No monthly activity to chart", class_="text-muted text-center"),
                    class_="empty-state"
                )
            
            df = pd.DataFrame(monthly_data)
            
            # Create Plotly chart
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=df['month'],
                y=df['revenue'],
                mode='lines+markers',
                name='Revenue',
                line=dict(color='green', width=2),
                marker=dict(size=6)
            ))
            
            fig.add_trace(go.Scatter(
                x=df['month'],
                y=df['expenses'],
                mode='lines+markers',
                name='Expenses',
                line=dict(color='red', width=2),
                marker=dict(size=6)
            ))
            
            fig.add_trace(go.Scatter(
                x=df['month'],
                y=df['net_income'],
                mode='lines+markers',
                name='Net Income',
                line=dict(color='blue', width=3),
                marker=dict(size=8)
            ))
            
            fig.update_layout(
                title="Monthly Income Activity from GL Data",
                xaxis_title="Month",
                yaxis_title="Amount (ETH)",
                template="plotly_white",
                height=400,
                hovermode='x unified'
            )
            
            return ui.div(
                ui.HTML(fig.to_html(full_html=False, include_plotlyjs="cdn")),
                style="height: 400px;"
            )
            
        except Exception as e:
            print(f"Error in dashboard_income_trend: {e}")
            import traceback
            traceback.print_exc()
            return ui.div(
                ui.p("Error loading income trend", class_="text-danger text-center"),
                class_="error-state"
            )
        
    @output
    @render.ui
    def dashboard_balance_summary():
        try:
            # Use correct account ranges for balance summary
            account_ranges = {
                "assets": (10000, 20000),
                "liabilities": (20000, 30000),
                "capital": (30000, 40000),
                "income_4": (40000, 50000),
                "expenses": (80000, 90000),
                "income_9": (90000, 100000)
            }
            
            changes = calculate_financial_metrics(account_ranges)
            
            if not changes:
                return ui.div(
                    ui.p("No balance data available", class_="text-muted text-center"),
                    class_="empty-state"
                )
            
            from shiny import ui
            
            # Define display categories with colors
            categories = [
                ("Assets", "assets", "var(--bs-primary)"),
                ("Liabilities", "liabilities", "var(--bs-warning)"),
                ("Capital", "capital", "var(--bs-info)"),
                ("Income (4xxx)", "income_4", "var(--bs-success)"),
                ("Expenses", "expenses", "var(--bs-danger)"),
                ("Income (9xxx)", "income_9", "var(--bs-success)")
            ]
            
            balance_items = []
            
            for display_name, range_key, color in categories:
                balance = changes.get(range_key, 0)
                
                if abs(balance) > 0.0001:  # Only show meaningful balances
                    balance_items.append(
                        ui.div(
                            ui.div(
                                ui.span(display_name, style=f"font-weight: 600; color: {color};"),
                                ui.span(f"{balance:+,.4f} ETH", 
                                       style=f"float: right; font-weight: 500; color: {'var(--bs-success)' if balance >= 0 else 'var(--bs-danger)'};")
                            ),
                            ui.div(
                                ui.small("Current period activity", class_="text-muted")
                            ),
                            style="padding: 0.75rem 0; border-bottom: 1px solid var(--bs-border-color-translucent);"
                        )
                    )
            
            if not balance_items:
                return ui.div(
                    ui.p("No significant account activity this period", class_="text-muted text-center"),
                    class_="empty-state"
                )
            
            # Add balance check - Assets = Liabilities + Capital + Retained Earnings
            total_assets = changes.get("assets", 0)
            total_liabilities = changes.get("liabilities", 0)
            total_capital = changes.get("capital", 0)
            total_income = changes.get("income_4", 0) + changes.get("income_9", 0)
            total_expenses = changes.get("expenses", 0)
            retained_earnings = total_income - total_expenses
            
            total_liab_cap_re = total_liabilities + total_capital + retained_earnings
            balance_check = abs(total_assets - total_liab_cap_re) < 0.0001
            
            balance_items.append(
                ui.div(
                    ui.hr(),
                    ui.div(
                        ui.span("Period Balance Check", style="font-weight: 600;"),
                        ui.span("✅ Balanced" if balance_check else "❌ Imbalanced", 
                               style=f"float: right; color: {'var(--bs-success)' if balance_check else 'var(--bs-danger)'};")
                    ),
                    style="padding-top: 0.5rem;"
                )
            )
            
            return ui.div(
                *balance_items,
                style="max-height: 400px; overflow-y: auto;"
            )
            
        except Exception as e:
            print(f"Error in dashboard_balance_summary: {e}")
            import traceback
            traceback.print_exc()
            return ui.div(
                ui.p("Error loading balance summary", class_="text-danger text-center"),
                class_="error-state"
            )

    @output
    @render.code
    def nav_selection_debug():
        client = selected_client()
        fund = selected_fund() if hasattr(input, 'fund') and input.fund() else "N/A"
        section = input.main_section() if hasattr(input, 'main_section') else "N/A"
        return f"Client: {client}\nFund: {fund}\nSection: {section}"
