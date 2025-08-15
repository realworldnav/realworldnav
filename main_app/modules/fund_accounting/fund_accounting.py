from shiny import ui, render, reactive
from shinywidgets import output_widget, render_plotly
from faicons import icon_svg
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
from ...s3_utils import load_GL_file, load_COA_file


# === UI ===
def fund_accounting_ui():
    return ui.page_fluid(
        ui.h2("NAV Dashboard", class_="mt-3"),

        # Date range selection with uniform styling
        ui.card(
            ui.card_header("Select NAV Range"),
            ui.card_body(
                ui.input_radio_buttons(
                    id="nav_range_mode",
                    label=None,
                    choices={
                        "MTD": "Month to Date",
                        "QTD": "Quarter to Date",
                        "YTD": "Year to Date",
                        "ITD": "Inception to Date",
                        "Custom": "Custom Range"
                    },
                    selected="MTD",
                    inline=True
                ),
                ui.output_ui("custom_date_range_ui")
            )
        ),


        ui.layout_column_wrap(
            ui.value_box("Current NAV", ui.output_ui("nav_current"), showcase=icon_svg("sack-dollar")),
            ui.value_box("Change", ui.output_ui("nav_change"), showcase=ui.output_ui("nav_change_icon")),
            ui.value_box("Percent Change", ui.output_ui("nav_change_percent"), showcase=icon_svg("percent")),
            fill=False,
        ),

        ui.layout_columns(
            ui.card(
                output_widget("nav_chart"),
                full_screen=True,
            ),


            ui.card(
                ui.card_header("Latest Data"),
                ui.output_data_frame("latest_summary"),

            ui.value_box(
            "Contributed Capital",
            ui.output_ui("contributed_capital_box"),
            showcase=icon_svg("hand-holding-dollar"),
        ),            ),
            col_widths=[9, 3]
        ),
        ui.card(
                output_widget("net_income_chart"),
                full_screen=True,
            ),


    )


# === SERVER ===
def register_outputs(output, input, selected_fund=None, selected_report_date=None, session=None):
    @reactive.calc
    def gl_data():
        """Load and prepare GL data"""
        from .helpers import gl_data_for_fund
        return gl_data_for_fund(selected_fund)

    @reactive.calc
    def daily_balances():
        """Calculate daily account balances from GL data"""
        df = gl_data()
        
        # Group by date and account, sum up debits and credits
        daily_data = []
        
        for date in df['date'].dt.date.unique():
            if pd.isna(date):
                continue
                
            date_entries = df[df['date'].dt.date == date]
            
            for account in date_entries['GL_Acct_Number'].unique():
                if pd.isna(account):
                    continue
                    
                account_entries = date_entries[date_entries['GL_Acct_Number'] == account]
                
                # Calculate running balance up to this date
                historical_entries = df[
                    (df['date'].dt.date <= date) & 
                    (df['GL_Acct_Number'] == account)
                ]
                
                total_debits = historical_entries['debit_crypto'].sum()
                total_credits = historical_entries['credit_crypto'].sum()
                
                # For assets (1000s) and expenses (5000s+): debit balance
                # For liabilities (2000s), equity (3000s), revenue (4000s): credit balance
                if str(account).startswith(('1', '5', '6', '7', '8', '9')):
                    balance = total_debits - total_credits
                else:
                    balance = total_credits - total_debits
                
                daily_data.append({
                    'Report Date': pd.Timestamp(date),
                    'GL_Acct_Number': account,
                    'Balance': balance
                })
        
        return pd.DataFrame(daily_data)

    @reactive.calc
    def nav_data():
        """Get NAV time series from GL data"""
        from .helpers import daily_nav_series
        nav_series = daily_nav_series()
        
        
        return nav_series

    @reactive.calc
    def nav_delta():
        """Calculate NAV change based on selected period"""
        from .helpers import calculate_nav_for_date
        
        # Get the period from the input
        mode = input.nav_range_mode()
        
        # Get latest date from GL
        gl_df = gl_data()
        if gl_df.empty:
            return None, None, None
            
        latest_date = gl_df['date'].max()
        
        # Calculate start date based on mode
        if mode == "MTD":
            start_date = latest_date.replace(day=1)
        elif mode == "QTD":
            start_date = pd.Timestamp(latest_date.year, ((latest_date.month - 1) // 3) * 3 + 1, 1)
        elif mode == "YTD":
            start_date = pd.Timestamp(latest_date.year, 1, 1)
        elif mode == "ITD":
            start_date = gl_df['date'].min()
        elif mode == "Custom" and input.nav_custom_range() is not None:
            date_range = input.nav_custom_range()
            start_date = pd.to_datetime(date_range[0])
            latest_date = pd.to_datetime(date_range[1])
        else:
            # Default to month to date
            start_date = latest_date.replace(day=1)
        
        # Calculate NAV for both dates
        _, _, nav_start = calculate_nav_for_date(start_date)
        _, _, nav_end = calculate_nav_for_date(latest_date)
        
        # Calculate change
        delta = nav_end - nav_start
        
        
        return nav_end, nav_start, delta
    @reactive.calc
    def filtered_nav_data():
        nav = nav_data()
        if nav.empty:
            return nav

        start, end = nav_range()
        
        if start is None or end is None:
            return nav
            
        filtered = nav[(nav.index >= start) & (nav.index <= end)]
        
        return filtered


    @output 
    @render.ui
    def nav_current():
        nav_now, _, _ = nav_delta()
        return f"{nav_now:,.2f}" if nav_now is not None else "N/A"

    @output
    @render.ui
    def nav_change():
        _, _, delta = nav_delta()
        return f"{delta:+,.2f}" if delta is not None else "N/A"

    @output
    @render.ui
    def nav_change_percent():
        nav_now, nav_then, delta = nav_delta()
        if None in (nav_now, nav_then, delta) or nav_then == 0:
            return "N/A"
        pct = (delta / nav_then) * 100
        return f"{pct:+.2f}%"

    @output
    @render.ui
    def nav_change_icon():
        _, _, delta = nav_delta()
        if delta is None:
            return icon_svg("circle-minus")
        icon = icon_svg("arrow-up") if delta >= 0 else icon_svg("arrow-down")
        icon.add_class(f"text-{'success' if delta >= 0 else 'danger'}")
        return icon

    @output
    @render_plotly
    def nav_chart():
        nav = filtered_nav_data()
        if nav.empty:
            return go.Figure()

        x_vals = nav.index.to_pydatetime()
        y_vals = nav.values

        # Handle potential issue with empty or invalid x_vals/y_vals
        if len(x_vals) == 0 or len(y_vals) == 0:
            empty_fig = go.Figure()
            empty_fig.add_annotation(
                text="No NAV data available for selected date range",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False
            )
            return empty_fig

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=x_vals,
            y=y_vals,
            mode="lines+markers",
            name="NAV",
            line=dict(width=3),
        ))

        # Apply theme configuration piece by piece
        from ...theme_manager import theme_manager
        layout_config = theme_manager.get_plotly_layout_config()
        
        # Apply layout settings explicitly
        fig.update_layout(
            paper_bgcolor=layout_config["paper_bgcolor"],
            plot_bgcolor=layout_config["plot_bgcolor"],
            font=layout_config["font"],
            title=layout_config["title"],
            colorway=layout_config["colorway"],
            legend=layout_config["legend"],
            hoverlabel=layout_config["hoverlabel"],
            hovermode="x unified"
        )
        
        # Override title text specifically
        fig.update_layout(title_text="NAV Over Time")
        
        # Get theme colors directly  
        theme_data = theme_manager.get_current_theme_data()
        text_color = theme_data.get("colors", {}).get("text_primary", "#f8fafc")
        
        # Apply axis configurations with subtle, transparent grid lines
        fig.update_xaxes(
            title_text="Date",
            title_font={"color": text_color, "size": 12},
            type="date",
            range=[x_vals[0], x_vals[-1]],
            tickfont={"color": text_color, "size": 10},
            linecolor="rgba(148, 163, 184, 0.3)",
            gridcolor="rgba(148, 163, 184, 0.15)", 
            tickcolor=text_color,
            zerolinecolor="rgba(148, 163, 184, 0.4)",
            showgrid=True,
            showline=True,
            zeroline=True,
            gridwidth=0.5
        )
        
        fig.update_yaxes(
            title_text="Net Asset Value",
            title_font={"color": text_color, "size": 12},
            tickfont={"color": text_color, "size": 10},
            linecolor="rgba(148, 163, 184, 0.3)",
            gridcolor="rgba(148, 163, 184, 0.15)",
            tickcolor=text_color,
            zerolinecolor="rgba(148, 163, 184, 0.4)", 
            showgrid=True,
            showline=True,
            zeroline=True,
            gridwidth=0.5
        )
        return fig
    
    @reactive.calc
    def nav_range():
        nav = nav_data()
        if nav.empty:
            return None, None

        try:
            # Get mode with fallback to default
            mode = input.nav_range_mode() if hasattr(input, 'nav_range_mode') else "MTD"
        except:
            mode = "MTD"  # Default to MTD if input not available
            
        try:
            # Get custom range with fallback 
            user_range = input.nav_custom_range() if hasattr(input, 'nav_custom_range') else None
        except:
            user_range = None
            
        # Get available data range - ensure timezone consistency
        data_start = nav.index.min()
        data_end = nav.index.max()
        
        if mode == "Custom":
            if user_range is not None:
                start = pd.to_datetime(user_range[0], utc=True)
                end = pd.to_datetime(user_range[1], utc=True)
            else:
                # Default custom range to full data range
                start = data_start
                end = data_end
        elif mode == "MTD":
            # Try MTD, fallback to available data if no data in current month
            end_naive = data_end.replace(tzinfo=None) if hasattr(data_end, 'tz') and data_end.tz is not None else data_end
            start_naive = end_naive.replace(day=1)
            start = pd.Timestamp(start_naive, tz='UTC')
            end = data_end  # Use timezone-aware data_end
            
            # If MTD range has no data, use full available range
            if start < data_start:
                start = data_start
                
        elif mode == "QTD":
            # Try QTD, fallback to available data if no data in current quarter
            end_naive = data_end.replace(tzinfo=None) if hasattr(data_end, 'tz') and data_end.tz is not None else data_end
            start_naive = pd.Timestamp(end_naive.year, ((end_naive.month - 1) // 3) * 3 + 1, 1)
            start = pd.Timestamp(start_naive, tz='UTC')
            end = data_end  # Use timezone-aware data_end
            
            # If QTD range has no data, use full available range
            if start < data_start:
                start = data_start
                
        elif mode == "YTD":
            # Try YTD, fallback to available data if no data in current year
            end_naive = data_end.replace(tzinfo=None) if hasattr(data_end, 'tz') and data_end.tz is not None else data_end
            start_naive = pd.Timestamp(end_naive.year, 1, 1)
            start = pd.Timestamp(start_naive, tz='UTC')
            end = data_end  # Use timezone-aware data_end
            
            # If YTD range has no data, use full available range  
            if start < data_start:
                start = data_start
                
        elif mode == "ITD":
            start = data_start
            end = data_end
        else:
            start = data_start
            end = data_end
        return start, end

    @reactive.calc
    def contributed_capital():
        """Calculate contributed capital (equity accounts 3xxx)"""
        df = gl_data()
        if df.empty:
            return 0
            
        latest_date = df['date'].max()
        from ..financial_reporting.data_processor import safe_date_compare
        historical_df = df[safe_date_compare(df['date'], latest_date)].copy()
        historical_df['GL_Acct_Number'] = historical_df['GL_Acct_Number'].astype(str)
        
        # Filter for equity accounts (3xxx)
        equity_df = historical_df[historical_df['GL_Acct_Number'].str.startswith('3')]
        
        if equity_df.empty:
            return 0
        
        # Equity accounts have normal credit balance
        total_credits = equity_df['credit_crypto'].sum()
        total_debits = equity_df['debit_crypto'].sum()
        
        return total_credits - total_debits

    @output
    @render.data_frame
    def latest_summary():
        """Show latest NAV breakdown"""
        from .helpers import calculate_nav_for_date
        
        gl_df = gl_data()
        if gl_df.empty:
            return pd.DataFrame({"Metric": ["No Data"], "Value": ["No GL data available"]})
        
        latest_date = gl_df['date'].max()
        total_assets, total_liabilities, nav = calculate_nav_for_date(latest_date)
        
        summary = pd.DataFrame({
            "Metric": ["Latest Date", "Total Assets", "Total Liabilities", "NAV"],
            "Value": [
                latest_date.strftime("%Y-%m-%d"),
                f"{total_assets:,.2f}",
                f"{total_liabilities:,.2f}",
                f"{nav:,.2f}"
            ]
        })
        
        return summary
    @output
    @render.ui
    def contributed_capital_box():
        cap = contributed_capital()
        return f"{cap:,.2f} ETH"
    
    @reactive.calc
    def net_income_series():
        """Calculate net income over time (income - expenses)"""
        df = gl_data()
        if df.empty:
            return pd.Series(dtype=float)
            
        df['GL_Acct_Number'] = df['GL_Acct_Number'].astype(str)
        
        # Get unique dates
        unique_dates = sorted(df['date'].dropna().unique())
        
        income_data = []
        for date in unique_dates:
            from ..financial_reporting.data_processor import safe_date_compare
            historical_df = df[safe_date_compare(df['date'], date)].copy()
            
            # Income accounts (4xxx and 9xxx) - normal credit balance, flip sign for positive display
            income_df = historical_df[historical_df['GL_Acct_Number'].str.match(r'^[49]')]
            total_income = income_df['credit_crypto'].sum() - income_df['debit_crypto'].sum()
            
            # Expense accounts (8xxx) - normal debit balance, flip sign for positive display  
            expense_df = historical_df[historical_df['GL_Acct_Number'].str.startswith('8')]
            total_expenses = -(expense_df['debit_crypto'].sum() - expense_df['credit_crypto'].sum())
            
            net_income = total_income - total_expenses
            income_data.append({'date': date, 'net_income': net_income})
        
        income_df = pd.DataFrame(income_data)
        income_series = pd.Series(income_df['net_income'].values, index=pd.DatetimeIndex(income_df['date']))
        
        return income_series.sort_index()
    
    @output
    @render_plotly
    def net_income_chart():
        income = net_income_series()
        if income.empty:
            return go.Figure()
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=income.index.to_pydatetime(),
            y=income.values,
            mode="lines+markers",
            name="Net Income",
            line=dict(width=2, color="green")
        ))
        # Apply theme configuration piece by piece
        from ...theme_manager import theme_manager
        layout_config = theme_manager.get_plotly_layout_config()
        
        # Apply layout settings explicitly
        fig.update_layout(
            paper_bgcolor=layout_config["paper_bgcolor"],
            plot_bgcolor=layout_config["plot_bgcolor"],
            font=layout_config["font"],
            title=layout_config["title"],
            colorway=layout_config["colorway"],
            legend=layout_config["legend"],
            hoverlabel=layout_config["hoverlabel"],
            hovermode="x unified"
        )
        
        # Override title text specifically
        fig.update_layout(title_text="Net Income Over Time")
        
        # Get theme colors directly
        theme_data = theme_manager.get_current_theme_data()
        text_color = theme_data.get("colors", {}).get("text_primary", "#f8fafc")
        
        # Apply axis configurations with subtle, transparent grid lines
        fig.update_xaxes(
            title_text="Date",
            title_font={"color": text_color, "size": 12},
            type="date",
            tickfont={"color": text_color, "size": 10},
            linecolor="rgba(148, 163, 184, 0.3)",
            gridcolor="rgba(148, 163, 184, 0.15)",
            tickcolor=text_color,
            zerolinecolor="rgba(148, 163, 184, 0.4)",
            showgrid=True,
            showline=True,
            zeroline=True,
            gridwidth=0.5
        )
        
        fig.update_yaxes(
            title_text="Net Income",
            title_font={"color": text_color, "size": 12},
            tickfont={"color": text_color, "size": 10},
            linecolor="rgba(148, 163, 184, 0.3)",
            gridcolor="rgba(148, 163, 184, 0.15)",
            tickcolor=text_color,
            zerolinecolor="rgba(148, 163, 184, 0.4)",
            showgrid=True,
            showline=True,
            zeroline=True,
            gridwidth=0.5
        )
        return fig

    
    @output
    @render.ui
    def custom_date_range_ui():
        """Conditionally show custom date range when Custom is selected"""
        if input.nav_range_mode() == "Custom":
            # Get available data range from GL data
            gl_df = gl_data()
            if not gl_df.empty:
                min_date = gl_df['date'].min().date()  # Convert to date object
                max_date = gl_df['date'].max().date()  # Convert to date object
            else:
                # Fallback dates if no data
                min_date = None
                max_date = None
            
            return ui.div(
                ui.input_date_range(
                    "nav_custom_range", 
                    "Custom date range:", 
                    start=min_date, 
                    end=max_date
                ),
                class_="custom-date-container"
            )
        else:
            return ui.div()  # Empty div when not needed

    # ============================================================================
    # PCAP Server Functions
    # ============================================================================
    
    # PCAP reactive state
    pcap_data = reactive.value(None)
    pcap_summary = reactive.value(None)
    export_status = reactive.value(None)
    
    # PDF Export selection state
    available_funds = reactive.value({})
    available_lps = reactive.value([])
    
    @output
    @render.ui
    def pcap_date_input():
        """Create dynamic date input based on available GL data"""
        try:
            gl_df = gl_data()
            
            if gl_df.empty or 'date' not in gl_df.columns or gl_df['date'].isna().all():
                return ui.input_date(
                    "pcap_as_of_date",
                    "As of Date",
                    value=datetime.now().date(),
                    min=datetime.now().date(),
                    max=datetime.now().date()
                )
            
            # Get date range from GL data
            min_date = gl_df['date'].min().date()
            max_date = gl_df['date'].max().date()
            
            return ui.input_date(
                "pcap_as_of_date",
                "As of Date",
                value=max_date,  # Default to latest date
                min=min_date,
                max=max_date
            )
        except Exception as e:
            return ui.input_date(
                "pcap_as_of_date",
                "As of Date",
                value=datetime.now().date(),
                min=datetime.now().date(),
                max=datetime.now().date()
            )
    
    @output
    @render.ui
    def fund_selection():
        """Create fund selection dropdown"""
        # Use the same fund structure as the main application
        funds = {
            "fund_i_class_B_ETH": "Fund I - Class B",
            "fund_ii_class_B_ETH": "Fund II - Class B", 
            "holdings_class_B_ETH": "Holdings - Class B"
        }
        
        available_funds.set(funds)
        
        return ui.div(
            ui.input_select(
                "pcap_fund_select",
                "Select Fund",
                choices=funds,
                selected="fund_i_class_B_ETH"  # Default to Fund I - Class B (matches GL data)
            ),
            class_="dropdown-with-arrow"
        )
    
    @output
    @render.ui
    def lp_selection():
        """Create LP selection dropdown based on GL data"""
        try:
            gl_df = gl_data()
            
            if gl_df.empty or 'limited_partner_ID' not in gl_df.columns:
                return ui.div(
                    ui.input_select(
                        "pcap_lp_select",
                        "Select Limited Partner",
                        choices={"ALL": "All LPs"},
                        selected="ALL"
                    ),
                    class_="dropdown-with-arrow"
                )
        except Exception as e:
            return ui.div(
                ui.input_select(
                    "pcap_lp_select",
                    "Select Limited Partner",
                    choices={"ALL": "All LPs"},
                    selected="ALL"
                ),
                class_="dropdown-with-arrow"
            )
        
        # Get unique LP IDs from the GL data
        lp_ids = sorted(gl_df['limited_partner_ID'].dropna().unique())
        lp_choices = {lp_id: f"LP {lp_id}" for lp_id in lp_ids}
        lp_choices["ALL"] = "All LPs"  # Option to generate for all LPs
        
        available_lps.set(lp_ids)
        
        return ui.div(
            ui.input_select(
                "pcap_lp_select",
                "Select Limited Partner", 
                choices=lp_choices,
                selected="ALL"  # Default to all LPs
            ),
            class_="dropdown-with-arrow"
        )
    
    # Old PCAP generation disabled - now using GL-based PCAP
    
    @reactive.effect
    @reactive.event(input.generate_pcap)
    def generate_pcap_report():
        """Generate simple PCAP report with line items from SCPC"""
        print("Generating Simple PCAP report...")
        from .simple_pcap_function import simple_pcap_function
        simple_pcap_function(pcap_data, pcap_summary, input)
    
    @output
    @render.ui  
    def pcap_status():
        """Display PCAP generation status"""
        return ui.p("Ready to generate PCAP", class_="text-muted")
    
    def format_currency_amount(amount, currency):
        """Format amount based on currency type"""
        if currency == 'ETH':
            return f"{amount:,.6f} ETH"
        else:  # USD
            return f"${amount:,.2f}"
    @output
    @render.ui
    def pcap_status():
        """Display PCAP generation status"""
        return ui.p("Ready to generate PCAP", class_="text-muted")
    
    def format_currency_amount(amount, currency):
        """Format amount based on currency type"""
        if currency == 'ETH':
            return f"{amount:,.6f} ETH"
            print("\n=== Data Source Check ===")
            print(f"✓ GL Data: {len(gl_df)} records")
            
            # TODO: Add these data sources when available
            print("⚠ LP Commitments: Not loaded (would contain commitment amounts, terms)")
            print("⚠ COA Data: Not loaded (would help classify accounts)")
            print("⚠ Master TB: Not loaded (would provide additional balances)")
            print("⚠ Wallet Mappings: Not loaded (would map crypto addresses)")
            
            # Create date range with proper timezone handling
            from .PCAP.excess import ensure_timezone_aware
            
            try:
                min_date = gl_df['date'].min()
                max_date = gl_df['date'].max()
                
                print(f"Date range extraction: min={min_date}, max={max_date}")
                
                # Ensure as_of_date is timezone-aware to match GL data
                as_of_date_tz = ensure_timezone_aware(as_of_date)
                
                # Compare timezone-aware timestamps
                max_date = min(max_date, as_of_date_tz)
                
                print(f"Final date range: {min_date} to {max_date}")
                
                # Create date range (timezone will be inherited from min_date/max_date)
                date_range = pd.date_range(min_date, max_date, freq='D')
                print(f"Created date range with {len(date_range)} days")
                
            except Exception as date_error:
                error_msg = f"Error creating date range: {str(date_error)}"
                print(f"Date error details: {error_msg}")
                print(f"GL date column type: {gl_df['date'].dtype}")
                print(f"GL date sample: {gl_df['date'].head()}")
                pcap_data.set(pd.DataFrame())
                pcap_summary.set({'error': error_msg})
                return
            
            # Create grid and populate with actual GL transactions
            grid_data = []
            for lp_id in lp_ids:
                for date in date_range:
                    # Initialize with zeros - use column names expected by PCAP function
                    row_data = {
                        'limited_partner_ID': lp_id,
                        'date': date,
                        'cap_contrib_bod': Decimal('0'),  # Beginning of day contributions
                        'cap_contrib_eod': Decimal('0'),  # End of day contributions
                        'cap_dist_bod': Decimal('0'),     # Beginning of day distributions
                        'cap_dist_eod': Decimal('0'),     # End of day distributions
                        'mgmt_fee_amt': Decimal('0'),
                        'gp_incentive_amt': Decimal('0'), # GP incentive fees
                        'fund_pnl_amt': Decimal('0'),
                        'beg_bal': Decimal('0'),          # Beginning balance
                        'allocated_pnl': Decimal('0'),
                        'end_bal': Decimal('0')           # Ending balance
                    }
                    
                    # Get actual GL entries for this LP and date
                    if 'limited_partner_ID' in gl_df.columns:
                        lp_entries = gl_df[
                            (gl_df['limited_partner_ID'] == lp_id) & 
                            (gl_df['date'].dt.date == date.date())
                        ]
                        
                        if not lp_entries.empty:
                            # Look for capital contribution accounts (typically 3xxx accounts)
                            contrib_entries = lp_entries[
                                lp_entries['account_name'].str.contains('capital_contributions', case=False, na=False) |
                                lp_entries['GL_Acct_Number'].astype(str).str.startswith('3', na=False)
                            ]
                            if not contrib_entries.empty:
                                # Capital contributions: credits increase capital
                                # For simplicity, put all contributions in End of Day
                                row_data['cap_contrib_eod'] = Decimal(str(contrib_entries['credit_crypto'].sum()))
                            
                            # Look for distribution accounts
                            dist_entries = lp_entries[
                                lp_entries['account_name'].str.contains('distribution', case=False, na=False)
                            ]
                            if not dist_entries.empty:
                                # Distributions: debits reduce capital
                                # For simplicity, put all distributions in End of Day
                                row_data['cap_dist_eod'] = Decimal(str(dist_entries['debit_crypto'].sum()))
                            
                            # Look for management fee accounts
                            mgmt_entries = lp_entries[
                                lp_entries['account_name'].str.contains('management|fee', case=False, na=False)
                            ]
                            if not mgmt_entries.empty:
                                row_data['mgmt_fee_amt'] = Decimal(str(mgmt_entries['debit_crypto'].sum()))
                    
                    grid_data.append(row_data)
            
            grid_df = pd.DataFrame(grid_data)
            print(f"Created PCAP grid: {len(grid_df)} rows for {len(lp_ids)} LPs over {len(date_range)} days")
            
            # Calculate running balances for each LP
            print("Calculating running balances...")
            for lp_id in lp_ids:
                lp_mask = grid_df['limited_partner_ID'] == lp_id
                lp_data = grid_df[lp_mask].copy().sort_values('date')
                
                running_balance = Decimal('0')
                for idx, row in lp_data.iterrows():
                    # Set beginning balance
                    grid_df.at[idx, 'beg_bal'] = running_balance
                    
                    # Calculate daily change
                    daily_contrib = (row['cap_contrib_bod'] + row['cap_contrib_eod'])
                    daily_dist = (row['cap_dist_bod'] + row['cap_dist_eod'])
                    daily_fees = row['mgmt_fee_amt'] + row['gp_incentive_amt']
                    daily_pnl = row['allocated_pnl']  # Will be calculated from fund P&L later
                    
                    # Update running balance
                    running_balance += daily_contrib - daily_dist - daily_fees + daily_pnl
                    
                    # Set ending balance
                    grid_df.at[idx, 'end_bal'] = running_balance
                    
            print("Running balances calculated")
            
            # Calculate actual fund P&L from GL data
            fund_pnl_by_group = {}
            
            # Identify P&L accounts (revenue/income and expense accounts)
            revenue_accounts = []
            expense_accounts = []
            
            if 'GL_Acct_Number' in gl_df.columns:
                # Revenue accounts typically start with 4
                revenue_accounts = gl_df[gl_df['GL_Acct_Number'].astype(str).str.startswith('4', na=False)]['GL_Acct_Number'].unique()
                # Expense accounts typically start with 5, 6, 7, 8, 9
                expense_accounts = gl_df[gl_df['GL_Acct_Number'].astype(str).str.match(r'^[56789]', na=False)]['GL_Acct_Number'].unique()
                
                print(f"Found {len(revenue_accounts)} revenue accounts and {len(expense_accounts)} expense accounts")
            
            for date in date_range:
                # Ensure consistent timezone handling for date comparison
                if hasattr(date, 'tz_localize') and date.tz is None:
                    date_normalized = date.tz_localize('UTC')
                else:
                    date_normalized = date
                
                daily_gl = gl_df[gl_df['date'].dt.date == date_normalized.date()]
                
                if not daily_gl.empty:
                    # Calculate P&L from revenue and expense accounts
                    daily_revenue = Decimal('0')
                    daily_expenses = Decimal('0')
                    
                    if len(revenue_accounts) > 0:
                        revenue_entries = daily_gl[daily_gl['GL_Acct_Number'].isin(revenue_accounts)]
                        if not revenue_entries.empty:
                            # Revenue accounts: credits increase, debits decrease
                            daily_revenue = Decimal(str(revenue_entries['credit_crypto'].sum() - revenue_entries['debit_crypto'].sum()))
                    
                    if len(expense_accounts) > 0:
                        expense_entries = daily_gl[daily_gl['GL_Acct_Number'].isin(expense_accounts)]
                        if not expense_entries.empty:
                            # Expense accounts: debits increase, credits decrease
                            daily_expenses = Decimal(str(expense_entries['debit_crypto'].sum() - expense_entries['credit_crypto'].sum()))
                    
                    # Net income = Revenue - Expenses
                    daily_pnl = daily_revenue - daily_expenses
                    fund_pnl_by_group[date_normalized.strftime('%Y-%m-%d')] = daily_pnl
                    
                    if daily_pnl != 0:
                        print(f"Date {date_normalized.date()}: Revenue {daily_revenue}, Expenses {daily_expenses}, Net P&L {daily_pnl}")
                else:
                    fund_pnl_by_group[date_normalized.strftime('%Y-%m-%d')] = Decimal('0')
            
            # Convert fund_pnl_by_group dict to DataFrame format expected by PCAP function
            fund_pnl_df_list = []
            for date_str, pnl_amount in fund_pnl_by_group.items():
                fund_pnl_df_list.append({
                    'date': pd.to_datetime(date_str),
                    'SCPC': 'Net Income',  # Default SCPC category
                    'schedule_ranking': 1,  # Default ranking
                    'amount': float(pnl_amount)
                })
            
            fund_pnl_df = pd.DataFrame(fund_pnl_df_list)
            print(f"Created fund P&L DataFrame: {len(fund_pnl_df)} rows")
            
            # Run PCAP allocation
            grid_with_allocation, alloc_rows, allocation_summary_df = run_partner_capital_pcap_allocation(grid_df, fund_pnl_df)
            
            # Add required columns for PCAP processing
            if 'running_nav' not in grid_with_allocation.columns:
                grid_with_allocation['running_nav'] = grid_with_allocation['end_bal']
            
            if 'gp_management_fee' not in grid_with_allocation.columns:
                grid_with_allocation['gp_management_fee'] = Decimal('0')
            
            if 'gp_carried_interest' not in grid_with_allocation.columns:
                grid_with_allocation['gp_carried_interest'] = Decimal('0')
            
            if 'gp_total_incentive' not in grid_with_allocation.columns:
                grid_with_allocation['gp_total_incentive'] = Decimal('0')
            
            # Load COA data for PCAP generation
            from ...s3_utils import load_COA_file
            try:
                df_coa = load_COA_file()
                print(f"Loaded COA data: {len(df_coa)} records")
                if not df_coa.empty and 'SCPC' in df_coa.columns:
                    print(f"COA SCPCs available: {df_coa['SCPC'].nunique()}")
                    print(f"COA columns: {list(df_coa.columns)}")
            except Exception as coa_error:
                print(f"Warning: Could not load COA data: {coa_error}")
                df_coa = pd.DataFrame()
            
            # Create complete PCAP report with COA data
            pcap_report = create_complete_fund_pcap_with_gp(
                grid_with_allocation, 
                pd.DataFrame(),  # Empty allocations_df for now
                df_coa=df_coa
            )
            
            # Calculate summary with safe column access
            # Extract data from the new PCAP summary report structure
            # The new format has Line_Item and ITD columns instead of daily columns
            total_contributions = 0
            total_distributions = 0
            total_pnl = 0
            fund_nav = 0
            
            if not pcap_report.empty and 'ITD' in pcap_report.columns and 'Line_Item' in pcap_report.columns:
                # Extract contributions
                contrib_rows = pcap_report[pcap_report['Line_Item'].str.contains('Capital contributions', case=False, na=False)]
                if not contrib_rows.empty:
                    total_contributions = float(contrib_rows['ITD'].sum())
                
                # Extract distributions  
                dist_rows = pcap_report[pcap_report['Line_Item'].str.contains('Capital distributions', case=False, na=False)]
                if not dist_rows.empty:
                    total_distributions = float(abs(dist_rows['ITD'].sum()))  # Make positive for display
                
                # Extract P&L
                pnl_rows = pcap_report[pcap_report['Line_Item'].str.contains('Allocated profit', case=False, na=False)]
                if not pnl_rows.empty:
                    total_pnl = float(pnl_rows['ITD'].sum())
                
                # Extract ending balance (NAV)
                nav_rows = pcap_report[pcap_report['Line_Item'].str.contains('Ending capital balance', case=False, na=False)]
                if not nav_rows.empty:
                    fund_nav = float(nav_rows['ITD'].sum())
            
            print(f"🔍 Fund NAV Calculation Debug ({currency}):")
            print(f"  - PCAP Report Shape: {pcap_report.shape}")
            print(f"  - PCAP Report Columns: {list(pcap_report.columns)}")
            
            # Check if date column exists for date range
            if 'date' in pcap_report.columns and not pcap_report['date'].isna().all():
                print(f"  - Date Range: {pcap_report['date'].min()} to {pcap_report['date'].max()}")
            else:
                print(f"  - Date Range: No date column in PCAP report")
                
            print(f"  - Total Contributions: {total_contributions:,.6f}")
            print(f"  - Total Distributions: {total_distributions:,.6f}")
            print(f"  - Total P&L: {total_pnl:,.6f}")
            print(f"  - Manual NAV Calc: {total_contributions - total_distributions + total_pnl:,.6f}")
            # The new PCAP report structure doesn't have running_nav, it's a summary report
            print(f"  - PCAP Report Summary (ITD values from report):")
            
            # Try to extract NAV from the summary data
            if not pcap_report.empty and 'ITD' in pcap_report.columns:
                # Look for ending balance line items
                ending_balance_rows = pcap_report[
                    pcap_report['Line_Item'].str.contains('Ending capital balance', case=False, na=False)
                ]
                if not ending_balance_rows.empty:
                    fund_nav_from_report = float(ending_balance_rows['ITD'].sum())
                    print(f"  - Fund NAV from PCAP Report: {fund_nav_from_report:,.6f}")
                    fund_nav = fund_nav_from_report  # Use the report value
                else:
                    print(f"  - No ending balance found in PCAP report")
                    
                # Show a sample of the report data
                print(f"  - Sample PCAP line items:")
                for _, row in pcap_report.head(3).iterrows():
                    print(f"    {row['Line_Item']}: ITD = {row['ITD']:,.6f}")
            else:
                print(f"  - No ITD column found in PCAP report")
            
            print(f"  - Final Fund NAV: {fund_nav:,.6f}")
            
            summary = {
                'as_of_date': as_of_date,
                'currency': currency,
                'total_contributions': total_contributions,
                'total_distributions': total_distributions,
                'fund_nav': fund_nav,
                'gp_incentives': 0,  # Will need to extract from report if available
                'num_lps': len(pcap_report['limited_partner_ID'].unique()) if not pcap_report.empty and 'limited_partner_ID' in pcap_report.columns else 0
            }
            
            # Store results
            pcap_data.set(pcap_report)
            pcap_summary.set(summary)
    @output
    @render.ui
    def pcap_status():
        """Display PCAP generation status"""
        summary = pcap_summary.get()
        
        if summary is None:
            return ui.div(
                ui.p("No PCAP report generated yet. Click 'Generate PCAP' to create a report."),
                class_="text-muted"
            )
        
        if 'error' in summary:
            return ui.div(
                ui.p(f"Error: {summary['error']}", class_="text-danger"),
                class_="alert alert-danger"
            )
        
        return ui.div(
            ui.p(f"✅ PCAP report generated successfully"),
            ui.p(f"As of: {summary.get('as_of_date', 'N/A')}"),
            ui.p(f"Currency: {summary.get('currency', 'N/A')}"),
            ui.p(f"Limited Partners: {summary.get('num_lps', 0)}"),
            class_="alert alert-success"
        )
    
    def format_currency_amount(amount, currency):
        """Format amount based on currency type"""
        if currency == 'ETH':
            return f"{amount:,.6f} ETH"
        else:  # USD
            return f"${amount:,.2f}"
    
    @output
    @render.ui
    def pcap_total_contributions():
        """Display total LP contributions - Current Month"""
        data = pcap_data.get()
        summary = pcap_summary.get()
        
        if data is None or data.empty or summary is None or 'error' in summary:
            return "N/A"
            
        currency = summary.get('currency', 'ETH')
        
        # Get current month contributions from PCAP report
        contrib_rows = data[data['Line_Item'] == 'Capital Contributions']
        if not contrib_rows.empty:
            amount = contrib_rows['Current_Month'].sum()
        else:
            amount = 0
            
        return format_currency_amount(amount, currency)
    
    @output
    @render.ui
    def pcap_total_distributions():
        """Display total LP distributions - Current Month"""
        data = pcap_data.get()
        summary = pcap_summary.get()
        
        if data is None or data.empty or summary is None or 'error' in summary:
            return "N/A"
            
        currency = summary.get('currency', 'ETH')
        
        # Get current month distributions from PCAP report (make positive for display)
        dist_rows = data[data['Line_Item'] == 'Capital Distributions']
        if not dist_rows.empty:
            amount = abs(dist_rows['Current_Month'].sum())  # Make positive for display
        else:
            amount = 0
            
        return format_currency_amount(amount, currency)
    
    @output
    @render.ui
    def pcap_fund_nav():
        """Display fund NAV - Current Month Ending Balance"""
        data = pcap_data.get()
        summary = pcap_summary.get()
        
        if data is None or data.empty or summary is None or 'error' in summary:
            return "N/A"
            
        currency = summary.get('currency', 'ETH')
        
        # Get current month ending capital from PCAP report
        ending_rows = data[data['Line_Item'] == 'Ending Capital']
        if not ending_rows.empty:
            amount = ending_rows['Current_Month'].sum()
        else:
            amount = 0
            
        return format_currency_amount(amount, currency)
    
    @output
    @render.ui
    def pcap_gp_incentives():
        """Display GP total incentives - Current Month"""
        data = pcap_data.get()
        summary = pcap_summary.get()
        
        if data is None or data.empty or summary is None or 'error' in summary:
            return "N/A"
            
        currency = summary.get('currency', 'ETH')
        
        # Get current month GP incentive fees from PCAP report (make positive for display)
        gp_rows = data[data['Line_Item'] == 'GP Incentive Fees']
        if not gp_rows.empty:
            amount = abs(gp_rows['Current_Month'].sum())  # Make positive for display
        else:
            amount = 0
            
        return format_currency_amount(amount, currency)
    
    @output
    @render.data_frame
    def pcap_lp_summary():
        """LP Summary section removed per user request"""
        # Return empty DataFrame since UI section was removed
        return pd.DataFrame()
    
    @output
    @render.data_frame
    def pcap_daily_details():
        """Display daily PCAP details"""
        data = pcap_data.get()
        
        if data is None or data.empty:
            summary = pcap_summary.get()
            currency = summary.get('currency', 'USD') if summary else 'USD'
            
            if currency == 'ETH':
                return pd.DataFrame({
                    'Date': ['No data available'],
                    'LP': [''],
                    'Beginning Balance': ['0.000000 ETH'],
                    'Contributions': ['0.000000 ETH'],
                    'Distributions': ['0.000000 ETH'],
                    'Allocated P&L': ['0.000000 ETH'],
                    'Ending Balance': ['0.000000 ETH']
                })
            else:
                return pd.DataFrame({
                    'Date': ['No data available'],
                    'LP': [''],
                    'Beginning Balance': ['$0.00'],
                    'Contributions': ['$0.00'],
                    'Distributions': ['$0.00'],
                    'Allocated P&L': ['$0.00'],
                    'Ending Balance': ['$0.00']
                })
        
        # Handle new PCAP summary structure vs old daily structure  
        if 'Line_Item' in data.columns and 'ITD' in data.columns:
            # New PCAP summary structure - show the summary report
            display_cols = ['Line_Item', 'SCPC', 'Category', 'Current_Month', 'Current_Quarter', 'Current_Year', 'ITD']
            available_display_cols = [col for col in display_cols if col in data.columns]
            details = data[available_display_cols].copy()
            
            # No date formatting needed for summary data
        else:
            # Old daily structure - prepare daily details with available columns
            available_cols = ['limited_partner_ID']
            column_mapping = {'limited_partner_ID': 'LP'}
            
            # Only add date if it exists
            if 'date' in data.columns:
                available_cols.insert(0, 'date')
                column_mapping['date'] = 'Date'
            
            if 'beg_bal' in data.columns:
                available_cols.append('beg_bal')
                column_mapping['beg_bal'] = 'Beginning Balance'
            if 'cap_contrib' in data.columns:
                available_cols.append('cap_contrib')
                column_mapping['cap_contrib'] = 'Contributions'
            if 'cap_dist' in data.columns:
                available_cols.append('cap_dist')
                column_mapping['cap_dist'] = 'Distributions'
            if 'allocated_pnl' in data.columns:
                available_cols.append('allocated_pnl')
                column_mapping['allocated_pnl'] = 'Allocated P&L'
            if 'end_bal' in data.columns:
                available_cols.append('end_bal')
                column_mapping['end_bal'] = 'Ending Balance'
            elif 'running_nav' in data.columns:
                available_cols.append('running_nav')
                column_mapping['running_nav'] = 'Ending Balance'
            
            details = data[available_cols].copy()
            details.rename(columns=column_mapping, inplace=True)
            
            # Format date if it exists
            if 'Date' in details.columns:
                details['Date'] = pd.to_datetime(details['Date']).dt.strftime('%Y-%m-%d')
        
        # Format numeric columns based on currency
        summary = pcap_summary.get()
        currency = summary.get('currency', 'USD') if summary else 'USD'
        
        # Format numeric columns based on structure
        if 'Line_Item' in data.columns and 'ITD' in data.columns:
            # New PCAP structure - format time period columns
            numeric_cols = ['Current_Month', 'Current_Quarter', 'Current_Year', 'ITD']
        else:
            # Old structure - format balance columns
            numeric_cols = ['Beginning Balance', 'Contributions', 'Distributions', 'Allocated P&L', 'Ending Balance']
            
        for col in numeric_cols:
            if col in details.columns:
                if currency == 'ETH':
                    details[col] = details[col].apply(lambda x: f"{float(x):,.6f} ETH")
                else:  # USD
                    details[col] = details[col].apply(lambda x: f"${float(x):,.2f}")
        
        return details
    
    
    @render.download(filename=lambda: f"PCAP_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf")
    async def export_pcap_pdf():
        """Generate PCAP PDF report using existing PDF creator"""
        print("📄 Generating PCAP PDF report...")
        try:
            data = pcap_data.get()
            summary = pcap_summary.get()
            
            if data is None or data.empty:
                # Return empty bytes if no data
                yield b""
                return
            
            # Get user selections from PCAP generation inputs
            selected_fund_id = input.pcap_fund_select() if hasattr(input, 'pcap_fund_select') else "fund_i_class_B_ETH"
            currency = input.pcap_currency() if hasattr(input, 'pcap_currency') else "ETH"
            as_of_date = input.pcap_as_of_date()
            
            import json
            import os
            from datetime import datetime
            
            # Get performance metrics
            metrics = summary.get('performance_metrics', {}) if summary else {}
            net_irr = metrics.get('net_irr', 'N/A')
            gross_moic = metrics.get('gross_moic', 'N/A')
            capital_committed = metrics.get('capital_committed', 'N/A')
            capital_called = metrics.get('capital_called', 'N/A')
            
            # Convert PCAP data to statement_of_changes format with 6 decimal places
            statement_of_changes = []
            for _, row in data.iterrows():
                statement_of_changes.append({
                    "label": row['Line_Item'],
                    "mtd": round(float(row['MTD']), 6) if pd.notnull(row['MTD']) else 0.0,
                    "qtd": round(float(row['QTD']), 6) if pd.notnull(row['QTD']) else 0.0,
                    "ytd": round(float(row['YTD']), 6) if pd.notnull(row['YTD']) else 0.0,
                    "itd": round(float(row['ITD']), 6) if pd.notnull(row['ITD']) else 0.0
                })
            
            # Create commitment summary with 6 decimal places
            def format_metric(value):
                if value == 'N/A' or value == '-':
                    return value
                try:
                    return f"{float(value):.6f}"
                except:
                    return value
            
            commitment_summary = {
                "Total commitments": format_metric(capital_committed),
                "Capital called": format_metric(capital_called),
                "Remaining commitments": "-"
            }
            
            # Create performance metrics (Net IRR stays as is with 2 decimals, others get 6)
            performance_metrics = {
                "Net IRR": net_irr,  # Already formatted with 2 decimals
                "Gross MOIC": format_metric(gross_moic),
                "NAV per unit": "-"
            }
            
            # Create the JSON data structure expected by the PDF creator
            # Convert date to human readable format (no time components)
            if hasattr(as_of_date, 'strftime'):
                main_date_str = as_of_date.strftime("%B %d, %Y")  # e.g., "July 31, 2024"
                period_ended_date = as_of_date.strftime("%Y%m%d")  # for filename
            else:
                # If it's already a string, try to parse and reformat
                try:
                    parsed_date = datetime.strptime(str(as_of_date), "%Y-%m-%d")
                    main_date_str = parsed_date.strftime("%B %d, %Y")
                    period_ended_date = parsed_date.strftime("%Y%m%d")
                except:
                    main_date_str = str(as_of_date)
                    period_ended_date = datetime.now().strftime("%Y%m%d")
            
            json_data = {
                "main_date": main_date_str,
                "currency": currency.upper(),
                "statement_of_changes": statement_of_changes,
                "commitment_summary": commitment_summary,
                "performance_metrics": performance_metrics
            }
            
            # Create JSON directory if it doesn't exist
            pdf_creator_dir = os.path.join(os.path.dirname(__file__), "PCAP", "PDF Creator")
            json_dir = os.path.join(pdf_creator_dir, "json_data")
            os.makedirs(json_dir, exist_ok=True)
            
            # Generate JSON filename including period ended date
            json_filename = f"investor_data_{period_ended_date}_to_{period_ended_date}.json"
            json_path = os.path.join(json_dir, json_filename)
            
            # Save JSON data
            with open(json_path, 'w') as f:
                json.dump(json_data, f, indent=2)
            
            print(f"📄 JSON data saved to: {json_path}")
            
            # Call the existing PDF generator
            try:
                import subprocess
                import sys
                
                # Change to PDF creator directory
                original_cwd = os.getcwd()
                os.chdir(pdf_creator_dir)
                
                # Run the simplified PDF generator script
                result = subprocess.run([sys.executable, "simple_pdf_gen.py"], 
                                      capture_output=True, text=True, cwd=pdf_creator_dir)
                
                # Change back to original directory
                os.chdir(original_cwd)
                
                if result.returncode == 0:
                    print(f"✅ PDF generated successfully")
                    print(f"PDF output: {result.stdout}")
                    
                    # Look for the generated PDF file
                    pdf_files = [f for f in os.listdir(pdf_creator_dir) if f.endswith('.pdf') and 'PCAP_Report' in f]
                    if pdf_files:
                        # Get the most recent PDF file
                        latest_pdf = max(pdf_files, key=lambda x: os.path.getctime(os.path.join(pdf_creator_dir, x)))
                        pdf_path = os.path.join(pdf_creator_dir, latest_pdf)
                        
                        # Read PDF file and return content for download
                        try:
                            with open(pdf_path, 'rb') as f:
                                pdf_content = f.read()
                            
                            print(f"📄 PDF ready for download: {latest_pdf}")
                            print(f"📄 PDF content type: {type(pdf_content)}")
                            print(f"📄 PDF content size: {len(pdf_content)} bytes")
                            
                            if isinstance(pdf_content, bytes):
                                yield pdf_content
                            else:
                                print(f"❌ Unexpected content type: {type(pdf_content)}")
                                yield b""
                                
                        except Exception as read_error:
                            print(f"❌ Error reading PDF file: {read_error}")
                            yield b""
                    else:
                        print("❌ No PDF file found after generation")
                        yield b""
                else:
                    error_msg = f"PDF generation failed: {result.stderr}"
                    print(f"❌ {error_msg}")
                    yield b""
                    
            except Exception as pdf_error:
                error_msg = f"Error calling PDF generator: {str(pdf_error)}"
                print(f"❌ {error_msg}")
                yield b""
                
        except Exception as e:
            error_msg = f"Error generating PDF: {str(e)}"
            print(f"❌ {error_msg}")
            import traceback
            traceback.print_exc()
            yield b""
    
    
    @output
    @render.ui
    def pcap_export_status():
        """Display export status messages"""
        status = export_status.get()
        
        if status is None:
            return ui.div(
                ui.p("Export status will be shown here after export operations."),
                class_="text-muted"
            )
        
        if status.get('success'):
            icon_class = "text-success"
            status_icon = "✅"
        else:
            icon_class = "text-danger"
            status_icon = "❌"
        
        export_type = status.get('type', 'export').upper()
        message = status.get('message', 'Export completed')
        
        status_content = [
            ui.p(f"{status_icon} {export_type}: {message}", class_=icon_class)
        ]
        
        # Show fund and LP selection if available
        if status.get('success') and (status.get('fund') or status.get('lp')):
            details = []
            if status.get('fund'):
                details.append(f"Fund: {status['fund']}")
            if status.get('lp'):
                lp_text = "All LPs" if status['lp'] == "ALL" else f"LP: {status['lp']}"
                details.append(lp_text)
            
            if details:
                status_content.append(
                    ui.p(" | ".join(details), class_="text-info small")
                )
        
        # Show file paths if available
        if status.get('files') and status.get('success'):
            status_content.append(
                ui.p(f"Files saved to: {status['files'][0].rsplit('/', 1)[0] if status['files'] else 'output directory'}", 
                     class_="text-muted small")
            )
        
        return ui.div(*status_content)

    # Missing PCAP UI outputs that were removed during cleanup
    @output
    @render.ui  
    def pcap_results_header():
        """Display PCAP results header"""
        data = pcap_data.get()
        summary = pcap_summary.get()
        
        if data is None or data.empty:
            return ui.div()
            
        if summary and summary.get('error'):
            return ui.div(
                ui.h4("❌ PCAP Generation Failed", class_="text-danger"),
                class_="mt-4"
            )
            
        # Show summary info
        num_lps = summary.get('num_lps', 0) if summary else 0
        return ui.div(
            ui.h4(f"✅ PCAP Report Generated", class_="text-success"),
            ui.p(f"Generated data for {num_lps} Limited Partners", class_="text-muted"),
            class_="mt-4"
        )
    
    @output
    @render.ui
    def pcap_detailed_results():
        """Display detailed PCAP results table with performance metrics"""
        data = pcap_data.get()
        summary = pcap_summary.get()
        
        if data is None or data.empty:
            return ui.div(
                ui.p("No PCAP data available. Generate a PCAP report first.", class_="text-muted"),
                class_="mt-3"
            )
        
        # Convert DataFrame to HTML table for display
        try:
            # Format the data nicely for display
            display_df = data.copy()
            
            # If it's the summary structure, format it nicely
            if 'Line_Item' in display_df.columns:
                # Format all numeric columns to 6 decimal places
                numeric_columns = ['MTD', 'QTD', 'YTD', 'ITD']
                for col in numeric_columns:
                    if col in display_df.columns:
                        display_df[col] = display_df[col].apply(
                            lambda x: f"{float(x):.6f}" if pd.notnull(x) and str(x) != 'nan' else "0.000000"
                        )
                
                html_table = display_df.to_html(
                    classes="table table-striped table-hover", 
                    table_id="pcap-results-table",
                    index=False,
                    escape=False
                )
            else:
                # Legacy daily structure - limit columns for display
                cols_to_show = ['date', 'limited_partner_ID', 'running_nav', 'daily_pnl']
                available_cols = [col for col in cols_to_show if col in display_df.columns]
                if available_cols:
                    display_df = display_df[available_cols].head(100)  # Limit rows
                
                html_table = display_df.to_html(
                    classes="table table-striped table-hover",
                    table_id="pcap-results-table", 
                    index=False,
                    escape=False,
                    float_format=lambda x: f"{x:.6f}" if pd.notnull(x) and isinstance(x, (int, float)) else str(x)
                )
            
            # Create performance metrics display
            performance_section = ui.div()  # Default empty
            if summary and summary.get('performance_metrics'):
                metrics = summary['performance_metrics']
                net_irr = metrics.get('net_irr', 'N/A')
                gross_moic = metrics.get('gross_moic', 'N/A')
                capital_committed = metrics.get('capital_committed', 'N/A')
                capital_called = metrics.get('capital_called', 'N/A')
                currency = metrics.get('currency', 'ETH')
                fund_name = metrics.get('fund_name', 'Unknown Fund')
                
                # Create fund and currency context
                fund_display = fund_name.replace('_', ' ').replace('fund i', 'Fund I').replace('class B', 'Class B')
                
                performance_section = ui.div(
                    ui.hr(),
                    ui.h5("Performance Metrics", class_="mt-4 mb-3"),
                    ui.div(
                        ui.div(
                            ui.span(f"Fund: {fund_display} | Currency: {currency.upper()}", class_="text-muted mb-3 d-block"),
                            class_="mb-3"
                        ),
                        ui.div(
                            ui.strong("Net IRR: "),
                            ui.span(net_irr, class_="text-primary"),
                            class_="mb-2"
                        ),
                        ui.div(
                            ui.strong("Gross MOIC: "),
                            ui.span(gross_moic, class_="text-success"), 
                            ui.span(f" ({currency.upper()})", class_="text-muted ms-1"),
                            class_="mb-2"
                        ),
                        ui.div(
                            ui.strong("Capital Committed: "),
                            ui.span(capital_committed, class_="text-info"),
                            ui.span(f" {currency.upper()}", class_="text-muted ms-1"),
                            class_="mb-2"
                        ),
                        ui.div(
                            ui.strong("Capital Called: "),
                            ui.span(capital_called, class_="text-warning"),
                            ui.span(f" {currency.upper()}", class_="text-muted ms-1"),
                            class_="mb-2"
                        ),
                        class_="performance-metrics"
                    )
                )
            
            # Return table and performance metrics separately
            table_card = ui.card(
                ui.card_header("PCAP Report Results"),
                ui.card_body(
                    ui.HTML(html_table),
                    style="max-height: 500px; overflow-y: auto;"
                ),
                class_="mt-3"
            )
            
            # Create separate performance metrics card if metrics exist
            if summary and summary.get('performance_metrics'):
                metrics = summary['performance_metrics']
                net_irr = metrics.get('net_irr', 'N/A')
                gross_moic = metrics.get('gross_moic', 'N/A')
                capital_committed = metrics.get('capital_committed', 'N/A')
                capital_called = metrics.get('capital_called', 'N/A')
                currency = metrics.get('currency', 'ETH')
                fund_name = metrics.get('fund_name', 'Unknown Fund')
                
                fund_display = fund_name.replace('_', ' ').replace('fund i', 'Fund I').replace('class B', 'Class B')
                
                performance_card = ui.card(
                    ui.card_header("Performance Metrics"),
                    ui.card_body(
                        ui.div(
                            ui.span(f"Fund: {fund_display} | Currency: {currency.upper()}", class_="text-muted mb-3 d-block"),
                            class_="mb-3"
                        ),
                        ui.div(
                            ui.strong("Net IRR: "),
                            ui.span(net_irr, class_="text-primary"),
                            class_="mb-2"
                        ),
                        ui.div(
                            ui.strong("Gross MOIC: "),
                            ui.span(gross_moic, class_="text-success"), 
                            ui.span(f" ({currency.upper()})", class_="text-muted ms-1"),
                            class_="mb-2"
                        ),
                        ui.div(
                            ui.strong("Capital Committed: "),
                            ui.span(capital_committed, class_="text-info"),
                            ui.span(f" {currency.upper()}", class_="text-muted ms-1"),
                            class_="mb-2"
                        ),
                        ui.div(
                            ui.strong("Capital Called: "),
                            ui.span(capital_called, class_="text-warning"),
                            ui.span(f" {currency.upper()}", class_="text-muted ms-1"),
                            class_="mb-2"
                        )
                    ),
                    class_="mt-3"
                )
                
                return ui.div(table_card, performance_card)
            else:
                return table_card
            
        except Exception as e:
            return ui.div(
                ui.p(f"Error displaying PCAP data: {str(e)}", class_="text-danger"),
                class_="mt-3"
            )
    
    @output
    @render.ui
    def pcap_summary_charts():
        """Display PCAP summary charts placeholder - removed per user request"""
        return ui.div()  # Return empty div, no summary metrics
