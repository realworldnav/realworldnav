"""
General Ledger Analytics Module

Provides comprehensive analytics and reporting features for general ledger data including:
- Transaction volume analysis
- Account balance trends  
- Category breakdowns
- Time-based analysis
- Financial ratios and metrics
"""

from shiny import ui, reactive, render
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from ...s3_utils import load_GL_file, load_COA_file


def register_gl_analytics_outputs(output, input, session, selected_fund):
    """Register all GL analytics outputs"""
    
    @reactive.calc
    def gl_analytics_data():
        """Load and prepare GL data for analytics"""
        try:
            gl_df = load_GL_file()
            coa_df = load_COA_file()
            
            if gl_df.empty or coa_df.empty:
                return pd.DataFrame(), pd.DataFrame()
            
            # Filter by selected fund
            fund_id = selected_fund()
            if fund_id and 'fund_id' in gl_df.columns:
                gl_df = gl_df[gl_df['fund_id'] == fund_id]
            
            # Ensure date column is properly formatted
            if 'date' in gl_df.columns:
                gl_df['date'] = pd.to_datetime(gl_df['date'], errors='coerce')
            
            # Add month and year columns for grouping
            gl_df['month'] = gl_df['date'].dt.to_period('M').astype(str)
            gl_df['year'] = gl_df['date'].dt.year
            
            # Merge with COA for account categories
            gl_df = gl_df.merge(
                coa_df[['account_name', 'GL_Acct_Number', 'GL_Acct_Name']], 
                on='account_name', 
                how='left'
            )
            
            # Add account categories based on account number ranges
            def get_account_category(acct_num):
                try:
                    num = int(acct_num)
                    first_digit = int(str(num)[0])
                    if first_digit == 1:
                        return "Assets"
                    elif first_digit == 2:
                        return "Liabilities"
                    elif first_digit == 3:
                        return "Capital"
                    elif first_digit == 4:
                        return "Other Income"
                    elif first_digit == 8:
                        return "Expenses"
                    elif first_digit == 9:
                        return "Income"
                    else:
                        return "Other"
                except:
                    return "Unknown"
            
            gl_df['account_category'] = gl_df['GL_Acct_Number'].apply(get_account_category)
            
            return gl_df, coa_df
            
        except Exception as e:
            print(f"Error in gl_analytics_data: {e}")
            return pd.DataFrame(), pd.DataFrame()
    
    @output
    @render.ui
    def gl_analytics_summary():
        """Display summary statistics for GL analytics"""
        try:
            gl_df, coa_df = gl_analytics_data()
            
            if gl_df.empty:
                return ui.div(
                    ui.h5("No Data Available", class_="text-muted text-center"),
                    ui.p("No general ledger data found for analysis", class_="text-muted text-center"),
                    class_="empty-state p-4"
                )
            
            # Calculate summary metrics
            total_transactions = len(gl_df)
            unique_accounts = gl_df['account_name'].nunique()
            date_range = f"{gl_df['date'].min().strftime('%Y-%m-%d')} to {gl_df['date'].max().strftime('%Y-%m-%d')}"
            total_volume = abs(pd.to_numeric(gl_df['debit_USD'], errors='coerce').fillna(0).sum()) + \
                          abs(pd.to_numeric(gl_df['credit_USD'], errors='coerce').fillna(0).sum())
            
            # Category breakdown
            category_counts = gl_df['account_category'].value_counts()
            
            return ui.layout_column_wrap(
                ui.value_box(
                    "Total Transactions",
                    f"{total_transactions:,}",
                    theme="primary"
                ),
                ui.value_box(
                    "Unique Accounts",
                    f"{unique_accounts:,}",
                    theme="info"
                ),
                ui.value_box(
                    "Date Range",
                    date_range,
                    theme="secondary"
                ),
                ui.value_box(
                    "Total Volume (USD)",
                    f"${total_volume:,.2f}",
                    theme="success"
                ),
                fill=False
            )
            
        except Exception as e:
            print(f"Error in gl_analytics_summary: {e}")
            return ui.div(
                ui.p("Error loading analytics summary", class_="text-danger"),
                class_="error-state"
            )
    
    @output
    @render.ui
    def transaction_volume_chart():
        """Display transaction volume over time"""
        try:
            gl_df, _ = gl_analytics_data()
            
            if gl_df.empty:
                return ui.div(
                    ui.p("No transaction data for volume analysis", class_="text-muted text-center"),
                    class_="empty-state"
                )
            
            # Group by month and calculate volume
            monthly_volume = gl_df.groupby('month').agg({
                'transaction_id': 'count',
                'debit_USD': lambda x: pd.to_numeric(x, errors='coerce').fillna(0).sum(),
                'credit_USD': lambda x: pd.to_numeric(x, errors='coerce').fillna(0).sum()
            }).reset_index()
            
            monthly_volume['total_volume'] = monthly_volume['debit_USD'] + monthly_volume['credit_USD']
            monthly_volume = monthly_volume.sort_values('month')
            
            # Create plotly chart
            fig = go.Figure()
            
            # Add transaction count
            fig.add_trace(go.Scatter(
                x=monthly_volume['month'],
                y=monthly_volume['transaction_id'],
                mode='lines+markers',
                name='Transaction Count',
                yaxis='y',
                line=dict(color='blue', width=2),
                marker=dict(size=6)
            ))
            
            # Add volume on secondary y-axis
            fig.add_trace(go.Scatter(
                x=monthly_volume['month'],
                y=monthly_volume['total_volume'],
                mode='lines+markers',
                name='Volume (USD)',
                yaxis='y2',
                line=dict(color='green', width=2),
                marker=dict(size=6)
            ))
            
            fig.update_layout(
                title="Monthly Transaction Volume Analysis",
                xaxis=dict(title="Month"),
                yaxis=dict(title="Transaction Count", side="left"),
                yaxis2=dict(title="Volume (USD)", side="right", overlaying="y"),
                template="plotly_white",
                height=400,
                hovermode='x unified'
            )
            
            return ui.div(
                ui.HTML(fig.to_html(full_html=False, include_plotlyjs="cdn")),
                style="height: 400px;"
            )
            
        except Exception as e:
            print(f"Error in transaction_volume_chart: {e}")
            return ui.div(
                ui.p("Error loading volume chart", class_="text-danger"),
                class_="error-state"
            )
    
    @output
    @render.ui
    def account_category_breakdown():
        """Display breakdown by account category"""
        try:
            gl_df, _ = gl_analytics_data()
            
            if gl_df.empty:
                return ui.div(
                    ui.p("No data for category breakdown", class_="text-muted text-center"),
                    class_="empty-state"
                )
            
            # Calculate category metrics
            category_stats = gl_df.groupby('account_category').agg({
                'transaction_id': 'count',
                'debit_USD': lambda x: pd.to_numeric(x, errors='coerce').fillna(0).sum(),
                'credit_USD': lambda x: pd.to_numeric(x, errors='coerce').fillna(0).sum(),
                'account_name': 'nunique'
            }).reset_index()
            
            category_stats['total_volume'] = category_stats['debit_USD'] + category_stats['credit_USD']
            category_stats = category_stats.sort_values('total_volume', ascending=False)
            
            # Color mapping for categories
            color_map = {
                "Assets": "#28a745",
                "Liabilities": "#dc3545", 
                "Capital": "#6f42c1",
                "Other Income": "#17a2b8",
                "Expenses": "#fd7e14",
                "Income": "#007bff",
                "Other": "#6c757d",
                "Unknown": "#6c757d"
            }
            
            # Create pie chart
            fig = go.Figure(data=[go.Pie(
                labels=category_stats['account_category'],
                values=category_stats['total_volume'],
                hole=.3,
                marker_colors=[color_map.get(cat, "#6c757d") for cat in category_stats['account_category']]
            )])
            
            fig.update_layout(
                title="Transaction Volume by Account Category",
                template="plotly_white",
                height=400
            )
            
            return ui.div(
                ui.HTML(fig.to_html(full_html=False, include_plotlyjs="cdn")),
                style="height: 400px;"
            )
            
        except Exception as e:
            print(f"Error in account_category_breakdown: {e}")
            return ui.div(
                ui.p("Error loading category breakdown", class_="text-danger"),
                class_="error-state"
            )
    
    @output
    @render.ui
    def top_accounts_analysis():
        """Display analysis of most active accounts"""
        try:
            gl_df, _ = gl_analytics_data()
            
            if gl_df.empty:
                return ui.div(
                    ui.p("No data for top accounts analysis", class_="text-muted text-center"),
                    class_="empty-state"
                )
            
            # Calculate account activity metrics
            account_stats = gl_df.groupby(['account_name', 'account_category']).agg({
                'transaction_id': 'count',
                'debit_USD': lambda x: pd.to_numeric(x, errors='coerce').fillna(0).sum(),
                'credit_USD': lambda x: pd.to_numeric(x, errors='coerce').fillna(0).sum()
            }).reset_index()
            
            account_stats['total_volume'] = account_stats['debit_USD'] + account_stats['credit_USD']
            top_accounts = account_stats.nlargest(10, 'total_volume')
            
            # Create horizontal bar chart
            fig = go.Figure(data=[go.Bar(
                y=top_accounts['account_name'],
                x=top_accounts['total_volume'],
                orientation='h',
                marker_color='steelblue',
                text=top_accounts['transaction_id'],
                texttemplate='%{text} txns',
                textposition="inside"
            )])
            
            fig.update_layout(
                title="Top 10 Most Active Accounts by Volume",
                xaxis_title="Total Volume (USD)",
                yaxis_title="Account Name",
                template="plotly_white",
                height=500
            )
            
            return ui.div(
                ui.HTML(fig.to_html(full_html=False, include_plotlyjs="cdn")),
                style="height: 500px;"
            )
            
        except Exception as e:
            print(f"Error in top_accounts_analysis: {e}")
            return ui.div(
                ui.p("Error loading top accounts analysis", class_="text-danger"),
                class_="error-state"
            )
    
    @output
    @render.ui
    def balance_trends_chart():
        """Display account balance trends over time"""
        try:
            gl_df, coa_df = gl_analytics_data()
            
            if gl_df.empty:
                return ui.div(
                    ui.p("No data for balance trends", class_="text-muted text-center"),
                    class_="empty-state"
                )
            
            # Calculate running balances by category and month
            category_trends = []
            
            for category in gl_df['account_category'].unique():
                if pd.isna(category):
                    continue
                    
                cat_data = gl_df[gl_df['account_category'] == category]
                monthly_balances = cat_data.groupby('month').agg({
                    'debit_USD': lambda x: pd.to_numeric(x, errors='coerce').fillna(0).sum(),
                    'credit_USD': lambda x: pd.to_numeric(x, errors='coerce').fillna(0).sum()
                }).reset_index()
                
                # Calculate net change (accounting principles)
                if category in ["Assets", "Expenses"]:
                    monthly_balances['net_change'] = monthly_balances['debit_USD'] - monthly_balances['credit_USD']
                else:  # Liabilities, Capital, Income
                    monthly_balances['net_change'] = monthly_balances['credit_USD'] - monthly_balances['debit_USD']
                
                monthly_balances['category'] = category
                category_trends.append(monthly_balances)
            
            if not category_trends:
                return ui.div(
                    ui.p("No category data for trends", class_="text-muted text-center"),
                    class_="empty-state"
                )
            
            trends_df = pd.concat(category_trends, ignore_index=True)
            trends_df = trends_df.sort_values(['category', 'month'])
            
            # Create line chart
            fig = go.Figure()
            
            for category in trends_df['category'].unique():
                cat_data = trends_df[trends_df['category'] == category]
                fig.add_trace(go.Scatter(
                    x=cat_data['month'],
                    y=cat_data['net_change'],
                    mode='lines+markers',
                    name=category,
                    line=dict(width=2),
                    marker=dict(size=6)
                ))
            
            fig.update_layout(
                title="Account Category Balance Trends",
                xaxis_title="Month",
                yaxis_title="Net Change (USD)",
                template="plotly_white",
                height=400,
                hovermode='x unified'
            )
            
            return ui.div(
                ui.HTML(fig.to_html(full_html=False, include_plotlyjs="cdn")),
                style="height: 400px;"
            )
            
        except Exception as e:
            print(f"Error in balance_trends_chart: {e}")
            return ui.div(
                ui.p("Error loading balance trends", class_="text-danger"),
                class_="error-state"
            )


def gl_analytics_ui():
    """GL Analytics user interface"""
    return ui.div(
        # Summary metrics
        ui.card(
            ui.card_header(ui.HTML('<i class="bi bi-bar-chart me-2"></i>GL Analytics Overview')),
            ui.output_ui("gl_analytics_summary")
        ),

        # Charts layout
        ui.layout_columns(
            # Transaction volume over time
            ui.card(
                ui.card_header(ui.HTML('<i class="bi bi-graph-up me-2"></i>Transaction Volume Trends')),
                ui.output_ui("transaction_volume_chart"),
                full_screen=True
            ),
            # Account category breakdown
            ui.card(
                ui.card_header(ui.HTML('<i class="bi bi-pie-chart me-2"></i>Category Breakdown')),
                ui.output_ui("account_category_breakdown"),
                full_screen=True
            ),
            col_widths=[8, 4]
        ),

        ui.layout_columns(
            # Top accounts analysis
            ui.card(
                ui.card_header(ui.HTML('<i class="bi bi-trophy me-2"></i>Most Active Accounts')),
                ui.output_ui("top_accounts_analysis"),
                full_screen=True
            ),
            # Balance trends
            ui.card(
                ui.card_header(ui.HTML('<i class="bi bi-graph-up-arrow me-2"></i>Balance Trends')),
                ui.output_ui("balance_trends_chart"),
                full_screen=True
            ),
            col_widths=[6, 6]
        ),
        
        # Enhanced styling
        ui.tags.style("""
            /* GL Analytics specific styling */
            .empty-state {
                text-align: center;
                padding: 48px 24px;
                color: #6e6e73;
                background: rgba(255, 255, 255, 0.6);
                border-radius: 16px;
                border: 2px dashed rgba(0, 0, 0, 0.1);
            }
            
            .error-state {
                text-align: center;
                padding: 24px;
                background: rgba(255, 59, 48, 0.1);
                border: 1px solid rgba(255, 59, 48, 0.2);
                border-radius: 12px;
                color: #b91d1d;
            }
            
            /* Card enhancements for analytics */
            .card {
                background: rgba(255, 255, 255, 0.8);
                backdrop-filter: blur(10px);
                border: 1px solid rgba(0, 0, 0, 0.05);
                border-radius: 16px;
                box-shadow: 0 4px 20px rgba(0, 0, 0, 0.08);
                transition: all 0.3s ease;
                margin-bottom: 24px;
            }
            
            .card:hover {
                transform: translateY(-2px);
                box-shadow: 0 8px 30px rgba(0, 0, 0, 0.12);
            }
            
            .card-header {
                background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
                border-bottom: 1px solid rgba(0, 0, 0, 0.08);
                border-radius: 16px 16px 0 0 !important;
                padding: 20px 24px;
                font-weight: 600;
                font-size: 18px;
                color: #1d1d1f;
            }
            
            /* Value boxes */
            .valuebox {
                background: rgba(255, 255, 255, 0.9);
                backdrop-filter: blur(10px);
                border-radius: 16px;
                box-shadow: 0 4px 20px rgba(0, 0, 0, 0.08);
                border: 1px solid rgba(0, 0, 0, 0.05);
                transition: all 0.3s ease;
            }
            
            .valuebox:hover {
                transform: translateY(-2px);
                box-shadow: 0 8px 30px rgba(0, 0, 0, 0.12);
            }
        """)
    )