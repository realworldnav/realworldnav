# -*- coding: utf-8 -*-
"""
Cryptocurrency Tracker Module

A comprehensive token tracking system that provides:
- Overview dashboard with portfolio metrics
- FIFO (First In, First Out) cost basis tracking
- Integration with crypto token fetch module for blockchain data
"""

from shiny import ui, render, reactive
import pandas as pd
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Set
import logging
import json
import os

from ...services.fifo_tracker import FIFOTracker, build_fifo_ledger, convert_crypto_fetch_to_fifo_format
from ...services.gl_journal_builder import (
    build_crypto_journal_entries, 
    export_journal_entries_by_month,
    validate_journal_entries,
    create_summary_report
)
from ...services.portfolio_valuation import get_valuation_engine, refresh_portfolio_valuation
from ...services.price_service import get_price_service
from ...services.performance_metrics import get_performance_reporter
from ...services.export_service import get_export_service
from ...services.alert_service import get_alert_engine, create_price_alert, AlertPriority
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

# Set up logging
logger = logging.getLogger(__name__)


def crypto_tracker_ui():
    """Main crypto tracker UI with three tabs"""
    # Import the token fetcher UI here
    from .crypto_token_fetch import crypto_token_tracker_ui
    
    return ui.page_fluid(
        ui.h2("Cryptocurrency Tracker", class_="mt-3"),
        ui.p("Comprehensive token portfolio tracking and cost basis management", class_="text-muted mb-4"),
        
        # Four-tab navigation - enhanced with Transaction History
        ui.navset_card_tab(
            ui.nav_panel(
                ui.HTML('<i class="bi bi-pie-chart"></i> Overview'),
                crypto_overview_content()
            ),
            ui.nav_panel(
                ui.HTML('<i class="bi bi-clock-history"></i> Transaction History'),
                transaction_history_content()
            ),
            ui.nav_panel(
                ui.HTML('<i class="bi bi-calculator"></i> FIFO Tracker'),
                fifo_tracker_content()
            ),
            ui.nav_panel(
                ui.HTML('<i class="bi bi-download"></i> Token Fetcher'),
                crypto_token_tracker_ui()
            ),
        )
    )


def transaction_history_content():
    """Transaction history and analytics tab"""
    return ui.div(
        ui.row(
            ui.column(
                12,
                ui.h3("Transaction History", class_="mb-3"),
                ui.p("Comprehensive transaction tracking and analytics", class_="text-muted")
            )
        ),
        
        # Transaction Filters
        ui.row(
            ui.column(
                3,
                ui.card(
                    ui.card_header("Filters"),
                    ui.card_body(
                        ui.input_select(
                            "history_token_filter",
                            "Token:",
                            choices={"all": "All Tokens"},
                            selected="all"
                        ),
                        ui.input_select(
                            "history_type_filter",
                            "Transaction Type:",
                            choices={
                                "all": "All Types",
                                "buy": "Buys Only", 
                                "sell": "Sells Only",
                                "transfer": "Transfers Only"
                            },
                            selected="all"
                        ),
                        ui.input_date_range(
                            "history_date_range",
                            "Date Range:",
                            start=date(2024, 1, 1),
                            end=date.today()
                        ),
                        ui.input_action_button(
                            "apply_history_filters",
                            ui.HTML('<i class="bi bi-funnel"></i> Apply Filters'),
                            class_="btn-primary mt-3 w-100"
                        )
                    )
                )
            ),
            ui.column(
                9,
                ui.card(
                    ui.card_header("Transaction Analytics"),
                    ui.card_body(
                        ui.row(
                            ui.column(3, ui.output_ui("tx_count_metric")),
                            ui.column(3, ui.output_ui("tx_volume_metric")),
                            ui.column(3, ui.output_ui("tx_fees_metric")),
                            ui.column(3, ui.output_ui("avg_tx_size_metric"))
                        )
                    )
                )
            )
        ),
        
        # Transaction History Table
        ui.row(
            ui.column(
                12,
                ui.card(
                    ui.card_header("Transaction History"),
                    ui.card_body(
                        ui.div(
                            ui.input_action_button(
                                "export_transactions",
                                ui.HTML('<i class="bi bi-download"></i> Export CSV'),
                                class_="btn-outline-secondary btn-sm mb-3"
                            ),
                            class_="text-end"
                        ),
                        ui.output_data_frame("transaction_history_table")
                    )
                )
            )
        ),
        
        class_="mt-3"
    )


def crypto_overview_content():
    """Overview dashboard with portfolio metrics"""
    return ui.div(
        ui.row(
            ui.column(
                12,
                ui.h3("Portfolio Overview", class_="mb-3"),
                ui.p("Real-time portfolio metrics and performance analytics", class_="text-muted")
            )
        ),
        
        # Portfolio Summary Cards
        ui.row(
            ui.column(
                3,
                ui.card(
                    ui.card_header("Total Portfolio Value"),
                    ui.card_body(
                        ui.output_ui("portfolio_total_value")
                    )
                )
            ),
            ui.column(
                3,
                ui.card(
                    ui.card_header("Total Tokens"),
                    ui.card_body(
                        ui.output_ui("portfolio_token_count")
                    )
                )
            ),
            ui.column(
                3,
                ui.card(
                    ui.card_header("P&L Today"),
                    ui.card_body(
                        ui.output_ui("portfolio_daily_pnl")
                    )
                )
            ),
            ui.column(
                3,
                ui.card(
                    ui.card_header("P&L Total"),
                    ui.card_body(
                        ui.output_ui("portfolio_total_pnl")
                    )
                )
            )
        ),
        
        # Portfolio Visualizations
        ui.row(
            ui.column(
                6,
                ui.card(
                    ui.card_header("Portfolio Allocation"),
                    ui.card_body(
                        ui.output_ui("portfolio_allocation_chart")
                    )
                )
            ),
            ui.column(
                6,
                ui.card(
                    ui.card_header("Performance Chart"),
                    ui.card_body(
                        ui.output_ui("portfolio_performance_chart")
                    )
                )
            )
        ),
        
        # Portfolio Holdings Table
        ui.row(
            ui.column(
                12,
                ui.card(
                    ui.card_header("Token Holdings"),
                    ui.card_body(
                        ui.div(
                            ui.input_action_button(
                                "refresh_portfolio",
                                ui.HTML('<i class="bi bi-arrow-clockwise"></i> Refresh'),
                                class_="btn-outline-primary btn-sm mb-3 me-2"
                            ),
                            ui.input_action_button(
                                "export_portfolio_csv",
                                ui.HTML('<i class="bi bi-file-earmark-spreadsheet"></i> Export CSV'),
                                class_="btn-outline-success btn-sm mb-3 me-2"
                            ),
                            ui.input_action_button(
                                "export_portfolio_pdf",
                                ui.HTML('<i class="bi bi-file-earmark-pdf"></i> Export PDF'),
                                class_="btn-outline-danger btn-sm mb-3"
                            ),
                            class_="text-end"
                        ),
                        ui.output_data_frame("portfolio_holdings_table")
                    )
                )
            )
        ),
        
        class_="mt-3"
    )


def fifo_tracker_content():
    """FIFO Ledger - streamlined interface for fund tracking"""
    return ui.div(
        ui.row(
            ui.column(
                12,
                ui.h3("FIFO Ledger", class_="mb-3"),
                ui.p("Comprehensive fund tracking with FIFO cost basis calculations", class_="text-muted")
            )
        ),
        
        # New Top Row - Filters and Month-End Balances
        ui.row(
            # Left Half - Filters & Controls
            ui.column(
                6,
                ui.card(
                    ui.card_header(ui.HTML('<i class="bi bi-funnel"></i> Filters & Controls')),
                    ui.card_body(
                        ui.row(
                            ui.column(
                                6,
                                ui.output_ui("fifo_fund_filter_choices"),
                                ui.output_ui("fifo_wallet_filter_choices"),
                            ),
                            ui.column(
                                6,
                                ui.output_ui("fifo_date_range_input"),
                                ui.output_ui("fifo_calculation_progress"),
                            )
                        ),
                        ui.hr(),
                        ui.div(
                            ui.input_action_button(
                                "calculate_fifo",
                                ui.HTML('<i class="bi bi-calculator"></i> Calculate FIFO'),
                                class_="btn-primary me-2"
                            ),
                            ui.input_action_button(
                                "export_fifo_csv",
                                ui.HTML('<i class="bi bi-download"></i> Export CSV'),
                                class_="btn-secondary me-2"
                            ),
                            ui.input_action_button(
                                "refresh_ledger",
                                ui.HTML('<i class="bi bi-arrow-clockwise"></i> Refresh'),
                                class_="btn-outline-primary"
                            ),
                            class_="d-flex justify-content-center"
                        )
                    )
                )
            ),
            
            # Right Half - Month-End Balances
            ui.column(
                6,
                ui.card(
                    ui.card_header(ui.HTML('<i class="bi bi-calendar-month"></i> Month-End Balances')),
                    ui.card_body(
                        ui.output_ui("month_end_balances_display"),
                        style="max-height: 250px; overflow-y: auto;"
                    )
                )
            )
        ),
        
        # Main FIFO Ledger - Full Width
        ui.row(
            ui.column(
                12,
                ui.card(
                    ui.card_header(ui.HTML('<i class="bi bi-journal-bookmark"></i> FIFO Ledger')),
                    ui.card_body(
                        ui.output_data_frame("fifo_ledger_table"),
                        style="min-height: 500px;"
                    )
                ),
                class_="mt-3"
            )
        ),
        
        # FIFO Results Tables
        ui.row(
            ui.column(
                12,
                ui.navset_card_tab(
                    ui.nav_panel(
                        "Transactions Ready",
                        ui.div(
                            ui.row(
                                ui.column(
                                    8,
                                    ui.p("Transactions staged for FIFO processing", class_="text-muted small mb-3"),
                                ),
                                ui.column(
                                    4,
                                    ui.div(
                                        ui.input_action_button(
                                            "refresh_staged_transactions",
                                            "Refresh",
                                            class_="btn-outline-secondary btn-sm"
                                        ),
                                        class_="text-end"
                                    )
                                )
                            ),
                            ui.output_ui("staged_transactions_status"),
                            ui.output_data_frame("transactions_ready_table")
                        )
                    ),
                    ui.nav_panel(
                        "FIFO Transactions",
                        ui.output_data_frame("fifo_transactions_table")
                    )
                )
            )
        ),
        
        class_="mt-3"
    )




def register_crypto_tracker_outputs(output, input, session):
    """Register crypto tracker outputs including token fetcher"""
    
    # Import and register token fetcher outputs
    from .crypto_token_fetch import register_crypto_token_tracker_outputs
    register_crypto_token_tracker_outputs(output, input, session)
    
    # Overview Tab Outputs
    @output
    @render.ui
    def portfolio_total_value():
        try:
            # Get portfolio valuation
            engine = get_valuation_engine()
            
            # Try to load positions from staged transactions or FIFO data
            try:
                from .crypto_token_fetch import get_staged_transactions_global
                staged_df = get_staged_transactions_global()
                
                if not staged_df.empty:
                    engine.load_positions_from_staged_transactions(staged_df)
                    engine.update_market_values()
                    
                    total_value = engine.metrics.total_value_usd
                    if total_value > 0:
                        return ui.div(
                            ui.h2(f"${total_value:,.2f}", class_="text-success mb-0"),
                            ui.HTML('<small class="text-muted">Portfolio Value</small>')
                        )
            except Exception as e:
                logger.debug(f"Could not load portfolio data: {e}")
            
            return ui.div(
                ui.h2("$0.00", class_="text-secondary mb-0"),
                ui.HTML('<small class="text-muted">No positions loaded</small>')
            )
        except Exception as e:
            logger.error(f"Error calculating portfolio value: {e}")
            return ui.div(
                ui.h2("Error", class_="text-danger mb-0"),
                ui.HTML('<small class="text-muted">Calculation failed</small>')
            )
    
    @output
    @render.ui
    def portfolio_token_count():
        try:
            engine = get_valuation_engine()
            
            # Refresh data if needed
            try:
                from .crypto_token_fetch import get_staged_transactions_global
                staged_df = get_staged_transactions_global()
                
                if not staged_df.empty:
                    engine.load_positions_from_staged_transactions(staged_df)
                    token_count = engine.metrics.token_count
                    
                    return ui.div(
                        ui.h2(str(token_count), class_="text-primary mb-0"),
                        ui.HTML('<small class="text-muted">Unique Tokens</small>')
                    )
            except Exception as e:
                logger.debug(f"Could not load token count: {e}")
            
            return ui.div(
                ui.h2("0", class_="text-secondary mb-0"),
                ui.HTML('<small class="text-muted">Tokens</small>')
            )
        except Exception as e:
            logger.error(f"Error calculating token count: {e}")
            return ui.div(
                ui.h2("Error", class_="text-danger mb-0"),
                ui.HTML('<small class="text-muted">Count failed</small>')
            )
    
    @output
    @render.ui
    def portfolio_daily_pnl():
        try:
            engine = get_valuation_engine()
            
            # Refresh data if needed
            try:
                from .crypto_token_fetch import get_staged_transactions_global
                staged_df = get_staged_transactions_global()
                
                if not staged_df.empty:
                    engine.load_positions_from_staged_transactions(staged_df)
                    engine.update_market_values()
                    
                    daily_change_usd = engine.metrics.daily_change_usd
                    daily_change_pct = engine.metrics.daily_change_pct
                    
                    # Determine color based on performance
                    color_class = "text-success" if daily_change_usd >= 0 else "text-danger"
                    sign = "+" if daily_change_usd >= 0 else ""
                    
                    return ui.div(
                        ui.h2(f"${sign}{daily_change_usd:,.2f}", class_=f"{color_class} mb-0"),
                        ui.HTML(f'<small class="text-muted">{sign}{daily_change_pct:.2f}% (24h)</small>')
                    )
            except Exception as e:
                logger.debug(f"Could not load daily P&L: {e}")
            
            return ui.div(
                ui.h2("$0.00", class_="text-secondary mb-0"),
                ui.HTML('<small class="text-muted">+0.00%</small>')
            )
        except Exception as e:
            logger.error(f"Error calculating daily P&L: {e}")
            return ui.div(
                ui.h2("Error", class_="text-danger mb-0"),
                ui.HTML('<small class="text-muted">P&L failed</small>')
            )
    
    @output
    @render.ui
    def portfolio_total_pnl():
        try:
            engine = get_valuation_engine()
            
            # Refresh data if needed
            try:
                from .crypto_token_fetch import get_staged_transactions_global
                staged_df = get_staged_transactions_global()
                
                if not staged_df.empty:
                    engine.load_positions_from_staged_transactions(staged_df)
                    engine.update_market_values()
                    
                    total_pnl_usd = engine.metrics.total_unrealized_pnl_usd
                    total_pnl_pct = engine.metrics.total_unrealized_pnl_pct
                    
                    # Determine color based on performance
                    color_class = "text-success" if total_pnl_usd >= 0 else "text-danger"
                    sign = "+" if total_pnl_usd >= 0 else ""
                    
                    return ui.div(
                        ui.h2(f"${sign}{total_pnl_usd:,.2f}", class_=f"{color_class} mb-0"),
                        ui.HTML(f'<small class="text-muted">{sign}{total_pnl_pct:.2f}% Total</small>')
                    )
            except Exception as e:
                logger.debug(f"Could not load total P&L: {e}")
            
            return ui.div(
                ui.h2("$0.00", class_="text-secondary mb-0"),
                ui.HTML('<small class="text-muted">+0.00%</small>')
            )
        except Exception as e:
            logger.error(f"Error calculating total P&L: {e}")
            return ui.div(
                ui.h2("Error", class_="text-danger mb-0"),
                ui.HTML('<small class="text-muted">P&L failed</small>')
            )
    
    @output
    @render.data_frame
    def portfolio_holdings_table():
        try:
            engine = get_valuation_engine()
            
            # Refresh data if needed
            try:
                from .crypto_token_fetch import get_staged_transactions_global
                staged_df = get_staged_transactions_global()
                
                if not staged_df.empty:
                    engine.load_positions_from_staged_transactions(staged_df)
                    engine.update_market_values()
                    
                    # Get position summary
                    positions_df = engine.get_position_summary()
                    
                    if not positions_df.empty:
                        # Format for display
                        display_df = positions_df.copy()
                        
                        # Create display columns
                        result_df = pd.DataFrame({
                            'Token': display_df['symbol'],
                            'Balance': display_df['quantity'].round(6),
                            'Price (USD)': display_df['current_price_usd'].apply(lambda x: f"${x:,.2f}" if x > 0 else "$0.00"),
                            'Value (USD)': display_df['market_value_usd'].apply(lambda x: f"${x:,.2f}" if x > 0 else "$0.00"),
                            'Avg Cost (USD)': display_df['avg_cost_usd'].apply(lambda x: f"${x:,.2f}" if x > 0 else "$0.00"),
                            '24h Change': display_df['change_24h_pct'].apply(lambda x: f"{x:+.2f}%" if abs(x) > 0 else "0.00%"),
                            'Unrealized P&L': display_df.apply(
                                lambda row: f"${row['unrealized_pnl_usd']:+,.2f} ({row['unrealized_pnl_pct']:+.2f}%)" 
                                if abs(row['unrealized_pnl_usd']) > 0 else "$0.00 (0.00%)", axis=1
                            )
                        })
                        
                        return result_df
                        
            except Exception as e:
                logger.debug(f"Could not load portfolio holdings: {e}")
            
            # Fallback to placeholder
            placeholder_df = pd.DataFrame({
                'Token': ['No positions loaded'],
                'Balance': [0.0],
                'Price (USD)': ['$0.00'],
                'Value (USD)': ['$0.00'], 
                'Avg Cost (USD)': ['$0.00'],
                '24h Change': ['0.00%'],
                'Unrealized P&L': ['$0.00 (0.00%)']
            })
            return placeholder_df
            
        except Exception as e:
            logger.error(f"Error creating portfolio holdings table: {e}")
            error_df = pd.DataFrame({
                'Token': ['Error'],
                'Balance': [0.0],
                'Price (USD)': ['Error'],
                'Value (USD)': ['Error'],
                'Avg Cost (USD)': ['Error'],
                '24h Change': ['Error'],
                'Unrealized P&L': ['Error']
            })
            return error_df
    
    @output
    @render.ui
    def portfolio_allocation_chart():
        """Portfolio allocation pie chart"""
        try:
            engine = get_valuation_engine()
            
            # Get current positions
            try:
                from .crypto_token_fetch import get_staged_transactions_global
                staged_df = get_staged_transactions_global()
                
                if not staged_df.empty:
                    engine.load_positions_from_staged_transactions(staged_df)
                    engine.update_market_values()
                    
                    allocation_data = engine.get_allocation_data()
                    
                    if allocation_data:
                        # Create pie chart
                        fig = go.Figure(data=[
                            go.Pie(
                                labels=[item['symbol'] for item in allocation_data],
                                values=[item['value'] for item in allocation_data],
                                hovertemplate='<b>%{label}</b><br>' +
                                             'Value: $%{value:,.2f}<br>' +
                                             'Percentage: %{percent}<br>' +
                                             '<extra></extra>',
                                textinfo='label+percent',
                                textposition='auto',
                                marker=dict(
                                    colors=[item['color'] for item in allocation_data],
                                    line=dict(color='#FFFFFF', width=2)
                                )
                            )
                        ])
                        
                        fig.update_layout(
                            height=300,
                            margin=dict(t=20, b=20, l=20, r=20),
                            showlegend=True,
                            legend=dict(orientation="v", yanchor="middle", y=0.5, xanchor="left", x=1.05),
                            font=dict(size=12)
                        )
                        
                        return ui.HTML(fig.to_html(include_plotlyjs="cdn"))
                    
            except Exception as e:
                logger.debug(f"Could not create allocation chart: {e}")
            
            # Fallback message
            return ui.div(
                ui.HTML('<div class="text-center text-muted p-4">'),
                ui.HTML('<i class="bi bi-pie-chart" style="font-size: 3rem; opacity: 0.3;"></i>'),
                ui.HTML('<p class="mt-2">No portfolio data available</p>'),
                ui.HTML('<small>Load transactions to view allocation</small>'),
                ui.HTML('</div>')
            )
            
        except Exception as e:
            logger.error(f"Error creating allocation chart: {e}")
            return ui.div(
                ui.HTML('<div class="text-center text-danger p-4">'),
                ui.HTML('<i class="bi bi-exclamation-triangle" style="font-size: 2rem;"></i>'),
                ui.HTML('<p class="mt-2">Chart Error</p>'),
                ui.HTML('</div>')
            )
    
    @output
    @render.ui
    def portfolio_performance_chart():
        """Portfolio performance over time"""
        try:
            # Get price service to create a sample performance chart
            price_service = get_price_service()
            
            # For now, create a sample chart showing ETH price trend
            # In a full implementation, this would show portfolio value over time
            try:
                eth_history = price_service.get_historical_prices('ETH', 30)
                
                if not eth_history.empty:
                    fig = go.Figure()
                    
                    fig.add_trace(go.Scatter(
                        x=eth_history.index,
                        y=eth_history['price_usd'],
                        mode='lines',
                        name='ETH Price',
                        line=dict(color='#627EEA', width=2),
                        hovertemplate='<b>ETH Price</b><br>' +
                                     'Date: %{x}<br>' +
                                     'Price: $%{y:,.2f}<br>' +
                                     '<extra></extra>'
                    ))
                    
                    fig.update_layout(
                        height=300,
                        margin=dict(t=20, b=20, l=20, r=20),
                        xaxis_title='Date',
                        yaxis_title='Price (USD)',
                        showlegend=False,
                        xaxis=dict(showgrid=True, gridwidth=1, gridcolor='rgba(128,128,128,0.2)'),
                        yaxis=dict(showgrid=True, gridwidth=1, gridcolor='rgba(128,128,128,0.2)'),
                        plot_bgcolor='rgba(0,0,0,0)',
                        font=dict(size=12)
                    )
                    
                    return ui.HTML(fig.to_html(include_plotlyjs="cdn"))
                    
            except Exception as e:
                logger.debug(f"Could not create performance chart: {e}")
            
            # Fallback message
            return ui.div(
                ui.HTML('<div class="text-center text-muted p-4">'),
                ui.HTML('<i class="bi bi-graph-up" style="font-size: 3rem; opacity: 0.3;"></i>'),
                ui.HTML('<p class="mt-2">Performance chart coming soon</p>'),
                ui.HTML('<small>Portfolio history will be tracked over time</small>'),
                ui.HTML('</div>')
            )
            
        except Exception as e:
            logger.error(f"Error creating performance chart: {e}")
            return ui.div(
                ui.HTML('<div class="text-center text-danger p-4">'),
                ui.HTML('<i class="bi bi-exclamation-triangle" style="font-size: 2rem;"></i>'),
                ui.HTML('<p class="mt-2">Chart Error</p>'),
                ui.HTML('</div>')
            )
    
    # Portfolio refresh handler
    @reactive.effect
    @reactive.event(input.refresh_portfolio)
    def handle_portfolio_refresh():
        """Handle portfolio refresh button click"""
        try:
            # Clear price service cache to force fresh data
            price_service = get_price_service()
            price_service.clear_cache()
            
            # Clear valuation engine positions to force recalculation
            engine = get_valuation_engine()
            engine.clear_positions()
            
            logger.info("Portfolio data refreshed - cache cleared")
            
        except Exception as e:
            logger.error(f"Error refreshing portfolio: {e}")
    
    # Portfolio export handlers
    @reactive.effect
    @reactive.event(input.export_portfolio_csv)
    def handle_portfolio_csv_export():
        """Handle portfolio CSV export"""
        try:
            export_service = get_export_service()
            file_path = export_service.export_portfolio_summary('csv')
            logger.info(f"Portfolio CSV exported to {file_path}")
        except Exception as e:
            logger.error(f"Error exporting portfolio CSV: {e}")
    
    @reactive.effect
    @reactive.event(input.export_portfolio_pdf) 
    def handle_portfolio_pdf_export():
        """Handle portfolio PDF export"""
        try:
            export_service = get_export_service()
            file_path = export_service.export_portfolio_summary('pdf')
            logger.info(f"Portfolio PDF exported to {file_path}")
        except Exception as e:
            logger.error(f"Error exporting portfolio PDF: {e}")
    
    @reactive.effect
    @reactive.event(input.export_transactions)
    def handle_transaction_export():
        """Handle transaction history export"""
        try:
            export_service = get_export_service()
            file_path = export_service.export_transaction_history('csv')
            logger.info(f"Transaction history exported to {file_path}")
        except Exception as e:
            logger.error(f"Error exporting transactions: {e}")
    
    # Transaction History Tab Outputs
    
    @output
    @render.ui
    def tx_count_metric():
        """Transaction count metric"""
        try:
            from .crypto_token_fetch import get_staged_transactions_global
            staged_df = get_staged_transactions_global()
            
            if not staged_df.empty:
                count = len(staged_df)
                return ui.div(
                    ui.h4(str(count), class_="text-primary mb-0"),
                    ui.HTML('<small class="text-muted">Total Transactions</small>')
                )
        except:
            pass
        
        return ui.div(
            ui.h4("0", class_="text-secondary mb-0"),
            ui.HTML('<small class="text-muted">Transactions</small>')
        )
    
    @output
    @render.ui  
    def tx_volume_metric():
        """Transaction volume metric"""
        try:
            from .crypto_token_fetch import get_staged_transactions_global
            staged_df = get_staged_transactions_global()
            
            if not staged_df.empty:
                total_volume = staged_df.get('token_value_usd', staged_df.get('token_value_eth', pd.Series([0]))).sum()
                return ui.div(
                    ui.h4(f"${total_volume:,.0f}", class_="text-success mb-0"),
                    ui.HTML('<small class="text-muted">Total Volume</small>')
                )
        except:
            pass
        
        return ui.div(
            ui.h4("$0", class_="text-secondary mb-0"),
            ui.HTML('<small class="text-muted">Volume</small>')
        )
    
    @output
    @render.ui
    def tx_fees_metric():
        """Transaction fees metric"""
        return ui.div(
            ui.h4("$0", class_="text-warning mb-0"),
            ui.HTML('<small class="text-muted">Fees Tracked</small>')
        )
    
    @output
    @render.ui
    def avg_tx_size_metric():
        """Average transaction size metric"""
        try:
            from .crypto_token_fetch import get_staged_transactions_global
            staged_df = get_staged_transactions_global()
            
            if not staged_df.empty:
                volumes = staged_df.get('token_value_usd', staged_df.get('token_value_eth', pd.Series([0])))
                avg_size = volumes.mean() if len(volumes) > 0 else 0
                return ui.div(
                    ui.h4(f"${avg_size:,.0f}", class_="text-info mb-0"),
                    ui.HTML('<small class="text-muted">Avg Size</small>')
                )
        except:
            pass
        
        return ui.div(
            ui.h4("$0", class_="text-secondary mb-0"),
            ui.HTML('<small class="text-muted">Average</small>')
        )
    
    @output
    @render.data_frame
    def transaction_history_table():
        """Transaction history table with filtering"""
        try:
            from .crypto_token_fetch import get_staged_transactions_global
            staged_df = get_staged_transactions_global()
            
            if staged_df.empty:
                placeholder_df = pd.DataFrame({
                    'Date': ['No transactions available'],
                    'Token': ['-'],
                    'Type': ['-'],
                    'Amount': [0.0],
                    'Value (USD)': ['$0.00'],
                    'Wallet': ['-'],
                    'Hash': ['-']
                })
                return placeholder_df
            
            # Create display DataFrame
            display_df = staged_df.copy()
            
            # Format columns for display
            display_df['Date'] = pd.to_datetime(display_df['date']).dt.strftime('%Y-%m-%d %H:%M:%S')
            display_df['Token'] = display_df.get('token_name', display_df.get('asset', 'Unknown'))
            display_df['Type'] = display_df.get('side', 'Unknown').str.title()
            display_df['Amount'] = display_df.get('token_amount', 0.0).round(6)
            
            # Format values
            usd_values = display_df.get('token_value_usd', display_df.get('token_value_eth', pd.Series([0])))
            display_df['Value (USD)'] = usd_values.apply(lambda x: f"${x:,.2f}" if pd.notna(x) and x > 0 else "$0.00")
            
            # Format wallet addresses
            if 'wallet_id' in display_df.columns:
                display_df['Wallet'] = display_df['wallet_id'].apply(
                    lambda x: f"{str(x)[:6]}...{str(x)[-4:]}" if pd.notna(x) and str(x) else "-"
                )
            else:
                display_df['Wallet'] = '-'
            
            # Format transaction hashes
            if 'hash' in display_df.columns:
                display_df['Hash'] = display_df['hash'].apply(
                    lambda x: f"{str(x)[:8]}...{str(x)[-6:]}" if pd.notna(x) and str(x) else "-"
                )
            else:
                display_df['Hash'] = '-'
            
            # Select and return display columns
            result_df = display_df[['Date', 'Token', 'Type', 'Amount', 'Value (USD)', 'Wallet', 'Hash']].copy()
            
            # Sort by date (newest first)
            result_df = result_df.sort_values('Date', ascending=False)
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error creating transaction history table: {e}")
            error_df = pd.DataFrame({
                'Date': ['Error'],
                'Token': ['Error'],
                'Type': ['Error'],
                'Amount': [0.0],
                'Value (USD)': ['Error'],
                'Wallet': ['Error'],
                'Hash': ['Error']
            })
            return error_df
    
    # FIFO Tab Outputs
    
    # Reactive values for FIFO calculations
    fifo_results = reactive.Value(pd.DataFrame())
    fifo_positions = reactive.Value(pd.DataFrame())
    journal_entries = reactive.Value(pd.DataFrame())
    validation_report = reactive.Value({})
    
    @output
    @render.ui
    def fifo_token_selector():
        """Dynamic token selector based on available transaction data"""
        try:
            # Try to get token data from crypto_fetch module
            from .crypto_token_fetch import get_approved_tokens_data
            approved_tokens = get_approved_tokens_data()
            
            if not approved_tokens.empty:
                token_choices = {row['symbol']: row['name'] for _, row in approved_tokens.iterrows()}
                token_choices = {"": "All Tokens", **token_choices}
            else:
                token_choices = {"": "No tokens available", "ETH": "Ethereum", "USDC": "USD Coin"}
                
        except Exception:
            # Fallback choices
            token_choices = {"": "All Tokens", "ETH": "Ethereum", "USDC": "USD Coin"}
        
        return ui.input_select(
            "fifo_token_select",
            "Token:",
            choices=token_choices,
            selected=""
        )
    
    @output
    @render.ui
    def fifo_summary_display():
        """Display FIFO calculation summary"""
        fifo_df = fifo_results.get()
        
        if fifo_df.empty:
            return ui.div(
                ui.div(
                    ui.HTML('<i class="bi bi-info-circle text-info"></i>'),
                    ui.h5("FIFO Calculation Ready", class_="text-info ms-2"),
                    class_="d-flex align-items-center mb-3"
                ),
                ui.p("Select a token and date range, then click 'Calculate FIFO' to view cost basis calculations."),
                ui.div(
                    ui.HTML('<strong>Features:</strong>'),
                    ui.HTML('<ul class="mt-2"><li>First In, First Out cost basis tracking</li><li>Tax-compliant calculations</li><li>Realized vs unrealized gains</li><li>Export capabilities for tax reporting</li></ul>')
                )
            )
        
        # Calculate summary statistics (ETH-based)
        total_realized_gain_eth = fifo_df['realized_gain_eth'].sum()
        total_proceeds_eth = fifo_df['proceeds_eth'].sum()
        total_cost_basis_eth = fifo_df['cost_basis_sold_eth'].sum()
        unique_assets = fifo_df['asset'].nunique()
        total_transactions = len(fifo_df)
        
        # Calculate USD equivalents if price_eth is available
        if 'price_eth' in fifo_df.columns and fifo_df['price_eth'].sum() > 0:
            avg_eth_price = fifo_df['price_eth'].mean()
            total_realized_gain_usd = total_realized_gain_eth * avg_eth_price
            total_proceeds_usd = total_proceeds_eth * avg_eth_price
            total_cost_basis_usd = total_cost_basis_eth * avg_eth_price
        else:
            total_realized_gain_usd = 0
            total_proceeds_usd = 0
            total_cost_basis_usd = 0
        
        gain_color = "text-success" if total_realized_gain_eth >= 0 else "text-danger"
        gain_icon = "bi-arrow-up" if total_realized_gain_eth >= 0 else "bi-arrow-down"
        
        return ui.div(
            ui.div(
                ui.HTML('<i class="bi bi-graph-up text-success"></i>'),
                ui.h5("FIFO Results", class_="text-success ms-2"),
                class_="d-flex align-items-center mb-3"
            ),
            ui.row(
                ui.column(6, 
                    ui.p(f"**Total Transactions:** {total_transactions}"),
                    ui.p(f"**Unique Assets:** {unique_assets}")
                ),
                ui.column(6,
                    ui.p(f"**Total Proceeds (ETH):** {total_proceeds_eth:.8f}"),
                    ui.p(f"**Total Cost Basis (ETH):** {total_cost_basis_eth:.8f}")
                )
            ),
            ui.div(
                ui.HTML(f'<i class="bi {gain_icon} {gain_color}"></i>'),
                ui.h4(f"{total_realized_gain_eth:.8f} ETH", class_=f"{gain_color} ms-2"),
                ui.HTML(f'<small class="text-muted ms-2">Realized Gain/Loss (${total_realized_gain_usd:,.2f})</small>'),
                class_="d-flex align-items-center mt-3"
            )
        )
    
    @output
    @render.data_frame
    def transactions_ready_table():
        """Display transactions staged for FIFO processing"""
        try:
            from .crypto_token_fetch import get_staged_transactions_global, get_staged_transactions_trigger_global
            
            # Create reactive dependency on the trigger to ensure updates
            trigger_value = get_staged_transactions_trigger_global()
            print(f"🔄 Transactions ready table triggered with value: {trigger_value}")
            
            staged_df = get_staged_transactions_global()
            print(f"📊 Retrieved staged transactions: {len(staged_df)} rows")
            
            if not staged_df.empty:
                print(f"📋 Staged transaction columns: {list(staged_df.columns)}")
                print(f"📋 Sample staged transaction: {staged_df.iloc[0].to_dict() if len(staged_df) > 0 else 'None'}")
            
            if staged_df.empty:
                # Placeholder data
                placeholder_df = pd.DataFrame({
                    'Status': ['No transactions staged for FIFO processing'],
                    'Date': ['-'],
                    'Wallet ID': ['-'],
                    'Token': ['-'],
                    'Side': ['-'],
                    'Amount': [0.0],
                    'ETH Value': [0.0],
                    'USD Value': [0.0],
                    'Hash': ['-']
                })
                print("📋 Displaying empty state for transactions ready table")
                return render.DataGrid(placeholder_df, selection_mode="row", height="300px")
            
            # Format staged transactions for display
            display_data = []
            
            for _, row in staged_df.iterrows():
                # Handle different possible column names from crypto_token_fetch
                date_value = row.get('date', row.get('timestamp', ''))
                if pd.notna(date_value):
                    try:
                        date_formatted = pd.to_datetime(date_value).strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        date_formatted = str(date_value)
                else:
                    date_formatted = '-'
                
                # Handle wallet address - could be wallet_id, wallet_address, from_address, to_address
                wallet = row.get('wallet_id', row.get('wallet_address', row.get('from_address', row.get('to_address', ''))))
                if pd.notna(wallet) and len(str(wallet)) > 10:
                    wallet_display = f"{str(wallet)[:6]}...{str(wallet)[-4:]}"
                else:
                    wallet_display = str(wallet) if pd.notna(wallet) else '-'
                
                # Handle token name
                token = row.get('token_name', row.get('asset', row.get('token_symbol', '-')))
                
                # Handle side/direction
                side = row.get('side', row.get('direction', '-'))
                
                # Handle amounts - ensure numeric values
                try:
                    amount = float(row.get('token_amount', row.get('qty', 0)))
                except (ValueError, TypeError):
                    amount = 0.0
                
                try:
                    eth_value = float(row.get('token_value_eth', row.get('amount_eth', 0)))
                except (ValueError, TypeError):
                    eth_value = 0.0
                
                try:
                    usd_value = float(row.get('token_value_usd', row.get('amount_usd', 0)))
                except (ValueError, TypeError):
                    usd_value = 0.0
                
                # Handle transaction hash
                tx_hash = row.get('tx_hash', row.get('hash', '-'))
                if pd.notna(tx_hash) and len(str(tx_hash)) > 10:
                    hash_display = f"{str(tx_hash)[:8]}...{str(tx_hash)[-6:]}"
                else:
                    hash_display = str(tx_hash) if pd.notna(tx_hash) else '-'
                
                display_row = {
                    'Status': 'Ready for FIFO',
                    'Date': date_formatted,
                    'Wallet ID': wallet_display,
                    'Token': str(token),
                    'Side': str(side).upper() if pd.notna(side) else '-',
                    'Amount': f"{amount:.6f}",
                    'ETH Value': f"{eth_value:.6f}",
                    'USD Value': f"${usd_value:,.2f}",
                    'Hash': hash_display
                }
                display_data.append(display_row)
            
            display_df = pd.DataFrame(display_data)
            print(f"📋 Displaying {len(display_df)} formatted transactions in ready table")
            
            return render.DataGrid(
                display_df,
                selection_mode="row",
                height="400px",
                filters=True
            )
            
        except Exception as e:
            logger.error(f"Error loading staged transactions: {e}")
            import traceback
            traceback.print_exc()
            
            error_df = pd.DataFrame({
                'Status': [f'Error: {str(e)}'],
                'Date': ['-'],
                'Wallet ID': ['-'],
                'Token': ['-'],
                'Side': ['-'],
                'Amount': ['0.0'],
                'ETH Value': ['0.0'],
                'USD Value': ['$0.00'],
                'Hash': ['-']
            })
            return render.DataGrid(error_df, selection_mode="row", height="300px")
    
    @output
    @render.ui
    def staged_transactions_status():
        """Display status information about staged transactions"""
        try:
            from .crypto_token_fetch import get_staged_transactions_global, get_staged_transactions_trigger_global
            from datetime import datetime
            
            trigger_value = get_staged_transactions_trigger_global()
            staged_df = get_staged_transactions_global()
            
            if staged_df.empty:
                return ui.div(
                    ui.HTML('<small class="text-muted">No transactions currently staged. Use the Token Fetcher tab to load and stage transactions.</small>'),
                    class_="mb-2"
                )
            else:
                current_time = datetime.now().strftime("%H:%M:%S")
                return ui.div(
                    ui.HTML(f'<small class="text-success">✓ {len(staged_df)} transactions ready for FIFO processing (Last updated: {current_time})</small>'),
                    class_="mb-2"
                )
        except Exception as e:
            return ui.div(
                ui.HTML('<small class="text-danger">Error loading staging status</small>'),
                class_="mb-2"
            )
    
    @reactive.effect
    @reactive.event(input.refresh_staged_transactions)
    def refresh_staged_transactions_handler():
        """Handle refresh button click for staged transactions"""
        try:
            from .crypto_token_fetch import get_staged_transactions_trigger_global
            
            # Force a reactive invalidation by calling the trigger function
            # This will cause the transactions_ready_table to re-render
            trigger_value = get_staged_transactions_trigger_global()
            print(f"🔄 Manual refresh triggered for staged transactions (trigger: {trigger_value})")
            
            # Note: In a more robust implementation, we might want to:
            # 1. Clear and reload from S3 storage
            # 2. Validate staged transaction integrity
            # 3. Show loading state during refresh
            
        except Exception as e:
            logger.error(f"Error during staged transactions refresh: {e}")
    
    @output
    @render.data_frame
    def fifo_transactions_table():
        """Display FIFO transaction results"""
        fifo_df = fifo_results.get()
        
        if fifo_df.empty:
            # Placeholder data
            placeholder_df = pd.DataFrame({
                'Date': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                'Side': ['buy'],
                'Token': ['ETH'],
                'Wallet': ['-'],
                'Qty': [1.0],
                'Total ETH': [1.0],
                'Unit Price (ETH)': [1.0],
                'Proceeds (ETH)': [0.0],
                'Cost Basis (ETH)': [0.0],
                'Remaining Qty': [1.0],
                'Remaining Cost (ETH)': [1.0],
                'ETH/USD Price': [3200.00]
            })
            return placeholder_df
        
        # Format FIFO results for display with new columns
        display_df = fifo_df.copy()
        display_df['Date'] = pd.to_datetime(display_df['date']).dt.strftime('%Y-%m-%d %H:%M:%S')
        display_df['Side'] = display_df['side']
        display_df['Token'] = display_df['asset']
        
        # Format wallet address (shortened)
        if 'wallet_address' in display_df.columns:
            display_df['Wallet'] = display_df['wallet_address'].apply(
                lambda x: f"{str(x)[:6]}...{str(x)[-4:]}" if pd.notna(x) and str(x) else "-"
            )
        else:
            display_df['Wallet'] = '-'
            
        display_df['Qty'] = display_df['qty'].round(6)
        display_df['Total ETH'] = display_df['total_eth'].round(8)
        display_df['Unit Price (ETH)'] = display_df['unit_price_eth'].round(8)
        display_df['Proceeds (ETH)'] = display_df['proceeds_eth'].round(8)
        display_df['Cost Basis (ETH)'] = display_df['cost_basis_sold_eth'].round(8)
        display_df['Remaining Qty'] = display_df['remaining_qty'].round(6)
        display_df['Remaining Cost (ETH)'] = display_df['remaining_cost_basis_eth'].round(8)
        
        # Add ETH/USD price if available
        if 'price_eth' in display_df.columns:
            display_df['ETH/USD Price'] = display_df['price_eth'].round(2)
        else:
            display_df['ETH/USD Price'] = 0
        
        return display_df[[
            'Date', 'Side', 'Token', 'Wallet', 'Qty', 'Total ETH', 'Unit Price (ETH)',
            'Proceeds (ETH)', 'Cost Basis (ETH)', 
            'Remaining Qty', 'Remaining Cost (ETH)', 'ETH/USD Price'
        ]]
    
    @output
    @render.data_frame
    def fifo_positions_table():
        """Display current FIFO positions"""
        positions_df = fifo_positions.get()
        
        if positions_df.empty:
            placeholder_df = pd.DataFrame({
                'Wallet': ['No positions'],
                'Asset': ['-'],
                'Token Amount': [0.0],
                'ETH Value': [0.0],
                'USD Value': [0.0],
                'Cost Basis (ETH)': [0.0],
                'Cost Basis (USD)': [0.0],
                'Remaining Qty': [0.0],
                'Remaining Cost (ETH)': [0.0],
                'Lot Count': [0]
            })
            return placeholder_df
        
        # Format positions for display
        display_df = positions_df.copy()
        
        # Format wallet address (shortened)
        if 'wallet_address' in display_df.columns:
            display_df['Wallet'] = display_df['wallet_address'].apply(
                lambda x: f"{str(x)[:6]}...{str(x)[-4:]}" if pd.notna(x) and str(x) else "-"
            )
        else:
            display_df['Wallet'] = '-'
        
        display_df['Asset'] = display_df['asset']
        display_df['Qty'] = display_df['qty'].round(6)
        display_df['Cost Basis (ETH)'] = display_df['cost_basis_eth'].round(8)
        display_df['Avg Unit Price (ETH)'] = display_df['avg_unit_price_eth'].round(8)
        display_df['Lot Count'] = display_df['lot_count']
        
        # Calculate USD value if we have ETH price
        if 'price_eth' in positions_df.columns:
            display_df['Cost Basis (USD)'] = (display_df['cost_basis_eth'] * positions_df.get('price_eth', 3200)).round(2)
        else:
            display_df['Cost Basis (USD)'] = 0
        
        return display_df[['Wallet', 'Asset', 'Qty', 'Cost Basis (ETH)', 'Avg Unit Price (ETH)', 'Cost Basis (USD)', 'Lot Count']]
    
    @output
    @render.data_frame
    def fifo_journal_entries_table():
        """Display generated journal entries"""
        journal_df = journal_entries.get()
        
        if journal_df.empty:
            placeholder_df = pd.DataFrame({
                'Date': ['No entries'],
                'Account': ['-'],
                'Debit': [0.0],
                'Credit': [0.0],
                'Description': ['-']
            })
            return placeholder_df
        
        # Format journal entries for display
        display_df = journal_df.copy()
        display_df['Date'] = pd.to_datetime(display_df['date']).dt.strftime('%Y-%m-%d %H:%M:%S')
        display_df['Account'] = display_df['account']
        display_df['Debit'] = display_df['debit_USD'].round(2)
        display_df['Credit'] = display_df['credit_USD'].round(2)
        display_df['Description'] = display_df['description']
        
        return display_df[['Date', 'Account', 'Debit', 'Credit', 'Description']]
    
    @output
    @render.ui
    def fifo_validation_report():
        """Display validation report for journal entries"""
        validation = validation_report.get()
        
        if not validation:
            return ui.div(
                ui.p("No validation data available. Generate journal entries first."),
                class_="text-muted"
            )
        
        is_balanced = validation.get('is_balanced', False)
        total_debits = validation.get('total_debits', 0)
        total_credits = validation.get('total_credits', 0)
        errors = validation.get('errors', [])
        unbalanced_txs = validation.get('unbalanced_transactions', [])
        
        status_color = "text-success" if is_balanced else "text-danger"
        status_icon = "bi-check-circle" if is_balanced else "bi-exclamation-triangle"
        status_text = "Balanced" if is_balanced else "Unbalanced"
        
        content = [
            ui.div(
                ui.HTML(f'<i class="bi {status_icon} {status_color}"></i>'),
                ui.h5(f"Journal Entries: {status_text}", class_=f"{status_color} ms-2"),
                class_="d-flex align-items-center mb-3"
            ),
            ui.p(f"**Total Debits:** ${total_debits:,.2f}"),
            ui.p(f"**Total Credits:** ${total_credits:,.2f}"),
        ]
        
        if errors:
            content.append(ui.div(
                ui.h6("Errors:", class_="text-danger"),
                ui.HTML('<br>'.join([f"• {error}" for error in errors])),
                class_="mt-3"
            ))
        
        if unbalanced_txs:
            content.append(ui.div(
                ui.h6("Unbalanced Transactions:", class_="text-warning"),
                ui.HTML('<br>'.join([
                    f"• {tx['hash'][:10]}...: Dr ${tx['debits']:.2f}, Cr ${tx['credits']:.2f}"
                    for tx in unbalanced_txs[:5]  # Show first 5
                ])),
                class_="mt-3"
            ))
        
        return ui.div(*content)
    
    # Balance verification outputs
    balance_verification = reactive.Value(pd.DataFrame())
    
    @output
    @render.ui
    def balance_verification_status():
        """Display balance verification status"""
        verification_df = balance_verification.get()
        
        if verification_df.empty:
            return ui.div(
                ui.HTML('<small class="text-muted">Click "Verify Balances" to compare FIFO positions with Etherscan balances</small>'),
                class_="mb-2"
            )
        
        total_checks = len(verification_df)
        matches = len(verification_df[verification_df['status'].str.contains('✅')])
        mismatches = len(verification_df[verification_df['status'].str.contains('❌')])
        minor_diffs = len(verification_df[verification_df['status'].str.contains('⚠️')])
        
        if matches == total_checks:
            status_color = "success"
            status_icon = "bi-check-circle"
            status_text = f"All {total_checks} balances verified successfully!"
        elif mismatches > 0:
            status_color = "danger"
            status_icon = "bi-exclamation-triangle"
            status_text = f"{mismatches} mismatches found out of {total_checks} checks"
        else:
            status_color = "warning"
            status_icon = "bi-info-circle"
            status_text = f"{minor_diffs} minor differences found out of {total_checks} checks"
        
        return ui.div(
            ui.HTML(f'<i class="bi {status_icon} text-{status_color}"></i>'),
            ui.span(status_text, class_=f"text-{status_color} ms-2"),
            class_="d-flex align-items-center mb-3"
        )
    
    @output
    @render.data_frame
    def balance_verification_table():
        """Display balance verification comparison table"""
        verification_df = balance_verification.get()
        
        if verification_df.empty:
            placeholder_df = pd.DataFrame({
                'Status': ['No verification data'],
                'Wallet': ['-'],
                'Asset': ['-'],
                'FIFO Balance': [0.0],
                'Etherscan Balance': [0.0],
                'Difference': [0.0],
                'Diff %': [0.0]
            })
            return placeholder_df
        
        # Format verification results for display
        display_df = verification_df.copy()
        
        # Format wallet address (shortened)
        display_df['Wallet'] = display_df['wallet_address'].apply(
            lambda x: f"{str(x)[:6]}...{str(x)[-4:]}" if pd.notna(x) and str(x) else "-"
        )
        
        display_df['Status'] = display_df['status']
        display_df['Asset'] = display_df['asset']
        display_df['FIFO Balance'] = display_df['fifo_balance'].round(6)
        display_df['Etherscan Balance'] = display_df['etherscan_balance'].round(6)
        display_df['Difference'] = display_df['difference'].round(6)
        display_df['Diff %'] = display_df['difference_percent'].round(2)
        display_df['Last Checked'] = pd.to_datetime(display_df['last_checked']).dt.strftime('%H:%M:%S')
        
        return display_df[['Status', 'Wallet', 'Asset', 'FIFO Balance', 'Etherscan Balance', 'Difference', 'Diff %', 'Last Checked']]
    
    # Handle balance verification
    @reactive.effect
    @reactive.event(input.verify_balances)
    async def handle_balance_verification():
        """Verify FIFO positions against Etherscan balances"""
        logger.info("Balance verification requested")
        
        # Start native Shiny progress
        with ui.Progress(min=0, max=100) as progress:
            try:
                progress.set(10, message="Loading FIFO positions...", detail="Getting current positions data")
                
                positions_df = fifo_positions.get()
                
                if positions_df.empty:
                    progress.set(100, message="No positions to verify", detail="Calculate FIFO first")
                    logger.warning("No FIFO positions available for verification. Calculate FIFO first.")
                    return
                
                progress.set(30, message="Initializing Etherscan checker...", detail="Setting up API connection")
                
                # Import and initialize Etherscan checker
                from ...services.etherscan_balance_checker import get_etherscan_checker
                checker = get_etherscan_checker()
                
                progress.set(50, message="Fetching balances from Etherscan...", detail=f"Checking {len(positions_df)} positions")
                
                # Define common token contracts
                token_contracts = {
                    'USDC': {'address': '0xA0b86a33E6441644663FB5CDDFEF68e36E6c6C46', 'decimals': 6},
                    'USDT': {'address': '0xdAC17F958D2ee523a2206206994597C13D831ec7', 'decimals': 6},
                    'DAI': {'address': '0x6B175474E89094C44Da98b954EedeAC495271d0F', 'decimals': 18},
                    'WETH': {'address': '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2', 'decimals': 18},
                }
                
                progress.set(70, message="Comparing balances...", detail="Calculating differences and status")
                
                # Perform verification
                verification_df = checker.verify_wallet_balances(positions_df, token_contracts)
                
                progress.set(90, message="Processing results...", detail="Formatting verification data")
                
                # Store results
                balance_verification.set(verification_df)
                
                progress.set(100, message="Balance verification completed!", 
                           detail=f"Verified {len(verification_df)} positions")
                
                logger.info(f"Balance verification completed for {len(verification_df)} positions")
                
            except Exception as e:
                logger.error(f"Error in balance verification: {e}")
                progress.set(0, message="Verification failed", detail=str(e))
    
    # Handle FIFO calculation
    @reactive.effect
    @reactive.event(input.calculate_fifo)
    def handle_fifo_calculation():
        """Process FIFO calculation when button is clicked"""
        logger.info("FIFO calculation requested")
        
        try:
            # Get staged transaction data from crypto_fetch module
            from .crypto_token_fetch import get_staged_transactions_global, clear_staged_transactions_global
            transactions_df = get_staged_transactions_global()
            
            if transactions_df.empty:
                logger.warning("No staged transactions available for FIFO calculation. Please fetch and stage transactions first.")
                return
            
            # Apply fund filter
            fund_filter = input.fifo_fund_filter() if hasattr(input, 'fifo_fund_filter') else "all"
            if fund_filter and fund_filter != "all":
                fund_columns = [col for col in transactions_df.columns if 'fund' in col.lower()]
                if fund_columns:
                    fund_col = fund_columns[0]
                    transactions_df = transactions_df[transactions_df[fund_col] == fund_filter]
                    logger.info(f"Filtered to fund '{fund_filter}': {len(transactions_df)} transactions")
            
            # Apply wallet filter
            wallet_filter = input.fifo_wallet_filter() if hasattr(input, 'fifo_wallet_filter') else "all"
            if wallet_filter and wallet_filter != "all":
                # wallet_filter contains first 10 chars of address, so we need partial matching
                wallet_columns = [col for col in transactions_df.columns if 'wallet' in col.lower() or 'address' in col.lower()]
                if wallet_columns:
                    wallet_col = wallet_columns[0]
                    # Use startswith matching for wallet filter
                    transactions_df = transactions_df[
                        transactions_df[wallet_col].astype(str).str.startswith(wallet_filter)
                    ]
                    logger.info(f"Filtered to wallet starting with '{wallet_filter}': {len(transactions_df)} transactions")
            
            # Filter by date range
            date_range = input.fifo_date_range()
            if date_range:
                start_date, end_date = date_range
                # Ensure consistent datetime handling
                date_col = pd.to_datetime(transactions_df['date'])
                if date_col.dt.tz is not None:
                    transactions_df['date'] = date_col.dt.tz_localize(None)
                else:
                    transactions_df['date'] = date_col
                    
                start_dt = pd.to_datetime(start_date)
                end_dt = pd.to_datetime(end_date)
                
                transactions_df = transactions_df[
                    (transactions_df['date'] >= start_dt) &
                    (transactions_df['date'] <= end_dt)
                ]
                logger.info(f"Filtered to date range {start_date} to {end_date}: {len(transactions_df)} transactions")
            
            if transactions_df.empty:
                logger.warning("No transactions found for selected criteria")
                fifo_results.set(pd.DataFrame())
                fifo_positions.set(pd.DataFrame())
                return
            
            # Convert to FIFO format
            logger.info(f"Converting {len(transactions_df)} transactions to FIFO format")
            print(f"🔄 Input transaction columns: {list(transactions_df.columns)}")
            print(f"🔄 Sample input row: {transactions_df.iloc[0].to_dict() if len(transactions_df) > 0 else 'None'}")
            
            fifo_input_df = convert_crypto_fetch_to_fifo_format(transactions_df)
            logger.info(f"FIFO input format: {len(fifo_input_df)} rows, columns: {list(fifo_input_df.columns)}")
            print(f"🔄 FIFO input columns: {list(fifo_input_df.columns)}")
            print(f"🔄 Sample FIFO input row: {fifo_input_df.iloc[0].to_dict() if len(fifo_input_df) > 0 else 'None'}")
            
            # Process through FIFO with new ETH-based approach
            logger.info("Processing transactions through FIFO with ETH-based cost basis...")
            fifo_df = build_fifo_ledger(fifo_input_df)
            logger.info(f"FIFO processing complete: {len(fifo_df)} result rows")
            logger.info(f"FIFO output columns: {list(fifo_df.columns)}")
            print(f"✅ FIFO output columns: {list(fifo_df.columns)}")
            print(f"✅ Sample FIFO output row: {fifo_df.iloc[0].to_dict() if len(fifo_df) > 0 else 'None'}")
            fifo_results.set(fifo_df)
            
            # Get current positions using the new simplified tracker
            tracker = FIFOTracker()
            for _, row in fifo_input_df.iterrows():
                # Ensure proper string conversion for position calculation
                try:
                    tracker.process(
                        fund_id=str(row['fund_id']),
                        wallet=str(row['wallet_address']),
                        asset=str(row['asset']),
                        side=str(row['side']),
                        qty=abs(float(row['qty'])),  # Use absolute value
                        unit_price_eth=float(row.get('unit_price_eth', 0)),
                        date=row['date'],
                        tx_hash=str(row['hash']),
                        price_eth=float(row.get('price_eth', 0)),
                        log=False  # Don't log for position calculation
                    )
                except Exception as e:
                    logger.warning(f"Error processing position for row: {e}, skipping row {row.name}")
            
            positions_df = tracker.get_all_positions()
            fifo_positions.set(positions_df)
            
            # Clear staged transactions after successful processing
            clear_staged_transactions_global()
            logger.info(f"FIFO calculation completed. Processed {len(fifo_df)} transactions and cleared staging area")
            
        except Exception as e:
            logger.error(f"Error in FIFO calculation: {e}")
            import traceback
            traceback.print_exc()
            print(f"❌ FIFO calculation failed: {str(e)}")
            # Set empty results on error
            fifo_results.set(pd.DataFrame())
            fifo_positions.set(pd.DataFrame())
    
    # Handle journal entry generation
    @reactive.effect
    @reactive.event(input.generate_journal_entries)
    def handle_journal_generation():
        """Generate journal entries from FIFO results"""
        logger.info("Journal entry generation requested")
        
        try:
            fifo_df = fifo_results.get()
            
            if fifo_df.empty:
                logger.warning("No FIFO data available. Calculate FIFO first.")
                return
            
            # Generate journal entries
            journal_df = build_crypto_journal_entries(fifo_df)
            journal_entries.set(journal_df)
            
            # Validate journal entries
            validation = validate_journal_entries(journal_df)
            validation_report.set(validation)
            
            logger.info(f"Generated {len(journal_df)} journal entries")
            
        except Exception as e:
            logger.error(f"Error generating journal entries: {e}")
    
    # Handle CSV export
    @reactive.effect
    @reactive.event(input.export_fifo_csv)
    def handle_csv_export():
        """Export FIFO results and journal entries to CSV"""
        logger.info("CSV export requested")
        
        try:
            fifo_df = fifo_results.get()
            journal_df = journal_entries.get()
            
            if fifo_df.empty:
                logger.warning("No FIFO data to export")
                return
            
            # Create export directory
            export_dir = "fifo_exports"
            os.makedirs(export_dir, exist_ok=True)
            
            # Export FIFO results
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            fifo_filename = f"{export_dir}/fifo_results_{timestamp}.csv"
            fifo_df.to_csv(fifo_filename, index=False)
            
            # Export journal entries if available
            if not journal_df.empty:
                journal_filename = f"{export_dir}/journal_entries_{timestamp}.csv"
                journal_df.to_csv(journal_filename, index=False)
                
                # Export by month
                export_journal_entries_by_month(journal_df, export_dir)
            
            logger.info(f"Files exported to {export_dir}/")
            
        except Exception as e:
            logger.error(f"Error exporting CSV: {e}")
    
    # New FIFO Ledger Functions
    
    @output
    @render.data_frame
    def fifo_ledger_table():
        """Main FIFO Ledger DataGrid with exact column specification"""
        fifo_df = fifo_results.get()
        
        if fifo_df.empty:
            # Return placeholder DataFrame with exact column structure requested by user
            placeholder_df = pd.DataFrame({
                'fund_id': ['fund_i_class_B_ETH'],
                'wallet_address': ['0x1234567890abcdef...'],
                'asset': ['ETH'],
                'date': [pd.Timestamp.now()],
                'hash': ['0xabcdef123456...'],
                'side': ['buy'],
                'qty': [1.0],
                'amount (eth)': [1.0],
                'price_eth': [3200.0],
                'unit_price_eth': [1.0],
                'proceeds_eth': [0.0],
                'cost_basis_sold_eth': [0.0],
                'realized_gain_eth': [0.0],
                'remaining_qty': [1.0],
                'remaining_cost_basis_eth': [1.0]
            })
            return render.DataGrid(
                placeholder_df,
                selection_mode="rows",
                filters=True,
                height="500px"
            )
        
        # Use the FIFO DataFrame directly with exact column specification
        display_df = fifo_df.copy()
        
        # Ensure all required columns exist and are properly formatted
        required_columns = [
            'fund_id', 'wallet_address', 'asset', 'date', 'hash', 'side', 
            'qty', 'amount (eth)', 'price_eth', 'unit_price_eth', 
            'proceeds_eth', 'cost_basis_sold_eth', 'realized_gain_eth', 
            'remaining_qty', 'remaining_cost_basis_eth'
        ]
        
        # Add missing columns with default values if needed
        for col in required_columns:
            if col not in display_df.columns:
                if col in ['qty', 'amount (eth)', 'price_eth', 'unit_price_eth', 
                          'proceeds_eth', 'cost_basis_sold_eth', 'realized_gain_eth', 
                          'remaining_qty', 'remaining_cost_basis_eth']:
                    display_df[col] = 0.0
                elif col == 'date':
                    display_df[col] = pd.Timestamp.now()
                else:
                    display_df[col] = ''
        
        # Format date column for better display
        if 'date' in display_df.columns:
            display_df['date'] = pd.to_datetime(display_df['date']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Shorten wallet addresses for better display
        if 'wallet_address' in display_df.columns:
            display_df['wallet_address'] = display_df['wallet_address'].apply(
                lambda x: f"{str(x)[:6]}...{str(x)[-4:]}" if pd.notna(x) and len(str(x)) > 10 else str(x)
            )
        
        # Shorten transaction hashes for better display  
        if 'hash' in display_df.columns:
            display_df['hash'] = display_df['hash'].apply(
                lambda x: f"{str(x)[:8]}...{str(x)[-6:]}" if pd.notna(x) and len(str(x)) > 14 else str(x)
            )
        
        # Ensure proper column order
        existing_cols = [col for col in required_columns if col in display_df.columns]
        extra_cols = [col for col in display_df.columns if col not in required_columns]
        final_df = display_df[existing_cols + extra_cols]
        
        # Apply any additional filtering at display level if needed
        try:
            fund_filter = input.fifo_fund_filter() if hasattr(input, 'fifo_fund_filter') else "all"
            wallet_filter = input.fifo_wallet_filter() if hasattr(input, 'fifo_wallet_filter') else "all"
            
            if fund_filter and fund_filter != "all":
                final_df = final_df[final_df['fund_id'] == fund_filter]
            
            if wallet_filter and wallet_filter != "all":
                # Use startswith matching since wallet_filter is truncated
                final_df = final_df[final_df['wallet_address'].str.startswith(wallet_filter[:6], na=False)]
                
        except Exception as e:
            logger.debug(f"Could not apply display filters: {e}")
        
        return render.DataGrid(
            final_df,
            selection_mode="rows",
            filters=True,
            height="500px"
        )
    
    @output
    @render.ui
    def month_end_balances_display():
        """Month-end balances summary card"""
        fifo_df = fifo_results.get()
        
        if fifo_df.empty:
            return ui.div(
                ui.div(
                    ui.HTML('<i class="bi bi-info-circle text-muted"></i>'),
                    ui.p("No balances available", class_="text-muted mb-0 ms-2"),
                    class_="d-flex align-items-center"
                ),
                ui.p("Calculate FIFO first to see month-end balances.", class_="small text-muted mt-2")
            )
        
        # Calculate month-end balances
        try:
            # Convert timestamp to datetime if needed
            if 'timestamp' in fifo_df.columns:
                fifo_df['date'] = pd.to_datetime(fifo_df['timestamp'])
            elif 'date' in fifo_df.columns:
                fifo_df['date'] = pd.to_datetime(fifo_df['date'])
            else:
                return ui.p("Date information not available", class_="text-muted")
            
            # Group by month and calculate ending balances
            fifo_df['month_end'] = fifo_df['date'].dt.to_period('M')
            
            # Get the last entry for each asset/wallet/month combination
            month_end_balances = fifo_df.groupby(['asset', 'wallet_address', 'month_end']).agg({
                'running_balance': 'last',
                'date': 'last'
            }).reset_index()
            
            balance_cards = []
            
            # Group by month for display
            for month_period in month_end_balances['month_end'].unique():
                month_data = month_end_balances[month_end_balances['month_end'] == month_period]
                month_str = str(month_period)
                
                # Create summary for this month
                total_positions = len(month_data)
                unique_assets = month_data['asset'].nunique()
                
                balance_items = []
                for _, row in month_data.iterrows():
                    wallet_short = f"{str(row['wallet_address'])[:6]}..." if pd.notna(row['wallet_address']) else "Unknown"
                    balance_items.append(
                        ui.div(
                            ui.div(
                                ui.strong(f"{row['asset']}"),
                                ui.small(f" ({wallet_short})", class_="text-muted ms-1"),
                                class_="d-flex justify-content-between"
                            ),
                            ui.div(
                                f"{float(row['running_balance']):.6f}",
                                class_="text-end small"
                            ),
                            class_="border-bottom py-1"
                        )
                    )
                
                balance_cards.append(
                    ui.card(
                        ui.card_header(
                            ui.div(
                                ui.strong(month_str),
                                ui.badge(f"{total_positions} positions", color="primary", class_="ms-2"),
                                class_="d-flex justify-content-between align-items-center"
                            )
                        ),
                        ui.card_body(
                            *balance_items[:10],  # Limit to first 10 items to avoid clutter
                            ui.small(f"...and {max(0, len(balance_items)-10)} more", class_="text-muted") if len(balance_items) > 10 else "",
                            class_="p-2"
                        ),
                        class_="mb-2"
                    )
                )
            
            if balance_cards:
                return ui.div(*balance_cards)
            else:
                return ui.p("No month-end data available", class_="text-muted")
                
        except Exception as e:
            logger.error(f"Error generating month-end balances: {e}")
            return ui.div(
                ui.HTML('<i class="bi bi-exclamation-triangle text-warning"></i>'),
                ui.p("Error loading balances", class_="text-warning ms-2"),
                class_="d-flex align-items-center"
            )
    
    # Reactive Filters for FIFO Ledger
    
    @reactive.calc
    def get_wallet_mapping_data():
        """Load and cache wallet mapping data"""
        try:
            from ...s3_utils import load_WALLET_file
            wallet_df = load_WALLET_file()
            return wallet_df
        except Exception as e:
            logger.warning(f"Could not load wallet mapping file: {e}")
            return pd.DataFrame()
    
    @reactive.calc
    def get_available_fund_choices():
        """Get fund choices from wallet mapping and staged transactions"""
        choices = {"all": "All Funds"}
        
        try:
            # Get funds from wallet mapping
            wallet_df = get_wallet_mapping_data()
            if not wallet_df.empty:
                # Look for fund_id column or similar
                fund_columns = [col for col in wallet_df.columns if 'fund' in col.lower()]
                if fund_columns:
                    fund_col = fund_columns[0]  # Use first fund-related column
                    unique_funds = wallet_df[fund_col].dropna().unique()
                    for fund in sorted(unique_funds):
                        if pd.notna(fund) and str(fund).strip():
                            choices[str(fund)] = str(fund)
            
            # Also check staged transactions as fallback
            from .crypto_token_fetch import get_staged_transactions_global
            staged_df = get_staged_transactions_global()
            if not staged_df.empty:
                fund_columns = [col for col in staged_df.columns if 'fund' in col.lower()]
                if fund_columns:
                    fund_col = fund_columns[0]
                    unique_funds = staged_df[fund_col].dropna().unique()
                    for fund in sorted(unique_funds):
                        if pd.notna(fund) and str(fund).strip() and str(fund) not in choices:
                            choices[str(fund)] = str(fund)
                            
        except Exception as e:
            logger.warning(f"Error getting fund choices: {e}")
        
        return choices
    
    @reactive.calc  
    def get_available_wallet_choices():
        """Get wallet choices with friendly names from wallet mapping"""
        choices = {"all": "All Wallets"}
        
        try:
            wallet_df = get_wallet_mapping_data()
            if not wallet_df.empty:
                # Look for wallet address and friendly name columns
                address_columns = [col for col in wallet_df.columns if 'address' in col.lower() or 'wallet' in col.lower()]
                name_columns = [col for col in wallet_df.columns if 'name' in col.lower() or 'friendly' in col.lower() or 'display' in col.lower()]
                
                if address_columns and name_columns:
                    address_col = address_columns[0]
                    name_col = name_columns[0]
                    
                    for _, row in wallet_df.iterrows():
                        address = row.get(address_col)
                        friendly_name = row.get(name_col)
                        
                        if pd.notna(address) and pd.notna(friendly_name):
                            # Use first 10 chars of address as key, friendly name as display
                            address_key = str(address)[:10] if len(str(address)) > 10 else str(address)
                            display_name = f"{friendly_name} ({str(address)[:6]}...{str(address)[-4:]})" if len(str(address)) > 10 else f"{friendly_name} ({address})"
                            choices[address_key] = display_name
                
                # If no friendly names, fall back to just addresses
                elif address_columns:
                    address_col = address_columns[0]
                    unique_addresses = wallet_df[address_col].dropna().unique()
                    for address in sorted(unique_addresses):
                        if pd.notna(address) and str(address).strip():
                            address_key = str(address)[:10] if len(str(address)) > 10 else str(address)
                            display_name = f"{str(address)[:6]}...{str(address)[-4:]}" if len(str(address)) > 10 else str(address)
                            choices[address_key] = display_name
                            
        except Exception as e:
            logger.warning(f"Error getting wallet choices: {e}")
        
        return choices
    
    @reactive.effect
    def update_fund_filter_choices():
        """Update fund filter choices reactively"""
        try:
            choices = get_available_fund_choices()
            # Note: In Shiny, we can't directly update select choices from server
            # This would need to be handled differently in the UI
            logger.debug(f"Fund choices available: {list(choices.keys())}")
        except Exception as e:
            logger.error(f"Error updating fund filter: {e}")
    
    @reactive.effect  
    def update_wallet_filter_choices():
        """Update wallet filter choices reactively"""
        try:
            choices = get_available_wallet_choices()
            logger.debug(f"Wallet choices available: {len(choices)} options")
        except Exception as e:
            logger.error(f"Error updating wallet filter: {e}")
    
    @output
    @render.ui
    def fifo_fund_filter_choices():
        """Dynamic fund selector based on wallet mapping and staged transaction data"""
        try:
            choices = get_available_fund_choices()
            current_selection = input.fifo_fund_filter() if hasattr(input, 'fifo_fund_filter') else "all"
            
            return ui.input_select(
                "fifo_fund_filter",
                "Fund:",
                choices=choices,
                selected=current_selection if current_selection in choices else "all"
            )
        except Exception as e:
            logger.error(f"Error creating fund filter: {e}")
            return ui.input_select(
                "fifo_fund_filter",
                "Fund:",
                choices={"all": "All Funds"},
                selected="all"
            )
    
    @output
    @render.ui  
    def fifo_wallet_filter_choices():
        """Dynamic wallet selector with friendly names from wallet mapping"""
        try:
            choices = get_available_wallet_choices()
            current_selection = input.fifo_wallet_filter() if hasattr(input, 'fifo_wallet_filter') else "all"
            
            return ui.input_select(
                "fifo_wallet_filter",
                "Wallet:",
                choices=choices,
                selected=current_selection if current_selection in choices else "all"
            )
        except Exception as e:
            logger.error(f"Error creating wallet filter: {e}")
            return ui.input_select(
                "fifo_wallet_filter",
                "Wallet:",
                choices={"all": "All Wallets"},
                selected="all"
            )
    
    @reactive.calc
    def get_intelligent_date_range():
        """Calculate intelligent date range defaults based on available transaction data"""
        try:
            # Get data from staged transactions
            from .crypto_token_fetch import get_staged_transactions_global
            staged_df = get_staged_transactions_global()
            
            if not staged_df.empty:
                # Look for date columns
                date_columns = [col for col in staged_df.columns if 'date' in col.lower() or 'timestamp' in col.lower()]
                if date_columns:
                    date_col = date_columns[0]
                    date_series = pd.to_datetime(staged_df[date_col])
                    
                    # Get min and max dates from the data
                    min_date = date_series.min()
                    max_date = date_series.max()
                    
                    if pd.notna(min_date) and pd.notna(max_date):
                        return {
                            'start': min_date.date(),
                            'end': max_date.date()
                        }
            
            # Also check wallet mapping for date hints
            wallet_df = get_wallet_mapping_data()
            if not wallet_df.empty:
                date_columns = [col for col in wallet_df.columns if 'date' in col.lower()]
                if date_columns:
                    date_col = date_columns[0]
                    date_series = pd.to_datetime(wallet_df[date_col])
                    min_date = date_series.min()
                    max_date = date_series.max()
                    
                    if pd.notna(min_date) and pd.notna(max_date):
                        return {
                            'start': min_date.date(),
                            'end': max_date.date()
                        }
                        
        except Exception as e:
            logger.debug(f"Could not determine intelligent date range: {e}")
        
        # Default fallback
        return {
            'start': date(2024, 1, 1),
            'end': date.today()
        }
    
    @output
    @render.ui
    def fifo_date_range_input():
        """Dynamic date range input with intelligent defaults"""
        try:
            date_range = get_intelligent_date_range()
            
            return ui.input_date_range(
                "fifo_date_range",
                "Date Range:",
                start=date_range['start'],
                end=date_range['end']
            )
        except Exception as e:
            logger.error(f"Error creating date range input: {e}")
            return ui.input_date_range(
                "fifo_date_range",
                "Date Range:",
                start=date(2024, 1, 1),
                end=date.today()
            )
    
    # Handle refresh ledger button
    @reactive.effect
    @reactive.event(input.refresh_ledger)
    def handle_refresh_ledger():
        """Refresh the FIFO ledger display"""
        logger.info("FIFO Ledger refresh requested")
        
        try:
            # Trigger recalculation by invalidating reactive values
            fifo_results.set(fifo_results.get())
            logger.info("FIFO Ledger refreshed successfully")
        except Exception as e:
            logger.error(f"Error refreshing FIFO ledger: {e}")