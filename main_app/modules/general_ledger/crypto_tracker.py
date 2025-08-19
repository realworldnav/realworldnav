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
    """FIFO cost basis tracking interface"""
    return ui.div(
        ui.row(
            ui.column(
                12,
                ui.h3("FIFO Cost Basis Tracker", class_="mb-3"),
                ui.p("First In, First Out cost basis calculation for tax reporting", class_="text-muted")
            )
        ),
        
        # FIFO Controls
        ui.row(
            ui.column(
                4,
                ui.card(
                    ui.card_header("FIFO Settings"),
                    ui.card_body(
                        ui.output_ui("fifo_token_selector"),
                        ui.input_date_range(
                            "fifo_date_range", 
                            "Date Range:",
                            start=date(2024, 1, 1),
                            end=date.today()
                        ),
                        ui.input_action_button(
                            "calculate_fifo",
                            ui.HTML('<i class="bi bi-calculator"></i> Calculate FIFO'),
                            class_="btn-primary mt-3 w-100"
                        ),
                        ui.output_ui("fifo_calculation_progress"),
                        ui.br(),
                        ui.input_action_button(
                            "generate_journal_entries",
                            ui.HTML('<i class="bi bi-file-earmark-arrow-down"></i> Generate Journal Entries'),
                            class_="btn-success mt-2 w-100"
                        ),
                        ui.br(),
                        ui.input_action_button(
                            "export_fifo_csv",
                            ui.HTML('<i class="bi bi-download"></i> Export CSV'),
                            class_="btn-secondary mt-2 w-100"
                        )
                    )
                )
            ),
            ui.column(
                8,
                ui.card(
                    ui.card_header("FIFO Summary"),
                    ui.card_body(
                        ui.output_ui("fifo_summary_display")
                    )
                )
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
                    ),
                    ui.nav_panel(
                        "Current Positions",
                        ui.div(
                            ui.row(
                                ui.column(
                                    6,
                                    ui.input_date(
                                        "positions_as_of_date",
                                        "View positions as of:",
                                        value=date.today(),
                                        max=date.today()
                                    )
                                ),
                                ui.column(
                                    6,
                                    ui.input_action_button(
                                        "refresh_positions",
                                        ui.HTML('<i class="bi bi-arrow-clockwise"></i> Refresh Positions'),
                                        class_="btn-outline-secondary mt-4"
                                    )
                                )
                            ),
                            ui.hr(),
                            ui.output_data_frame("fifo_positions_table")
                        )
                    ),
                    ui.nav_panel(
                        "Journal Entries",
                        ui.output_data_frame("fifo_journal_entries_table")
                    ),
                    ui.nav_panel(
                        "Validation Report",
                        ui.output_ui("fifo_validation_report")
                    ),
                    ui.nav_panel(
                        "Balance Verification",
                        ui.div(
                            ui.row(
                                ui.column(
                                    8,
                                    ui.h5("FIFO Positions vs Etherscan Balances", class_="mb-3"),
                                    ui.p("Compare calculated FIFO positions with actual blockchain balances", class_="text-muted small mb-3")
                                ),
                                ui.column(
                                    4,
                                    ui.input_action_button(
                                        "verify_balances",
                                        ui.HTML('<i class="bi bi-shield-check"></i> Verify Balances'),
                                        class_="btn-primary mt-3 w-100"
                                    )
                                )
                            ),
                            ui.output_ui("balance_verification_status"),
                            ui.output_data_frame("balance_verification_table")
                        )
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
                    'Intercompany': ['-']
                })
                print("📋 Displaying empty state for transactions ready table")
                return placeholder_df
            
            # Format staged transactions for display
            display_df = staged_df.copy()
            display_df['Status'] = 'Ready for FIFO'
            print(f"📋 Displaying {len(display_df)} staged transactions in ready table")
            # Handle timezone-aware dates for display (preserve full timestamp)
            date_col = pd.to_datetime(display_df['date'])
            if date_col.dt.tz is not None:
                date_col = date_col.dt.tz_localize(None)
            display_df['Date'] = date_col.dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Format wallet_id (shortened)
            if 'wallet_id' in display_df.columns:
                display_df['Wallet ID'] = display_df['wallet_id'].apply(
                    lambda x: f"{str(x)[:6]}...{str(x)[-4:]}" if pd.notna(x) and str(x) else "-"
                )
            else:
                display_df['Wallet ID'] = '-'
            
            display_df['Token'] = display_df.get('token_name', display_df.get('asset', '-'))
            display_df['Side'] = display_df.get('side', display_df.get('direction', '-'))
            display_df['Amount'] = display_df.get('token_amount', 0.0).round(6)
            display_df['ETH Value'] = display_df.get('token_value_eth', 0.0).round(6)
            display_df['USD Value'] = display_df.get('token_value_usd', 0.0).round(2)
            
            # Intercompany flag
            if 'intercompany' in display_df.columns:
                display_df['Intercompany'] = display_df['intercompany'].apply(lambda x: 'Yes' if x else 'No')
            else:
                display_df['Intercompany'] = 'No'
            
            return display_df[['Status', 'Date', 'Wallet ID', 'Token', 'Side', 'Amount', 'ETH Value', 'USD Value', 'Intercompany']]
            
        except Exception as e:
            logger.error(f"Error loading staged transactions: {e}")
            error_df = pd.DataFrame({
                'Status': ['Error loading'],
                'Date': ['-'],
                'Wallet ID': ['-'],
                'Token': ['-'],
                'Side': ['-'],
                'Amount': [0.0],
                'ETH Value': [0.0],
                'USD Value': [0.0],
                'Intercompany': ['-']
            })
            return error_df
    
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
            
            # Filter by selected token if specified
            selected_token = input.fifo_token_select()
            if selected_token and selected_token != "":
                transactions_df = transactions_df[
                    transactions_df['token_name'].str.upper() == selected_token.upper()
                ]
            
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
            
            if transactions_df.empty:
                logger.warning("No transactions found for selected criteria")
                fifo_results.set(pd.DataFrame())
                fifo_positions.set(pd.DataFrame())
                return
            
            # Convert to FIFO format
            logger.info(f"Converting {len(transactions_df)} transactions to FIFO format")
            fifo_input_df = convert_crypto_fetch_to_fifo_format(transactions_df)
            logger.info(f"FIFO input format: {len(fifo_input_df)} rows, columns: {list(fifo_input_df.columns)}")
            
            # Process through FIFO with new ETH-based approach
            logger.info("Processing transactions through FIFO with ETH-based cost basis...")
            fifo_df = build_fifo_ledger(fifo_input_df)
            logger.info(f"FIFO processing complete: {len(fifo_df)} result rows")
            logger.info(f"FIFO output columns: {list(fifo_df.columns)}")
            fifo_results.set(fifo_df)
            
            # Get current positions using the new simplified tracker
            tracker = FIFOTracker()
            for _, row in fifo_input_df.iterrows():
                # Use unit_price_eth from the converted data
                tracker.process(
                    fund_id=row['fund_id'],
                    wallet=row['wallet_address'],
                    asset=row['asset'],
                    side=row['side'],
                    qty=abs(row['qty']),  # Use absolute value
                    unit_price_eth=row.get('unit_price_eth', 0),
                    date=row['date'],
                    tx_hash=row['hash'],
                    price_eth=row.get('price_eth', 0),
                    log=False  # Don't log for position calculation
                )
            
            positions_df = tracker.get_all_positions()
            fifo_positions.set(positions_df)
            
            # Clear staged transactions after successful processing
            clear_staged_transactions_global()
            logger.info(f"FIFO calculation completed. Processed {len(fifo_df)} transactions and cleared staging area")
            
        except Exception as e:
            logger.error(f"Error in FIFO calculation: {e}")
    
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