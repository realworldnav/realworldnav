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
                        ui.div(
                            ui.input_select(
                                "history_token_filter",
                                "Token:",
                                choices={"all": "All Tokens"},
                                selected="all"
                            ),
                            class_="custom-dropdown"
                        ),
                        ui.div(
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
                            class_="custom-dropdown"
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
                                ui.output_ui("fifo_token_filter_choices"),
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
                                "save_fifo_ledger",
                                ui.HTML('<i class="bi bi-save"></i> Save Ledger'),
                                class_="btn-success me-2"
                            ),
                            ui.input_action_button(
                                "clear_saved_ledger",
                                ui.HTML('<i class="bi bi-trash"></i> Clear Saved'),
                                class_="btn-outline-danger me-2"
                            ),
                            ui.input_action_button(
                                "export_fifo_csv",
                                ui.HTML('<i class="bi bi-download"></i> Export CSV'),
                                class_="btn-secondary me-2"
                            ),
                            ui.input_action_button(
                                "refresh_ledger",
                                ui.HTML('<i class="bi bi-arrow-clockwise"></i> Refresh'),
                                class_="btn-outline-primary me-2"
                            ),
                            ui.input_action_button(
                                "clear_staged_transactions",
                                ui.HTML('<i class="bi bi-x-circle"></i> Clear Staged'),
                                class_="btn-outline-warning"
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
        
        # Ledger Status Display
        ui.row(
            ui.column(
                12,
                ui.output_ui("fifo_ledger_status_display"),
                class_="mb-3"
            )
        ),
        
        # Transaction Processing Rules Documentation
        ui.row(
            ui.column(
                12,
                ui.card(
                    ui.card_header(
                        ui.div(
                            ui.HTML('<i class="bi bi-gear-fill text-info"></i> Transaction Processing Rules'),
                            ui.HTML('''
                                <button class="btn btn-sm btn-outline-info float-end" type="button" 
                                        data-bs-toggle="collapse" data-bs-target="#rules_documentation" 
                                        aria-expanded="false" aria-controls="rules_documentation">
                                    <i class="bi bi-info-circle"></i>
                                </button>
                            '''),
                            class_="d-flex justify-content-between align-items-center"
                        )
                    ),
                    ui.card_body(
                        ui.output_ui("transaction_rules_status"),
                        ui.div(
                            ui.output_ui("transaction_rules_documentation"),
                            id="rules_documentation",
                            class_="collapse mt-3"
                        )
                    )
                ),
                class_="mb-3"
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
        
        # Transaction Details Card - Below Table
        ui.row(
            ui.column(
                12,
                ui.card(
                    ui.card_header(ui.HTML('<i class="bi bi-info-circle"></i> Transaction Details')),
                    ui.card_body(
                        ui.output_ui("fifo_transaction_details_card"),
                        style="min-height: 300px;"
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
    
    # Create reactive bridge for global staged transactions
    staged_transactions_reactive_trigger = reactive.value(0)
    
    def _sync_global_staged_trigger():
        """Bridge global trigger to reactive trigger - called on demand only"""
        from .crypto_token_fetch import get_staged_transactions_trigger_global
        global_trigger = get_staged_transactions_trigger_global()
        current_reactive = staged_transactions_reactive_trigger.get()
        
        if global_trigger != current_reactive:
            print(f"🔗 REACTIVE SYNC: Global trigger {global_trigger} differs from reactive {current_reactive} - syncing...")
            staged_transactions_reactive_trigger.set(global_trigger)
            print(f"✅ REACTIVE SYNC: Updated reactive trigger to {global_trigger}")
        
        return global_trigger
    
    # Periodic sync effect (runs separately from render functions)
    @reactive.effect
    def _periodic_sync_effect():
        """Periodic sync effect that doesn't interfere with render functions"""
        reactive.invalidate_later(3.0)  # Check every 3 seconds
        try:
            _sync_global_staged_trigger()
        except Exception as e:
            print(f"🔗 Periodic sync error: {e}")
    
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
    
    # Reactive values for month-end balances table
    month_end_table_data = reactive.Value(pd.DataFrame())
    month_end_header_date = reactive.Value("")
    
    # Reactive value for FIFO ledger row selection
    fifo_selection = reactive.Value({"rows": []})
    
    # Reactive filtered dataset - centralized filtering logic
    @reactive.calc
    def filtered_fifo_data():
        """Apply all filters to FIFO data and return filtered DataFrame"""
        try:
            # Get base FIFO data
            fifo_df = fifo_results.get()
            
            if fifo_df.empty:
                return pd.DataFrame()
            
            # Start with copy of full data
            filtered_df = fifo_df.copy()
            
            # Apply fund filter
            try:
                fund_filter = input.fifo_fund_filter() if hasattr(input, 'fifo_fund_filter') else "all"
                if fund_filter and fund_filter != "all":
                    filtered_df = filtered_df[filtered_df['fund_id'] == fund_filter]
                    logger.debug(f"Applied fund filter '{fund_filter}': {len(filtered_df)} rows remaining")
            except Exception as e:
                logger.debug(f"Could not apply fund filter: {e}")
            
            # Apply wallet filter  
            try:
                wallet_filter = input.fifo_wallet_filter() if hasattr(input, 'fifo_wallet_filter') else "all"
                if wallet_filter and wallet_filter != "all":
                    # Use exact matching since wallet addresses are already lowercase
                    filtered_df = filtered_df[filtered_df['wallet_address'].str.lower() == wallet_filter.lower()]
                    logger.debug(f"Applied wallet filter '{wallet_filter}': {len(filtered_df)} rows remaining")
            except Exception as e:
                logger.debug(f"Could not apply wallet filter: {e}")
            
            # Apply token filter
            try:
                token_filter = input.fifo_token_filter() if hasattr(input, 'fifo_token_filter') else "all"
                if token_filter and token_filter != "all":
                    filtered_df = filtered_df[filtered_df['asset'] == token_filter]
                    logger.debug(f"Applied token filter '{token_filter}': {len(filtered_df)} rows remaining")
            except Exception as e:
                logger.debug(f"Could not apply token filter: {e}")
            
            logger.debug(f"Total filtered FIFO data: {len(filtered_df)} rows")
            return filtered_df
            
        except Exception as e:
            logger.error(f"Error in filtered_fifo_data calculation: {e}")
            return pd.DataFrame()
    
    # Track whether FIFO data was auto-loaded or freshly calculated
    fifo_data_source = reactive.Value("none")  # "none", "auto-loaded", "calculated"
    
    # Auto-load saved FIFO ledger on startup (if no staged transactions)
    def auto_load_saved_ledger():
        """Automatically load saved FIFO ledger on app startup"""
        try:
            from ...s3_utils import check_fifo_ledger_exists, load_fifo_ledger_file
            
            # Always check for staged transactions first
            try:
                from .crypto_token_fetch import get_staged_transactions_global
                staged_df = get_staged_transactions_global()
                if not staged_df.empty:
                    logger.info(f"Found {len(staged_df)} staged transactions - auto-load will not replace them")
                    # Don't auto-load if we have staged transactions, but still check for saved ledger
                    if check_fifo_ledger_exists():
                        fifo_data_source.set("saved-available")
                    return False
            except Exception as check_e:
                logger.debug(f"Could not check staged transactions: {check_e}")
            
            # Check if there's a saved ledger
            if not check_fifo_ledger_exists():
                logger.info("No saved FIFO ledger found in S3")
                fifo_data_source.set("none")
                return False
            
            # Load the saved data only if no staged transactions
            logger.info("Loading saved FIFO ledger from S3...")
            saved_data = load_fifo_ledger_file()
            
            # Extract the DataFrames
            saved_fifo_df = saved_data.get('fifo_transactions', pd.DataFrame())
            saved_positions_df = saved_data.get('fifo_positions', pd.DataFrame())
            saved_journal_df = saved_data.get('journal_entries', pd.DataFrame())
            metadata = saved_data.get('metadata', {})
            
            # Only load if we have valid data
            if not saved_fifo_df.empty:
                # Set the reactive values
                fifo_results.set(saved_fifo_df)
                fifo_positions.set(saved_positions_df)
                journal_entries.set(saved_journal_df)
                fifo_data_source.set("auto-loaded")  # Track that data was auto-loaded
                
                logger.info(f"Successfully loaded saved FIFO ledger: {len(saved_fifo_df)} transactions, {len(saved_positions_df)} positions")
                
                # Show notification to user
                transaction_count = len(saved_fifo_df)
                save_timestamp = metadata.get('save_timestamp', 'Unknown')
                
                # Format timestamp for display
                try:
                    from datetime import datetime
                    dt = datetime.fromisoformat(save_timestamp.replace('Z', '+00:00'))
                    formatted_time = dt.strftime('%Y-%m-%d %H:%M')
                except:
                    formatted_time = save_timestamp[:10] if save_timestamp else 'Unknown'
                
                ui.notification_show(
                    f"Loaded saved FIFO ledger from {formatted_time} ({transaction_count} transactions)",
                    type="info",
                    duration=5
                )
                
                return True
            else:
                logger.warning("Saved FIFO ledger exists but contains no transaction data")
                fifo_data_source.set("saved-empty")
                return False
                
        except Exception as e:
            logger.error(f"Error auto-loading saved FIFO ledger: {e}")
            import traceback
            traceback.print_exc()
            fifo_data_source.set("error")
            return False
    
    # Trigger auto-load on startup - run as regular function
    try:
        auto_load_saved_ledger()
    except Exception as e:
        logger.warning(f"Could not auto-load FIFO ledger on startup: {e}")
        fifo_data_source.set("error")
    
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
        
        return ui.div(
            ui.input_select(
                "fifo_token_select",
                "Token:",
                choices=token_choices,
                selected=""
            ),
            class_="custom-dropdown"
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
        
        # Apply token filter to summary statistics if set
        display_df = fifo_df.copy()
        try:
            token_filter = input.fifo_token_filter() if hasattr(input, 'fifo_token_filter') else "all"
            if token_filter and token_filter != "all" and 'asset' in display_df.columns:
                display_df = display_df[display_df['asset'] == token_filter]
        except Exception as e:
            logger.debug(f"Could not apply token filter to summary: {e}")
        
        # Calculate summary statistics (ETH-based) on filtered data
        total_realized_gain_eth = display_df['realized_gain_eth'].sum()
        total_proceeds_eth = display_df['proceeds_eth'].sum()
        total_cost_basis_eth = display_df['cost_basis_sold_eth'].sum()
        unique_assets = display_df['asset'].nunique()
        total_transactions = len(display_df)
        
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
        
        # Add filter indicator
        filter_text = ""
        try:
            token_filter = input.fifo_token_filter() if hasattr(input, 'fifo_token_filter') else "all"
            if token_filter and token_filter != "all":
                filter_text = f" - Filtered: {token_filter}"
        except:
            pass
        
        return ui.div(
            ui.div(
                ui.HTML('<i class="bi bi-graph-up text-success"></i>'),
                ui.h5(f"FIFO Results{filter_text}", class_="text-success ms-2"),
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
            trigger_value = staged_transactions_reactive_trigger.get()
            print(f"🔄 REACTIVE TABLE: transactions_ready_table triggered with reactive value: {trigger_value}")
            
            print(f"🔍 REACTIVE TABLE: Attempting to retrieve staged transactions...")
            
            # Add more detailed debugging
            try:
                staged_df = get_staged_transactions_global()
                print(f"📊 Retrieved staged transactions: {len(staged_df)} rows")
                print(f"🔍 DEBUG: Staged DataFrame shape: {staged_df.shape}")
                print(f"🔍 DEBUG: Staged DataFrame empty check: {staged_df.empty}")
                print(f"🔍 DEBUG: Staged DataFrame type: {type(staged_df)}")
                
                # Deep debugging of the DataFrame
                if hasattr(staged_df, 'index'):
                    print(f"🔍 DEBUG: DataFrame index: {staged_df.index}")
                if hasattr(staged_df, 'columns'):
                    print(f"🔍 DEBUG: DataFrame columns: {list(staged_df.columns)}")
                
            except Exception as get_error:
                print(f"❌ ERROR getting staged transactions: {get_error}")
                import traceback
                traceback.print_exc()
                staged_df = pd.DataFrame()  # Fallback to empty
            
            if not staged_df.empty:
                print(f"📋 Staged transaction columns: {list(staged_df.columns)}")
                print(f"📋 Column data types: {staged_df.dtypes.to_dict()}")
                print(f"📋 Sample staged transaction: {staged_df.iloc[0].to_dict() if len(staged_df) > 0 else 'None'}")
                
                # Check for Fund/fund_id column specifically
                fund_columns = [col for col in staged_df.columns if 'fund' in col.lower()]
                print(f"📋 Fund-related columns found: {fund_columns}")
                
                # Check data quality
                null_counts = staged_df.isnull().sum()
                print(f"📋 Null value counts: {null_counts[null_counts > 0].to_dict()}")
            else:
                print(f"🔍 DEBUG: No staged transactions found - investigating...")
                # Try to get some debug info about the global state
                try:
                    from .crypto_token_fetch import _global_staged_transactions
                    print(f"🔍 DEBUG: Direct global access - shape: {_global_staged_transactions.shape}")
                    print(f"🔍 DEBUG: Direct global access - empty: {_global_staged_transactions.empty}")
                    print(f"🔍 DEBUG: Direct global access - type: {type(_global_staged_transactions)}")
                    print(f"🔍 DEBUG: Direct global access - memory id: {id(_global_staged_transactions)}")
                except Exception as debug_e:
                    print(f"🔍 DEBUG: Could not access global state directly: {debug_e}")
                    
                # Check if the function calls are working
                try:
                    print(f"🔍 DEBUG: Testing function import...")
                    from .crypto_token_fetch import get_staged_transactions_global as test_func
                    print(f"🔍 DEBUG: Function imported successfully: {test_func}")
                    test_result = test_func()
                    print(f"🔍 DEBUG: Test function call result shape: {test_result.shape}")
                except Exception as func_error:
                    print(f"❌ ERROR testing function: {func_error}")
                    import traceback
                    traceback.print_exc()
            
            if staged_df.empty:
                # Placeholder data - use consistent string types for display
                placeholder_df = pd.DataFrame({
                    'Status': ['No transactions staged for FIFO processing'],
                    'Date': ['-'],
                    'Fund': ['-'],
                    'Wallet ID': ['-'],
                    'Token': ['-'],
                    'Side': ['-'],
                    'Amount': ['0.000000'],
                    'ETH Value': ['0.000000'],
                    'USD Value': ['$0.00'],
                    'Hash': ['-']
                })
                print("📋 Displaying empty state for transactions ready table")
                return render.DataGrid(placeholder_df, selection_mode="row", height="300px")
            
            # Format staged transactions for display
            display_data = []
            print(f"🔧 Processing {len(staged_df)} staged transactions for display...")
            
            for idx, row in staged_df.iterrows():
                try:
                    print(f"🔧 Processing transaction {idx}: {row.to_dict()}")
                    
                    # Handle different possible column names from crypto_token_fetch
                    date_value = row.get('date', row.get('timestamp', ''))
                    if pd.notna(date_value):
                        try:
                            date_formatted = pd.to_datetime(date_value).strftime('%Y-%m-%d %H:%M:%S')
                        except:
                            date_formatted = str(date_value)
                    else:
                        date_formatted = '-'
                    
                    # Handle Fund/fund_id column specifically
                    fund_value = row.get('Fund', row.get('fund_id', row.get('fund', 'Unknown Fund')))
                    fund_display = str(fund_value) if pd.notna(fund_value) else 'Unknown Fund'
                    
                    # Handle wallet address - could be wallet_id, wallet_address, from_address, to_address
                    wallet = row.get('wallet_id', row.get('wallet_address', row.get('from_address', row.get('to_address', ''))))
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
                        'Fund': fund_display,
                        'Wallet ID': wallet_display,
                        'Token': str(token),
                        'Side': str(side).upper() if pd.notna(side) else '-',
                        'Amount': f"{amount:.6f}",
                        'ETH Value': f"{eth_value:.6f}",
                        'USD Value': f"${usd_value:,.2f}",
                        'Hash': hash_display
                    }
                    display_data.append(display_row)
                    print(f"✅ Successfully processed transaction {idx}")
                    
                except Exception as row_error:
                    print(f"❌ Error processing transaction {idx}: {row_error}")
                    logger.error(f"Error processing transaction row {idx}: {row_error}")
                    # Add placeholder row for failed transactions
                    display_data.append({
                        'Status': 'Error',
                        'Date': '-',
                        'Fund': 'Error',
                        'Wallet ID': '-',
                        'Token': 'Error',
                        'Side': '-',
                        'Amount': '0.0',
                        'ETH Value': '0.0',
                        'USD Value': '$0.00',
                        'Hash': '-'
                    })
            
            if not display_data:
                print("⚠️ No valid transactions could be processed for display")
                # Return empty state if all transactions failed to process
                placeholder_df = pd.DataFrame({
                    'Status': ['No valid transactions'],
                    'Date': ['-'],
                    'Fund': ['-'],
                    'Wallet ID': ['-'],
                    'Token': ['-'],
                    'Side': ['-'],
                    'Amount': ['0.0'],
                    'ETH Value': ['0.0'],
                    'USD Value': ['$0.00'],
                    'Hash': ['-']
                })
                return render.DataGrid(placeholder_df, selection_mode="row", height="400px")
            
            display_df = pd.DataFrame(display_data)
            print(f"📋 Displaying {len(display_df)} formatted transactions in ready table")
            print(f"📋 Final display columns: {list(display_df.columns)}")
            
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
                'Fund': ['-'],
                'Wallet ID': ['-'],
                'Token': ['-'],
                'Side': ['-'],
                'Amount': ['0.000000'],
                'ETH Value': ['0.000000'],
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
            
            # Force a reactive invalidation by updating the reactive trigger
            # This will cause the transactions_ready_table to re-render
            global_trigger = get_staged_transactions_trigger_global()
            staged_transactions_reactive_trigger.set(global_trigger)
            print(f"🔄 Manual refresh triggered - synced reactive trigger to global value: {global_trigger}")
            
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
        print(f"DEBUG FIFO transactions table columns: {list(display_df.columns)}")
        
        display_df['Date'] = pd.to_datetime(display_df['date']).dt.strftime('%Y-%m-%d %H:%M:%S')
        display_df['Side'] = display_df['side']
        display_df['Token'] = display_df['asset']
        
        # Format wallet address (full)
        if 'wallet_address' in display_df.columns:
            display_df['Wallet'] = display_df['wallet_address'].apply(
                lambda x: str(x) if pd.notna(x) and str(x) else "-"
            )
        else:
            display_df['Wallet'] = '-'
            
        display_df['Qty'] = display_df['qty'].round(6)
        
        # Handle the amount column - check which name is available
        if 'amount (eth)' in display_df.columns:
            display_df['Total ETH'] = display_df['amount (eth)'].round(8)
        elif 'total_eth' in display_df.columns:
            display_df['Total ETH'] = display_df['total_eth'].round(8)
        else:
            display_df['Total ETH'] = 0.0
            
        display_df['Unit Price (ETH)'] = display_df['unit_price_eth'].round(8) if 'unit_price_eth' in display_df.columns else 0.0
        display_df['Proceeds (ETH)'] = display_df['proceeds_eth'].round(8) if 'proceeds_eth' in display_df.columns else 0.0
        display_df['Cost Basis (ETH)'] = display_df['cost_basis_sold_eth'].round(8) if 'cost_basis_sold_eth' in display_df.columns else 0.0
        display_df['Remaining Qty'] = display_df['remaining_qty'].round(6) if 'remaining_qty' in display_df.columns else 0.0
        display_df['Remaining Cost (ETH)'] = display_df['remaining_cost_basis_eth'].round(8) if 'remaining_cost_basis_eth' in display_df.columns else 0.0
        
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
        
        # Apply token filter if set
        try:
            token_filter = input.fifo_token_filter() if hasattr(input, 'fifo_token_filter') else "all"
            if token_filter and token_filter != "all" and 'asset' in display_df.columns:
                display_df = display_df[display_df['asset'] == token_filter]
                logger.info(f"Applied token filter '{token_filter}' to positions: {len(display_df)} positions")
        except Exception as e:
            logger.debug(f"Could not apply token filter to positions: {e}")
        
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
            
            print(f"🔍 FIFO CALC DEBUG: Retrieved {len(transactions_df)} staged transactions")
            print(f"🔍 FIFO CALC DEBUG: DataFrame shape: {transactions_df.shape}")
            print(f"🔍 FIFO CALC DEBUG: DataFrame empty: {transactions_df.empty}")
            
            if transactions_df.empty:
                logger.warning("No staged transactions available for FIFO calculation. Please fetch and stage transactions first.")
                print(f"❌ FIFO CALC DEBUG: No staged transactions found - cannot proceed")
                return
                
            print(f"✅ FIFO CALC DEBUG: Found staged transactions, proceeding with filters...")
            
            # Apply fund filter
            fund_filter = input.fifo_fund_filter() if hasattr(input, 'fifo_fund_filter') else "all"
            print(f"🔍 FIFO CALC DEBUG: Fund filter = '{fund_filter}'")
            if fund_filter and fund_filter != "all":
                fund_columns = [col for col in transactions_df.columns if 'fund' in col.lower()]
                print(f"🔍 FIFO CALC DEBUG: Fund columns found: {fund_columns}")
                if fund_columns:
                    fund_col = fund_columns[0]
                    before_count = len(transactions_df)
                    transactions_df = transactions_df[transactions_df[fund_col] == fund_filter]
                    print(f"🔍 FIFO CALC DEBUG: Fund filter '{fund_filter}' reduced from {before_count} to {len(transactions_df)} transactions")
                    logger.info(f"Filtered to fund '{fund_filter}': {len(transactions_df)} transactions")
            
            # Apply wallet filter
            wallet_filter = input.fifo_wallet_filter() if hasattr(input, 'fifo_wallet_filter') else "all"
            if wallet_filter and wallet_filter != "all":
                # wallet_filter now contains full lowercase address for exact matching
                wallet_columns = [col for col in transactions_df.columns if 'wallet' in col.lower() or 'address' in col.lower()]
                if wallet_columns:
                    wallet_col = wallet_columns[0]
                    # Use exact matching for wallet filter on lowercase addresses
                    before_count = len(transactions_df)
                    transactions_df = transactions_df[
                        transactions_df[wallet_col].astype(str).str.lower() == wallet_filter.lower()
                    ]
                    print(f"🔍 FIFO CALC DEBUG: Wallet filter '{wallet_filter}' reduced from {before_count} to {len(transactions_df)} transactions")
                    logger.info(f"Filtered to wallet '{wallet_filter}': {len(transactions_df)} transactions")
            
            # Filter by date range
            date_range = input.fifo_date_range()
            if date_range:
                start_date, end_date = date_range
                logger.info(f"Date range filter: {start_date} to {end_date}")
                
                # Debug: Show sample dates before filtering
                if not transactions_df.empty:
                    sample_dates = transactions_df['date'].head(3).tolist()
                    logger.info(f"Sample transaction dates before filtering: {sample_dates}")
                
                # Ensure consistent datetime handling
                date_col = pd.to_datetime(transactions_df['date'])
                if date_col.dt.tz is not None:
                    transactions_df['date'] = date_col.dt.tz_localize(None)
                else:
                    transactions_df['date'] = date_col
                    
                start_dt = pd.to_datetime(start_date)
                # Make end_dt include the entire end day (23:59:59)
                end_dt = pd.to_datetime(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
                
                logger.info(f"Filter range (datetime): {start_dt} to {end_dt}")
                logger.info(f"Transaction date range: {transactions_df['date'].min()} to {transactions_df['date'].max()}")
                
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
            fifo_data_source.set("calculated")  # Track that data was freshly calculated
            
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
            
            # Smart staging area management
            data_source = fifo_data_source.get()
            if data_source == "calculated":
                # Fresh calculation from staged transactions - clear them
                clear_staged_transactions_global()
                logger.info(f"FIFO calculation completed. Processed {len(fifo_df)} transactions and cleared staging area")
            else:
                # Keep staged transactions when working with saved data
                logger.info(f"FIFO calculation completed. Processed {len(fifo_df)} transactions (preserved staging area)")
                # Note: User can manually clear staged transactions if needed
            
        except Exception as e:
            logger.error(f"Error in FIFO calculation: {e}")
            import traceback
            traceback.print_exc()
            print(f"❌ FIFO calculation failed: {str(e)}")
            # Set empty results on error
            fifo_results.set(pd.DataFrame())
            fifo_positions.set(pd.DataFrame())
    
    # Handle FIFO ledger save
    @reactive.effect
    @reactive.event(input.save_fifo_ledger)
    def handle_fifo_save():
        """Save FIFO ledger results to S3"""
        logger.info("FIFO ledger save requested")
        
        try:
            from ...s3_utils import save_fifo_ledger_file
            
            # Get current FIFO data
            fifo_df = fifo_results.get()
            positions_df = fifo_positions.get()
            journal_df = journal_entries.get()
            
            if fifo_df.empty:
                logger.warning("No FIFO data to save. Calculate FIFO first.")
                ui.notification_show(
                    "No FIFO data to save. Please calculate FIFO first.",
                    type="warning",
                    duration=3
                )
                return
            
            # Prepare metadata
            metadata = {
                'fund_filter': input.fifo_fund_filter() if hasattr(input, 'fifo_fund_filter') else 'all',
                'wallet_filter': input.fifo_wallet_filter() if hasattr(input, 'fifo_wallet_filter') else 'all',
                'token_filter': input.fifo_token_filter() if hasattr(input, 'fifo_token_filter') else 'all',
                'date_range_start': input.fifo_date_range()[0].isoformat() if hasattr(input, 'fifo_date_range') and input.fifo_date_range() else None,
                'date_range_end': input.fifo_date_range()[1].isoformat() if hasattr(input, 'fifo_date_range') and input.fifo_date_range() else None,
                'user_action': 'manual_save'
            }
            
            # Save to S3
            success = save_fifo_ledger_file(fifo_df, positions_df, journal_df, metadata)
            
            if success:
                logger.info("FIFO ledger saved successfully to S3")
                ui.notification_show(
                    f"FIFO ledger saved successfully! ({len(fifo_df)} transactions, {len(positions_df)} positions)",
                    type="success",
                    duration=5
                )
                # Refresh the status display by invalidating the cache
                from ...s3_utils import load_fifo_ledger_file
                if hasattr(load_fifo_ledger_file, 'cache_clear'):
                    load_fifo_ledger_file.cache_clear()
            else:
                logger.error("Failed to save FIFO ledger to S3")
                ui.notification_show(
                    "Failed to save FIFO ledger. Please check your S3 connection.",
                    type="error",
                    duration=5
                )
                
        except Exception as e:
            logger.error(f"Error saving FIFO ledger: {e}")
            import traceback
            traceback.print_exc()
            ui.notification_show(
                f"Error saving FIFO ledger: {str(e)}",
                type="error",
                duration=5
            )
    
    # Handle clear saved ledger
    @reactive.effect
    @reactive.event(input.clear_saved_ledger)
    def handle_clear_saved_ledger():
        """Clear saved FIFO ledger from S3"""
        logger.info("Clear saved ledger requested")
        
        try:
            from ...s3_utils import delete_fifo_ledger_file, check_fifo_ledger_exists
            
            # Check if there's a saved ledger to clear
            if not check_fifo_ledger_exists():
                ui.notification_show(
                    "No saved ledger found to clear.",
                    type="warning",
                    duration=3
                )
                return
            
            # Delete from S3
            success = delete_fifo_ledger_file()
            
            if success:
                logger.info("Saved FIFO ledger cleared successfully from S3")
                ui.notification_show(
                    "Saved ledger cleared successfully!",
                    type="success",
                    duration=3
                )
                # Refresh the status display by invalidating the cache
                from ...s3_utils import load_fifo_ledger_file
                if hasattr(load_fifo_ledger_file, 'cache_clear'):
                    load_fifo_ledger_file.cache_clear()
            else:
                logger.error("Failed to clear saved FIFO ledger from S3")
                ui.notification_show(
                    "Failed to clear saved ledger. Please check your S3 connection.",
                    type="error",
                    duration=5
                )
                
        except Exception as e:
            logger.error(f"Error clearing saved FIFO ledger: {e}")
            import traceback
            traceback.print_exc()
            ui.notification_show(
                f"Error clearing saved ledger: {str(e)}",
                type="error",
                duration=5
            )
    
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
        """Main FIFO Ledger DataGrid using centralized filtered dataset"""
        # Use the reactive filtered dataset instead of applying filters here
        filtered_df = filtered_fifo_data()
        
        if filtered_df.empty:
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
        
        # Use the filtered DataFrame directly
        display_df = filtered_df.copy()
        
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
        
        # Keep full wallet addresses for better filtering visibility
        if 'wallet_address' in display_df.columns:
            display_df['wallet_address'] = display_df['wallet_address'].apply(
                lambda x: str(x) if pd.notna(x) else "-"
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
        
        return render.DataGrid(
            final_df,
            selection_mode="row",
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
        
        # Calculate month-end balances as a table
        try:
            # Convert date to datetime if needed
            if 'date' in fifo_df.columns:
                fifo_df['date'] = pd.to_datetime(fifo_df['date'])
            else:
                return ui.p("Date information not available", class_="text-muted")
            
            # Get date range from current filter for header
            try:
                date_range = input.fifo_date_range()
                if date_range and len(date_range) == 2:
                    end_date = date_range[1]
                    header_date = pd.to_datetime(end_date).strftime('%B %d, %Y')
                else:
                    # Fallback to max date in data
                    header_date = fifo_df['date'].max().strftime('%B %d, %Y')
            except:
                header_date = fifo_df['date'].max().strftime('%B %d, %Y')
            
            # For each wallet and asset combination, get the most recent remaining_qty
            fifo_df_sorted = fifo_df.sort_values('date')
            latest_balances = fifo_df_sorted.groupby(['wallet_address', 'asset']).last().reset_index()
            
            # Filter to show non-zero remaining quantities
            active_balances = latest_balances[latest_balances['remaining_qty'] != 0]
            
            if active_balances.empty:
                return ui.div(
                    ui.h5(f"Ending Balances as of {header_date}", class_="text-center mb-3"),
                    ui.p("No active balances found", class_="text-muted text-center")
                )
            
            # Create pivot table with wallets as rows and tokens as columns
            balance_pivot = active_balances.pivot_table(
                index='wallet_address', 
                columns='asset', 
                values='remaining_qty', 
                fill_value=0
            ).reset_index()
            
            # Get fund information for each wallet
            fund_mapping = {}
            if 'fund_id' in active_balances.columns:
                for _, row in active_balances[['wallet_address', 'fund_id']].drop_duplicates().iterrows():
                    fund_mapping[row['wallet_address']] = row['fund_id']
            
            # Create table data
            table_data = []
            
            # Get all unique assets for column headers
            assets = [col for col in balance_pivot.columns if col != 'wallet_address']
            
            for _, row in balance_pivot.iterrows():
                wallet_address = row['wallet_address']
                wallet_display = wallet_address
                fund_display = fund_mapping.get(wallet_address, 'Unknown Fund')
                
                # Create row data
                row_data = {
                    'Wallet': f"{wallet_display}",
                    'Fund': fund_display
                }
                
                # Add asset columns
                for asset in assets:
                    qty = row.get(asset, 0)
                    if qty != 0:
                        if qty > 0:
                            row_data[asset] = f"{qty:.6f}"
                        else:
                            row_data[asset] = f"{qty:.6f} (SHORT)"
                    else:
                        row_data[asset] = "-"
                
                table_data.append(row_data)
            
            # Create DataFrame for display
            display_df = pd.DataFrame(table_data)
            
            if display_df.empty:
                return ui.div(
                    ui.h5(f"Ending Balances as of {header_date}", class_="text-center mb-3"),
                    ui.p("No data to display", class_="text-muted text-center")
                )
            
            # Store the data for the table render function
            month_end_table_data.set(display_df)
            month_end_header_date.set(header_date)
            
            # Return the UI structure with a placeholder for the table
            return ui.div(
                ui.h5(f"Ending Balances as of {header_date}", class_="text-center mb-3"),
                ui.output_data_frame("month_end_balances_table")
            )
                
        except Exception as e:
            logger.error(f"Error generating month-end balances: {e}")
            return ui.div(
                ui.HTML('<i class="bi bi-exclamation-triangle text-warning"></i>'),
                ui.p("Error loading balances", class_="text-warning ms-2"),
                class_="d-flex align-items-center"
            )
    
    @output
    @render.data_frame
    def month_end_balances_table():
        """Render the month-end balances table"""
        try:
            # Get the stored table data
            table_data = month_end_table_data.get()
            
            if table_data.empty:
                # Return placeholder table
                placeholder_df = pd.DataFrame({
                    'Wallet': ['No data available'],
                    'ETH': [0.0],
                    'USDC': [0.0]
                })
                return render.DataGrid(
                    placeholder_df,
                    selection_mode="none",
                    filters=False,
                    height="300px"
                )
            
            return render.DataGrid(
                table_data,
                selection_mode="none",
                filters=False,
                height="300px"
            )
            
        except Exception as e:
            logger.error(f"Error rendering month-end balances table: {e}")
            # Return error placeholder
            error_df = pd.DataFrame({
                'Error': ['Failed to load data']
            })
            return render.DataGrid(
                error_df,
                selection_mode="none",
                filters=False,
                height="300px"
            )
    
    @output
    @render.ui
    def fifo_ledger_status_display():
        """Display comprehensive ledger status and workflow guidance"""
        try:
            from ...s3_utils import check_fifo_ledger_exists, load_fifo_ledger_file
            from .crypto_token_fetch import get_staged_transactions_global
            
            # Get current state information
            has_saved_ledger = check_fifo_ledger_exists()
            staged_df = get_staged_transactions_global()
            staged_count = len(staged_df)
            data_source = fifo_data_source.get()
            fifo_df = fifo_results.get()
            has_calculated_data = not fifo_df.empty
            
            status_parts = []
            
            # Show saved ledger status
            if has_saved_ledger:
                saved_data = load_fifo_ledger_file()
                metadata = saved_data.get('metadata', {})
                
                if metadata:
                    save_timestamp = metadata.get('save_timestamp', 'Unknown')
                    transaction_count = metadata.get('fifo_transaction_count', 0)
                    
                    # Format timestamp for display
                    try:
                        from datetime import datetime
                        dt = datetime.fromisoformat(save_timestamp.replace('Z', '+00:00'))
                        formatted_time = dt.strftime('%Y-%m-%d %H:%M')
                    except:
                        formatted_time = save_timestamp[:10] if save_timestamp else 'Unknown'
                    
                    status_parts.append(
                        ui.div(
                            ui.HTML('<i class="bi bi-cloud-check text-success"></i>'),
                            ui.span(f"Saved Ledger: {transaction_count} transactions from {formatted_time}", 
                                   class_="text-success fw-bold ms-2"),
                            class_="d-flex align-items-center"
                        )
                    )
            
            # Show staged transactions status
            if staged_count > 0:
                status_parts.append(
                    ui.div(
                        ui.HTML('<i class="bi bi-layers text-warning"></i>'),
                        ui.span(f"Staged Transactions: {staged_count} ready for FIFO", 
                               class_="text-warning fw-bold ms-2"),
                        class_="d-flex align-items-center mt-1"
                    )
                )
            
            # Show current calculation status
            if has_calculated_data:
                calculated_count = len(fifo_df)
                if data_source == "auto-loaded":
                    source_text = "loaded from S3"
                    icon_class = "bi-cloud-download text-info"
                elif data_source == "calculated":
                    source_text = "calculated from staged"
                    icon_class = "bi-calculator text-primary"
                else:
                    source_text = "unknown source"
                    icon_class = "bi-question-circle text-muted"
                
                status_parts.append(
                    ui.div(
                        ui.HTML(f'<i class="{icon_class}"></i>'),
                        ui.span(f"Current FIFO: {calculated_count} transactions ({source_text})", 
                               class_="fw-bold ms-2"),
                        class_="d-flex align-items-center mt-1"
                    )
                )
            
            # Workflow guidance
            if not status_parts:
                # No data at all
                return ui.div(
                    ui.div(
                        ui.HTML('<i class="bi bi-info-circle text-info"></i>'),
                        ui.span("Ready to start - Fetch transactions and push to FIFO", 
                               class_="text-info fw-bold ms-2"),
                        class_="d-flex align-items-center"
                    ),
                    class_="alert alert-light border-info py-2 px-3"
                )
            else:
                # Show all status information
                return ui.div(
                    *status_parts,
                    class_="alert alert-light border-secondary py-2 px-3"
                )
            
        except Exception as e:
            logger.error(f"Error displaying ledger status: {e}")
            return ui.div()
    
    # Transaction Rules Documentation
    
    @output
    @render.ui
    def transaction_rules_status():
        """Display current rule application status"""
        try:
            # Check if we have rule stats from the last processing
            from ...services.blockchain_service import BlockchainService
            
            # For now, just show static information since rule stats are per-session
            return ui.div(
                ui.div(
                    ui.HTML('<i class="bi bi-check-circle-fill text-success"></i>'),
                    ui.strong("Transaction Processing Rules Active", class_="text-success ms-2"),
                    class_="d-flex align-items-center mb-2"
                ),
                ui.p(
                    "All blockchain transactions are processed through 7 comprehensive rules to ensure accurate buy/sell classification and FIFO compatibility.",
                    class_="small text-muted mb-0"
                )
            )
        except Exception as e:
            logger.error(f"Error displaying rules status: {e}")
            return ui.div(
                ui.p("Rules status unavailable", class_="text-warning")
            )
    
    @output
    @render.ui
    def transaction_rules_documentation():
        """Display comprehensive rules documentation"""
        try:
            return ui.div(
                ui.HTML('''
                <div class="row">
                    <div class="col-md-6">
                        <h6 class="text-primary mb-3">📋 Processing Rules</h6>
                        
                        <div class="mb-3">
                            <strong class="text-success">Rule 0: Wallet Filtering</strong>
                            <p class="small text-muted mb-1">Only process transactions involving known fund wallets</p>
                            <span class="badge bg-secondary">Foundation</span>
                        </div>
                        
                        <div class="mb-3">
                            <strong class="text-info">Rule 1: WETH Wrapping</strong>
                            <p class="small text-muted mb-1">Split ETH deposits to WETH contract into: ETH sell + WETH buy</p>
                            <span class="badge bg-info">DeFi</span>
                        </div>
                        
                        <div class="mb-3">
                            <strong class="text-info">Rule 2: WETH Unwrapping</strong>
                            <p class="small text-muted mb-1">Split WETH withdrawals into: WETH sell + ETH buy</p>
                            <span class="badge bg-info">DeFi</span>
                        </div>
                        
                        <div class="mb-3">
                            <strong class="text-warning">Rule 3: Token Normalization</strong>
                            <p class="small text-muted mb-1">Standardize token symbols (e.g., BLUR → BLUR POOL)</p>
                            <span class="badge bg-warning">Cleanup</span>
                        </div>
                    </div>
                    
                    <div class="col-md-6">
                        <h6 class="text-primary mb-3">🛡️ Security & Processing</h6>
                        
                        <div class="mb-3">
                            <strong class="text-danger">Rule 4: Phishing Filtering</strong>
                            <p class="small text-muted mb-1">Remove transactions involving known scam/phishing addresses</p>
                            <span class="badge bg-danger">Security</span>
                        </div>
                        
                        <div class="mb-3">
                            <strong class="text-primary">Rule 5: Token Mints</strong>
                            <p class="small text-muted mb-1">Add ETH payment transactions for token mints from 0x0</p>
                            <span class="badge bg-primary">Economics</span>
                        </div>
                        
                        <div class="mb-3">
                            <strong class="text-primary">Rule 6: Token Burns</strong>
                            <p class="small text-muted mb-1">Add ETH receipt transactions for token burns to 0x0</p>
                            <span class="badge bg-primary">Economics</span>
                        </div>
                        
                        <div class="mb-3">
                            <strong class="text-success">Rule 7: Direction-Based Correction</strong>
                            <p class="small text-muted mb-1">Correct buy/sell classification based on transaction flow context</p>
                            <span class="badge bg-success">Core Logic</span>
                        </div>
                        
                        <div class="alert alert-info mt-3">
                            <h6><i class="bi bi-lightbulb"></i> Why Rules Matter</h6>
                            <ul class="small mb-0">
                                <li><strong>Accurate FIFO:</strong> Correct buy/sell classification prevents inventory errors</li>
                                <li><strong>Economic Substance:</strong> Rules capture true economic transactions, not just blockchain direction</li>
                                <li><strong>DeFi Compatibility:</strong> Handles complex patterns like wrapping, minting, burning</li>
                                <li><strong>Security:</strong> Filters out phishing and scam transactions automatically</li>
                            </ul>
                        </div>
                    </div>
                </div>
                ''')
            )
        except Exception as e:
            logger.error(f"Error displaying rules documentation: {e}")
            return ui.div(
                ui.p("Documentation unavailable", class_="text-warning")
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
    
    # Note: Removed get_available_fund_choices() and get_available_wallet_choices() 
    # These are now replaced by dynamic data-driven filters that extract unique values 
    # directly from FIFO ledger data and staged transactions
    
    # Note: Removed old reactive effects for updating filter choices
    # The new data-driven filter functions are self-contained and reactive
    
    @output
    @render.ui
    def fifo_fund_filter_choices():
        """Dynamic fund selector based on unique funds in FIFO ledger data"""
        try:
            # Get current FIFO data to extract unique funds
            fifo_df = fifo_results.get()
            
            # Start with "All Funds" option
            choices = {"all": "All Funds"}
            
            # If we have FIFO data, extract unique funds
            if not fifo_df.empty and 'fund_id' in fifo_df.columns:
                unique_funds = fifo_df['fund_id'].dropna().unique()
                unique_funds = sorted(unique_funds)  # Sort alphabetically
                
                # Add each unique fund to choices
                for fund in unique_funds:
                    choices[fund] = fund
                
                logger.info(f"Found {len(unique_funds)} unique funds in FIFO ledger")
            else:
                # If no FIFO data yet, try to get from staged transactions
                try:
                    from .crypto_token_fetch import get_staged_transactions_global
                    staged_df = get_staged_transactions_global()
                    
                    if not staged_df.empty:
                        # Look for fund columns
                        fund_columns = [col for col in staged_df.columns if col.lower() in ['fund_id', 'fund', 'Fund']]
                        if fund_columns:
                            fund_col = fund_columns[0]
                            unique_funds = staged_df[fund_col].dropna().unique()
                            unique_funds = sorted(unique_funds)
                            
                            for fund in unique_funds:
                                choices[fund] = fund
                                
                            logger.info(f"Found {len(unique_funds)} unique funds in staged transactions")
                except Exception as e:
                    logger.debug(f"Could not get funds from staged transactions: {e}")
            
            # Get current selection to preserve it if valid
            current_selection = "all"
            if hasattr(input, 'fifo_fund_filter'):
                user_selection = input.fifo_fund_filter()
                if user_selection in choices:
                    current_selection = user_selection
            
            return ui.input_select(
                "fifo_fund_filter",
                "Fund:",
                choices=choices,
                selected=current_selection
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
        """Dynamic wallet selector based on unique wallets in FIFO ledger data"""
        try:
            # Get the currently selected fund to trigger reactivity
            selected_fund = input.fifo_fund_filter() if hasattr(input, 'fifo_fund_filter') else "all"
            
            # Get current FIFO data to extract unique wallets
            fifo_df = fifo_results.get()
            
            # Start with "All Wallets" option
            choices = {"all": "All Wallets"}
            
            # If we have FIFO data, extract unique wallets
            if not fifo_df.empty and 'wallet_address' in fifo_df.columns:
                # Filter by fund first if not "all"
                wallet_df = fifo_df.copy()
                if selected_fund != "all" and 'fund_id' in wallet_df.columns:
                    wallet_df = wallet_df[wallet_df['fund_id'] == selected_fund]
                
                unique_wallets = wallet_df['wallet_address'].dropna().unique()
                unique_wallets = sorted(unique_wallets)  # Sort alphabetically
                
                # Add each unique wallet to choices with full address
                for wallet in unique_wallets:
                    wallet_str = str(wallet).lower()
                    # Use full address as key and display (no truncation)
                    choices[wallet_str] = wallet_str
                
                logger.info(f"Found {len(unique_wallets)} unique wallets for fund '{selected_fund}'")
            else:
                # If no FIFO data yet, try to get from staged transactions
                try:
                    from .crypto_token_fetch import get_staged_transactions_global
                    staged_df = get_staged_transactions_global()
                    
                    if not staged_df.empty:
                        # Look for wallet columns
                        wallet_columns = [col for col in staged_df.columns if 'wallet' in col.lower() or col in ['from_address', 'to_address']]
                        if wallet_columns:
                            wallet_col = wallet_columns[0]
                            
                            # Filter by fund first if not "all"
                            wallet_df = staged_df.copy()
                            if selected_fund != "all":
                                fund_columns = [col for col in staged_df.columns if col.lower() in ['fund_id', 'fund', 'Fund']]
                                if fund_columns:
                                    fund_col = fund_columns[0]
                                    wallet_df = wallet_df[wallet_df[fund_col] == selected_fund]
                            
                            unique_wallets = wallet_df[wallet_col].dropna().unique()
                            unique_wallets = sorted(unique_wallets)
                            
                            for wallet in unique_wallets:
                                wallet_str = str(wallet).lower()
                                # Use full address as key and display (no truncation)
                                choices[wallet_str] = wallet_str
                                
                            logger.info(f"Found {len(unique_wallets)} unique wallets in staged transactions")
                except Exception as e:
                    logger.debug(f"Could not get wallets from staged transactions: {e}")
            
            # Always reset to "all" when fund changes to avoid invalid selections
            current_selection = "all"
            
            # Only preserve wallet selection if it's valid for the current fund
            if hasattr(input, 'fifo_wallet_filter'):
                user_selection = input.fifo_wallet_filter()
                if user_selection in choices:
                    current_selection = user_selection
            
            print(f"🔄 Updating wallet filter for fund '{selected_fund}': {len(choices)} wallet options")
            
            return ui.input_select(
                "fifo_wallet_filter",
                "Wallet:",
                choices=choices,
                selected=current_selection
            )
        except Exception as e:
            logger.error(f"Error creating wallet filter: {e}")
            return ui.input_select(
                "fifo_wallet_filter",
                "Wallet:",
                choices={"all": "All Wallets"},
                selected="all"
            )
    
    @output
    @render.ui
    def fifo_token_filter_choices():
        """Dynamic token selector based on unique tokens in FIFO ledger"""
        try:
            # Get current FIFO data to extract unique tokens
            fifo_df = fifo_results.get()
            
            # Start with "All Tokens" option
            choices = {"all": "All Tokens"}
            
            # If we have FIFO data, extract unique tokens
            if not fifo_df.empty and 'asset' in fifo_df.columns:
                unique_tokens = fifo_df['asset'].dropna().unique()
                unique_tokens = sorted(unique_tokens)  # Sort alphabetically
                
                # Add each unique token to choices
                for token in unique_tokens:
                    choices[token] = token
                
                logger.info(f"Found {len(unique_tokens)} unique tokens in FIFO ledger")
            else:
                # If no FIFO data yet, try to get from staged transactions
                try:
                    from .crypto_token_fetch import get_staged_transactions_global
                    staged_df = get_staged_transactions_global()
                    
                    if not staged_df.empty:
                        # Look for asset/token columns
                        token_columns = [col for col in staged_df.columns if col.lower() in ['asset', 'token', 'token_name', 'symbol']]
                        if token_columns:
                            token_col = token_columns[0]
                            unique_tokens = staged_df[token_col].dropna().unique()
                            unique_tokens = sorted(unique_tokens)
                            
                            for token in unique_tokens:
                                choices[token] = token
                                
                            logger.info(f"Found {len(unique_tokens)} unique tokens in staged transactions")
                except Exception as e:
                    logger.debug(f"Could not get tokens from staged transactions: {e}")
            
            # Get current selection to preserve it if valid
            current_selection = "all"
            if hasattr(input, 'fifo_token_filter'):
                user_selection = input.fifo_token_filter()
                if user_selection in choices:
                    current_selection = user_selection
            
            return ui.input_select(
                "fifo_token_filter",
                "Token:",
                choices=choices,
                selected=current_selection
            )
        except Exception as e:
            logger.error(f"Error creating token filter: {e}")
            return ui.input_select(
                "fifo_token_filter", 
                "Token:",
                choices={"all": "All Tokens"},
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
    
    # Handle clear staged transactions button
    @reactive.effect
    @reactive.event(input.clear_staged_transactions)
    def handle_clear_staged_transactions():
        """Clear all staged transactions"""
        logger.info("Clear staged transactions requested")
        
        try:
            from .crypto_token_fetch import clear_staged_transactions_global
            clear_staged_transactions_global()
            
            ui.notification_show(
                "Staged transactions cleared successfully",
                type="success",
                duration=3
            )
            logger.info("Staged transactions cleared successfully")
        except Exception as e:
            logger.error(f"Error clearing staged transactions: {e}")
            ui.notification_show(
                f"Error clearing staged transactions: {str(e)}",
                type="error",
                duration=5
            )
    
    # Handle FIFO ledger row selection
    @reactive.effect
    def handle_fifo_selection():
        """Handle row selection from FIFO ledger table"""
        try:
            # Get cell selection from FIFO ledger table
            cell_selection = input.fifo_ledger_table_cell_selection()
            selected_rows = list(cell_selection.get("rows", [])) if cell_selection else []
            
            print(f"DEBUG FIFO selection: {selected_rows}")
            
            # Update reactive value
            fifo_selection.set({"rows": selected_rows})
            
        except Exception as e:
            print(f"DEBUG FIFO selection error: {e}")
            fifo_selection.set({"rows": []})
    
    @output
    @render.ui
    def fifo_transaction_details_card():
        """Display selected FIFO transaction details with Etherscan link"""
        try:
            # Get selected rows
            selection_info = fifo_selection.get()
            selected_rows = selection_info.get("rows", [])
            
            # Get FILTERED FIFO data - this ensures row indices match the displayed table
            fifo_df = filtered_fifo_data()
            
            if fifo_df.empty:
                return ui.div(
                    ui.div(
                        ui.HTML('<i class="bi bi-info-circle text-muted"></i>'),
                        ui.p("No FIFO data available", class_="text-muted mb-0 ms-2"),
                        class_="d-flex align-items-center"
                    ),
                    ui.p("Calculate FIFO first to view transaction details.", class_="small text-muted mt-2")
                )
            
            if not selected_rows:
                return ui.div(
                    ui.div(
                        ui.HTML('<i class="bi bi-hand-index text-primary"></i>'),
                        ui.p("Select a transaction", class_="text-primary mb-0 ms-2"),
                        class_="d-flex align-items-center"
                    ),
                    ui.p("Click on a FIFO transaction row to view details and access Etherscan.", class_="small text-muted mt-2")
                )
            
            # Get the selected transaction
            selected_idx = selected_rows[0]
            if selected_idx >= len(fifo_df):
                return ui.div(
                    ui.p("Invalid selection", class_="text-danger")
                )
            
            tx_row = fifo_df.iloc[selected_idx]
            
            # Extract transaction details
            tx_hash = str(tx_row.get('hash', ''))
            date_str = tx_row.get('date', 'Unknown')
            asset = str(tx_row.get('asset', 'Unknown'))
            side = str(tx_row.get('side', 'Unknown'))
            qty = float(tx_row.get('qty', 0))
            wallet_address = str(tx_row.get('wallet_address', ''))
            
            # Format date
            if hasattr(date_str, 'strftime'):
                date_str = date_str.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(date_str, str):
                try:
                    date_str = pd.to_datetime(date_str).strftime('%Y-%m-%d %H:%M:%S')
                except:
                    pass
            
            # Format wallet address (full)
            wallet_display = wallet_address
            
            # Create Etherscan URL
            etherscan_url = f"https://etherscan.io/tx/{tx_hash}" if tx_hash else ""
            
            return ui.div(
                # Transaction header
                ui.div(
                    ui.HTML('<i class="bi bi-receipt text-success"></i>'),
                    ui.strong("FIFO Transaction Selected", class_="text-success ms-2"),
                    class_="d-flex align-items-center mb-3"
                ),
                
                # Transaction details
                ui.div(
                    ui.div(
                        ui.HTML('<small class="text-muted">Hash</small>'),
                        ui.div(ui.code(tx_hash[:16] + "..." if len(tx_hash) > 16 else tx_hash, class_="small"), class_="mt-1"),
                        class_="mb-2"
                    ),
                    ui.div(
                        ui.HTML('<small class="text-muted">Date</small>'),
                        ui.div(date_str, class_="mt-1 small"),
                        class_="mb-2"
                    ),
                    ui.div(
                        ui.HTML('<small class="text-muted">Asset</small>'),
                        ui.div(asset, class_="mt-1 small fw-bold"),
                        class_="mb-2"
                    ),
                    ui.div(
                        ui.HTML('<small class="text-muted">Side</small>'),
                        ui.div(
                            ui.span(side.upper(), class_="badge bg-success" if side.lower() == "buy" else "badge bg-danger"),
                            class_="mt-1"
                        ),
                        class_="mb-2"
                    ),
                    ui.div(
                        ui.HTML('<small class="text-muted">Quantity</small>'),
                        ui.div(f"{qty:,.6f}", class_="mt-1 small font-monospace"),
                        class_="mb-2"
                    ),
                    ui.div(
                        ui.HTML('<small class="text-muted">Wallet</small>'),
                        ui.div(wallet_display, class_="mt-1 small font-monospace"),
                        class_="mb-3"
                    ),
                ),
                
                # Etherscan link
                ui.hr(),
                ui.div(
                    ui.HTML(f'<a href="{etherscan_url}" target="_blank" class="btn btn-primary w-100" {"" if etherscan_url else "disabled"}>'),
                    ui.HTML('<i class="bi bi-box-arrow-up-right me-2"></i>View on Etherscan'),
                    ui.HTML('</a>'),
                    class_="text-center"
                ) if etherscan_url else ui.div(
                    ui.p("No transaction hash available", class_="text-muted text-center")
                )
            )
            
        except Exception as e:
            logger.error(f"Error displaying FIFO transaction details: {e}")
            return ui.div(
                ui.p(f"Error loading transaction details: {str(e)}", class_="text-danger")
            )