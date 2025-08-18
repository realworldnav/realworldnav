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

# Set up logging
logger = logging.getLogger(__name__)


def crypto_tracker_ui():
    """Main crypto tracker UI with three tabs"""
    # Import the token fetcher UI here
    from .crypto_token_fetch import crypto_token_tracker_ui
    
    return ui.page_fluid(
        ui.h2("Cryptocurrency Tracker", class_="mt-3"),
        ui.p("Comprehensive token portfolio tracking and cost basis management", class_="text-muted mb-4"),
        
        # Three-tab navigation - now includes Token Fetcher
        ui.navset_card_tab(
            ui.nav_panel(
                ui.HTML('<i class="bi bi-pie-chart"></i> Overview'),
                crypto_overview_content()
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
        
        # Portfolio Holdings Table
        ui.row(
            ui.column(
                12,
                ui.card(
                    ui.card_header("Token Holdings"),
                    ui.card_body(
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
        return ui.div(
            ui.h2("$0.00", class_="text-success mb-0"),
            ui.HTML('<small class="text-muted">Coming Soon</small>')
        )
    
    @output
    @render.ui
    def portfolio_token_count():
        return ui.div(
            ui.h2("0", class_="text-primary mb-0"),
            ui.HTML('<small class="text-muted">Tokens</small>')
        )
    
    @output
    @render.ui
    def portfolio_daily_pnl():
        return ui.div(
            ui.h2("$0.00", class_="text-secondary mb-0"),
            ui.HTML('<small class="text-muted">+0.00%</small>')
        )
    
    @output
    @render.ui
    def portfolio_total_pnl():
        return ui.div(
            ui.h2("$0.00", class_="text-secondary mb-0"),
            ui.HTML('<small class="text-muted">+0.00%</small>')
        )
    
    @output
    @render.data_frame
    def portfolio_holdings_table():
        # Placeholder data structure for portfolio holdings
        placeholder_df = pd.DataFrame({
            'Token': ['Coming Soon'],
            'Symbol': ['-'],
            'Balance': [0.0],
            'Value (USD)': ['$0.00'],
            'Price': ['$0.00'],
            '24h Change': ['0.00%'],
            'P&L': ['$0.00']
        })
        return placeholder_df
    
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
        # USD equivalents for reference
        total_realized_gain_usd = fifo_df['realized_gain_usd'].sum()
        total_proceeds_usd = fifo_df['proceeds_usd'].sum()
        total_cost_basis_usd = fifo_df['cost_basis_sold_usd'].sum()
        unique_assets = fifo_df['asset'].nunique()
        total_transactions = len(fifo_df)
        
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
                'Quantity': [1.0],
                'Price (USD)': [3200.00],
                'Proceeds': [0.0],
                'Cost Basis': [0.0],
                'Realized Gain': [0.0],
                'Remaining Qty': [1.0]
            })
            return placeholder_df
        
        # Format FIFO results for display
        display_df = fifo_df.copy()
        display_df['Date'] = pd.to_datetime(display_df['date']).dt.strftime('%Y-%m-%d %H:%M:%S')
        display_df['Side'] = display_df['side']
        display_df['Token'] = display_df['asset']
        # Token amounts (for display)
        display_df['Token Amount'] = display_df['token_amount'].round(6)
        display_df['ETH Value'] = display_df['eth_value'].round(8)
        # Show ETH-based cost basis (primary)
        display_df['Proceeds (ETH)'] = display_df['proceeds_eth'].round(8)
        display_df['Cost Basis (ETH)'] = display_df['cost_basis_sold_eth'].round(8)
        display_df['Gain (ETH)'] = display_df['realized_gain_eth'].round(8)
        display_df['Remaining ETH'] = display_df['remaining_eth_value'].round(8)
        # Also show USD equivalents
        display_df['Proceeds (USD)'] = display_df['proceeds_usd'].round(2)
        display_df['Cost Basis (USD)'] = display_df['cost_basis_sold_usd'].round(2)
        display_df['Gain (USD)'] = display_df['realized_gain_usd'].round(2)
        display_df['Remaining Tokens'] = display_df['remaining_token_amount'].round(6)
        
        return display_df[[
            'Date', 'Side', 'Token', 'Token Amount', 'ETH Value', 
            'Proceeds (ETH)', 'Cost Basis (ETH)', 'Gain (ETH)', 'Remaining ETH',
            'Proceeds (USD)', 'Cost Basis (USD)', 'Gain (USD)', 'Remaining Tokens'
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
        display_df['Token Amount'] = display_df['token_amount'].round(6)
        display_df['ETH Value'] = display_df['eth_value'].round(8)
        display_df['USD Value'] = display_df['usd_value'].round(2)
        display_df['Cost Basis (ETH)'] = display_df['cost_basis_eth'].round(8)
        display_df['Cost Basis (USD)'] = display_df['cost_basis_usd'].round(2)
        
        # Add remaining quantity and cost basis columns
        display_df['Remaining Qty'] = display_df.get('remaining_qty', display_df['token_amount']).round(6)
        display_df['Remaining Cost (ETH)'] = display_df.get('remaining_cost_basis_eth', display_df['cost_basis_eth']).round(8)
        display_df['Lot Count'] = display_df['lot_count']
        
        return display_df[['Wallet', 'Asset', 'Token Amount', 'ETH Value', 'USD Value', 'Cost Basis (ETH)', 'Cost Basis (USD)', 
                          'Remaining Qty', 'Remaining Cost (ETH)', 'Lot Count']]
    
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
            
            # Process through FIFO
            logger.info("Processing transactions through FIFO...")
            fifo_df = build_fifo_ledger(fifo_input_df)
            logger.info(f"FIFO processing complete: {len(fifo_df)} result rows")
            fifo_results.set(fifo_df)
            
            # Get current positions
            tracker = FIFOTracker()
            for _, row in fifo_input_df.iterrows():
                # Calculate ETH value for this transaction
                token_qty = row['qty']
                price_eth = row['price_eth']
                eth_value = token_qty * price_eth
                
                tracker.process(
                    fund_id=row['fund_id'],
                    wallet=row['wallet_address'],
                    asset=row['asset'],
                    side=row['side'],
                    token_amount=token_qty,    # Token amount for display
                    eth_value=eth_value,       # ETH value for FIFO cost basis
                    date=row['date'],
                    tx_hash=row['hash'],
                    log=False,  # Don't log for position calculation
                    price_usd=row['price_usd'],
                    eth_usd_rate=row['eth_usd_rate']
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