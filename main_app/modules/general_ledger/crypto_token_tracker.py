# -*- coding: utf-8 -*-
"""
Streamlined Cryptocurrency Token Tracker

A focused tool for discovering and managing cryptocurrency token approvals:
- Fund selection → Wallet selection → Date range → Fetch transactions
- Token verification (verified/unverified with manual approval)
- Clean, simple interface without FIFO calculations or complex analytics
"""

from shiny import ui, render, reactive
import pandas as pd
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional
import logging

from ...s3_utils import load_WALLET_file, load_approved_tokens_file, save_approved_tokens_file
from ...services.blockchain_service import BlockchainService
from ...services.token_classifier import TokenClassifier
from ...config.blockchain_config import INFURA_URL, VERIFIED_TOKENS

# Set up logging
logger = logging.getLogger(__name__)


def crypto_token_tracker_ui():
    """Simple crypto token tracker UI"""
    return ui.page_fluid(
        ui.h2("Cryptocurrency Token Tracker", class_="mt-3"),
        ui.p("Discover and manage cryptocurrency tokens from blockchain transactions", class_="text-muted"),
        
        # Control Panel
        ui.card(
            ui.card_header(ui.HTML('<i class="fas fa-filter"></i> Transaction Filters')),
            ui.card_body(
                ui.row(
                    ui.column(
                        3,
                        ui.input_select(
                            "token_fund_select",
                            "Fund:",
                            choices={"all": "All Funds", "fund_i_class_B_ETH": "Fund I Class B", 
                                   "fund_ii_class_B_ETH": "Fund II Class B", "holdings_class_B_ETH": "Holdings"},
                            selected="fund_i_class_B_ETH"
                        )
                    ),
                    ui.column(
                        3,
                        ui.output_ui("token_wallet_select_ui")
                    ),
                    ui.column(
                        3,
                        ui.input_date_range(
                            "token_date_range",
                            "Date Range:",
                            start=date(2024, 7, 30),
                            end=date(2024, 7, 31)
                        )
                    ),
                    ui.column(
                        3,
                        ui.div(
                            ui.input_action_button(
                                "test_button_simple",
                                "TEST BUTTON (Simple)",
                                class_="btn-warning mt-2 mb-2"
                            ),
                            ui.input_action_button(
                                "fetch_token_transactions",
                                ui.HTML('<i class="fas fa-download"></i> Fetch Transactions'),
                                class_="btn-primary mt-2"
                            ),
                            class_="d-grid gap-2"
                        )
                    )
                )
            )
        ),
        
        # Status and Results
        ui.div(
            ui.output_ui("blockchain_service_status"),
            ui.output_ui("fetch_status"),
            class_="mt-3"
        ),
        
        # Results Tabs
        ui.navset_card_tab(
            ui.nav_panel(
                ui.HTML('<i class="fas fa-check-circle text-success"></i> Verified Tokens'),
                ui.div(
                    ui.p("Tokens from the verified whitelist (automatically approved)", class_="text-muted small"),
                    ui.output_data_frame("verified_tokens_table")
                )
            ),
            ui.nav_panel(
                ui.HTML('<i class="fas fa-question-circle text-warning"></i> Unverified Tokens'),
                ui.div(
                    ui.p("Tokens requiring manual review and approval", class_="text-muted small"),
                    ui.output_ui("unverified_tokens_actions"),
                    ui.output_data_frame("unverified_tokens_table")
                )
            ),
            ui.nav_panel(
                ui.HTML('<i class="fas fa-shield-alt text-info"></i> Approved Tokens'),
                ui.div(
                    ui.p("Previously approved tokens from manual review", class_="text-muted small"),
                    ui.output_data_frame("approved_tokens_table")
                )
            )
        )
    )


def register_crypto_token_tracker_outputs(output, input, session):
    """Register all crypto token tracker outputs"""
    
    # Initialize blockchain service
    blockchain_service = reactive.value(None)
    
    # Store fetched transactions
    fetched_transactions = reactive.value(pd.DataFrame())
    
    # Initialize blockchain service on startup
    @reactive.effect
    def init_blockchain_service():
        try:
            service = BlockchainService()
            blockchain_service.set(service)
            logger.info("Blockchain service initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing blockchain service: {e}")
    
    # Test button handler - DIAGNOSTIC
    @reactive.event(input.test_button_simple)
    def test_button_handler():
        print("🔥 TEST BUTTON CLICKED! Event handler is working!")
        logger.info("🔥 TEST BUTTON CLICKED! Event handler is working!")
    
    # Show blockchain service status
    @output
    @render.ui
    def blockchain_service_status():
        service = blockchain_service.get()
        if service is None:
            return ui.div(
                ui.div(
                    ui.HTML('<i class="fas fa-exclamation-triangle text-warning"></i> Blockchain service not initialized'),
                    class_="alert alert-warning"
                )
            )
        else:
            return ui.div(
                ui.div(
                    ui.HTML('<i class="fas fa-check-circle text-success"></i> Blockchain service ready'),
                    class_="alert alert-success"
                )
            )
    
    # Dynamic wallet selection based on fund
    @output
    @render.ui
    def token_wallet_select_ui():
        try:
            fund_filter = input.token_fund_select() if hasattr(input, 'token_fund_select') else "fund_i_class_B_ETH"
            
            # Load wallet data from S3
            wallet_df = load_WALLET_file()
            
            if wallet_df.empty:
                return ui.input_selectize(
                    "token_wallet_select",
                    "Wallets:",
                    choices={"all": "All Wallets (No Data)"},
                    selected="all",
                    multiple=True
                )
            
            # Filter wallets by selected fund
            if fund_filter and fund_filter != "all":
                fund_wallets = wallet_df[wallet_df["fund_id"] == fund_filter].copy()
            else:
                fund_wallets = wallet_df.copy()
            
            if fund_wallets.empty:
                return ui.input_selectize(
                    "token_wallet_select",
                    "Wallets:",
                    choices={"all": f"All Wallets (No wallets for {fund_filter})"},
                    selected="all",
                    multiple=True
                )
            
            # Create wallet choices with friendly names
            wallet_choices = {"all": "All Wallets"}
            for _, row in fund_wallets.iterrows():
                addr = str(row['wallet_address'])
                name = str(row.get('wallet_name', addr[:10] + "..."))
                wallet_choices[addr] = f"{name} ({addr[:6]}...{addr[-4:]})"
            
            return ui.input_selectize(
                "token_wallet_select",
                "Wallets:",
                choices=wallet_choices,
                selected="all",
                multiple=True
            )
            
        except Exception as e:
            logger.error(f"Error generating wallet selector: {e}")
            return ui.input_selectize(
                "token_wallet_select",
                "Wallets:",
                choices={"all": "Error loading wallets"},
                selected="all",
                multiple=True
            )
    
    # Fetch transactions from blockchain - SIMPLIFIED FOR TESTING
    @reactive.event(input.fetch_token_transactions)
    def fetch_token_transactions():
        print("🚀 FETCH TRANSACTIONS BUTTON CLICKED!")
        logger.info("🚀 FETCH TRANSACTIONS BUTTON CLICKED!")
        
        # Set some test data to verify the reactive system works
        test_data = pd.DataFrame({
            'tx_hash': ['0x123...', '0x456...'],
            'token_symbol': ['USDC', 'WETH'],
            'amount': [100, 0.5]
        })
        fetched_transactions.set(test_data)
        print(f"Set test data with {len(test_data)} rows")
        
        # TODO: Re-add full blockchain functionality once basic clicking works
    
    # Show fetch status
    @output
    @render.ui
    def fetch_status():
        df = fetched_transactions.get()
        if df.empty:
            return ui.div()
        
        total_txns = len(df)
        unique_tokens = df['token_symbol'].nunique() if 'token_symbol' in df.columns else 0
        
        return ui.div(
            ui.card(
                ui.card_body(
                    ui.div(
                        ui.div(
                            ui.h4(f"{total_txns:,}", class_="text-primary mb-0"),
                            ui.small("Transactions Found", class_="text-muted")
                        ),
                        ui.div(
                            ui.h4(f"{unique_tokens:,}", class_="text-info mb-0"),
                            ui.small("Unique Tokens", class_="text-muted")
                        ),
                        class_="d-flex justify-content-around"
                    )
                )
            ),
            class_="mb-3"
        )
    
    # Verified tokens table
    @output
    @render.data_frame
    def verified_tokens_table():
        df = fetched_transactions.get()
        if df.empty or 'token_address' not in df.columns:
            return pd.DataFrame(columns=['Token', 'Symbol', 'Address', 'Transaction Count'])
        
        # Filter for verified tokens
        verified_addresses = [addr.lower() for addr in VERIFIED_TOKENS.values()]
        verified_df = df[df['token_address'].str.lower().isin(verified_addresses)].copy()
        
        if verified_df.empty:
            return pd.DataFrame(columns=['Token', 'Symbol', 'Address', 'Transaction Count'])
        
        # Aggregate by token
        token_summary = verified_df.groupby(['token_address', 'token_symbol']).size().reset_index(name='Transaction Count')
        
        # Add token names from VERIFIED_TOKENS
        def get_token_name(address):
            for name, addr in VERIFIED_TOKENS.items():
                if addr.lower() == address.lower():
                    return name
            return "Unknown"
        
        token_summary['Token'] = token_summary['token_address'].apply(get_token_name)
        token_summary = token_summary.rename(columns={
            'token_symbol': 'Symbol',
            'token_address': 'Address'
        })
        
        return token_summary[['Token', 'Symbol', 'Address', 'Transaction Count']]
    
    # Unverified tokens table
    @output
    @render.data_frame
    def unverified_tokens_table():
        df = fetched_transactions.get()
        if df.empty or 'token_address' not in df.columns:
            return pd.DataFrame(columns=['Symbol', 'Address', 'Transaction Count', 'Action'])
        
        # Filter for unverified tokens
        verified_addresses = [addr.lower() for addr in VERIFIED_TOKENS.values()]
        unverified_df = df[~df['token_address'].str.lower().isin(verified_addresses)].copy()
        
        # Also exclude already approved tokens
        try:
            approved_tokens = load_approved_tokens_file()
            approved_addresses = [addr.lower() for addr in approved_tokens]
            unverified_df = unverified_df[~unverified_df['token_address'].str.lower().isin(approved_addresses)]
        except:
            pass  # If no approved tokens file, continue
        
        if unverified_df.empty:
            return pd.DataFrame(columns=['Symbol', 'Address', 'Transaction Count'])
        
        # Aggregate by token
        token_summary = unverified_df.groupby(['token_address', 'token_symbol']).size().reset_index(name='Transaction Count')
        token_summary = token_summary.rename(columns={
            'token_symbol': 'Symbol',
            'token_address': 'Address'
        })
        
        return token_summary[['Symbol', 'Address', 'Transaction Count']]
    
    # Approved tokens table
    @output
    @render.data_frame
    def approved_tokens_table():
        try:
            approved_tokens = load_approved_tokens_file()
            if not approved_tokens:
                return pd.DataFrame(columns=['Address', 'Approved Date'])
            
            # Create simple approved tokens display
            approved_df = pd.DataFrame({
                'Address': list(approved_tokens),
                'Status': ['Approved'] * len(approved_tokens)
            })
            
            return approved_df
            
        except Exception as e:
            logger.error(f"Error loading approved tokens: {e}")
            return pd.DataFrame(columns=['Address', 'Status'])
    
    # Action buttons for unverified tokens
    @output
    @render.ui
    def unverified_tokens_actions():
        df = fetched_transactions.get()
        if df.empty or 'token_address' not in df.columns:
            return ui.div()
        
        # Check if there are unverified tokens
        verified_addresses = [addr.lower() for addr in VERIFIED_TOKENS.values()]
        unverified_df = df[~df['token_address'].str.lower().isin(verified_addresses)].copy()
        
        if unverified_df.empty:
            return ui.div(
                ui.div(
                    ui.HTML('<i class="fas fa-check-circle text-success"></i> All tokens are verified!'),
                    class_="alert alert-success"
                )
            )
        
        return ui.div(
            ui.p("Review unverified tokens and approve those you trust:", class_="small text-muted mb-2"),
            ui.div(
                ui.input_text(
                    "token_address_to_approve",
                    "Token Address to Approve:",
                    placeholder="0x..."
                ),
                ui.div(
                    ui.input_action_button(
                        "approve_token_btn",
                        ui.HTML('<i class="fas fa-check"></i> Approve Token'),
                        class_="btn-success btn-sm me-2"
                    ),
                    ui.input_action_button(
                        "refresh_token_data",
                        ui.HTML('<i class="fas fa-sync"></i> Refresh'),
                        class_="btn-secondary btn-sm"
                    ),
                    class_="mt-2"
                ),
                class_="mb-3"
            )
        )
    
    # Handle token approval
    @reactive.event(input.approve_token_btn)
    def approve_token():
        try:
            token_address = input.token_address_to_approve()
            if not token_address or len(token_address) < 10:
                logger.warning("Invalid token address provided")
                return
            
            # Load current approved tokens
            try:
                approved_tokens = load_approved_tokens_file()
            except:
                approved_tokens = set()
            
            # Add new token
            approved_tokens.add(token_address.strip())
            
            # Save back to S3
            save_approved_tokens_file(approved_tokens)
            
            logger.info(f"Token {token_address} approved and saved to S3")
            
            # Clear the input
            # Note: In a real implementation, you'd want to trigger a UI update
            
        except Exception as e:
            logger.error(f"Error approving token: {e}")
    
    # Handle refresh
    @reactive.event(input.refresh_token_data)
    def refresh_token_data():
        # Trigger a re-fetch by calling the same logic as the fetch button
        try:
            # This will refresh all the tables
            input.fetch_token_transactions()
        except:
            pass