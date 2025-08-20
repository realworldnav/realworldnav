# -*- coding: utf-8 -*-
"""
Cryptocurrency Token Tracker

A comprehensive tool for discovering and managing cryptocurrency token transactions:
- Fund and wallet selection with date range filtering
- Real-time blockchain transaction fetching via Infura/Web3
- Token categorization (verified/unverified/approved)
- Manual token approval workflow with S3 persistence
"""

from shiny import ui, render, reactive
import pandas as pd
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Set
import logging
from decimal import Decimal
import requests
from functools import lru_cache

from ...s3_utils import load_WALLET_file, load_approved_tokens_file, save_approved_tokens_file
from ...services.blockchain_service import BlockchainService
from ...services.token_classifier import TokenClassifier
from ...config.blockchain_config import INFURA_URL, VERIFIED_TOKENS, TOKEN_STATUS

# Set up logging
logger = logging.getLogger(__name__)

# Global tracking for dynamic button handlers
_registered_handlers = set()
_address_to_sanitized_id = {}
_sanitized_id_to_address = {}

def sanitize_address_for_id(address: str) -> str:
    """
    Sanitize token address for use as HTML ID.
    Replaces problematic characters with safe alternatives.
    """
    if address in _address_to_sanitized_id:
        return _address_to_sanitized_id[address]
    
    # Remove 0x prefix and replace with 'addr_'
    sanitized = address.replace('0x', 'addr_').replace('-', '_').replace('.', '_')
    
    # Store bidirectional mapping
    _address_to_sanitized_id[address] = sanitized
    _sanitized_id_to_address[sanitized] = address
    
    return sanitized

def get_address_from_sanitized_id(sanitized_id: str) -> str:
    """Get original address from sanitized ID"""
    return _sanitized_id_to_address.get(sanitized_id, sanitized_id)


@lru_cache(maxsize=500)
def get_token_info_from_address(token_address: str) -> Dict[str, str]:
    """
    Get token name and symbol from contract address using CoinGecko API.
    Returns dict with 'name' and 'symbol' keys.
    """
    try:
        # Clean the address
        address = token_address.lower().strip()
        
        # Check if it's in our verified tokens first
        for symbol, addr in VERIFIED_TOKENS.items():
            if addr.lower() == address:
                return {"name": symbol, "symbol": symbol, "source": "verified"}
        
        # Try CoinGecko API
        url = f"https://api.coingecko.com/api/v3/coins/ethereum/contract/{address}"
        
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            return {
                "name": data.get("name", "Unknown Token"),
                "symbol": data.get("symbol", "UNKNOWN").upper(),
                "source": "coingecko"
            }
        elif response.status_code == 429:
            # Rate limited - use fallback immediately without logging too much
            pass
        else:
            logger.warning(f"CoinGecko API returned status {response.status_code} for {address}")
            
    except Exception as e:
        logger.error(f"Error fetching token info for {token_address}: {e}")
    
    # Fallback to address truncation
    return {
        "name": f"Token {token_address[:6]}...{token_address[-4:]}",
        "symbol": "UNKNOWN", 
        "source": "fallback"
    }


def crypto_token_tracker_ui():
    """Crypto token tracker UI with blockchain integration"""
    return ui.page_fluid(
        ui.h2("Cryptocurrency Token Tracker", class_="mt-3"),
        ui.p("Discover and manage cryptocurrency tokens from blockchain transactions", class_="text-muted"),
        
        # Control Panel
        ui.card(
            ui.card_header(ui.HTML('<i class="bi bi-funnel"></i> Transaction Filters')),
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
                                "fetch_token_transactions",
                                ui.HTML('<i class="bi bi-download"></i> Fetch Transactions'),
                                class_="btn-primary mt-4"
                            ),
                            # Native Shiny progress will appear automatically during fetch
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
        
        # New 2-Tab Results Layout
        ui.navset_card_tab(
            # Tab 1: Review - For reviewing and approving unverified tokens and transactions
            ui.nav_panel(
                ui.HTML('<i class="bi bi-search text-warning"></i> Review'),
                ui.div(
                    ui.p("Review transactions and approve unverified tokens", class_="text-muted small mb-3"),
                    
                    # Status Dashboard
                    ui.output_ui("review_status_dashboard"),
                    
                    # Main Review Layout - Split between transactions and token approval
                    ui.row(
                        # Left Panel (70%) - Transactions Table
                        ui.column(
                            8,
                            ui.card(
                                ui.card_header(
                                    ui.div(
                                        ui.HTML('<i class="bi bi-list-ul text-primary"></i> All Transactions'),
                                        ui.div(
                                            ui.output_ui("transaction_filter_controls"),
                                            class_="float-end"
                                        ),
                                        class_="d-flex justify-content-between align-items-center"
                                    )
                                ),
                                ui.card_body(
                                    ui.output_data_frame("review_transactions_table")
                                )
                            )
                        ),
                        
                        # Right Panel (30%) - Token Approval Sidebar
                        ui.column(
                            4,
                            ui.card(
                                ui.card_header(ui.HTML('<i class="bi bi-shield-exclamation text-warning"></i> Token Approval')),
                                ui.card_body(
                                    ui.output_ui("token_approval_sidebar")
                                )
                            ),
                            
                            # Token Management Section
                            ui.card(
                                ui.card_header(ui.HTML('<i class="bi bi-gear text-info"></i> Token Management')),
                                ui.card_body(
                                    ui.output_ui("token_management_section")
                                )
                            )
                        )
                    )
                )
            ),
            
            # Tab 2: Ready - For approved transactions ready for FIFO processing
            ui.nav_panel(
                ui.HTML('<i class="bi bi-check-circle text-success"></i> Ready'),
                ui.div(
                    ui.p("Approved transactions ready for FIFO processing", class_="text-muted small mb-3"),
                    
                    # Ready Status and Controls
                    ui.div(
                        ui.output_ui("ready_status_summary"),
                        class_="mb-3"
                    ),
                    
                    # FIFO Push Controls
                    ui.card(
                        ui.card_header(ui.HTML('<i class="bi bi-arrow-right text-success"></i> FIFO Processing')),
                        ui.card_body(
                            ui.row(
                                ui.column(
                                    8,
                                    ui.input_action_button(
                                        "push_to_fifo",
                                        ui.HTML('<i class="bi bi-arrow-right"></i> Push All Ready Transactions to FIFO'),
                                        class_="btn-success btn-lg"
                                    ),
                                    ui.HTML('<small class="text-muted d-block mt-2">This will stage all approved transactions for FIFO processing</small>')
                                ),
                                ui.column(
                                    4,
                                    ui.output_ui("push_status_display")
                                )
                            )
                        )
                    ),
                    
                    # Ready Transactions Table
                    ui.row(
                        ui.column(
                            9,
                            ui.card(
                                ui.card_header(ui.HTML('<i class="bi bi-check-square text-success"></i> Ready Transactions')),
                                ui.card_body(
                                    ui.output_data_frame("ready_transactions_table")
                                )
                            )
                        ),
                        ui.column(
                            3,
                            ui.card(
                                ui.card_header("Transaction Details"),
                                ui.card_body(
                                    ui.output_ui("transaction_details_card")
                                )
                            )
                        )
                    )
                )
            )
        )
    )


def register_crypto_token_tracker_outputs(output, input, session):
    """Register all crypto token tracker outputs with blockchain integration"""
    
    # Helper function for wallet-to-fund mapping
    @reactive.calc
    def get_wallet_to_fund_mapping():
        """Create mapping dictionary from wallet_address to fund_id"""
        try:
            wallet_df = load_WALLET_file()
            if not wallet_df.empty and 'wallet_address' in wallet_df.columns and 'fund_id' in wallet_df.columns:
                # Create mapping dictionary, handling case variations
                mapping = {}
                for _, row in wallet_df.iterrows():
                    wallet_addr = str(row['wallet_address']).strip()
                    fund_id = str(row['fund_id']).strip()
                    if wallet_addr and fund_id:
                        # Store both original and lowercase versions for flexible matching
                        mapping[wallet_addr] = fund_id
                        mapping[wallet_addr.lower()] = fund_id
                return mapping
        except Exception as e:
            logger.warning(f"Could not load wallet mapping: {e}")
        return {}
    
    # Initialize blockchain service
    blockchain_service = reactive.value(None)
    
    # Store fetched transactions
    fetched_transactions = reactive.value(pd.DataFrame())
    
    # Store fetch progress
    fetch_in_progress = reactive.value(False)
    fetch_progress_message = reactive.value("")
    
    # Track approved tokens changes for reactive updates
    approved_tokens_updated = reactive.value(0)
    
    # Track save status for user feedback
    save_status_msg = reactive.value("")
    
    # Store transactions staged for FIFO processing
    staged_transactions = reactive.value(pd.DataFrame())
    
    # Track push to FIFO status
    push_status = reactive.value("")
    
    # Progress tracking for long-running operations  
    fetch_progress_percent = reactive.value(-1)  # -1 means no operation
    push_progress_percent = reactive.value(-1)   # -1 means no operation
    
    # Reactive trigger for staged transactions updates (for cross-module reactivity)
    staged_transactions_trigger = reactive.value(0)
    
    # Initialize blockchain service on startup
    @reactive.effect
    def init_blockchain_service():
        try:
            service = BlockchainService()
            blockchain_service.set(service)
            logger.info("Blockchain service initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing blockchain service: {e}")
    
    # Show blockchain service status
    @output
    @render.ui
    def blockchain_service_status():
        service = blockchain_service.get()
        if service is None:
            return ui.div(
                ui.div(
                    ui.HTML('<i class="bi bi-exclamation-triangle text-warning"></i> Blockchain service not initialized'),
                    class_="alert alert-warning"
                )
            )
        else:
            return ui.div()  # Service is ready, no need to show status
    
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
    
    # Show inline fetch progress right under the button
    @output
    @render.ui
    def fetch_progress_inline():
        is_fetching = fetch_in_progress.get()
        print(f"🔍 Inline progress check - is_fetching: {is_fetching}")
        if is_fetching:
            print("📊 Showing inline progress!")
            return ui.div(
                ui.HTML(f'<i class="bi bi-arrow-clockwise text-primary me-1"></i>{fetch_progress_message.get()}'),
                ui.div(
                    ui.div(
                        class_="progress-bar progress-bar-striped progress-bar-animated bg-primary",
                        style="width: 100%",
                        role="progressbar"
                    ),
                    class_="progress mt-2",
                    style="height: 8px"
                ),
                class_="mt-2",
                style="font-size: 0.9em;"
            )
        return ui.div()
    
    # Fetch transactions from blockchain
    @reactive.effect
    @reactive.event(input.fetch_token_transactions)
    async def fetch_token_transactions():
        print("🚀 FETCH TRANSACTIONS BUTTON CLICKED!")
        logger.info("Fetch transactions button clicked")
        
        service = blockchain_service.get()
        if not service:
            print("❌ Blockchain service not initialized")
            logger.error("Blockchain service not initialized")
            fetch_progress_message.set("Error: Blockchain service not initialized")
            return
        
        print("✅ Blockchain service is ready")
        
        # Start native Shiny progress
        with ui.Progress(min=0, max=100) as progress:
            try:
                # Step 1: Initialize progress tracking
                progress.set(5, message="Preparing to fetch transactions...", detail="Initializing blockchain service")
                print("🟢 Started native Shiny progress tracking")
                fetch_in_progress.set(True)
                
                # Step 2: Parse parameters
                progress.set(15, message="Parsing parameters...", detail="Getting fund, wallet, and date selections")
                
                # Get selected parameters
                fund_id = input.token_fund_select()
                wallet_selection = input.token_wallet_select()
                date_range = input.token_date_range()
                
                # Parse date range with timezone awareness
                from datetime import timezone
                start_date = datetime.combine(date_range[0], datetime.min.time()).replace(tzinfo=timezone.utc)
                end_date = datetime.combine(date_range[1], datetime.max.time()).replace(tzinfo=timezone.utc)
                
                print(f"📅 Date range selected: {date_range[0]} to {date_range[1]}")
                print(f"📅 Converted to datetime: {start_date} to {end_date}")
                logger.info(f"Fetching transactions for fund: {fund_id}, wallets: {wallet_selection}, dates: {start_date} to {end_date}")
                
                # Step 3: Prepare wallet addresses
                progress.set(25, message="Preparing wallet addresses...", detail=f"Processing {len(wallet_selection) if wallet_selection and 'all' not in wallet_selection else 'all'} wallets")
                
                # Prepare wallet addresses
                wallet_addresses = None
                if wallet_selection and "all" not in wallet_selection:
                    wallet_addresses = wallet_selection
                
                # Use fund_id only if not "all"
                fund_param = None if fund_id == "all" else fund_id
                
                # Enhanced progress callback for blockchain service
                def update_progress(current, total, tx_count):
                    # Calculate percentage for blockchain fetching (25-70%)
                    if total > 0:
                        wallet_progress = (current / total) * 45  # 45% of progress for wallet processing
                        overall_progress = 25 + wallet_progress
                        progress.set(overall_progress, 
                                   message=f"Fetching from blockchain...", 
                                   detail=f"Processing wallet {current}/{total} - Found {tx_count} transactions")
                
                # Step 4: Start blockchain fetch
                progress.set(30, message="Fetching transactions from blockchain...", 
                           detail="This may take a few moments depending on date range")
                
                try:
                    print(f"🚀 Starting blockchain fetch with parameters:")
                    print(f"   📅 Start date: {start_date}")
                    print(f"   📅 End date: {end_date}")
                    print(f"   💰 Fund: {fund_param}")
                    print(f"   👛 Wallets: {len(wallet_addresses) if wallet_addresses else 'All'}")
                    
                    df = service.fetch_transactions_for_period(
                        start_date=start_date,
                        end_date=end_date,
                        fund_id=fund_param,
                        wallet_addresses=wallet_addresses,
                        progress_callback=update_progress
                    )
                    
                    print(f"📊 Raw fetch result: {len(df)} transactions")
                    if not df.empty:
                        print(f"🔍 Raw columns: {list(df.columns)}")
                        print(f"🔍 First transaction sample: {df.iloc[0].to_dict() if len(df) > 0 else 'None'}")
                except Exception as blockchain_error:
                    if "429" in str(blockchain_error) or "Too Many Requests" in str(blockchain_error):
                        fetch_progress_message.set("Rate limited by Infura. Please wait a few minutes and try again with a smaller date range.")
                        logger.error(f"Rate limited by Infura: {blockchain_error}")
                        return
                    else:
                        raise  # Re-raise if not a rate limit error
                
                # Step 5: Process fetched data
                progress.set(75, message="Processing fetched data...", detail="Analyzing blockchain results")
                
                if df.empty:
                    progress.set(100, message="No transactions found", detail="Try adjusting your search criteria")
                    fetch_progress_message.set("No transactions found for the selected criteria")
                    fetched_transactions.set(pd.DataFrame())
                else:
                    # Step 6: Start data processing
                    progress.set(80, message="Processing transaction data...", detail=f"Processing {len(df)} transactions")
                    
                    # Process the fetched data - make outgoing amounts negative
                    processed_df = df.copy()
                    
                    # Filter by date range to ensure we only get transactions in the selected range
                    if 'date' in processed_df.columns:
                        print(f"📅 Before date filtering: {len(processed_df)} transactions")
                        processed_df['date'] = pd.to_datetime(processed_df['date'])
                        date_mask = (
                            (processed_df['date'] >= start_date) & 
                            (processed_df['date'] <= end_date)
                        )
                        processed_df = processed_df[date_mask].copy()
                        print(f"📅 After date filtering: {len(processed_df)} transactions")
                    
                    # Step 7: Field normalization
                    progress.set(90, message="Normalizing transaction fields...", detail="Formatting data for display")
                    
                    # Note: Amount signing is now handled in blockchain_service based on side/qty
                    # The blockchain service already provides properly signed quantities in 'qty' field
                    print(f"🔍 Processing columns: {list(processed_df.columns)}")
                    
                    # Field normalization: Map qty → token_amount for display compatibility
                    if 'qty' in processed_df.columns:
                        processed_df['token_amount'] = processed_df['qty'].abs()  # Use absolute value for display
                        print(f"📊 Mapped 'qty' to 'token_amount' (absolute values for display)")
                    
                    # Quantity validation: Ensure proper signs are preserved for FIFO processing
                    if 'side' in processed_df.columns:
                        print(f"📊 Side distribution: {processed_df['side'].value_counts().to_dict()}")
                        if 'qty' in processed_df.columns:
                            buy_qty_check = processed_df[processed_df['side'] == 'buy']['qty']
                            sell_qty_check = processed_df[processed_df['side'] == 'sell']['qty']
                            print(f"📊 Buy quantities (should be positive): {buy_qty_check.describe()}")
                            print(f"📊 Sell quantities (should be negative): {sell_qty_check.describe()}")
                    
                    # Step 8: Complete processing
                    progress.set(100, message="Fetch completed successfully!", 
                               detail=f"Successfully processed {len(processed_df)} transactions")
                    
                    logger.info(f"Fetched and processed {len(processed_df)} transactions")
                    fetched_transactions.set(processed_df)
                    fetch_progress_message.set(f"✅ Successfully fetched {len(processed_df)} transactions!")
                
            except Exception as e:
                logger.error(f"Error fetching transactions: {e}")
                fetch_progress_message.set(f"Error: {str(e)}")
                fetched_transactions.set(pd.DataFrame())
                progress.set(0, message="Error occurred", detail=str(e))
                raise e
            
            finally:
                print("🔴 Setting fetch_in_progress to False")
                fetch_in_progress.set(False)
                # Progress bar will auto-close when the 'with' block exits
    
    # Show fetch status summary
    @output
    @render.ui
    def fetch_status():
        df = fetched_transactions.get()
        if df.empty:
            return ui.div()
        
        # Calculate statistics
        total_txns = len(df)
        unique_tokens = df['token_symbol'].nunique() if 'token_symbol' in df.columns else 0
        unique_wallets = df['wallet_address'].nunique() if 'wallet_address' in df.columns else 0
        
        # Calculate value totals
        total_value_usd = df['token_value_usd'].sum() if 'token_value_usd' in df.columns else 0
        
        return ui.div(
            ui.card(
                ui.card_body(
                    ui.row(
                        ui.column(
                            3,
                            ui.div(
                                ui.h4(f"{total_txns:,}", class_="text-primary mb-0"),
                                ui.HTML('<small class="text-muted">Transactions</small>')
                            )
                        ),
                        ui.column(
                            3,
                            ui.div(
                                ui.h4(f"{unique_tokens:,}", class_="text-info mb-0"),
                                ui.HTML('<small class="text-muted">Unique Tokens</small>')
                            )
                        ),
                        ui.column(
                            3,
                            ui.div(
                                ui.h4(f"{unique_wallets:,}", class_="text-success mb-0"),
                                ui.HTML('<small class="text-muted">Active Wallets</small>')
                            )
                        ),
                        ui.column(
                            3,
                            ui.div(
                                ui.h4(f"${total_value_usd:,.2f}", class_="text-warning mb-0"),
                                ui.HTML('<small class="text-muted">Total Volume</small>')
                            )
                        )
                    )
                )
            ),
            class_="mb-3"
        )
    
    # Review Status Dashboard
    @output
    @render.ui
    def review_status_dashboard():
        df = fetched_transactions.get()
        if df.empty:
            return ui.div()
        
        # Calculate statistics for the dashboard
        total_txns = len(df)
        
        # Get verified and approved addresses
        verified_addresses = {addr.lower() for addr in VERIFIED_TOKENS.values()}
        try:
            approved_tokens = load_approved_tokens_file()
            approved_addresses = {addr.lower() for addr in approved_tokens}
        except:
            approved_addresses = set()
        
        # Count transactions by status
        verified_txns = len(df[df['token_address'].str.lower().isin(verified_addresses)])
        approved_txns = len(df[df['token_address'].str.lower().isin(approved_addresses)])
        unverified_txns = total_txns - verified_txns - approved_txns
        
        # Count unique tokens by status
        verified_tokens_count = df[df['token_address'].str.lower().isin(verified_addresses)]['token_address'].nunique()
        approved_tokens_count = df[df['token_address'].str.lower().isin(approved_addresses)]['token_address'].nunique()
        unverified_tokens_count = df[
            (~df['token_address'].str.lower().isin(verified_addresses)) &
            (~df['token_address'].str.lower().isin(approved_addresses))
        ]['token_address'].nunique()
        
        return ui.div(
            ui.row(
                ui.column(
                    3,
                    ui.div(
                        ui.h4(f"{total_txns:,}", class_="text-primary mb-0"),
                        ui.HTML('<small class="text-muted">Total Transactions</small>')
                    )
                ),
                ui.column(
                    3,
                    ui.div(
                        ui.h4(f"{verified_txns + approved_txns:,}", class_="text-success mb-0"),
                        ui.HTML('<small class="text-muted">Ready Transactions</small>')
                    )
                ),
                ui.column(
                    3,
                    ui.div(
                        ui.h4(f"{unverified_txns:,}", class_="text-warning mb-0"),
                        ui.HTML('<small class="text-muted">Need Review</small>')
                    )
                ),
                ui.column(
                    3,
                    ui.div(
                        ui.h4(f"{unverified_tokens_count:,}", class_="text-danger mb-0"),
                        ui.HTML('<small class="text-muted">Tokens to Approve</small>')
                    )
                )
            ),
            class_="mb-3 p-3 border rounded bg-light"
        )
    
    # Enhanced Review Transactions Table with proper IN/OUT flow
    @output
    @render.data_frame
    def review_transactions_table():
        df = fetched_transactions.get()
        if df.empty:
            return pd.DataFrame(columns=['Date', 'Fund', 'Wallet ID', 'Token', 'IN/OUT', 'Token Amount', 'Value (ETH)', 'Value (USD)', 'Status', 'Hash'])
        
        # Get wallet-to-fund mapping
        wallet_to_fund = get_wallet_to_fund_mapping()
        
        # Get verified and approved addresses
        verified_addresses = {addr.lower() for addr in VERIFIED_TOKENS.values()}
        try:
            approved_tokens = load_approved_tokens_file()
            approved_addresses = {addr.lower() for addr in approved_tokens}
        except:
            approved_addresses = set()
        
        # Prepare display dataframe with enhanced columns
        formatted_data = []
        
        for _, row in df.iterrows():
            token_addr = str(row.get('token_address', '')).lower()
            
            # Determine transaction direction and amount sign
            side = row.get('side', '').lower()
            direction = row.get('direction', '').upper()
            qty = float(row.get('qty', 0))
            
            # Determine IN/OUT and proper amount signing
            if side == 'sell' or direction == 'OUT' or qty < 0:
                in_out = "OUT"
                amount_multiplier = -1 if qty > 0 else 1  # Ensure negative for OUT
            else:
                in_out = "IN" 
                amount_multiplier = 1 if qty > 0 else -1   # Ensure positive for IN
            
            # Use token_amount for display, properly signed
            token_amount = float(row.get('token_amount', 0)) * amount_multiplier
            
            # Determine status with enhanced icons
            if token_addr in verified_addresses:
                status = "✅ Verified"
                status_class = "success"
            elif token_addr in approved_addresses:
                status = "☑️ Approved"
                status_class = "info"
            else:
                status = "⚠️ Needs Review"
                status_class = "warning"
            
            # Format wallet ID (shortened)
            wallet_id = str(row.get('wallet_id', ''))
            if len(wallet_id) > 10:
                wallet_id_display = f"{wallet_id[:6]}...{wallet_id[-4:]}"
            else:
                wallet_id_display = wallet_id
            
            # Get fund ID from wallet mapping
            fund_id = wallet_to_fund.get(wallet_id, wallet_to_fund.get(wallet_id.lower(), 'Unknown Fund'))
            
            # Format hash (shortened)
            tx_hash = str(row.get('tx_hash', ''))
            if len(tx_hash) > 10:
                hash_display = f"{tx_hash[:8]}...{tx_hash[-4:]}"
            else:
                hash_display = tx_hash
            
            formatted_row = {
                'Date': pd.to_datetime(row.get('date', '')).strftime('%Y-%m-%d %H:%M') if row.get('date') else '',
                'Fund': fund_id,
                'Wallet ID': wallet_id_display,
                'Token': row.get('token_symbol', 'Unknown'),
                'IN/OUT': in_out,
                'Token Amount': f"{token_amount:,.6f}",
                'Value (ETH)': f"{float(row.get('token_value_eth', 0)):,.6f} ETH" if row.get('token_value_eth') else '0 ETH',
                'Value (USD)': f"${float(row.get('token_value_usd', 0)):,.2f}" if row.get('token_value_usd') else '$0.00',
                'Status': status,
                'Hash': hash_display,
                '_status_class': status_class,  # Hidden column for styling
                '_token_address': token_addr,  # Hidden column for approval actions
                '_full_hash': tx_hash,  # Hidden column for Etherscan
                '_wallet_id': str(row.get('wallet_id', '')),  # Full wallet ID
                '_row_index': len(formatted_data)  # For selection tracking
            }
            formatted_data.append(formatted_row)
        
        final_df = pd.DataFrame(formatted_data)
        if final_df.empty:
            return final_df
        
        # Sort by status (unverified first, then by date)
        status_priority = {'⚠️ Needs Review': 0, '☑️ Approved': 1, '✅ Verified': 2}
        final_df['_sort_priority'] = final_df['Status'].map(status_priority)
        final_df = final_df.sort_values(['_sort_priority', 'Date'], ascending=[True, False])
        
        from shiny import render
        return render.DataGrid(
            final_df[['Date', 'Fund', 'Wallet ID', 'Token', 'IN/OUT', 'Token Amount', 'Value (ETH)', 'Value (USD)', 'Status', 'Hash']].head(100),
            selection_mode="row",
            height="500px",
            filters=True
        )
    
    # Token Approval Sidebar
    @output
    @render.ui
    def token_approval_sidebar():
        # Watch for changes in approved tokens to trigger refresh
        approved_tokens_updated.get()
        
        df = fetched_transactions.get()
        if df.empty or 'token_address' not in df.columns:
            return ui.div(
                ui.div(
                    ui.HTML('<i class="bi bi-info-circle text-muted"></i>'),
                    ui.p("No transactions to review", class_="text-muted mb-0 ms-2"),
                    class_="d-flex align-items-center"
                ),
                ui.p("Please fetch transactions first.", class_="small text-muted mt-2")
            )
        
        # Get verified and approved addresses
        verified_addresses = {addr.lower() for addr in VERIFIED_TOKENS.values()}
        try:
            approved_tokens = load_approved_tokens_file()
            approved_addresses = {addr.lower() for addr in approved_tokens}
        except:
            approved_addresses = set()
        
        # Filter for unverified tokens
        unverified_df = df[
            (~df['token_address'].str.lower().isin(verified_addresses)) &
            (~df['token_address'].str.lower().isin(approved_addresses))
        ].copy()
        
        if unverified_df.empty:
            return ui.div(
                ui.div(
                    ui.HTML('<i class="bi bi-check-circle text-success"></i>'),
                    ui.strong("All tokens approved!", class_="text-success ms-2"),
                    class_="d-flex align-items-center mb-2"
                ),
                ui.p("All transaction tokens are either verified or manually approved.", class_="small text-muted")
            )
        
        # Get unique unverified tokens with stats
        token_stats = unverified_df.groupby(['token_address', 'token_symbol']).agg({
            'tx_hash': 'count',
            'token_value_usd': 'sum'
        }).reset_index()
        token_stats.columns = ['Address', 'Symbol', 'Tx_Count', 'Total_USD']
        token_stats = token_stats.sort_values('Tx_Count', ascending=False)
        
        # Create approval interface
        approval_components = []
        
        # Individual token approval section header (removed bulk actions as requested)
        
        # Individual token approval cards
        approval_components.append(ui.h6(f"Tokens Needing Approval ({len(token_stats)})", class_="text-danger mb-2"))
        
        for _, token in token_stats.head(10).iterrows():  # Show top 10 tokens
            token_addr = token['Address']
            token_symbol = token['Symbol']
            tx_count = token['Tx_Count']
            total_usd = token['Total_USD']
            
            # Sanitize address for use in HTML IDs
            sanitized_id = sanitize_address_for_id(token_addr)
            
            # Get token info for display
            token_info = get_token_info_from_address(token_addr)
            
            # Determine risk level by volume
            if total_usd > 1000:
                risk_badge = ui.span("Low Risk", class_="badge bg-success")
            elif total_usd > 100:
                risk_badge = ui.span("Medium Risk", class_="badge bg-warning")
            else:
                risk_badge = ui.span("High Risk", class_="badge bg-danger")
            
            token_card = ui.div(
                ui.card(
                    ui.card_body(
                        # Token header
                        ui.div(
                            ui.div(
                                ui.strong(token_symbol, class_="text-primary"),
                                ui.br(),
                                ui.HTML(f'<small class="text-muted">{token_info["name"]}</small>')
                            ),
                            risk_badge,
                            class_="d-flex justify-content-between align-items-center mb-2"
                        ),
                        
                        # Token stats
                        ui.div(
                            ui.HTML(f'<small class="text-muted d-block">Transactions: {tx_count}</small>'),
                            ui.HTML(f'<small class="text-muted d-block">Volume: ${total_usd:,.2f}</small>'),
                            ui.HTML(f'<small class="text-muted d-block">Address: {token_addr[:8]}...{token_addr[-6:]}</small>'),
                            class_="mb-2"
                        ),
                        
                        # Action buttons with sanitized IDs
                        ui.div(
                            ui.input_action_button(
                                f"approve_token_{sanitized_id}",
                                ui.HTML('<i class="bi bi-check"></i>'),
                                class_="btn-success btn-sm me-1",
                                title=f"Approve {token_symbol}"
                            ),
                            ui.input_action_button(
                                f"reject_token_{sanitized_id}",
                                ui.HTML('<i class="bi bi-x"></i>'),
                                class_="btn-danger btn-sm me-1",
                                title=f"Reject {token_symbol}"
                            ),
                            ui.input_action_button(
                                f"info_token_{sanitized_id}",
                                ui.HTML('<i class="bi bi-info"></i>'),
                                class_="btn-info btn-sm",
                                title=f"View {token_symbol} on Etherscan"
                            ),
                            class_="d-flex justify-content-center"
                        )
                    )
                ),
                class_="mb-2"
            )
            approval_components.append(token_card)
        
        if len(token_stats) > 10:
            approval_components.append(
                ui.div(
                    ui.p(f"... and {len(token_stats) - 10} more tokens", class_="text-muted small text-center"),
                    class_="mt-2"
                )
            )
        
        return ui.div(*approval_components)
    
    # Token Management Section
    @output
    @render.ui
    def token_management_section():
        # Watch for changes in approved tokens
        approved_tokens_updated.get()
        
        try:
            approved_tokens = load_approved_tokens_file()
            approved_count = len(approved_tokens) if approved_tokens else 0
        except:
            approved_count = 0
        
        return ui.div(
            ui.h6("Token Management", class_="text-info mb-2"),
            
            # Add token manually
            ui.div(
                ui.input_text(
                    "new_approved_token_address",
                    "Add Token Address:",
                    placeholder="0x...",
                    value=""
                ),
                ui.input_action_button(
                    "add_approved_token",
                    ui.HTML('<i class="bi bi-plus"></i> Add'),
                    class_="btn-success btn-sm mt-2"
                ),
                class_="mb-3"
            ),
            
            # Stats
            ui.div(
                ui.p(f"Approved Tokens: {approved_count}", class_="small text-muted mb-1"),
                ui.p(f"Verified Tokens: {len(VERIFIED_TOKENS)}", class_="small text-muted mb-1"),
                class_="border-top pt-2"
            ),
            
            # Management actions
            ui.div(
                ui.input_action_button(
                    "view_approved_tokens",
                    ui.HTML('<i class="bi bi-list"></i> View All Approved'),
                    class_="btn-outline-info btn-sm me-1"
                ),
                ui.input_action_button(
                    "export_token_list",
                    ui.HTML('<i class="bi bi-download"></i> Export'),
                    class_="btn-outline-secondary btn-sm"
                ),
                class_="d-flex justify-content-center mt-2"
            )
        )
    
    # Transaction Filter Controls
    @output
    @render.ui
    def transaction_filter_controls():
        df = fetched_transactions.get()
        if df.empty:
            return ui.div()
        
        return ui.div(
            ui.input_selectize(
                "status_filter",
                "Filter by Status:",
                choices={
                    "all": "All Transactions",
                    "verified": "Verified Only", 
                    "approved": "Approved Only",
                    "unverified": "Needs Review Only"
                },
                selected="all",
                multiple=False
            ),
            class_="d-flex align-items-center"
        )
    
    # Ready Status Summary
    @output
    @render.ui
    def ready_status_summary():
        df = fetched_transactions.get()
        if df.empty:
            return ui.div(
                ui.div(
                    ui.HTML('<i class="bi bi-info-circle text-muted"></i>'),
                    ui.p("No transactions available", class_="text-muted mb-0 ms-2"),
                    class_="d-flex align-items-center"
                )
            )
        
        # Get verified and approved addresses
        verified_addresses = {addr.lower() for addr in VERIFIED_TOKENS.values()}
        try:
            approved_tokens = load_approved_tokens_file()
            approved_addresses = {addr.lower() for addr in approved_tokens}
        except:
            approved_addresses = set()
        
        # Count ready transactions (verified + approved)
        ready_df = df[
            (df['token_address'].str.lower().isin(verified_addresses)) |
            (df['token_address'].str.lower().isin(approved_addresses))
        ]
        
        total_ready = len(ready_df)
        total_value = ready_df['token_value_usd'].sum() if not ready_df.empty else 0
        unique_tokens = ready_df['token_address'].nunique() if not ready_df.empty else 0
        
        return ui.div(
            ui.row(
                ui.column(
                    4,
                    ui.div(
                        ui.h3(f"{total_ready:,}", class_="text-success mb-0"),
                        ui.HTML('<small class="text-muted">Ready Transactions</small>')
                    )
                ),
                ui.column(
                    4,
                    ui.div(
                        ui.h3(f"${total_value:,.2f}", class_="text-primary mb-0"),
                        ui.HTML('<small class="text-muted">Total Value</small>')
                    )
                ),
                ui.column(
                    4,
                    ui.div(
                        ui.h3(f"{unique_tokens:,}", class_="text-info mb-0"),
                        ui.HTML('<small class="text-muted">Approved Tokens</small>')
                    )
                )
            ),
            class_="text-center p-3 border rounded bg-light"
        )
    
    # Enhanced Ready Transactions Table
    @output
    @render.data_frame
    def ready_transactions_table():
        df = fetched_transactions.get()
        if df.empty:
            return pd.DataFrame(columns=['Date', 'Fund', 'Wallet ID', 'Token', 'IN/OUT', 'Token Amount', 'Value (ETH)', 'Value (USD)', 'Status', 'Hash'])
        
        # Get wallet-to-fund mapping
        wallet_to_fund = get_wallet_to_fund_mapping()
        
        # Get verified and approved addresses
        verified_addresses = {addr.lower() for addr in VERIFIED_TOKENS.values()}
        try:
            approved_tokens = load_approved_tokens_file()
            approved_addresses = {addr.lower() for addr in approved_tokens}
        except:
            approved_addresses = set()
        
        # Filter for ready transactions (verified + approved)
        ready_df = df[
            (df['token_address'].str.lower().isin(verified_addresses)) |
            (df['token_address'].str.lower().isin(approved_addresses))
        ].copy()
        
        if ready_df.empty:
            return pd.DataFrame(columns=['Date', 'Fund', 'Wallet ID', 'Token', 'IN/OUT', 'Token Amount', 'Value (ETH)', 'Value (USD)', 'Status', 'Hash'])
        
        # Prepare display dataframe with enhanced columns
        formatted_data = []
        
        for _, row in ready_df.iterrows():
            token_addr = str(row.get('token_address', '')).lower()
            
            # Determine transaction direction and amount sign
            side = row.get('side', '').lower()
            direction = row.get('direction', '').upper()
            qty = float(row.get('qty', 0))
            
            # Determine IN/OUT and proper amount signing
            if side == 'sell' or direction == 'OUT' or qty < 0:
                in_out = "OUT"
                amount_multiplier = -1 if qty > 0 else 1  # Ensure negative for OUT
            else:
                in_out = "IN" 
                amount_multiplier = 1 if qty > 0 else -1   # Ensure positive for IN
            
            # Use token_amount for display, properly signed
            token_amount = float(row.get('token_amount', 0)) * amount_multiplier
            
            # Determine status
            if token_addr in verified_addresses:
                status = "✅ Verified"
            elif token_addr in approved_addresses:
                status = "☑️ Approved"
            else:
                status = "❓ Unknown"  # Should not happen but safety check
            
            # Format wallet ID (shortened)
            wallet_id = str(row.get('wallet_id', ''))
            if len(wallet_id) > 10:
                wallet_id_display = f"{wallet_id[:6]}...{wallet_id[-4:]}"
            else:
                wallet_id_display = wallet_id
            
            # Get fund ID from wallet mapping
            fund_id = wallet_to_fund.get(wallet_id, wallet_to_fund.get(wallet_id.lower(), 'Unknown Fund'))
            
            # Format hash (shortened)
            tx_hash = str(row.get('tx_hash', ''))
            if len(tx_hash) > 10:
                hash_display = f"{tx_hash[:8]}...{tx_hash[-4:]}"
            else:
                hash_display = tx_hash
            
            formatted_row = {
                'Date': pd.to_datetime(row.get('date', '')).strftime('%Y-%m-%d %H:%M') if row.get('date') else '',
                'Fund': fund_id,
                'Wallet ID': wallet_id_display,
                'Token': row.get('token_symbol', 'Unknown'),
                'IN/OUT': in_out,
                'Token Amount': f"{token_amount:,.6f}",
                'Value (ETH)': f"{float(row.get('token_value_eth', 0)):,.6f} ETH" if row.get('token_value_eth') else '0 ETH',
                'Value (USD)': f"${float(row.get('token_value_usd', 0)):,.2f}" if row.get('token_value_usd') else '$0.00',
                'Status': status,
                'Hash': hash_display,
                '_full_hash': tx_hash,  # Hidden column for Etherscan
                '_wallet_id': str(row.get('wallet_id', '')),  # Full wallet ID
                '_row_index': len(formatted_data)  # For selection tracking
            }
            formatted_data.append(formatted_row)
        
        final_df = pd.DataFrame(formatted_data)
        if final_df.empty:
            return final_df
        
        # Sort by date (newest first)
        final_df = final_df.sort_values('Date', ascending=False)
        
        from shiny import render
        return render.DataGrid(
            final_df[['Date', 'Fund', 'Wallet ID', 'Token', 'IN/OUT', 'Token Amount', 'Value (ETH)', 'Value (USD)', 'Status', 'Hash']].head(100),
            selection_mode="row",
            height="400px",
            filters=True
        )

    # Store edited table data and transaction selection state
    edited_table_data = reactive.value(pd.DataFrame())
    selected_transaction = reactive.value(None)
    selected_transaction_hash = reactive.value(None)
    
    # Legacy approved tokens table (kept for backward compatibility but simplified)
    @output
    @render.data_frame
    def approved_tokens_table():
        # This table is no longer used in the new 2-tab UI but kept for compatibility
        try:
            approved_tokens = load_approved_tokens_file()
            if not approved_tokens:
                return pd.DataFrame(columns=['Token Name', 'Symbol', 'Token Address'])
            
            approved_data = []
            for token_addr in approved_tokens:
                token_info = get_token_info_from_address(token_addr)
                approved_data.append({
                    'Token Name': token_info['name'],
                    'Symbol': token_info['symbol'], 
                    'Token Address': token_addr
                })
            
            return pd.DataFrame(approved_data)
            
        except Exception as e:
            logger.error(f"Error loading approved tokens: {e}")
            return pd.DataFrame(columns=['Token Name', 'Symbol', 'Token Address'])
    
    # All transactions table with selection support
    @output
    @render.data_frame
    def all_transactions_table():
        df = fetched_transactions.get()
        if df.empty:
            return pd.DataFrame(columns=['Date', 'Fund', 'Hash', 'Wallet ID', 'Side', 'Token Name', 'Amount (of token)', 'Value (ETH)', 'Value (USD)', 'Intercompany', 'From', 'To'])
        
        # Get wallet-to-fund mapping
        wallet_to_fund = get_wallet_to_fund_mapping()
        
        # Prepare display dataframe - NEW STRUCTURE
        formatted_data = []
        
        for _, row in df.iterrows():
            formatted_row = {}
            
            # Date
            if 'date' in row:
                formatted_row['Date'] = pd.to_datetime(row['date']).strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_row['Date'] = ""
            
            # Fund - get from wallet mapping
            if 'wallet_id' in row and row['wallet_id']:
                wallet_id = str(row['wallet_id'])
                fund_id = wallet_to_fund.get(wallet_id, wallet_to_fund.get(wallet_id.lower(), 'Unknown Fund'))
                formatted_row['Fund'] = fund_id
            else:
                formatted_row['Fund'] = "Unknown Fund"
            
            # Hash (shortened)
            if 'tx_hash' in row:
                tx_hash = str(row['tx_hash'])
                formatted_row['Hash'] = f"{tx_hash[:10]}...{tx_hash[-6:]}" if len(tx_hash) > 16 else tx_hash
            else:
                formatted_row['Hash'] = ""
            
            # Wallet ID (shortened)
            if 'wallet_id' in row and row['wallet_id']:
                wallet_id = str(row['wallet_id'])
                formatted_row['Wallet ID'] = f"{wallet_id[:6]}...{wallet_id[-4:]}"
            else:
                formatted_row['Wallet ID'] = ""
            
            # Side (buy/sell)
            formatted_row['Side'] = row.get('side', "")
            
            # Token Name (using new field)
            formatted_row['Token Name'] = row.get('token_name', row.get('token_symbol', row.get('asset', 'Unknown')))
            
            # Amount (of token) - use signed qty for proper buy/sell display
            if 'qty' in row and pd.notna(row['qty']):
                formatted_row['Amount (of token)'] = f"{float(row['qty']):,.6f}"
            elif 'token_amount' in row and pd.notna(row['token_amount']):
                # Fallback to token_amount if qty not available
                formatted_row['Amount (of token)'] = f"{float(row['token_amount']):,.6f}"
            else:
                formatted_row['Amount (of token)'] = "0"
            
            # Value (ETH)
            if 'token_value_eth' in row and pd.notna(row['token_value_eth']):
                formatted_row['Value (ETH)'] = f"{float(row['token_value_eth']):.6f} ETH"
            else:
                formatted_row['Value (ETH)'] = "0 ETH"
            
            # Value (USD)
            if 'token_value_usd' in row and pd.notna(row['token_value_usd']):
                formatted_row['Value (USD)'] = f"${float(row['token_value_usd']):,.2f}"
            else:
                formatted_row['Value (USD)'] = "$0.00"
            
            # Intercompany flag
            intercompany = row.get('intercompany', False)
            formatted_row['Intercompany'] = "Yes" if intercompany else "No"
            
            # From (shortened address)
            if 'from_address' in row and row['from_address']:
                from_addr = str(row['from_address'])
                formatted_row['From'] = f"{from_addr[:6]}...{from_addr[-4:]}"
            else:
                formatted_row['From'] = ""
            
            # To (shortened address) 
            if 'to_address' in row and row['to_address']:
                to_addr = str(row['to_address'])
                formatted_row['To'] = f"{to_addr[:6]}...{to_addr[-4:]}"
            else:
                formatted_row['To'] = ""
            
            formatted_data.append(formatted_row)
        
        # Create final dataframe with exact column order
        final_df = pd.DataFrame(formatted_data)
        column_order = ['Date', 'Fund', 'Hash', 'Wallet ID', 'Side', 'Token Name', 'Amount (of token)', 'Value (ETH)', 'Value (USD)', 'Intercompany', 'From', 'To']
        
        # Ensure all columns exist
        for col in column_order:
            if col not in final_df.columns:
                final_df[col] = ""
        
        # Add original tx_hash as a hidden column for Etherscan lookup
        if not df.empty and 'tx_hash' in df.columns:
            # Create a mapping between display row and original tx_hash
            final_df['_original_tx_hash'] = df['tx_hash'].head(100).values
        
        from shiny import render
        return render.DataGrid(
            final_df[column_order].head(100),  # Limit to 100 rows for performance
            selection_mode="row"
        )
    
    # Legacy unverified tokens actions (keeping for backward compatibility but simplified)
    @output
    @render.ui
    def unverified_tokens_actions():
        # This function is no longer used in the new 2-tab UI
        # Keeping minimal implementation for backward compatibility
        return ui.div(
            ui.div(
                ui.input_action_button(
                    "refresh_token_data",
                    ui.HTML('<i class="bi bi-arrow-clockwise"></i> Refresh Tables'),
                    class_="btn-secondary btn-sm"
                ),
                class_="text-center"
            )
        )
    
    # Dynamic button handler registration system
    registered_token_handlers = reactive.value(set())
    
    # Token discovery and handler registration
    @reactive.effect
    @reactive.event(fetched_transactions)
    def discover_and_register_token_handlers():
        """Discover tokens in fetched transactions and register handlers"""
        df = fetched_transactions.get()
        if df.empty or 'token_address' not in df.columns:
            return
        
        # Get verified and approved addresses
        verified_addresses = {addr.lower() for addr in VERIFIED_TOKENS.values()}
        try:
            approved_tokens = load_approved_tokens_file()
            approved_addresses = {addr.lower() for addr in approved_tokens}
        except:
            approved_addresses = set()
        
        # Find unverified tokens that need handlers
        unverified_df = df[
            (~df['token_address'].str.lower().isin(verified_addresses)) &
            (~df['token_address'].str.lower().isin(approved_addresses))
        ]
        
        if not unverified_df.empty:
            unique_tokens = unverified_df['token_address'].unique()
            current_registered = registered_token_handlers.get()
            
            for token_addr in unique_tokens:
                if token_addr not in current_registered:
                    # Register handlers for this new token
                    create_token_button_handlers(token_addr)
                    current_registered.add(token_addr)
            
            registered_token_handlers.set(current_registered)
            logger.info(f"Registered handlers for {len(unique_tokens)} tokens")
    
    def create_token_button_handlers(token_address):
        """Create individual button handlers for a specific token"""
        sanitized_id = sanitize_address_for_id(token_address)
        
        # Create approve button handler
        @reactive.effect
        @reactive.event(lambda: getattr(input, f'approve_token_{sanitized_id}', lambda: 0)())
        def approve_handler():
            button_value = getattr(input, f'approve_token_{sanitized_id}', lambda: 0)()
            if button_value > 0:
                handle_token_approval(token_address)
        
        # Create reject button handler  
        @reactive.effect
        @reactive.event(lambda: getattr(input, f'reject_token_{sanitized_id}', lambda: 0)())
        def reject_handler():
            button_value = getattr(input, f'reject_token_{sanitized_id}', lambda: 0)()
            if button_value > 0:
                handle_token_rejection(token_address)
        
        # Create info button handler
        @reactive.effect
        @reactive.event(lambda: getattr(input, f'info_token_{sanitized_id}', lambda: 0)())
        def info_handler():
            button_value = getattr(input, f'info_token_{sanitized_id}', lambda: 0)()
            if button_value > 0:
                handle_token_info(token_address)
        
        logger.debug(f"Created handlers for token {token_address} (ID: {sanitized_id})")
    
    def handle_token_approval(token_address):
        """Handle approval of a specific token"""
        try:
            # Load current approved tokens
            try:
                approved_tokens = load_approved_tokens_file()
            except:
                approved_tokens = set()
            
            # Check if already approved
            if token_address in approved_tokens:
                logger.info(f"Token {token_address} is already approved")
                return
            
            # Add new token
            approved_tokens.add(token_address.strip())
            
            # Save back to S3
            save_approved_tokens_file(approved_tokens)
            
            # Get token info for logging
            token_info = get_token_info_from_address(token_address)
            logger.info(f"✅ Approved token: {token_info['name']} ({token_address})")
            
            # Trigger reactive updates for ALL tables
            approved_tokens_updated.set(approved_tokens_updated.get() + 1)
            
        except Exception as e:
            logger.error(f"Error approving token {token_address}: {e}")
    
    def handle_token_rejection(token_address):
        """Handle rejection of a specific token"""
        try:
            # Remove transactions with this token from fetched data
            current_df = fetched_transactions.get()
            filtered_df = current_df[current_df['token_address'] != token_address]
            fetched_transactions.set(filtered_df)
            
            # Get token info for logging
            token_info = get_token_info_from_address(token_address)
            logger.info(f"❌ Rejected token: {token_info['name']} ({token_address})")
            
        except Exception as e:
            logger.error(f"Error rejecting token {token_address}: {e}")
    
    def handle_token_info(token_address):
        """Handle info request for a specific token"""
        try:
            # Generate Etherscan token URL
            etherscan_url = f"https://etherscan.io/token/{token_address}"
            
            # Get token info for logging
            token_info = get_token_info_from_address(token_address)
            logger.info(f"🔍 Etherscan URL for {token_info['name']} ({token_address}): {etherscan_url}")
            
            # In a real web environment, this would open the URL in a new tab
            # For now, the URL is logged so users can copy it
            
        except Exception as e:
            logger.error(f"Error generating Etherscan link for token {token_address}: {e}")
    
    # Handle view approved tokens (kept for token management)
    @reactive.effect
    @reactive.event(input.view_approved_tokens)
    def view_approved_tokens():
        try:
            approved_tokens = load_approved_tokens_file()
            if not approved_tokens:
                logger.info("📋 No approved tokens to display")
                return
            
            logger.info(f"📋 Approved Tokens ({len(approved_tokens)}):")
            for i, token_addr in enumerate(approved_tokens, 1):
                token_info = get_token_info_from_address(token_addr)
                logger.info(f"  {i}. {token_info['name']} ({token_info['symbol']}) - {token_addr}")
                
        except Exception as e:
            logger.error(f"Error viewing approved tokens: {e}")
    
    # Handle export token list (kept for token management)
    @reactive.effect
    @reactive.event(input.export_token_list)
    def export_token_list():
        try:
            approved_tokens = load_approved_tokens_file()
            if not approved_tokens:
                logger.info("📤 No approved tokens to export")
                return
            
            # Create export data
            export_data = []
            for token_addr in approved_tokens:
                token_info = get_token_info_from_address(token_addr)
                export_data.append({
                    'address': token_addr,
                    'name': token_info['name'],
                    'symbol': token_info['symbol'],
                    'source': token_info['source']
                })
            
            # Create DataFrame and log export info
            import pandas as pd
            export_df = pd.DataFrame(export_data)
            
            # In a real implementation, this would trigger a file download
            logger.info(f"📤 Export data ready for {len(export_data)} approved tokens:")
            logger.info(f"Export preview:\n{export_df.to_string(index=False)}")
            
        except Exception as e:
            logger.error(f"Error exporting token list: {e}")
    
    # Handle view all approved tokens
    @reactive.effect
    @reactive.event(input.view_approved_tokens)
    def view_approved_tokens():
        try:
            # Load approved tokens
            approved_tokens = load_approved_tokens_file()
            
            if not approved_tokens:
                # Show modal with no tokens message
                m = ui.modal(
                    ui.div(
                        ui.p("No approved tokens found.", class_="text-muted"),
                        ui.p("Add tokens using the 'Add' button in the Token Management section.", class_="small")
                    ),
                    title="Approved Tokens",
                    size="lg",
                    easy_close=True,
                    footer=ui.modal_button("Close")
                )
                ui.modal_show(m)
                return
            
            # Create table data for approved tokens
            table_rows = []
            for i, token_addr in enumerate(sorted(approved_tokens), 1):
                token_info = get_token_info_from_address(token_addr)
                table_rows.append(
                    ui.tags.tr(
                        ui.tags.td(str(i), style="width: 50px;"),
                        ui.tags.td(
                            ui.div(
                                ui.strong(token_info['name']),
                                ui.br(),
                                ui.span(token_info['symbol'], class_="text-muted small")
                            )
                        ),
                        ui.tags.td(
                            ui.div(
                                ui.code(token_addr, class_="small"),
                                ui.br(),
                                ui.span(f"Source: {token_info['source']}", class_="text-muted small")
                            )
                        ),
                        ui.tags.td(
                            ui.div(
                                ui.tags.a(
                                    ui.HTML('<i class="bi bi-box-arrow-up-right"></i> Etherscan'),
                                    href=f"https://etherscan.io/token/{token_addr}",
                                    target="_blank",
                                    class_="btn btn-sm btn-outline-primary"
                                ),
                                style="text-align: right;"
                            )
                        )
                    )
                )
            
            # Create modal with approved tokens table
            m = ui.modal(
                ui.div(
                    ui.p(f"Total approved tokens: {len(approved_tokens)}", class_="mb-3"),
                    ui.div(
                        ui.tags.table(
                            ui.tags.thead(
                                ui.tags.tr(
                                    ui.tags.th("#", style="width: 50px;"),
                                    ui.tags.th("Token"),
                                    ui.tags.th("Address"),
                                    ui.tags.th("Actions", style="text-align: right;")
                                )
                            ),
                            ui.tags.tbody(*table_rows),
                            class_="table table-hover table-sm"
                        ),
                        style="max-height: 500px; overflow-y: auto;"
                    )
                ),
                title="All Approved Tokens",
                size="xl",
                easy_close=True,
                footer=ui.modal_button("Close")
            )
            ui.modal_show(m)
            
            logger.info(f"Displaying {len(approved_tokens)} approved tokens in modal")
            
        except Exception as e:
            logger.error(f"Error viewing approved tokens: {e}")
    
    # Handle legacy token approval (removed - no longer needed with new UI)
    # The approve_selected_token button no longer exists in the new 2-tab UI
    # All token approval is now handled by the dynamic button handler system
    
    # Handle refresh
    @reactive.effect
    @reactive.event(input.refresh_token_data)
    def refresh_token_data():
        # Trigger all reactive updates
        approved_tokens_updated.set(approved_tokens_updated.get() + 1)
        logger.info("Refresh button clicked - all tables will update")
    
    # Dynamic dropdown for token removal
    @output
    @render.ui
    def remove_token_dropdown():
        # Watch for changes in approved tokens
        approved_tokens_updated.get()
        
        try:
            approved_tokens = load_approved_tokens_file()
            if not approved_tokens:
                return ui.input_select(
                    "token_to_remove_select",
                    "Select Token to Remove:",
                    choices={"": "No tokens available"}
                )
            
            # Create dropdown choices with token info
            choices = {"": "Select token to remove..."}
            for token_addr in approved_tokens:
                token_info = get_token_info_from_address(token_addr)
                display_name = f"{token_info['name']} ({token_info['symbol']})"
                choices[token_addr] = display_name
            
            return ui.input_select(
                "token_to_remove_select",
                "Select Token to Remove:",
                choices=choices
            )
            
        except Exception as e:
            logger.error(f"Error creating remove token dropdown: {e}")
            return ui.input_select(
                "token_to_remove_select",
                "Select Token to Remove:",
                choices={"": "Error loading tokens"}
            )
    
    # Dynamic dropdown for Etherscan token lookup
    @output
    @render.ui
    def etherscan_token_dropdown():
        # Watch for changes in approved tokens
        approved_tokens_updated.get()
        
        try:
            approved_tokens = load_approved_tokens_file()
            if not approved_tokens:
                return ui.input_select(
                    "token_to_lookup_select",
                    "Select Token:",
                    choices={"": "No tokens available"}
                )
            
            # Create dropdown choices with token info
            choices = {"": "Select token to lookup..."}
            for token_addr in approved_tokens:
                token_info = get_token_info_from_address(token_addr)
                display_name = f"{token_info['name']} ({token_info['symbol']})"
                choices[token_addr] = display_name
            
            return ui.input_select(
                "token_to_lookup_select",
                "Select Token:",
                choices=choices
            )
            
        except Exception as e:
            logger.error(f"Error creating etherscan token dropdown: {e}")
            return ui.input_select(
                "token_to_lookup_select",
                "Select Token:",
                choices={"": "Error loading tokens"}
            )
    
    # Save status message display
    @output
    @render.ui
    def save_status_message():
        message = save_status_msg.get()
        if not message:
            return ui.div()
        
        # Determine message type based on content
        if "successfully" in message.lower() or "saved" in message.lower():
            return ui.div(
                ui.HTML(f'<i class="bi bi-check-circle text-success"></i>'),
                ui.span(message, class_="text-success ms-2"),
                class_="mt-2 d-flex align-items-center"
            )
        elif "error" in message.lower() or "failed" in message.lower():
            return ui.div(
                ui.HTML(f'<i class="bi bi-exclamation-triangle text-danger"></i>'),
                ui.span(message, class_="text-danger ms-2"),
                class_="mt-2 d-flex align-items-center"
            )
        else:
            return ui.div(
                ui.HTML(f'<i class="bi bi-info-circle text-info"></i>'),
                ui.span(message, class_="text-info ms-2"),
                class_="mt-2 d-flex align-items-center"
            )
    
    # Add approved token
    @reactive.effect
    @reactive.event(input.add_approved_token)
    def add_approved_token():
        try:
            new_address = input.new_approved_token_address()
            if not new_address or len(new_address.strip()) < 10:
                logger.warning("Invalid token address provided for adding")
                return
            
            # Clean the address
            clean_address = new_address.strip()
            if not clean_address.startswith('0x'):
                clean_address = '0x' + clean_address
            
            # Load current approved tokens
            try:
                approved_tokens = load_approved_tokens_file()
            except:
                approved_tokens = set()
            
            # Check if already exists
            if clean_address in approved_tokens:
                logger.info(f"Token {clean_address} is already approved")
                return
            
            # Add new token
            approved_tokens.add(clean_address)
            
            # Save to S3
            save_approved_tokens_file(approved_tokens)
            
            # Get token info for logging
            token_info = get_token_info_from_address(clean_address)
            logger.info(f"Added approved token: {token_info['name']} ({clean_address})")
            
            # Trigger reactive updates
            approved_tokens_updated.set(approved_tokens_updated.get() + 1)
            
        except Exception as e:
            logger.error(f"Error adding approved token: {e}")
    
    # Remove selected approved token
    @reactive.effect
    @reactive.event(input.remove_selected_token)
    def remove_selected_token():
        try:
            token_to_remove = input.token_to_remove_select()
            if not token_to_remove:
                logger.warning("No token selected for removal")
                return
            
            # Load current approved tokens
            try:
                approved_tokens = load_approved_tokens_file()
            except:
                approved_tokens = set()
            
            # Check if token exists
            if token_to_remove not in approved_tokens:
                logger.warning(f"Token {token_to_remove} not found in approved list")
                return
            
            # Remove token
            approved_tokens.discard(token_to_remove)
            
            # Save to S3
            save_approved_tokens_file(approved_tokens)
            
            # Get token info for logging
            token_info = get_token_info_from_address(token_to_remove)
            logger.info(f"Removed approved token: {token_info['name']} ({token_to_remove})")
            
            # Trigger reactive updates
            approved_tokens_updated.set(approved_tokens_updated.get() + 1)
            
        except Exception as e:
            logger.error(f"Error removing approved token: {e}")
    
    # Lookup selected token on Etherscan
    @reactive.effect
    @reactive.event(input.lookup_etherscan_token)
    def lookup_etherscan_token():
        try:
            token_to_lookup = input.token_to_lookup_select()
            if not token_to_lookup:
                logger.warning("No token selected for Etherscan lookup")
                return
            
            # Generate Etherscan token URL
            etherscan_url = f"https://etherscan.io/token/{token_to_lookup}"
            
            # Get token info for logging
            token_info = get_token_info_from_address(token_to_lookup)
            logger.info(f"Opening Etherscan for token: {token_info['name']} ({token_to_lookup}) at {etherscan_url}")
            
            # Open in new window/tab using JavaScript
            from shiny import ui
            ui.insert_ui(
                ui.tags.script(f'window.open("{etherscan_url}", "_blank");'),
                selector="body",
                where="afterEnd"
            )
            
        except Exception as e:
            logger.error(f"Error looking up token on Etherscan: {e}")
    
    # Save all table changes
    @reactive.effect
    @reactive.event(input.save_table_changes)
    def save_table_changes():
        try:
            # Get the edited data from our reactive store
            edited_data = edited_table_data.get()
            
            logger.info(f"Getting edited data from reactive store")
            logger.info(f"Edited data type: {type(edited_data)}")
            logger.info(f"Edited data shape: {edited_data.shape if hasattr(edited_data, 'shape') else 'N/A'}")
            
            if edited_data is None or len(edited_data) == 0:
                save_status_msg.set("⚠️ No data to save - table appears empty")
                logger.warning("No edited data in reactive store")
                return
            
            # Convert to DataFrame if needed
            if not isinstance(edited_data, pd.DataFrame):
                edited_df = pd.DataFrame(edited_data)
            else:
                edited_df = edited_data.copy()
            
            logger.info(f"Saving edited table data with {len(edited_df)} rows")
            logger.info(f"Columns: {list(edited_df.columns)}")
            
            # Validate required columns exist
            required_columns = ['Token Name', 'Symbol', 'Token Address', 'Approval Status']
            missing_columns = [col for col in required_columns if col not in edited_df.columns]
            if missing_columns:
                save_status_msg.set(f"❌ Missing required columns: {missing_columns}")
                return
            
            # Extract approved token addresses from the edited data
            approved_addresses = set()
            for _, row in edited_df.iterrows():
                token_address = str(row.get('Token Address', '')).strip()
                approval_status = str(row.get('Approval Status', '')).strip()
                token_name = str(row.get('Token Name', 'Unknown'))
                
                # Validate token address format
                if token_address and token_address.startswith('0x') and len(token_address) == 42:
                    # Check if token should be approved based on status
                    if approval_status.lower() in ['approved', 'approve', 'yes', 'true']:
                        approved_addresses.add(token_address)
                        logger.info(f"Keeping approved: {token_name} ({token_address})")
                    else:
                        logger.info(f"Removing (status: {approval_status}): {token_name} ({token_address})")
                else:
                    logger.warning(f"Invalid token address: {token_address}")
            
            # Save the updated approved tokens list to S3
            save_approved_tokens_file(approved_addresses)
            
            # Trigger reactive updates to refresh all components
            approved_tokens_updated.set(approved_tokens_updated.get() + 1)
            
            # Set success message
            save_status_msg.set(f"✅ Successfully saved {len(approved_addresses)} approved tokens")
            
            logger.info(f"Successfully saved {len(approved_addresses)} approved tokens to S3")
            
        except Exception as e:
            logger.error(f"Error saving table changes: {e}")
            save_status_msg.set(f"❌ Error saving: {str(e)}")
    
    # Debug table data access
    @reactive.effect
    @reactive.event(input.debug_table_data)
    def debug_table_data():
        try:
            logger.info("=== DEBUG: Table Data Access ===")
            
            # Try all available input methods
            available_methods = []
            
            # Check what input methods are available
            input_attrs = [attr for attr in dir(input) if 'approved_tokens_table' in attr]
            logger.info(f"Available input attributes: {input_attrs}")
            
            for attr in input_attrs:
                try:
                    method = getattr(input, attr)
                    if callable(method):
                        result = method()
                        available_methods.append(f"{attr}: {type(result)} (len={len(result) if hasattr(result, '__len__') else 'N/A'})")
                        logger.info(f"{attr}(): {type(result)} with length {len(result) if hasattr(result, '__len__') else 'N/A'}")
                        if hasattr(result, 'columns'):
                            logger.info(f"  Columns: {list(result.columns)}")
                    else:
                        available_methods.append(f"{attr}: {type(method)} (not callable)")
                except Exception as e:
                    available_methods.append(f"{attr}: ERROR - {e}")
                    logger.warning(f"Error calling {attr}: {e}")
            
            # Set debug message
            debug_msg = f"🐛 Debug Info:\n" + "\n".join(available_methods)
            save_status_msg.set(debug_msg)
            logger.info("=== END DEBUG ===")
            
        except Exception as e:
            logger.error(f"Error during debug: {e}")
            save_status_msg.set(f"🐛 Debug error: {str(e)}")
    
    # Show transaction details card with inline Etherscan link
    @output
    @render.ui
    def transaction_details_card():
        df = fetched_transactions.get()
        
        if df.empty:
            return ui.div(
                ui.div(
                    ui.HTML('<i class="bi bi-info-circle text-muted"></i>'),
                    ui.p("No transactions available", class_="text-muted mb-0 ms-2"),
                    class_="d-flex align-items-center"
                ),
                ui.p("Please fetch transactions first.", class_="small text-muted mt-2")
            )
        
        # Get selected rows from either review or ready table
        try:
            # Check current_selection first (this tracks both tables)
            selection_info = current_selection.get()
            print(f"DEBUG transaction_details_card: current_selection = {selection_info}")
            
            has_selection = selection_info["table"] is not None and len(selection_info["rows"]) > 0
            selected_rows = selection_info["rows"]
            selected_table = selection_info["table"]
            
            # Fallback to input-based approach for review table
            if not has_selection:
                try:
                    cell_selection_input = input.review_transactions_table_cell_selection()
                    selected_rows = list(cell_selection_input.get("rows", [])) if cell_selection_input else []
                    has_selection = len(selected_rows) > 0
                    selected_table = "review" if has_selection else None
                    print(f"DEBUG transaction_details_card: fallback input cell_selection = {cell_selection_input}")
                except:
                    pass
            
            print(f"DEBUG transaction_details_card: has_selection = {has_selection}, selected_table = {selected_table}, selected_rows = {selected_rows}")
            
        except Exception as e:
            print(f"DEBUG: Error getting selection: {e}")
            has_selection = False
            selected_rows = []
            selected_table = None
        
        if not has_selection:
            return ui.div(
                ui.div(
                    ui.HTML('<i class="bi bi-hand-index text-primary"></i>'),
                    ui.p("Select a transaction", class_="text-primary mb-0 ms-2"),
                    class_="d-flex align-items-center"
                ),
                ui.p("Click on a transaction row to view details and access Etherscan.", class_="small text-muted mt-2")
            )
        
        # Get the selected transaction data
        if has_selection and len(selected_rows) > 0:
            # Fallback to manual reconstruction if using cell_selection
            selected_idx = selected_rows[0]
            
            # We need to reconstruct the formatted data to match what's displayed in review_transactions_table
            # Get verified and approved addresses
            verified_addresses = {addr.lower() for addr in VERIFIED_TOKENS.values()}
            try:
                approved_tokens = load_approved_tokens_file()
                approved_addresses = {addr.lower() for addr in approved_tokens}
            except:
                approved_addresses = set()
            
            # Recreate the formatted data (same logic as review_transactions_table)
            formatted_data = []
            for _, row in df.iterrows():
                token_addr = str(row.get('token_address', ''))
                
                # Determine status
                if token_addr.lower() in verified_addresses:
                    status = "✅ Verified"
                elif token_addr.lower() in approved_addresses:
                    status = "☑️ Approved" 
                else:
                    status = "⚠️ Needs Review"
                
                formatted_row = {
                    'tx_hash': row.get('tx_hash', ''),
                    'date': row.get('date', ''),
                    'token_name': row.get('token_name', row.get('token_symbol', 'Unknown')),
                    'token_amount': row.get('token_amount', 0),
                    'token_value_usd': row.get('token_value_usd', 0),
                    'token_value_eth': row.get('token_value_eth', 0),
                    'direction': row.get('direction', 'Unknown'),
                    'from_address': row.get('from_address', ''),
                    'to_address': row.get('to_address', ''),
                    'status': status,
                    '_sort_priority': 0 if status == "⚠️ Needs Review" else (1 if status == "☑️ Approved" else 2)
                }
                formatted_data.append(formatted_row)
            
            # Sort the same way as review_transactions_table
            formatted_df = pd.DataFrame(formatted_data)
            if not formatted_df.empty:
                formatted_df = formatted_df.sort_values(['_sort_priority', 'date'], ascending=[True, False])
                formatted_df = formatted_df.head(100)  # Same limit as table
            
            # Now get the correct row
            if selected_idx < len(formatted_df):
                tx_row = formatted_df.iloc[selected_idx]
                tx_hash = tx_row['tx_hash']
                etherscan_url = f"https://etherscan.io/tx/{tx_hash}"
            else:
                tx_hash = ""
                etherscan_url = ""
        else:
            tx_hash = ""
            etherscan_url = ""
        
        if tx_hash:
            
            # Get additional transaction details for display
            date_str = tx_row.get('date', 'Unknown')
            if hasattr(date_str, 'strftime'):
                date_str = date_str.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(date_str, str):
                try:
                    date_str = pd.to_datetime(date_str).strftime('%Y-%m-%d %H:%M:%S')
                except:
                    pass
            
            token_name = tx_row.get('token_name', 'Unknown')
            direction = tx_row.get('direction', 'Unknown')
            amount = tx_row.get('token_amount', 0)
            value_usd = tx_row.get('token_value_usd', 0)
            value_eth = tx_row.get('token_value_eth', 0)
            from_addr = tx_row.get('from_address', '')
            to_addr = tx_row.get('to_address', '')
            
            return ui.div(
                # Transaction header
                ui.div(
                    ui.HTML('<i class="bi bi-receipt text-success"></i>'),
                    ui.strong("Transaction Selected", class_="text-success ms-2"),
                    class_="d-flex align-items-center mb-3"
                ),
                
                # Transaction details
                ui.div(
                    ui.div(
                        ui.HTML('<small class="text-muted">Hash</small>'),
                        ui.div(ui.code(tx_hash, class_="small"), class_="mt-1"),
                        class_="mb-2"
                    ),
                    ui.div(
                        ui.HTML('<small class="text-muted">Date</small>'),
                        ui.div(date_str, class_="mt-1 small"),
                        class_="mb-2"
                    ),
                    ui.div(
                        ui.HTML('<small class="text-muted">Token</small>'),
                        ui.div(token_name, class_="mt-1 small fw-bold"),
                        class_="mb-2"
                    ),
                    ui.div(
                        ui.HTML('<small class="text-muted">Direction</small>'),
                        ui.div(
                            ui.span(direction, class_="badge bg-success" if direction == "IN" else "badge bg-danger"),
                            class_="mt-1"
                        ),
                        class_="mb-2"
                    ),
                    ui.div(
                        ui.HTML('<small class="text-muted">Amount</small>'),
                        ui.div(f"{amount:,.6f}", class_="mt-1 small font-monospace"),
                        class_="mb-2"
                    ),
                    ui.div(
                        ui.HTML('<small class="text-muted">Value</small>'),
                        ui.div(
                            ui.div(f"{value_eth:.6f} ETH", class_="small font-monospace"),
                            ui.div(f"${value_usd:,.2f}", class_="small font-monospace text-success"),
                            class_="mt-1"
                        ),
                        class_="mb-3"
                    ),
                ),
                
                # Etherscan link
                ui.hr(),
                ui.div(
                    ui.HTML(f'<a href="{etherscan_url}" target="_blank" class="btn btn-primary w-100">'),
                    ui.HTML('<i class="bi bi-box-arrow-up-right me-2"></i>View on Etherscan'),
                    ui.HTML('</a>'),
                    class_="text-center"
                )
            )
        
        return ui.div(
            ui.div(
                ui.HTML('<i class="bi bi-exclamation-triangle text-warning"></i>'),
                ui.p("Invalid selection", class_="text-warning mb-0 ms-2"),
                class_="d-flex align-items-center"
            ),
            ui.p("Unable to retrieve transaction details. Please try selecting a different transaction.", class_="small text-muted mt-2")
        )
    
    # Create a reactive value to track selection changes
    current_selection = reactive.value({"table": None, "rows": []})
    
    # Handle transaction selection from review and ready tables
    @reactive.effect
    def handle_transaction_selection():
        """Handle transaction selection from either table using proper Shiny approach"""
        try:
            # Check which table has selection using proper cell_selection method
            review_selection = []
            ready_selection = []
            
            try:
                # Try accessing cell_selection through input instead
                review_cell_selection_input = input.review_transactions_table_cell_selection()
                review_selection = list(review_cell_selection_input.get("rows", [])) if review_cell_selection_input else []
                print(f"DEBUG handle_transaction_selection: review_selection (via input) = {review_selection}")
            except Exception as e:
                print(f"DEBUG handle_transaction_selection: review input error = {e}")
                try:
                    # Fallback to direct method
                    review_cell_selection = review_transactions_table.cell_selection()
                    review_selection = list(review_cell_selection.get("rows", [])) if review_cell_selection else []
                    print(f"DEBUG handle_transaction_selection: review_selection (direct) = {review_selection}")
                except Exception as e2:
                    print(f"DEBUG handle_transaction_selection: review direct error = {e2}")
                    review_selection = []
            
            try:
                # Try accessing cell_selection through input instead
                ready_cell_selection_input = input.ready_transactions_table_cell_selection()
                ready_selection = list(ready_cell_selection_input.get("rows", [])) if ready_cell_selection_input else []
                print(f"DEBUG handle_transaction_selection: ready_selection (via input) = {ready_selection}")
            except Exception as e:
                print(f"DEBUG handle_transaction_selection: ready input error = {e}")
                try:
                    # Fallback to direct method
                    ready_cell_selection = ready_transactions_table.cell_selection()
                    ready_selection = list(ready_cell_selection.get("rows", [])) if ready_cell_selection else []
                    print(f"DEBUG handle_transaction_selection: ready_selection (direct) = {ready_selection}")
                except Exception as e2:
                    print(f"DEBUG handle_transaction_selection: ready direct error = {e2}")
                    ready_selection = []
            
            df = fetched_transactions.get()
            if df.empty:
                return
            
            selected_row_index = None
            if review_selection:
                selected_row_index = review_selection[0]
                table_type = "review"
                current_selection.set({"table": "review", "rows": review_selection})
                print(f"DEBUG: Set current_selection to review: {review_selection}")
            elif ready_selection:
                selected_row_index = ready_selection[0]
                table_type = "ready"
                current_selection.set({"table": "ready", "rows": ready_selection})
                print(f"DEBUG: Set current_selection to ready: {ready_selection}")
            else:
                # No selection
                selected_transaction.set(None)
                selected_transaction_hash.set(None)
                current_selection.set({"table": None, "rows": []})
                print("DEBUG: No selection - cleared current_selection")
                return
            
            # Get the selected transaction data
            if selected_row_index is not None and selected_row_index < len(df):
                row_data = df.iloc[selected_row_index]
                
                # Format transaction data for display
                tx_hash = str(row_data.get('tx_hash', ''))
                token_symbol = row_data.get('token_symbol', 'Unknown')
                
                # Determine direction
                side = row_data.get('side', '').lower()
                direction = row_data.get('direction', '').upper()
                qty = float(row_data.get('qty', 0))
                
                if side == 'sell' or direction == 'OUT' or qty < 0:
                    in_out = "OUT"
                    amount_multiplier = -1 if qty > 0 else 1
                else:
                    in_out = "IN"
                    amount_multiplier = 1 if qty > 0 else -1
                
                token_amount = float(row_data.get('token_amount', 0)) * amount_multiplier
                
                transaction_data = {
                    'hash': tx_hash,
                    'token': token_symbol,
                    'direction': in_out,
                    'amount': f"{token_amount:,.6f} {token_symbol}",
                    'value_usd': f"${float(row_data.get('token_value_usd', 0)):,.2f}",
                    'value_eth': f"{float(row_data.get('token_value_eth', 0)):,.6f} ETH",
                    'date': pd.to_datetime(row_data.get('date', '')).strftime('%Y-%m-%d %H:%M') if row_data.get('date') else 'Unknown',
                    'wallet_id': str(row_data.get('wallet_id', ''))
                }
                
                selected_transaction.set(transaction_data)
                selected_transaction_hash.set(tx_hash)
                
                logger.info(f"Selected transaction: {tx_hash[:16]}... ({token_symbol} {in_out})")
            
        except Exception as e:
            logger.error(f"Error handling transaction selection: {e}")
    
    # Handle Etherscan button click
    @reactive.effect
    @reactive.event(input.view_transaction_etherscan)
    def view_transaction_on_etherscan():
        """Open selected transaction on Etherscan"""
        try:
            tx_hash = selected_transaction_hash.get()
            if not tx_hash:
                logger.warning("No transaction selected for Etherscan view")
                return
            
            # Generate Etherscan transaction URL
            etherscan_url = f"https://etherscan.io/tx/{tx_hash}"
            
            # Log the URL (in a real browser environment, this would open the URL)
            logger.info(f"🔍 Opening Etherscan for transaction: {tx_hash}")
            logger.info(f"📱 Etherscan URL: {etherscan_url}")
            
            # In a web environment, you could use JavaScript to open in new tab:
            # session.send_custom_message("open_url", {"url": etherscan_url})
            
        except Exception as e:
            logger.error(f"Error opening Etherscan for transaction: {e}")
    
    # Push transactions to FIFO staging
    @reactive.effect
    @reactive.event(input.push_to_fifo)
    async def push_transactions_to_fifo():
        """Push all fetched transactions to FIFO staging area"""
        print("🚀 PUSH TO FIFO BUTTON CLICKED!")
        logger.info("Push to FIFO button clicked")
        
        # Start native Shiny progress
        with ui.Progress(min=0, max=100) as progress:
            try:
                # Step 1: Load and validate transactions
                progress.set(10, message="Loading transactions...", detail="Validating fetched transaction data")
                transactions_df = fetched_transactions.get()
                print(f"📊 Fetched transactions: {len(transactions_df)} rows")
                
                if transactions_df.empty:
                    progress.set(100, message="No transactions to process", detail="Please fetch transactions first")
                    print("⚠️ No transactions available to push to FIFO")
                    logger.warning("No transactions available to push to FIFO")
                    return
                
                # Step 2: Start data normalization
                progress.set(25, message="Normalizing transaction data...", detail="Preparing data for FIFO processing")
                fifo_df = transactions_df.copy()
                
                # Ensure all required fields exist for FIFO processing
                print(f"🔧 Normalizing transaction data for FIFO processing...")
                print(f"   📋 Original columns: {list(fifo_df.columns)}")
                
                # Step 3: Map essential fields
                progress.set(40, message="Mapping essential fields...", detail="Converting blockchain data to FIFO format")
                
                # Preserve Fund column as fund_id for FIFO processing
                if 'Fund' in fifo_df.columns:
                    fifo_df['fund_id'] = fifo_df['Fund']
                    print(f"   🔄 Preserved 'Fund' → 'fund_id' for FIFO processing")
                
                # Map asset → token_name if token_name is missing
                if 'asset' in fifo_df.columns and 'token_name' not in fifo_df.columns:
                    fifo_df['token_name'] = fifo_df['asset']
                    print(f"   🔄 Mapped 'asset' → 'token_name'")
                
                # Ensure token_amount exists (should already be mapped from qty)
                if 'token_amount' not in fifo_df.columns and 'qty' in fifo_df.columns:
                    fifo_df['token_amount'] = fifo_df['qty'].abs()
                    print(f"   🔄 Mapped 'qty' → 'token_amount' (absolute values)")
                    
                # Step 4: Format dates and add missing fields
                progress.set(60, message="Formatting dates and fields...", detail="Ensuring all required fields are present")
                
                # Ensure proper date format
                if 'date' in fifo_df.columns:
                    fifo_df['date'] = pd.to_datetime(fifo_df['date'])
                    print(f"   🔄 Normalized date format")
                
                # Add any missing required fields with defaults
                required_fields = ['wallet_id', 'side', 'token_name', 'token_amount', 'token_value_eth', 'token_value_usd', 'intercompany']
                for field in required_fields:
                    if field not in fifo_df.columns:
                        if field == 'intercompany':
                            fifo_df[field] = False
                        elif field in ['token_amount', 'token_value_eth', 'token_value_usd']:
                            fifo_df[field] = 0.0
                        else:
                            fifo_df[field] = '-'
                        print(f"   ⚠️  Added missing field '{field}' with default value")
                
                print(f"   ✅ Final columns: {list(fifo_df.columns)}")
                print(f"   📊 Sample transaction: {fifo_df.iloc[0].to_dict() if len(fifo_df) > 0 else 'None'}")
                
                # Step 5: Store transactions locally
                progress.set(80, message="Staging transactions locally...", detail=f"Preparing {len(fifo_df)} transactions")
                staged_transactions.set(fifo_df.copy())
                print(f"✅ Staged transactions locally: {len(fifo_df)} rows")
                
                # Step 6: Store globally and complete
                progress.set(90, message="Updating global state...", detail="Making transactions available to FIFO tracker")
                set_staged_transactions_global(fifo_df)
                print(f"🌐 Staged transactions globally: {len(fifo_df)} rows")
                
                # Force multiple reactive updates to ensure all components see the changes
                current_trigger = staged_transactions_trigger.get()
                staged_transactions_trigger.set(current_trigger + 1)
                print(f"🔄 Triggered reactive update for staged transactions (trigger: {current_trigger + 1})")
                
                # Additional reactive invalidation to force UI updates
                import asyncio
                await asyncio.sleep(0.1)  # Small delay to ensure state propagation
                
                # Trigger additional update to ensure all reactive components are notified
                staged_transactions_trigger.set(current_trigger + 2)
                print(f"🔄 Additional reactive trigger sent (trigger: {current_trigger + 2})")
                
                # Log final global state for debugging
                final_check = get_staged_transactions_global()
                print(f"🔍 Final global state check: {len(final_check)} rows")
                print(f"🔍 Final global state columns: {list(final_check.columns) if not final_check.empty else 'Empty'}")
                
                # Complete progress
                progress.set(100, message="Push to FIFO completed!", 
                           detail=f"Successfully staged {len(fifo_df)} transactions")
                
                logger.info(f"Successfully staged {len(fifo_df)} transactions for FIFO processing")
                print("🎉 Push to FIFO completed successfully!")
                
                # Update status
                push_status.set(f"✅ Successfully staged {len(fifo_df)} transactions for FIFO processing")
                
            except Exception as e:
                print(f"❌ Error pushing transactions to FIFO: {e}")
                logger.error(f"Error pushing transactions to FIFO: {e}")
                push_status.set(f"❌ Error: {str(e)}")
                progress.set(0, message="Error occurred", detail=str(e))
                raise e
    
    # Display push status
    @output
    @render.ui
    def push_status_display():
        status = push_status.get()
        if status:
            if "✅" in status:
                return ui.div(
                    ui.HTML(f'<i class="bi bi-check-circle text-success"></i>'),
                    ui.span(status.replace("✅", ""), class_="text-success ms-2"),
                    class_="d-flex align-items-center mb-3 alert alert-success"
                )
            elif "❌" in status:
                return ui.div(
                    ui.HTML(f'<i class="bi bi-exclamation-circle text-danger"></i>'),
                    ui.span(status.replace("❌", ""), class_="text-danger ms-2"),
                    class_="d-flex align-items-center mb-3 alert alert-danger"
                )
        return ui.div()
    
    # Note: Progress indicators are now handled by native Shiny ui.Progress
    # The progress bars appear automatically during fetch and push operations


# Integration functions for FIFO tracker
def get_approved_tokens_data() -> pd.DataFrame:
    """
    Get approved tokens data for FIFO tracker integration.
    
    Returns:
        DataFrame with approved tokens data
    """
    try:
        approved_tokens_df = load_approved_tokens_file()
        return approved_tokens_df
    except Exception as e:
        logger.error(f"Error loading approved tokens data: {e}")
        return pd.DataFrame()


def get_stored_transactions_data() -> pd.DataFrame:
    """
    Get stored transaction data for FIFO tracker integration.
    This function returns transactions that have been staged for FIFO processing.
    
    Returns:
        DataFrame with transaction data containing columns:
        date, tx_hash, direction, token_name, token_amount, token_value_usd, 
        token_value_eth, from_address, to_address, eth_price_usd
    """
    try:
        # This is a global function so we can't access the reactive value directly
        # For now, we'll use a file-based approach or return empty
        # In a production system, this would load from:
        # 1. S3 storage where staged transactions are cached
        # 2. Local storage/database
        # 3. Reactive state (when called from within the same session)
        
        logger.info("Getting stored transactions data for FIFO processing")
        
        # TODO: Implement actual storage retrieval
        # For now, return empty - the FIFO tracker will handle this gracefully
        return pd.DataFrame()
        
    except Exception as e:
        logger.error(f"Error loading stored transactions data: {e}")
        return pd.DataFrame()


def store_transactions_data(transactions_df: pd.DataFrame) -> bool:
    """
    Store transaction data for later FIFO processing.
    
    Args:
        transactions_df: DataFrame with transaction data to store
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # TODO: Implement storage mechanism
        # This could store to:
        # 1. S3 bucket for persistence
        # 2. Local file cache
        # 3. Database
        
        logger.info(f"Storing {len(transactions_df)} transactions for FIFO processing")
        
        # For now, just log the action
        return True
        
    except Exception as e:
        logger.error(f"Error storing transactions data: {e}")
        return False


# Global storage for staged transactions (for cross-module access)
_global_staged_transactions = pd.DataFrame()
_global_staged_transactions_trigger = 0

def get_staged_transactions_global() -> pd.DataFrame:
    """
    Get globally staged transactions for FIFO processing.
    This is a workaround for accessing reactive values across modules.
    
    Returns:
        DataFrame with staged transaction data
    """
    global _global_staged_transactions
    return _global_staged_transactions.copy()

def set_staged_transactions_global(transactions_df: pd.DataFrame) -> None:
    """
    Set globally staged transactions for FIFO processing.
    
    Args:
        transactions_df: DataFrame with transaction data to stage
    """
    global _global_staged_transactions, _global_staged_transactions_trigger
    _global_staged_transactions = transactions_df.copy()
    _global_staged_transactions_trigger += 1
    logger.info(f"Globally staged {len(transactions_df)} transactions for FIFO processing (trigger: {_global_staged_transactions_trigger})")

def clear_staged_transactions_global() -> None:
    """
    Clear all globally staged transactions after FIFO processing.
    This removes transactions from the "Transactions Ready" table.
    """
    global _global_staged_transactions, _global_staged_transactions_trigger
    _global_staged_transactions = pd.DataFrame()
    _global_staged_transactions_trigger += 1
    logger.info(f"Cleared all staged transactions after FIFO processing (trigger: {_global_staged_transactions_trigger})")

def remove_processed_transactions_global(processed_tx_hashes: list) -> None:
    """
    Remove specific transactions from global staging after they've been processed.
    
    Args:
        processed_tx_hashes: List of transaction hashes that have been processed
    """
    global _global_staged_transactions, _global_staged_transactions_trigger
    
    if _global_staged_transactions.empty:
        return
    
    # Filter out processed transactions
    if 'tx_hash' in _global_staged_transactions.columns:
        mask = ~_global_staged_transactions['tx_hash'].isin(processed_tx_hashes)
        _global_staged_transactions = _global_staged_transactions[mask].copy()
    elif 'hash' in _global_staged_transactions.columns:
        mask = ~_global_staged_transactions['hash'].isin(processed_tx_hashes)
        _global_staged_transactions = _global_staged_transactions[mask].copy()
    
    _global_staged_transactions_trigger += 1
    logger.info(f"Removed {len(processed_tx_hashes)} processed transactions, {len(_global_staged_transactions)} remaining (trigger: {_global_staged_transactions_trigger})")

def get_staged_transactions_trigger_global() -> int:
    """
    Get the global staged transactions trigger value.
    This is used for reactive invalidation across modules.
    
    Returns:
        Current trigger value
    """
    global _global_staged_transactions_trigger
    return _global_staged_transactions_trigger
    
