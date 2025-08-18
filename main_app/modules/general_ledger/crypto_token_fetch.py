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
                                "fetch_token_transactions",
                                ui.HTML('<i class="fas fa-download"></i> Fetch Transactions'),
                                class_="btn-primary mt-4"
                            ),
                            ui.output_ui("fetch_progress_inline"),
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
                    
                    # Add/Remove controls
                    ui.card(
                        ui.card_header("Manage Approved Tokens"),
                        ui.card_body(
                            ui.row(
                                ui.column(
                                    6,
                                    ui.input_text(
                                        "new_approved_token_address",
                                        "Add Token Address:",
                                        placeholder="0x..."
                                    ),
                                    ui.input_action_button(
                                        "add_approved_token",
                                        ui.HTML('<i class="fas fa-plus"></i> Add Token'),
                                        class_="btn-success btn-sm mt-2"
                                    )
                                ),
                                ui.column(
                                    6,
                                    ui.p("Select a token from the dropdown, then click Remove:", class_="small text-muted"),
                                    ui.output_ui("remove_token_dropdown"),
                                    ui.input_action_button(
                                        "remove_selected_token",
                                        ui.HTML('<i class="fas fa-trash"></i> Remove Selected Token'),
                                        class_="btn-danger btn-sm mt-2"
                                    )
                                )
                            )
                        )
                    ),
                    
                    ui.output_data_frame("approved_tokens_table")
                )
            ),
            ui.nav_panel(
                ui.HTML('<i class="fas fa-list"></i> All Transactions'),
                ui.div(
                    ui.p("Complete transaction history", class_="text-muted small"),
                    # Push to FIFO button and status
                    ui.div(
                        ui.input_action_button(
                            "push_to_fifo",
                            ui.HTML('<i class="fas fa-arrow-right"></i> Push All Transactions to FIFO'),
                            class_="btn-primary mb-3"
                        ),
                        ui.HTML('<small class="text-muted ms-3">This will stage all fetched transactions for FIFO processing</small>'),
                        class_="d-flex align-items-center mb-3"
                    ),
                    # Push status display
                    ui.output_ui("push_status_display"),
                    ui.row(
                        ui.column(
                            9,
                            ui.output_data_frame("all_transactions_table")
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
    
    # Initialize blockchain service
    blockchain_service = reactive.value(None)
    
    # Store fetched transactions
    fetched_transactions = reactive.value(pd.DataFrame())
    
    # Store fetch progress
    fetch_in_progress = reactive.value(False)
    fetch_progress_message = reactive.value("")
    
    # Track approved tokens changes for reactive updates
    approved_tokens_updated = reactive.value(0)
    
    # Store transactions staged for FIFO processing
    staged_transactions = reactive.value(pd.DataFrame())
    
    # Track push to FIFO status
    push_status = reactive.value("")
    
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
                    ui.HTML('<i class="fas fa-exclamation-triangle text-warning"></i> Blockchain service not initialized'),
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
                ui.HTML(f'<i class="fas fa-spinner fa-spin text-primary me-1"></i>{fetch_progress_message.get()}'),
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
    def fetch_token_transactions():
        print("🚀 FETCH TRANSACTIONS BUTTON CLICKED!")
        logger.info("Fetch transactions button clicked")
        
        service = blockchain_service.get()
        if not service:
            print("❌ Blockchain service not initialized")
            logger.error("Blockchain service not initialized")
            fetch_progress_message.set("Error: Blockchain service not initialized")
            return
        
        print("✅ Blockchain service is ready")
        
        try:
            # Set progress flag
            print("🟢 Setting fetch_in_progress to True")
            fetch_in_progress.set(True)
            fetch_progress_message.set("Preparing to fetch transactions...")
            print("📝 Set progress message: Preparing to fetch transactions...")
            
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
            
            # Prepare wallet addresses
            wallet_addresses = None
            if wallet_selection and "all" not in wallet_selection:
                wallet_addresses = wallet_selection
            
            # Use fund_id only if not "all"
            fund_param = None if fund_id == "all" else fund_id
            
            # Progress callback
            def update_progress(current, total, tx_count):
                fetch_progress_message.set(f"Processing wallet {current}/{total} - Found {tx_count} transactions...")
            
            # Fetch transactions using BlockchainService
            fetch_progress_message.set("Fetching transactions from blockchain...")
            
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
            
            if df.empty:
                fetch_progress_message.set("No transactions found for the selected criteria")
                fetched_transactions.set(pd.DataFrame())
            else:
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
                
                # Remove legacy direction-based negation to prevent double negatives
                
                logger.info(f"Fetched and processed {len(processed_df)} transactions")
                fetched_transactions.set(processed_df)
                fetch_progress_message.set(f"✅ Successfully fetched {len(processed_df)} transactions!")
            
        except Exception as e:
            logger.error(f"Error fetching transactions: {e}")
            fetch_progress_message.set(f"Error: {str(e)}")
            fetched_transactions.set(pd.DataFrame())
        
        finally:
            # Keep progress visible for a moment so user can see it
            import time
            time.sleep(2)  # Show progress for 2 seconds after completion
            print("🔴 Setting fetch_in_progress to False")
            fetch_in_progress.set(False)
    
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
    
    # Verified tokens table
    @output
    @render.data_frame
    def verified_tokens_table():
        df = fetched_transactions.get()
        if df.empty or 'token_address' not in df.columns:
            return pd.DataFrame(columns=['Token', 'Symbol', 'Address', 'Transaction Count', 'Total Volume (USD)'])
        
        # Get verified token addresses (case-insensitive)
        verified_addresses = {addr.lower() for addr in VERIFIED_TOKENS.values()}
        
        # Filter for verified tokens
        verified_df = df[df['token_address'].str.lower().isin(verified_addresses)].copy()
        
        if verified_df.empty:
            return pd.DataFrame(columns=['Token', 'Symbol', 'Address', 'Transaction Count', 'Total Volume (USD)'])
        
        # Aggregate by token
        token_summary = verified_df.groupby(['token_address', 'token_symbol']).agg({
            'tx_hash': 'count',
            'token_value_usd': 'sum'
        }).reset_index()
        
        token_summary.columns = ['Address', 'Symbol', 'Transaction Count', 'Total Volume (USD)']
        
        # Add token names from VERIFIED_TOKENS
        def get_token_name(address):
            for name, addr in VERIFIED_TOKENS.items():
                if addr.lower() == address.lower():
                    return name
            return "Unknown"
        
        token_summary['Token'] = token_summary['Address'].apply(get_token_name)
        
        # Format the display
        token_summary['Total Volume (USD)'] = token_summary['Total Volume (USD)'].apply(lambda x: f"${x:,.2f}")
        
        return token_summary[['Token', 'Symbol', 'Address', 'Transaction Count', 'Total Volume (USD)']]
    
    # Unverified tokens table
    @output
    @render.data_frame
    def unverified_tokens_table():
        # Watch for changes in approved tokens to trigger refresh
        approved_tokens_updated.get()
        
        df = fetched_transactions.get()
        if df.empty or 'token_address' not in df.columns:
            return pd.DataFrame(columns=['Symbol', 'Address', 'Transaction Count', 'Total Volume (USD)', 'Risk Level'])
        
        # Get verified and approved addresses (fresh load each time)
        verified_addresses = {addr.lower() for addr in VERIFIED_TOKENS.values()}
        
        try:
            approved_tokens = load_approved_tokens_file()
            approved_addresses = {addr.lower() for addr in approved_tokens}
        except:
            approved_addresses = set()
        
        # Filter for unverified and unapproved tokens
        unverified_df = df[
            (~df['token_address'].str.lower().isin(verified_addresses)) &
            (~df['token_address'].str.lower().isin(approved_addresses))
        ].copy()
        
        if unverified_df.empty:
            return pd.DataFrame(columns=['Symbol', 'Address', 'Transaction Count', 'Total Volume (USD)', 'Risk Level'])
        
        # Aggregate by token
        token_summary = unverified_df.groupby(['token_address', 'token_symbol']).agg({
            'tx_hash': 'count',
            'token_value_usd': 'sum',
            'token_risk_level': lambda x: x.mode()[0] if not x.empty else 'Unknown'
        }).reset_index()
        
        token_summary.columns = ['Address', 'Symbol', 'Transaction Count', 'Total Volume (USD)', 'Risk Level']
        
        # Format the display
        token_summary['Total Volume (USD)'] = token_summary['Total Volume (USD)'].apply(lambda x: f"${x:,.2f}")
        
        # Sort by transaction count
        token_summary = token_summary.sort_values('Transaction Count', ascending=False)
        
        return token_summary[['Symbol', 'Address', 'Transaction Count', 'Total Volume (USD)', 'Risk Level']]
    
    # Approved tokens table
    @output
    @render.data_frame
    def approved_tokens_table():
        # Watch for changes in approved tokens
        approved_tokens_updated.get()
        
        try:
            approved_tokens = load_approved_tokens_file()
            if not approved_tokens:
                return pd.DataFrame(columns=['Token Name', 'Symbol', 'Token Address', 'Approval Status', 'Transaction Count'])
            
            # Get transaction data for approved tokens
            df = fetched_transactions.get()
            
            approved_data = []
            for token_addr in approved_tokens:
                # Get token info from API
                token_info = get_token_info_from_address(token_addr)
                
                tx_count = 0
                if not df.empty and 'token_address' in df.columns:
                    tx_count = len(df[df['token_address'].str.lower() == token_addr.lower()])
                
                approved_data.append({
                    'Token Name': token_info['name'],
                    'Symbol': token_info['symbol'],
                    'Token Address': token_addr,
                    'Approval Status': 'Approved',
                    'Transaction Count': tx_count
                })
            
            # Sort by token name for better organization
            approved_df = pd.DataFrame(approved_data)
            approved_df = approved_df.sort_values('Token Name') if not approved_df.empty else approved_df
            
            return approved_df
            
        except Exception as e:
            logger.error(f"Error loading approved tokens: {e}")
            return pd.DataFrame(columns=['Token Name', 'Symbol', 'Token Address', 'Approval Status', 'Transaction Count'])
    
    # All transactions table with selection support
    @output
    @render.data_frame
    def all_transactions_table():
        df = fetched_transactions.get()
        if df.empty:
            return pd.DataFrame(columns=['Date', 'Hash', 'Wallet ID', 'Side', 'Token Name', 'Amount (of token)', 'Value (ETH)', 'Value (USD)', 'Intercompany', 'From', 'To'])
        
        # Prepare display dataframe - NEW STRUCTURE
        formatted_data = []
        
        for _, row in df.iterrows():
            formatted_row = {}
            
            # Date
            if 'date' in row:
                formatted_row['Date'] = pd.to_datetime(row['date']).strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_row['Date'] = ""
            
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
        column_order = ['Date', 'Hash', 'Wallet ID', 'Side', 'Token Name', 'Amount (of token)', 'Value (ETH)', 'Value (USD)', 'Intercompany', 'From', 'To']
        
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
    
    # Action buttons for unverified tokens
    @output
    @render.ui
    def unverified_tokens_actions():
        # Watch for changes in approved tokens to trigger refresh
        approved_tokens_updated.get()
        
        df = fetched_transactions.get()
        if df.empty or 'token_address' not in df.columns:
            return ui.div()
        
        # Check if there are unverified tokens (fresh load each time)
        verified_addresses = {addr.lower() for addr in VERIFIED_TOKENS.values()}
        
        try:
            approved_tokens = load_approved_tokens_file()
            approved_addresses = {addr.lower() for addr in approved_tokens}
        except:
            approved_addresses = set()
        
        unverified_df = df[
            (~df['token_address'].str.lower().isin(verified_addresses)) &
            (~df['token_address'].str.lower().isin(approved_addresses))
        ]
        
        if unverified_df.empty:
            return ui.div(
                ui.div(
                    ui.HTML('<i class="fas fa-check-circle text-success"></i> All tokens are either verified or approved!'),
                    class_="alert alert-success"
                )
            )
        
        # Get unique unverified tokens for dropdown
        unique_tokens = unverified_df[['token_address', 'token_symbol']].drop_duplicates()
        
        token_choices = {}
        for _, row in unique_tokens.iterrows():
            addr = row['token_address']
            symbol = row['token_symbol']
            # Get full token name for better display
            token_info = get_token_info_from_address(addr)
            token_choices[addr] = f"{token_info['name']} ({symbol}) - {addr[:6]}...{addr[-4:]}"
        
        return ui.div(
            ui.p("Select a token to approve:", class_="small text-muted mb-2"),
            ui.div(
                ui.input_select(
                    "token_to_approve_select",
                    "Token to Approve:",
                    choices=token_choices,
                    selected=None
                ),
                ui.div(
                    ui.input_action_button(
                        "approve_selected_token",
                        ui.HTML('<i class="fas fa-check"></i> Approve Selected Token'),
                        class_="btn-success btn-sm me-2"
                    ),
                    ui.input_action_button(
                        "refresh_token_data",
                        ui.HTML('<i class="fas fa-sync"></i> Refresh Tables'),
                        class_="btn-secondary btn-sm"
                    ),
                    class_="mt-2"
                ),
                class_="mb-3"
            )
        )
    
    # Handle token approval
    @reactive.effect
    @reactive.event(input.approve_selected_token)
    def approve_token():
        try:
            token_address = input.token_to_approve_select()
            if not token_address:
                logger.warning("No token selected for approval")
                return
            
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
            logger.info(f"Approved token: {token_info['name']} ({token_address})")
            
            # Trigger reactive updates for ALL tables
            approved_tokens_updated.set(approved_tokens_updated.get() + 1)
            
        except Exception as e:
            logger.error(f"Error approving token: {e}")
    
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
    
    # Show transaction details card with inline Etherscan link
    @output
    @render.ui
    def transaction_details_card():
        df = fetched_transactions.get()
        
        if df.empty:
            return ui.div(
                ui.div(
                    ui.HTML('<i class="fas fa-info-circle text-muted"></i>'),
                    ui.p("No transactions available", class_="text-muted mb-0 ms-2"),
                    class_="d-flex align-items-center"
                ),
                ui.p("Please fetch transactions first.", class_="small text-muted mt-2")
            )
        
        # Get selected rows from the data grid
        selected_rows = input.all_transactions_table_selected_rows()
        
        if not selected_rows or len(selected_rows) == 0:
            return ui.div(
                ui.div(
                    ui.HTML('<i class="fas fa-hand-pointer text-primary"></i>'),
                    ui.p("Select a transaction", class_="text-primary mb-0 ms-2"),
                    class_="d-flex align-items-center"
                ),
                ui.p("Click on a transaction row to view details and access Etherscan.", class_="small text-muted mt-2")
            )
        
        # Get the first selected row index
        selected_idx = selected_rows[0]
        
        # Get transaction details from the original dataframe
        if selected_idx < len(df) and 'tx_hash' in df.columns:
            tx_row = df.iloc[selected_idx]
            tx_hash = tx_row['tx_hash']
            etherscan_url = f"https://etherscan.io/tx/{tx_hash}"
            
            # Get additional transaction details for display
            date_str = tx_row.get('date', 'Unknown')
            if hasattr(date_str, 'strftime'):
                date_str = date_str.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(date_str, str):
                try:
                    date_str = pd.to_datetime(date_str).strftime('%Y-%m-%d %H:%M:%S')
                except:
                    pass
            
            token_name = tx_row.get('token_name', tx_row.get('token_symbol', 'Unknown'))
            direction = tx_row.get('direction', 'Unknown')
            amount = tx_row.get('token_amount', 0)
            value_usd = tx_row.get('token_value_usd', 0)
            value_eth = tx_row.get('token_value_eth', 0)
            from_addr = tx_row.get('from_address', '')
            to_addr = tx_row.get('to_address', '')
            
            return ui.div(
                # Transaction header
                ui.div(
                    ui.HTML('<i class="fas fa-receipt text-success"></i>'),
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
                    ui.HTML('<i class="fas fa-external-link-alt me-2"></i>View on Etherscan'),
                    ui.HTML('</a>'),
                    class_="text-center"
                )
            )
        
        return ui.div(
            ui.div(
                ui.HTML('<i class="fas fa-exclamation-triangle text-warning"></i>'),
                ui.p("Invalid selection", class_="text-warning mb-0 ms-2"),
                class_="d-flex align-items-center"
            ),
            ui.p("Unable to retrieve transaction details. Please try selecting a different transaction.", class_="small text-muted mt-2")
        )
    
    # Push transactions to FIFO staging
    @reactive.effect
    @reactive.event(input.push_to_fifo)
    def push_transactions_to_fifo():
        """Push all fetched transactions to FIFO staging area"""
        print("🚀 PUSH TO FIFO BUTTON CLICKED!")
        logger.info("Push to FIFO button clicked")
        
        try:
            transactions_df = fetched_transactions.get()
            print(f"📊 Fetched transactions: {len(transactions_df)} rows")
            
            if transactions_df.empty:
                print("⚠️ No transactions available to push to FIFO")
                logger.warning("No transactions available to push to FIFO")
                return
            
            # Normalize data for FIFO processing and display compatibility
            fifo_df = transactions_df.copy()
            
            # Ensure all required fields exist for FIFO processing
            print(f"🔧 Normalizing transaction data for FIFO processing...")
            print(f"   📋 Original columns: {list(fifo_df.columns)}")
            
            # Map asset → token_name if token_name is missing
            if 'asset' in fifo_df.columns and 'token_name' not in fifo_df.columns:
                fifo_df['token_name'] = fifo_df['asset']
                print(f"   🔄 Mapped 'asset' → 'token_name'")
            
            # Ensure token_amount exists (should already be mapped from qty)
            if 'token_amount' not in fifo_df.columns and 'qty' in fifo_df.columns:
                fifo_df['token_amount'] = fifo_df['qty'].abs()
                print(f"   🔄 Mapped 'qty' → 'token_amount' (absolute values)")
                
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
            
            # Store transactions for FIFO processing
            staged_transactions.set(fifo_df.copy())
            print(f"✅ Staged transactions locally: {len(fifo_df)} rows")
            
            # Also store globally for cross-module access
            set_staged_transactions_global(fifo_df)
            print(f"🌐 Staged transactions globally: {len(fifo_df)} rows")
            
            logger.info(f"Successfully staged {len(fifo_df)} transactions for FIFO processing")
            print("🎉 Push to FIFO completed successfully!")
            
            # Update status
            push_status.set(f"✅ Successfully staged {len(fifo_df)} transactions for FIFO processing")
            
            # Trigger reactive update for cross-module access
            current_trigger = staged_transactions_trigger.get()
            staged_transactions_trigger.set(current_trigger + 1)
            print(f"🔄 Triggered reactive update for staged transactions (trigger: {current_trigger + 1})")
            
        except Exception as e:
            print(f"❌ Error pushing transactions to FIFO: {e}")
            logger.error(f"Error pushing transactions to FIFO: {e}")
            push_status.set(f"❌ Error: {str(e)}")
    
    # Display push status
    @output
    @render.ui
    def push_status_display():
        status = push_status.get()
        if status:
            if "✅" in status:
                return ui.div(
                    ui.HTML(f'<i class="fas fa-check-circle text-success"></i>'),
                    ui.span(status.replace("✅", ""), class_="text-success ms-2"),
                    class_="d-flex align-items-center mb-3 alert alert-success"
                )
            elif "❌" in status:
                return ui.div(
                    ui.HTML(f'<i class="fas fa-exclamation-circle text-danger"></i>'),
                    ui.span(status.replace("❌", ""), class_="text-danger ms-2"),
                    class_="d-flex align-items-center mb-3 alert alert-danger"
                )
        return ui.div()


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

def get_staged_transactions_trigger_global() -> int:
    """
    Get the global staged transactions trigger value.
    This is used for reactive invalidation across modules.
    
    Returns:
        Current trigger value
    """
    global _global_staged_transactions_trigger
    return _global_staged_transactions_trigger
    
