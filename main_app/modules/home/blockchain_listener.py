from shiny import reactive, render, ui
import pandas as pd
from datetime import datetime, timedelta
import asyncio
import os
from .blockchain_service import blockchain_service
import logging

logger = logging.getLogger(__name__)


def register_blockchain_listener_outputs(input, output, session, selected_fund):
    """Register server outputs for blockchain listener"""

    # Reactive values
    transaction_data = reactive.value(pd.DataFrame())
    last_refresh = reactive.value(datetime.now())
    initialization_status = reactive.value("initializing")
    error_message = reactive.value("")

    # Create wallet selector UI with friendly names filtered by fund
    @output
    @render.ui
    @reactive.event(selected_fund)  # Re-render when fund changes
    def wallet_selector_ui():
        """Create wallet selector dropdown with friendly names filtered by selected fund"""
        # Get current fund
        current_fund = selected_fund()
        wallet_choices = {}

        # Try to load wallet mappings
        try:
            from ...s3_utils import load_WALLET_file
            wallet_df = load_WALLET_file()

            if not wallet_df.empty:
                # Filter by selected fund
                fund_wallets = wallet_df[wallet_df['fund_id'] == current_fund]

                # Add "All Fund Wallets" option
                if not fund_wallets.empty:
                    wallet_choices["all_fund"] = f"📊 All {current_fund} Wallets ({len(fund_wallets)} wallets)"

                # Create choices dict with friendly name as display and address as value
                for _, row in fund_wallets.iterrows():
                    wallet_addr = str(row.get('wallet_address', '')).strip()
                    friendly_name = str(row.get('friendly_name', '')).strip()

                    if wallet_addr:
                        # Use friendly name if available, otherwise shortened address
                        display = friendly_name if friendly_name else f"{wallet_addr[:6]}...{wallet_addr[-4:]}"
                        wallet_choices[wallet_addr] = f"  {display} ({wallet_addr[:6]}...{wallet_addr[-4:]})"

                # If no wallets for this fund, show a message
                if len(wallet_choices) == 0:
                    wallet_choices["none"] = f"No wallets found for {current_fund}"

            else:
                # If no mappings loaded
                wallet_choices["none"] = "No wallet mappings available"

        except Exception as e:
            logger.warning(f"Could not load wallet mappings: {e}")
            wallet_choices["error"] = "Error loading wallets"

        # Add custom wallet option
        wallet_choices["custom"] = "➕ Enter Custom Address..."

        # Get first valid wallet as default
        default_selection = next((k for k in wallet_choices.keys() if k not in ["none", "error", "custom"]), "custom")

        return ui.div(
            ui.p(f"Fund: {current_fund}", class_="text-muted small mb-2"),
            ui.input_select(
                "wallet_address",
                "Monitor Wallet:",
                choices=wallet_choices,
                selected=default_selection,
                width="100%"
            )
        )

    # Get list of wallets to monitor based on selection
    @reactive.calc
    def get_monitored_wallets():
        """Get list of wallet addresses to monitor based on current selection"""
        try:
            wallet_selection = input.wallet_address()

            if wallet_selection == "all_fund":
                # Get all wallets for the selected fund
                from ...s3_utils import load_WALLET_file
                wallet_df = load_WALLET_file()
                current_fund = selected_fund()

                if not wallet_df.empty:
                    fund_wallets = wallet_df[wallet_df['fund_id'] == current_fund]
                    return fund_wallets['wallet_address'].str.strip().tolist()
                return []

            elif wallet_selection in ["none", "error", "custom"]:
                return []
            else:
                # Single wallet selected
                return [wallet_selection]
        except:
            return []

    # Initialize blockchain service on module load
    @reactive.effect
    def initialize_listener():
        """Initialize the blockchain listener on startup"""
        try:
            # Get wallets to monitor
            wallets = get_monitored_wallets()

            if not wallets:
                logger.warning("No wallets to monitor")
                initialization_status.set("no_wallets")
                return

            logger.info(f"Monitoring {len(wallets)} wallet(s)")

            # Check if we have API keys configured
            if not os.getenv('ETHERSCAN_API_KEY'):
                logger.warning("ETHERSCAN_API_KEY not found in environment. Using limited functionality.")
                initialization_status.set("limited")
                error_message.set("No Etherscan API key configured. Add ETHERSCAN_API_KEY to your environment.")
                return

            # For now, monitor the first wallet (TODO: support multiple)
            primary_wallet = wallets[0] if wallets else None
            if primary_wallet:
                blockchain_service.wallet_address = primary_wallet

                # Fetch initial data
                initial_data = blockchain_service.fetch_historical_transactions(limit=100)
                if not initial_data.empty:
                    transaction_data.set(initial_data)
                    initialization_status.set("active")
                    logger.info(f"Loaded {len(initial_data)} historical transactions")
                else:
                    initialization_status.set("no_data")
                    logger.warning("No historical transactions found")

            last_refresh.set(datetime.now())

        except Exception as e:
            logger.error(f"Failed to initialize blockchain listener: {e}")
            import traceback
            traceback.print_exc()
            initialization_status.set("error")
            error_message.set(f"Initialization error: {str(e)}")

    # Filter controls panel
    @output
    @render.ui
    def filter_controls():
        """Dynamic filter controls based on show_filters switch"""
        if not input.show_filters():
            return ui.div()  # Return empty div when filters hidden

        return ui.div(
            ui.layout_columns(
                ui.div(
                    ui.input_select(
                        "tx_type_filter",
                        "Transaction Type:",
                        {
                            "all": "All Transactions",
                            "in": "Incoming Only",
                            "out": "Outgoing Only"
                        },
                        selected="all",
                        width="100%"
                    ),
                ),
                ui.div(
                    ui.input_select(
                        "token_filter",
                        "Token Type:",
                        {
                            "all": "All Tokens",
                            "eth": "ETH Only",
                            "erc20": "ERC-20 Only",
                            "usdc": "USDC Only",
                            "usdt": "USDT Only"
                        },
                        selected="all",
                        width="100%"
                    ),
                ),
                ui.div(
                    ui.input_numeric(
                        "min_value",
                        "Min Value (ETH):",
                        value=0,
                        min=0,
                        step=0.001,
                        width="100%"
                    ),
                ),
                ui.div(
                    ui.input_select(
                        "time_range",
                        "Time Range:",
                        {
                            "all": "All Time",
                            "24h": "Last 24 Hours",
                            "7d": "Last 7 Days",
                            "30d": "Last 30 Days"
                        },
                        selected="all",
                        width="100%"
                    ),
                ),
                col_widths=[3, 3, 3, 3]
            ),
            class_="p-3"
        )

    # Connection status
    @output
    @render.ui
    def connection_status():
        status = initialization_status.get()

        if status == "active":
            if blockchain_service.is_connected():
                return ui.tags.strong("Live Monitoring", style="color: #28a745;")
            else:
                return ui.tags.strong("Etherscan Only", style="color: #ffc107;")
        elif status == "limited":
            return ui.tags.strong("Limited Mode", style="color: #ffc107;")
        elif status == "initializing":
            return ui.tags.strong("Initializing...", style="color: #6c757d;")
        elif status == "error":
            return ui.tags.strong("Error", style="color: #dc3545;")
        else:
            return ui.tags.strong("Not Connected", style="color: #dc3545;")

    @output
    @render.ui
    def connection_indicator():
        status = initialization_status.get()

        if status == "active":
            if blockchain_service.is_connected():
                return ui.HTML('<i class="bi bi-circle-fill connection-active"></i> WebSocket + API')
            else:
                return ui.HTML('<i class="bi bi-circle-fill" style="color: #ffc107;"></i> API Only')
        elif status == "limited":
            return ui.HTML('<i class="bi bi-exclamation-triangle-fill" style="color: #ffc107;"></i> Limited')
        elif status == "initializing":
            return ui.HTML('<i class="bi bi-hourglass-split"></i> Starting...')
        else:
            return ui.HTML('<i class="bi bi-circle-fill connection-inactive"></i> Offline')

    # Active wallet display
    @output
    @render.ui
    @reactive.event(input.wallet_address)  # Update when wallet changes
    def active_wallet_display():
        selection = input.wallet_address() if hasattr(input, 'wallet_address') else ""

        # Handle special selections
        if selection == "all_fund":
            current_fund = selected_fund()
            wallets = get_monitored_wallets()
            return ui.div(
                ui.tags.strong(f"All {current_fund} Wallets"),
                ui.br(),
                ui.tags.code(f"{len(wallets)} wallets monitored", class_="address-text small")
            )
        elif selection == "custom":
            return ui.tags.code("Custom wallet...", class_="address-text")
        elif selection in ["none", "error", ""]:
            return ui.tags.code("Not set", class_="address-text")

        # Regular wallet address
        if selection and len(selection) > 10:
            # Get friendly name from blockchain service
            friendly_name = blockchain_service.get_friendly_name(selection)

            # If we have a real friendly name (not shortened address)
            if not friendly_name.endswith("...") or len(friendly_name) > 15:
                return ui.div(
                    ui.tags.strong(friendly_name),
                    ui.br(),
                    ui.tags.code(selection[:6] + "..." + selection[-4:], class_="address-text small")
                )
            else:
                return ui.tags.code(selection[:6] + "..." + selection[-4:], class_="address-text")

        return ui.tags.code("Not set", class_="address-text")

    # Transaction counts
    @output
    @render.ui
    def transactions_today_count():
        df = transaction_data.get()
        if df.empty:
            return ui.tags.strong("0", style="font-size: 1.5em;")

        today = datetime.now().date()
        # Handle both string and datetime timestamps
        if 'timestamp' in df.columns:
            if isinstance(df.iloc[0]['timestamp'], str):
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            today_txs = df[df['timestamp'].dt.date == today]
        else:
            today_txs = pd.DataFrame()

        return ui.tags.strong(str(len(today_txs)), style="font-size: 1.5em;")

    @output
    @render.ui
    def transactions_change():
        df = transaction_data.get()
        if df.empty or 'timestamp' not in df.columns:
            return ui.HTML('<i class="bi bi-dash-circle text-muted"></i> No data')

        # Ensure timestamp is datetime
        if isinstance(df.iloc[0]['timestamp'], str):
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        today = datetime.now().date()
        yesterday = today - timedelta(days=1)

        today_count = len(df[df['timestamp'].dt.date == today])
        yesterday_count = len(df[df['timestamp'].dt.date == yesterday])

        if yesterday_count > 0:
            change = ((today_count - yesterday_count) / yesterday_count) * 100
            if change > 0:
                return ui.HTML(f'<i class="bi bi-arrow-up-circle-fill text-success"></i> +{change:.1f}% vs yesterday')
            else:
                return ui.HTML(f'<i class="bi bi-arrow-down-circle-fill text-danger"></i> {change:.1f}% vs yesterday')
        elif today_count > 0:
            return ui.HTML(f'<i class="bi bi-arrow-up-circle-fill text-success"></i> {today_count} new today')
        else:
            return ui.HTML('<i class="bi bi-dash-circle text-muted"></i> No comparison data')

    # Last transaction time
    @output
    @render.ui
    def last_transaction_time():
        df = transaction_data.get()
        if df.empty or 'timestamp' not in df.columns:
            return "No transactions"

        # Get most recent transaction
        if isinstance(df.iloc[0]['timestamp'], str):
            last_time = pd.to_datetime(df.iloc[0]['timestamp'])
        else:
            last_time = df.iloc[0]['timestamp']

        time_ago = datetime.now() - last_time

        if time_ago.total_seconds() < 60:
            return "Just now"
        elif time_ago.total_seconds() < 3600:
            minutes = int(time_ago.total_seconds() / 60)
            return f"{minutes} minute{'s' if minutes > 1 else ''} ago"
        elif time_ago.total_seconds() < 86400:
            hours = int(time_ago.total_seconds() / 3600)
            return f"{hours} hour{'s' if hours > 1 else ''} ago"
        else:
            return last_time.strftime("%b %d, %H:%M")

    # Auto refresh indicator
    @output
    @render.ui
    def auto_refresh_indicator():
        status = initialization_status.get()
        error = error_message.get()

        if error:
            return ui.HTML(f"""
                <span class="badge bg-danger" title="{error}">
                    <i class="bi bi-exclamation-triangle"></i> Error
                </span>
            """)
        elif status == "active":
            if blockchain_service.is_connected():
                return ui.HTML("""
                    <span class="badge bg-success">
                        <i class="bi bi-arrow-repeat"></i> Live Updates
                    </span>
                """)
            else:
                return ui.HTML("""
                    <span class="badge bg-warning">
                        <i class="bi bi-arrow-repeat"></i> Polling (30s)
                    </span>
                """)
        elif status == "initializing":
            return ui.HTML("""
                <span class="badge bg-info">
                    <i class="bi bi-hourglass-split"></i> Loading...
                </span>
            """)
        else:
            return ui.HTML("""
                <span class="badge bg-secondary">
                    <i class="bi bi-pause-circle"></i> Paused
                </span>
            """)

    # Filter transactions based on settings
    @reactive.calc
    def filtered_transactions():
        """Apply filters to transaction data"""
        df = transaction_data.get()

        if df.empty:
            return df

        # Apply transaction type filter
        if hasattr(input, 'tx_type_filter') and input.tx_type_filter() != "all":
            filter_type = input.tx_type_filter()
            if filter_type == "in":
                df = df[df['type'] == 'IN']
            elif filter_type == "out":
                df = df[df['type'] == 'OUT']

        # Apply token filter
        if hasattr(input, 'token_filter') and input.token_filter() != "all":
            token = input.token_filter()
            if token == "eth":
                df = df[df['token'] == 'ETH']
            elif token == "erc20":
                df = df[df['token'] != 'ETH']
            elif token == "usdc":
                df = df[df['token'].str.upper() == 'USDC']
            elif token == "usdt":
                df = df[df['token'].str.upper() == 'USDT']

        # Apply minimum value filter
        if hasattr(input, 'min_value') and input.min_value() > 0:
            df = df[df['amount'] >= input.min_value()]

        # Apply time range filter
        if hasattr(input, 'time_range') and input.time_range() != "all":
            time_range = input.time_range()
            now = datetime.now()

            # Ensure timestamp is datetime
            if 'timestamp' in df.columns and not df.empty:
                if isinstance(df.iloc[0]['timestamp'], str):
                    df['timestamp'] = pd.to_datetime(df['timestamp'])

                if time_range == "24h":
                    cutoff = now - timedelta(days=1)
                elif time_range == "7d":
                    cutoff = now - timedelta(days=7)
                elif time_range == "30d":
                    cutoff = now - timedelta(days=30)
                else:
                    cutoff = None

                if cutoff:
                    df = df[df['timestamp'] >= cutoff]

        return df

    # Main transaction table
    @output
    @render.data_frame
    def blockchain_transactions_table():
        # Use filtered transactions
        df = filtered_transactions()

        if df.empty:
            # Return empty DataFrame with proper columns
            display_df = pd.DataFrame(columns=['Status', 'Type', 'Hash', 'Block', 'From', 'To', 'Amount', 'Gas', 'Time'])
            return render.DataGrid(display_df, width="100%", height="500px")

        # Format display columns
        display_data = []
        for _, row in df.iterrows():
            # Format hash
            hash_str = row.get('hash', '')
            if len(hash_str) > 16:
                hash_display = hash_str[:10] + "..." + hash_str[-6:]
            else:
                hash_display = hash_str

            # Use friendly names if available, otherwise format addresses
            from_display = row.get('from_display', '')
            to_display = row.get('to_display', '')

            # Fallback to formatted addresses if no display names
            if not from_display:
                from_addr = row.get('from', '')
                if len(from_addr) > 12:
                    from_display = from_addr[:6] + "..." + from_addr[-4:]
                else:
                    from_display = from_addr

            if not to_display:
                to_addr = row.get('to', '')
                if len(to_addr) > 12:
                    to_display = to_addr[:6] + "..." + to_addr[-4:]
                else:
                    to_display = to_addr

            # Format timestamp
            if isinstance(row.get('timestamp'), str):
                time_display = row['timestamp']
            else:
                time_display = row.get('timestamp', datetime.now()).strftime("%Y-%m-%d %H:%M:%S")

            display_data.append({
                'Status': row.get('status', 'Unknown'),
                'Type': row.get('type', ''),
                'Hash': hash_display,
                'Block': row.get('block', 0),
                'From': from_display,
                'To': to_display,
                'Amount': f"{row.get('amount', 0):.6f} {row.get('token', 'ETH')}",
                'Gas': f"{row.get('gas_fee', 0):.6f} ETH",
                'Time': time_display
            })

        display_df = pd.DataFrame(display_data)

        # Limit to selected number of transactions
        limit = int(input.transaction_limit()) if hasattr(input, 'transaction_limit') else 100
        display_df = display_df.head(limit)

        return render.DataGrid(
            display_df,
            selection_mode="row",
            filters=True,
            width="100%",
            height="500px"
        )

    # Transaction details panel
    @output
    @render.ui
    def transaction_details_panel():
        selected = input.blockchain_transactions_table_selected_rows()

        if selected and len(selected) > 0:
            df = transaction_data.get()
            if df.empty or selected[0] >= len(df):
                return ui.div(
                    ui.p("Transaction data not available.", class_="text-muted"),
                    class_="text-center py-4"
                )

            tx = df.iloc[selected[0]]

            status_class = {
                "Confirmed": "status-confirmed",
                "Pending": "status-pending",
                "Failed": "status-failed"
            }.get(tx.get('status', 'Unknown'), "")

            # Build details panel
            details = ui.div(
                ui.h5("Selected Transaction"),
                ui.hr(),
                ui.layout_columns(
                    ui.div(
                        ui.strong("Transaction Hash:"),
                        ui.br(),
                        ui.code(tx.get('hash', 'N/A'), class_="transaction-hash"),
                    ),
                    ui.div(
                        ui.strong("Status:"),
                        ui.br(),
                        ui.span(tx.get('status', 'Unknown'), class_=f"status-badge {status_class}"),
                    ),
                    col_widths=[9, 3]
                ),
                ui.br(),
                ui.layout_columns(
                    ui.div(
                        ui.strong("From:"),
                        ui.br(),
                        ui.div(
                            ui.strong(tx.get('from_display', 'Unknown')),
                            ui.br(),
                            ui.code(tx.get('from', 'N/A'), class_="address-text"),
                        ),
                    ),
                    ui.div(
                        ui.strong("To:"),
                        ui.br(),
                        ui.div(
                            ui.strong(tx.get('to_display', 'Unknown')),
                            ui.br(),
                            ui.code(tx.get('to', 'N/A'), class_="address-text"),
                        ),
                    ),
                    col_widths=[6, 6]
                ),
                ui.br(),
                ui.layout_columns(
                    ui.div(
                        ui.strong("Amount:"),
                        ui.br(),
                        ui.span(f"{tx.get('amount', 0):.6f} {tx.get('token', 'ETH')}", class_="amount-text"),
                    ),
                    ui.div(
                        ui.strong("Gas Fee:"),
                        ui.br(),
                        ui.span(f"{tx.get('gas_fee', 0):.6f} ETH"),
                    ),
                    ui.div(
                        ui.strong("Block Number:"),
                        ui.br(),
                        ui.span(str(tx.get('block', 'N/A')), class_="block-number"),
                    ),
                    col_widths=[4, 4, 4]
                ),
            )

            # Add confirmations if available
            if 'confirmations' in tx and tx['confirmations'] > 0:
                details = ui.div(
                    details,
                    ui.br(),
                    ui.div(
                        ui.strong("Confirmations:"),
                        ui.br(),
                        ui.span(f"{tx['confirmations']:,}"),
                    ),
                )

            # Add timestamp
            if 'timestamp' in tx:
                if isinstance(tx['timestamp'], str):
                    time_str = tx['timestamp']
                else:
                    time_str = tx['timestamp'].strftime("%Y-%m-%d %H:%M:%S UTC")

                details = ui.div(
                    details,
                    ui.br(),
                    ui.div(
                        ui.strong("Timestamp:"),
                        ui.br(),
                        ui.span(time_str),
                    ),
                )

            # Add token info if available
            if 'token_name' in tx and tx.get('token_name'):
                details = ui.div(
                    details,
                    ui.br(),
                    ui.div(
                        ui.strong("Token:"),
                        ui.br(),
                        ui.span(f"{tx['token_name']} ({tx.get('token', 'Unknown')})"),
                    ),
                )

            # Add Etherscan link
            network = input.network() if hasattr(input, 'network') else "1"
            etherscan_base = {
                "1": "https://etherscan.io",
                "42161": "https://arbiscan.io",
                "10": "https://optimistic.etherscan.io",
                "137": "https://polygonscan.com",
                "8453": "https://basescan.org"
            }.get(network, "https://etherscan.io")

            details = ui.div(
                details,
                ui.br(),
                ui.div(
                    ui.a(
                        "View on Etherscan →",
                        href=f"{etherscan_base}/tx/{tx.get('hash', '')}",
                        target="_blank",
                        class_="btn btn-sm btn-outline-primary"
                    )
                )
            )

            return details

        else:
            return ui.div(
                ui.p("Select a transaction from the table above to view details.", class_="text-muted"),
                class_="text-center py-4"
            )

    # Handle refresh button
    @reactive.effect
    @reactive.event(input.refresh_data)
    def refresh_transactions():
        """Manually refresh transaction data"""
        try:
            wallet = input.wallet_address() if hasattr(input, 'wallet_address') else blockchain_service.wallet_address
            limit = int(input.transaction_limit()) if hasattr(input, 'transaction_limit') else 100

            # Re-initialize if wallet changed
            if wallet != blockchain_service.wallet_address:
                blockchain_service.wallet_address = wallet

            # Fetch fresh data
            fresh_data = blockchain_service.fetch_historical_transactions(limit=limit)
            if not fresh_data.empty:
                transaction_data.set(fresh_data)
                last_refresh.set(datetime.now())
                logger.info(f"Refreshed with {len(fresh_data)} transactions")

        except Exception as e:
            logger.error(f"Error refreshing transactions: {e}")
            error_message.set(f"Refresh error: {str(e)}")

    # Handle wallet address change
    @reactive.effect
    @reactive.event(input.wallet_address, ignore_none=True)
    def wallet_changed():
        """Handle wallet address change"""
        try:
            new_selection = input.wallet_address()
            logger.info(f"Wallet selection changed to: {new_selection}")

            # Handle special cases
            if new_selection in ["none", "error"]:
                logger.warning(f"Invalid selection: {new_selection}")
                return

            if new_selection == "custom":
                # TODO: Show custom wallet input dialog
                logger.info("Custom wallet option selected")
                return

            # Get the actual wallets to monitor
            wallets_to_monitor = get_monitored_wallets()

            if new_selection == "all_fund":
                # Monitor all fund wallets
                logger.info(f"Monitoring all fund wallets: {len(wallets_to_monitor)} wallets")
                if wallets_to_monitor:
                    # For now, fetch data from first wallet (TODO: aggregate all)
                    blockchain_service.wallet_address = wallets_to_monitor[0]
                else:
                    logger.warning("No wallets found for fund")
                    transaction_data.set(pd.DataFrame())
                    return
            else:
                # Single wallet selected
                if new_selection and len(new_selection) == 42 and new_selection.startswith('0x'):
                    blockchain_service.wallet_address = new_selection
                    logger.info(f"Switched to wallet: {new_selection}")
                else:
                    logger.warning(f"Invalid wallet address: {new_selection}")
                    return

            # Fetch fresh data for the new wallet
            logger.info(f"Fetching transactions for: {blockchain_service.wallet_address}")
            fresh_data = blockchain_service.fetch_historical_transactions(limit=int(input.transaction_limit() if hasattr(input, 'transaction_limit') else 100))

            if not fresh_data.empty:
                transaction_data.set(fresh_data)
                initialization_status.set("active")
                last_refresh.set(datetime.now())
                error_message.set("")
                logger.info(f"Loaded {len(fresh_data)} transactions for new wallet")
            else:
                transaction_data.set(pd.DataFrame())
                initialization_status.set("no_data")
                logger.warning("No transactions found for new wallet")

        except Exception as e:
            logger.error(f"Error changing wallet: {e}")
            import traceback
            traceback.print_exc()
            error_message.set(f"Error loading wallet: {str(e)}")

    # Periodic refresh for non-WebSocket mode
    @reactive.effect
    def periodic_refresh():
        """Periodically refresh data if not using WebSocket"""
        reactive.invalidate_later(30)  # Refresh every 30 seconds

        if initialization_status.get() == "active" and not blockchain_service.is_connected():
            try:
                # Get updated transactions
                updated_data = blockchain_service.get_all_transactions()
                if not updated_data.empty:
                    # Only update if there are changes
                    current = transaction_data.get()
                    if current.empty or len(updated_data) != len(current):
                        transaction_data.set(updated_data)
                        last_refresh.set(datetime.now())
                        logger.info(f"Auto-refreshed: {len(updated_data)} transactions")
            except Exception as e:
                logger.error(f"Error in periodic refresh: {e}")