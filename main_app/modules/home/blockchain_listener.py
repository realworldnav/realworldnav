from shiny import reactive, render, ui
import pandas as pd
from datetime import datetime, timedelta
import asyncio
import os
from .blockchain_service import blockchain_service
import logging

logger = logging.getLogger(__name__)


def register_blockchain_listener_outputs(input, output, session):
    """Register server outputs for blockchain listener"""

    # Reactive values
    transaction_data = reactive.value(pd.DataFrame())
    last_refresh = reactive.value(datetime.now())
    initialization_status = reactive.value("initializing")
    error_message = reactive.value("")

    # Initialize blockchain service on module load
    @reactive.effect
    def initialize_listener():
        """Initialize the blockchain listener on startup"""
        try:
            # Get initial wallet address
            try:
                wallet = input.wallet_address()
                logger.info(f"Got wallet from input: {wallet}")
            except:
                wallet = "0x3b2A51FEC517BBc7fEaf68AcFdb068b57870713F"
                logger.info(f"Using default wallet: {wallet}")

            # Check if we have API keys configured
            if not os.getenv('ETHERSCAN_API_KEY'):
                logger.warning("ETHERSCAN_API_KEY not found in environment. Using limited functionality.")
                initialization_status.set("limited")
                error_message.set("No Etherscan API key configured. Add ETHERSCAN_API_KEY to your environment.")
                return

            # Initialize the service (synchronously for now)
            blockchain_service.wallet_address = wallet

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
    def active_wallet_display():
        wallet = input.wallet_address() if hasattr(input, 'wallet_address') else "0x3b2A51FEC517BBc7fEaf68AcFdb068b57870713F"
        if wallet and len(wallet) > 10:
            return ui.tags.code(wallet[:6] + "..." + wallet[-4:], class_="address-text")
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

    # Main transaction table
    @output
    @render.data_frame
    def blockchain_transactions_table():
        df = transaction_data.get()

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

            # Format addresses
            from_addr = row.get('from', '')
            to_addr = row.get('to', '')
            if len(from_addr) > 12:
                from_display = from_addr[:6] + "..." + from_addr[-4:]
            else:
                from_display = from_addr
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
                        ui.strong("From Address:"),
                        ui.br(),
                        ui.code(tx.get('from', 'N/A'), class_="address-text"),
                    ),
                    ui.div(
                        ui.strong("To Address:"),
                        ui.br(),
                        ui.code(tx.get('to', 'N/A'), class_="address-text"),
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
                        "View on Block Explorer →",
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
        new_wallet = input.wallet_address()
        if new_wallet and len(new_wallet) == 42 and new_wallet.startswith('0x'):
            try:
                blockchain_service.wallet_address = new_wallet
                fresh_data = blockchain_service.fetch_historical_transactions(limit=100)
                if not fresh_data.empty:
                    transaction_data.set(fresh_data)
                    last_refresh.set(datetime.now())
                    error_message.set("")
            except Exception as e:
                logger.error(f"Error changing wallet: {e}")
                error_message.set(f"Invalid wallet: {str(e)}")

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