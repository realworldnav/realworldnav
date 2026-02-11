from shiny import reactive, render, ui
import pandas as pd
from datetime import datetime, timedelta, timezone
import asyncio
import concurrent.futures
import os
# LAZY IMPORT: blockchain_service is imported inside functions to avoid blocking app startup
# from .blockchain_service import blockchain_service  # Moved to lazy import
from .decoder_modal_ui import decoder_modal_ui, decoder_modal_styles
from .decoder_modal_outputs import register_decoder_modal_outputs
from .decoded_transactions_outputs import register_decoded_transactions_outputs
from .single_tx_outputs import register_single_tx_outputs
import logging

# Import the new DecoderRegistry - also lazy
DECODER_REGISTRY_AVAILABLE = False  # Will be set when needed

# Feature flag for new orchestrator (set via environment variable)
USE_ORCHESTRATOR = os.getenv("USE_NEW_BLOCKCHAIN_ORCHESTRATOR", "false").lower() == "true"

logger = logging.getLogger(__name__)

# Lazy-loaded module references with background pre-initialization
_blockchain_service = None
_blur_auto_decoder = None
_init_lock = None
_init_started = False
_init_complete = False


def _background_init():
    """Initialize blockchain service in background thread"""
    global _blockchain_service, _init_complete
    try:
        logger.info("Background: Starting blockchain service initialization...")
        from .blockchain_service import blockchain_service
        _blockchain_service = blockchain_service
        _init_complete = True
        logger.info("Background: Blockchain service initialization complete!")
    except Exception as e:
        logger.error(f"Background: Failed to initialize blockchain service: {e}")
        _init_complete = True  # Mark as complete even on error to avoid blocking


def start_background_init():
    """Start background initialization if not already started"""
    global _init_started, _init_lock
    import threading

    if _init_lock is None:
        _init_lock = threading.Lock()

    with _init_lock:
        if not _init_started:
            _init_started = True
            thread = threading.Thread(target=_background_init, daemon=True)
            thread.start()
            logger.info("Background initialization thread started")


def get_blockchain_service():
    """Get blockchain_service - waits for background init if needed"""
    global _blockchain_service, _init_complete

    # If already initialized, return immediately
    if _blockchain_service is not None:
        return _blockchain_service

    # If background init hasn't started, start it and wait
    if not _init_started:
        start_background_init()

    # Wait for background init to complete (with timeout)
    import time
    max_wait = 30  # Max 30 seconds
    waited = 0
    while not _init_complete and waited < max_wait:
        time.sleep(0.1)
        waited += 0.1

    if _blockchain_service is None:
        # Fallback: direct initialization
        logger.warning("Background init incomplete, doing direct initialization")
        from .blockchain_service import blockchain_service
        _blockchain_service = blockchain_service

    return _blockchain_service


def get_blur_auto_decoder():
    """Lazy load blur_auto_decoder"""
    global _blur_auto_decoder
    if _blur_auto_decoder is None:
        from .blur_auto_decoder import blur_auto_decoder
        _blur_auto_decoder = blur_auto_decoder
    return _blur_auto_decoder


def get_decoder_registry():
    """Lazy load DecoderRegistry"""
    global DECODER_REGISTRY_AVAILABLE
    try:
        from ...services.decoders import DecoderRegistry
        DECODER_REGISTRY_AVAILABLE = True
        return DecoderRegistry
    except ImportError:
        DECODER_REGISTRY_AVAILABLE = False
        return None


# New orchestrator lazy loading
_orchestrator = None
_orchestrator_init_started = False


def get_orchestrator(fund_wallets=None, fund_id=None):
    """
    Lazy load and initialize the BlockchainOrchestrator.

    This is only used when USE_ORCHESTRATOR=true.
    Falls back to the legacy blockchain_service otherwise.
    """
    global _orchestrator, _orchestrator_init_started

    if not USE_ORCHESTRATOR:
        return None

    if _orchestrator is not None:
        return _orchestrator

    if _orchestrator_init_started:
        # Already being initialized
        return None

    _orchestrator_init_started = True

    try:
        from ...services.blockchain import BlockchainOrchestrator
        logger.info("Initializing BlockchainOrchestrator...")

        _orchestrator = BlockchainOrchestrator(
            fund_wallets=fund_wallets or [],
            fund_id=fund_id or "drip_capital",
        )
        logger.info("BlockchainOrchestrator initialized successfully")
        return _orchestrator

    except Exception as e:
        logger.error(f"Failed to initialize BlockchainOrchestrator: {e}")
        _orchestrator_init_started = False
        return None


def reset_orchestrator():
    """Reset the orchestrator for re-initialization."""
    global _orchestrator, _orchestrator_init_started
    _orchestrator = None
    _orchestrator_init_started = False


# NOTE: Background initialization is now triggered by Connect button click
# instead of automatically on module import. This prevents unnecessary
# blockchain service initialization when users don't need the listener.
# start_background_init()  # Removed - now triggered by user action

# Thread pool for background operations
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)


def register_blockchain_listener_outputs(input, output, session, selected_fund):
    """Register server outputs for blockchain listener"""

    # Reactive values
    transaction_data = reactive.value(pd.DataFrame())
    last_refresh = reactive.value(datetime.now(timezone.utc))
    initialization_status = reactive.value("not_connected")  # Start as not connected
    error_message = reactive.value("")
    decoded_tx_cache = reactive.value({})  # Cache of decoded transactions
    decoder_registry = reactive.value(None)  # New multi-platform decoder registry
    decoder_registry_status = reactive.value("")  # Status message for decoder init (empty = no issue)
    registry_init_attempts = reactive.value(0)  # Retry counter for Web3 initialization
    decoded_refresh_trigger = reactive.value(0)  # Increment to force decoded transactions UI refresh
    connection_initiated = reactive.value(False)  # Gate for blockchain service initialization
    persisted_wallet_selection = reactive.value(None)  # Persist wallet selection across UI re-renders
    MAX_REGISTRY_INIT_ATTEMPTS = 3  # Max retries before giving up

    # Orchestrator-specific reactive values
    orchestrator_instance = reactive.value(None)  # BlockchainOrchestrator instance when USE_ORCHESTRATOR=true
    s3_sync_status = reactive.value({"synced": 0, "pending": 0, "last_sync": None})  # S3 sync status

    # Register decoder modal outputs
    set_current_tx = register_decoder_modal_outputs(input, output, session, selected_fund)

    # Register decoded transactions outputs (new tab)
    # Pass both registry and local cache for fallback when registry unavailable
    register_decoded_transactions_outputs(output, input, session, decoder_registry, decoded_tx_cache, decoded_refresh_trigger)

    # Register single transaction decoder outputs (Single TX tab)
    register_single_tx_outputs(input, output, session, selected_fund)

    # Unified listener content - handles both connect panel and listener UI
    @output
    @render.ui
    def listener_content():
        """Unified UI - shows connect panel OR listener content based on connection state"""
        # Establish dependency on selected_fund so wallet list updates when fund changes
        # (This is read outside isolate so it triggers re-render on fund change)
        _ = selected_fund()

        if not connection_initiated.get():
            # Not connected - show connection panel
            return ui.div(
                ui.div(
                    ui.HTML('<i class="bi bi-broadcast" style="font-size: 4rem; color: #6c757d;"></i>'),
                    ui.h3("Connect to Blockchain", class_="mt-3 mb-2"),
                    ui.p(
                        "Select a wallet and configure how many transactions to fetch.",
                        class_="text-muted mb-4"
                    ),
                    # Wallet selector - shown before connect
                    ui.div(
                        _build_wallet_selector(),
                        class_="mb-3",
                        style="max-width: 400px; margin: 0 auto;"
                    ),
                    # Transaction limit selector - shown before connect
                    ui.div(
                        ui.input_select(
                            "transaction_limit",
                            "Transactions to Fetch:",
                            {
                                "25": "Last 25",
                                "50": "Last 50 (Recommended)",
                                "100": "Last 100",
                                "200": "Last 200",
                                "500": "Last 500",
                            },
                            selected="50",
                            width="100%"
                        ),
                        class_="mb-4",
                        style="max-width: 400px; margin: 0 auto;"
                    ),
                    ui.input_action_button(
                        "connect_blockchain",
                        ui.HTML('<i class="bi bi-plug me-2"></i>Connect'),
                        class_="btn btn-primary connect-btn"
                    ),
                    ui.p(
                        "This will connect to Ethereum via Infura/Etherscan to fetch wallet transactions.",
                        class_="text-muted small mt-3"
                    ),
                    class_="connect-panel"
                )
            )

        # Return the full listener UI with tabbed transaction views
        return ui.div(
            # Combined Monitor Settings & Filters
            ui.card(
                ui.card_header("Monitor Settings"),
                ui.div(
                    # Main controls row
                    ui.layout_columns(
                        ui.div(
                            _build_wallet_selector(),
                        ),
                        ui.div(
                            ui.input_select(
                                "transaction_limit",
                                "Display Limit:",
                                {
                                    "25": "Last 25",
                                    "50": "Last 50",
                                    "100": "Last 100",
                                    "200": "Last 200",
                                    "500": "Last 500",
                                },
                                selected="50",
                                width="100%"
                            ),
                        ),
                        ui.div(
                            ui.input_select(
                                "network",
                                "Network:",
                                {
                                    "1": "Ethereum Mainnet",
                                },
                                selected="1",
                                width="100%"
                            ),
                        ),
                        col_widths=[6, 3, 3]
                    ),
                    class_="p-3"
                ),
                class_="mb-3"
            ),

            # Compact Status Row
            ui.layout_columns(
                ui.div(
                    ui.tags.div("STATUS", class_="text-muted small mb-1"),
                    ui.output_ui("connection_status"),
                    class_="p-2"
                ),
                ui.div(
                    ui.tags.div("WALLET", class_="text-muted small mb-1"),
                    ui.output_ui("active_wallet_display"),
                    class_="p-2"
                ),
                ui.div(
                    ui.tags.div("TODAY", class_="text-muted small mb-1"),
                    ui.output_ui("transactions_today_count"),
                    class_="p-2"
                ),
                ui.div(
                    ui.tags.div("LAST TX", class_="text-muted small mb-1"),
                    ui.output_ui("last_transaction_time"),
                    class_="p-2"
                ),
                col_widths=[3, 4, 2, 3],
                class_="bg-white rounded border mb-3 mx-0"
            ),

            # Tabbed Transaction Views - Decoded is PRIMARY
            ui.navset_card_tab(
                ui.nav_panel(
                    ui.HTML('<i class="bi bi-code-square me-1"></i> Decoded Transactions'),
                    ui.output_ui("decoded_transactions_tab_content"),
                ),
                ui.nav_panel(
                    ui.HTML('<i class="bi bi-list-ul me-1"></i> Raw Transactions'),
                    ui.output_ui("raw_transactions_tab_content"),
                ),
                id="listener_transaction_tabs"
            ),

            # S3 Sync Panel (only shown when orchestrator is available)
            ui.output_ui("s3_sync_panel")
        )

    # Handle Connect button click
    @reactive.effect
    @reactive.event(input.connect_blockchain)
    def handle_connect_click():
        """Handle Connect button click - starts blockchain service initialization"""
        # CRITICAL: Capture wallet selection BEFORE setting connection_initiated
        # because that triggers UI switch which destroys the old dropdown
        try:
            current_wallet = input.wallet_address()
            if current_wallet and current_wallet not in ["none", "error"]:
                persisted_wallet_selection.set(current_wallet)
                logger.info(f"Connect clicked - persisted wallet selection: {current_wallet[:10]}...")
        except Exception as e:
            logger.warning(f"Could not capture wallet selection: {e}")

        logger.info("Connect button clicked - initiating blockchain service...")
        connection_initiated.set(True)
        initialization_status.set("initializing")
        # Start background initialization
        start_background_init()

    # Helper function to build wallet selector (called directly, not an output)
    def _build_wallet_selector():
        """Build wallet selector dropdown with friendly names filtered by selected fund"""
        # Use reactive.isolate() to prevent establishing dependencies that would
        # cause re-renders. This function is called from render functions.
        with reactive.isolate():
            current_fund = selected_fund()
            persisted = persisted_wallet_selection.get()

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
                    wallet_choices["all_fund"] = f"All {current_fund} Wallets ({len(fund_wallets)} wallets)"

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
        wallet_choices["custom"] = "+ Enter Custom Address..."

        # Use persisted selection if valid, otherwise default to first wallet
        # NOTE: Do NOT call .set() here - that causes re-render loops!
        # The persist_wallet_selection effect handles initialization.
        if persisted and persisted in wallet_choices:
            default_selection = persisted
        else:
            default_selection = next((k for k in wallet_choices.keys() if k not in ["none", "error", "custom", "all_fund"]), "custom")

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

    # Persist wallet selection when user changes dropdown
    @reactive.effect
    def persist_wallet_selection():
        """Save wallet selection to reactive value so it persists across UI re-renders"""
        try:
            current = input.wallet_address()
            if current and current not in ["none", "error"]:
                persisted_wallet_selection.set(current)
        except:
            pass

    # Get list of wallets to monitor based on selection
    @reactive.calc
    def get_monitored_wallets():
        """Get list of wallet addresses to monitor based on current selection"""
        try:
            # Try input first, fall back to persisted value
            try:
                wallet_selection = input.wallet_address()
            except:
                wallet_selection = persisted_wallet_selection.get()

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

    # Background task for fetching transactions (non-blocking)
    @reactive.extended_task
    async def fetch_transactions_task(wallet_address: str, limit: int = 50):
        """Fetch transactions in background thread to avoid blocking UI"""
        loop = asyncio.get_event_loop()
        # Run the blocking call in a thread pool
        def _fetch():
            get_blockchain_service().wallet_address = wallet_address
            return get_blockchain_service().fetch_historical_transactions(limit=limit)

        return await loop.run_in_executor(_executor, _fetch)

    # Initialize blockchain service when Connect button clicked (non-blocking)
    @reactive.effect
    def initialize_listener():
        """Initialize the blockchain listener after Connect button clicked - triggers background fetch"""
        # Only run if user has clicked Connect
        if not connection_initiated.get():
            return

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
                # Set status to loading and trigger background fetch
                initialization_status.set("loading")
                # Get the user-selected transaction limit (default 50)
                try:
                    limit = int(input.transaction_limit())
                except:
                    limit = 50
                logger.info(f"Starting background fetch for wallet {primary_wallet[:10]}... (limit: {limit})")
                fetch_transactions_task(primary_wallet, limit)

            last_refresh.set(datetime.now(timezone.utc))

        except Exception as e:
            logger.error(f"Failed to initialize blockchain listener: {e}")
            import traceback
            traceback.print_exc()
            initialization_status.set("error")
            error_message.set(f"Initialization error: {str(e)}")

    # Handle completion of background fetch task
    @reactive.effect
    def handle_fetch_completion():
        """Handle when background fetch completes"""
        # Check task status before accessing result
        status = fetch_transactions_task.status()

        if status == "initial":
            # Task hasn't been started yet
            return
        elif status == "running":
            # Task is still running - do nothing, will be called again when complete
            return
        elif status == "error":
            # Task failed
            error = fetch_transactions_task.error()
            logger.error(f"Background fetch failed: {error}")
            initialization_status.set("error")
            error_message.set(f"Failed to fetch transactions: {str(error)}")
            return
        elif status == "success":
            # Task completed successfully - get result
            result = fetch_transactions_task.result()
            if result is not None and not result.empty:
                transaction_data.set(result)
                initialization_status.set("active")
                logger.info(f"Background fetch complete: {len(result)} transactions loaded")
            else:
                initialization_status.set("no_data")
                logger.warning("Background fetch complete: No transactions found")

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

    # ==================== NEW TAB CONTENT OUTPUTS ====================

    # Decoded Transactions Tab Content (PRIMARY VIEW)
    @output
    @render.ui
    def decoded_transactions_tab_content():
        """Render the decoded transactions card grid - this is the primary view"""
        from .decoded_transactions_ui import (
            transaction_card_ui, empty_state_ui, PLATFORM_COLORS, CATEGORY_NAMES
        )

        # Compact inline stats bar
        stats_bar = ui.div(
            ui.span(
                ui.HTML('<i class="bi bi-check-circle-fill text-primary me-1"></i>'),
                ui.output_text("decoded_total_count"),
                " Decoded",
                class_="me-4"
            ),
            ui.span(
                ui.HTML('<i class="bi bi-lightning-fill text-success me-1"></i>'),
                ui.output_text("decoded_auto_post_count"),
                " Auto-Post",
                class_="me-4"
            ),
            ui.span(
                ui.HTML('<i class="bi bi-hourglass-split text-warning me-1"></i>'),
                ui.output_text("decoded_review_queue_count"),
                " Review",
                class_="me-4"
            ),
            ui.span(
                ui.HTML('<i class="bi bi-shield-x text-secondary me-1"></i>'),
                ui.output_text("decoded_spam_count"),
                " Spam"
            ),
            class_="d-flex align-items-center bg-light rounded px-3 py-2 mb-3"
        )

        # Filter controls
        filter_card = ui.card(
            ui.card_header(
                ui.div(
                    ui.span("Filters", class_="fw-semibold"),
                    ui.input_action_button(
                        "refresh_decoded",
                        "Refresh",
                        class_="btn-sm btn-outline-primary ms-auto",
                        icon=ui.span(class_="bi bi-arrow-clockwise")
                    ),
                    class_="d-flex align-items-center w-100"
                )
            ),
            ui.layout_columns(
                ui.input_select(
                    "decoded_platform_filter",
                    "Platform",
                    choices={
                        "all": "All Platforms",
                        "blur": "Blur",
                        "arcade": "Arcade",
                        "nftfi": "NFTfi",
                        "gondi": "Gondi",
                        "zharta": "Zharta",
                        "generic": "Generic"
                    },
                    selected="all"
                ),
                ui.input_select(
                    "decoded_category_filter",
                    "Category",
                    choices={
                        "all": "All Categories",
                        "LOAN_ORIGINATION": "Loan Origination",
                        "LOAN_REPAYMENT": "Loan Repayment",
                        "LOAN_REFINANCE": "Refinance",
                        "ETH_TRANSFER": "ETH Transfer",
                        "ERC20_TRANSFER": "Token Transfer",
                        "CONTRACT_CALL": "Contract Call"
                    },
                    selected="all"
                ),
                ui.input_select(
                    "decoded_status_filter",
                    "Posting Status",
                    choices={
                        "all": "All Statuses",
                        "auto_post": "Auto-Post Ready",
                        "review_queue": "Review Queue",
                        "posted": "Posted"
                    },
                    selected="all"
                ),
                col_widths=[4, 4, 4]
            ),
            class_="mb-3"
        )

        # Transaction cards container (uses existing output from decoded_transactions_outputs.py)
        cards_container = ui.div(
            ui.output_ui("decoded_transaction_cards"),
            class_="decoded-cards-container",
            style="max-height: 500px; overflow-y: auto;"
        )

        # Bulk actions
        bulk_actions = ui.div(
            ui.input_action_button(
                "post_all_auto",
                "Post All Auto-Ready",
                class_="btn-success me-2",
                icon=ui.span(class_="bi bi-check2-all me-1")
            ),
            ui.download_button(
                "download_decoded_csv",
                "Export Journal Entries",
                class_="btn-outline-primary me-2",
                icon=ui.span(class_="bi bi-download me-1")
            ),
            ui.input_action_button(
                "clear_decoded_cache",
                "Clear Cache",
                class_="btn-outline-secondary",
                icon=ui.span(class_="bi bi-trash me-1")
            ),
            class_="mt-3 d-flex"
        )

        return ui.div(
            stats_bar,
            filter_card,
            cards_container,
            bulk_actions,
            class_="p-3"
        )

    # Raw Transactions Tab Content (SECONDARY VIEW - click opens Etherscan)
    @output
    @render.ui
    def raw_transactions_tab_content():
        """Render simplified raw transactions table - click any row to open in Etherscan"""
        df = transaction_data.get()

        if df.empty:
            return ui.div(
                ui.div(
                    ui.span(class_="bi bi-inbox display-4 text-muted"),
                    class_="text-center mb-3"
                ),
                ui.h5("No Transactions", class_="text-muted text-center"),
                ui.p(
                    "Transactions will appear here once fetched from the blockchain.",
                    class_="text-muted text-center small"
                ),
                class_="py-5"
            )

        # Build clickable table rows
        rows = []
        network = input.network() if hasattr(input, 'network') else "1"
        etherscan_base = {
            "1": "https://etherscan.io",
            "42161": "https://arbiscan.io",
            "10": "https://optimistic.etherscan.io",
            "137": "https://polygonscan.com",
            "8453": "https://basescan.org"
        }.get(network, "https://etherscan.io")

        limit = int(input.transaction_limit()) if hasattr(input, 'transaction_limit') else 50

        for _, row in df.head(limit).iterrows():
            tx_hash = row.get('hash', '')
            block = row.get('block', 0)
            from_addr = row.get('from', '')
            to_addr = row.get('to', '')
            amount = row.get('amount', 0)
            token = row.get('token', 'ETH')
            timestamp = row.get('timestamp', '')

            # Format display values
            hash_display = f"{tx_hash[:8]}...{tx_hash[-6:]}" if len(tx_hash) > 16 else tx_hash
            from_display = f"{from_addr[:6]}...{from_addr[-4:]}" if len(from_addr) > 12 else from_addr
            to_display = f"{to_addr[:6]}...{to_addr[-4:]}" if len(to_addr) > 12 else to_addr

            if isinstance(timestamp, str):
                time_display = timestamp[:16].replace('T', ' ')
            else:
                time_display = str(timestamp)[:16] if timestamp else ""

            etherscan_url = f"{etherscan_base}/tx/{tx_hash}"

            rows.append(
                ui.tags.tr(
                    ui.tags.td(
                        ui.tags.code(hash_display, class_="text-primary"),
                        style="font-family: monospace;"
                    ),
                    ui.tags.td(str(block)),
                    ui.tags.td(from_display, style="font-family: monospace; font-size: 0.85em;"),
                    ui.tags.td(ui.HTML("&rarr;"), class_="text-muted"),
                    ui.tags.td(to_display, style="font-family: monospace; font-size: 0.85em;"),
                    ui.tags.td(f"{amount:.4f} {token}", class_="text-end"),
                    ui.tags.td(time_display, class_="text-muted small"),
                    ui.tags.td(
                        ui.HTML('<i class="bi bi-box-arrow-up-right"></i>'),
                        class_="text-primary"
                    ),
                    onclick=f"window.open('{etherscan_url}', '_blank')",
                    style="cursor: pointer;",
                    class_="raw-tx-row"
                )
            )

        table = ui.tags.table(
            ui.tags.thead(
                ui.tags.tr(
                    ui.tags.th("Hash"),
                    ui.tags.th("Block"),
                    ui.tags.th("From"),
                    ui.tags.th(""),
                    ui.tags.th("To"),
                    ui.tags.th("Amount", class_="text-end"),
                    ui.tags.th("Time"),
                    ui.tags.th(""),  # Etherscan icon column
                )
            ),
            ui.tags.tbody(*rows),
            class_="table table-hover"
        )

        return ui.div(
            ui.p(
                ui.HTML('<i class="bi bi-info-circle me-1"></i>'),
                "Click any row to open the transaction on Etherscan",
                class_="text-muted small mb-3"
            ),
            ui.div(
                table,
                style="max-height: 500px; overflow-y: auto;"
            ),
            ui.tags.style("""
                .raw-tx-row:hover {
                    background-color: #f0f7ff !important;
                }
                .raw-tx-row:hover td {
                    color: #0066cc;
                }
            """),
            class_="p-3"
        )

    # ==================== END NEW TAB CONTENT ====================

    # Connection status
    @output
    @render.ui
    def connection_status():
        status = initialization_status.get()

        if status == "active":
            if get_blockchain_service().is_connected():
                return ui.tags.strong("Live Monitoring (WebSocket)", style="color: #28a745;")
            elif get_blockchain_service().is_infura_connected():
                return ui.tags.strong("Infura Connected", style="color: #28a745;")
            else:
                return ui.tags.strong("Etherscan Only", style="color: #ffc107;")
        elif status == "limited":
            return ui.tags.strong("Limited Mode", style="color: #ffc107;")
        elif status == "initializing":
            return ui.tags.strong("Initializing...", style="color: #6c757d;")
        elif status == "loading":
            return ui.tags.strong("Loading transactions...", style="color: #17a2b8;")
        elif status == "error":
            return ui.tags.strong("Error", style="color: #b45309;")
        else:
            return ui.tags.strong("Not Connected", style="color: #b45309;")

    @output
    @render.ui
    def connection_indicator():
        status = initialization_status.get()

        if status == "active":
            if get_blockchain_service().is_connected():
                return ui.HTML('<i class="bi bi-circle-fill connection-active"></i> WebSocket + Infura')
            elif get_blockchain_service().is_infura_connected():
                return ui.HTML('<i class="bi bi-circle-fill" style="color: #28a745;"></i> Infura HTTP')
            else:
                return ui.HTML('<i class="bi bi-circle-fill" style="color: #ffc107;"></i> Etherscan')
        elif status == "limited":
            return ui.HTML('<i class="bi bi-exclamation-triangle-fill" style="color: #ffc107;"></i> Limited')
        elif status == "initializing":
            return ui.HTML('<i class="bi bi-hourglass-split"></i> Starting...')
        elif status == "loading":
            return ui.HTML('<i class="bi bi-arrow-repeat spin-animation" style="color: #17a2b8;"></i> Loading...')
        else:
            return ui.HTML('<i class="bi bi-circle-fill connection-inactive"></i> Offline')

    # Decoder registry status banner (shown in decoded transactions tab)
    @output
    @render.ui
    def decoder_status_banner():
        status_raw = decoder_registry_status.get()
        if not status_raw:
            return ui.div()  # Empty - no status yet

        # Parse "level:message" format
        if ":" in status_raw:
            level, message = status_raw.split(":", 1)
        else:
            level, message = "info", status_raw

        if level == "error":
            return ui.div(
                ui.HTML(f"""
                    <div class="alert alert-danger d-flex align-items-center mb-2" role="alert">
                        <i class="bi bi-exclamation-triangle-fill me-2"></i>
                        <div><strong>Decoder Error:</strong> {message}</div>
                    </div>
                """)
            )
        elif level == "warning":
            return ui.div(
                ui.HTML(f"""
                    <div class="alert alert-warning d-flex align-items-center mb-2" role="alert">
                        <i class="bi bi-exclamation-circle-fill me-2"></i>
                        <div><strong>Decoder Warning:</strong> {message}</div>
                    </div>
                """)
            )
        elif level == "retrying":
            return ui.div(
                ui.HTML(f"""
                    <div class="alert alert-info d-flex align-items-center mb-2" role="alert">
                        <i class="bi bi-arrow-repeat spin-animation me-2"></i>
                        <div>{message}</div>
                    </div>
                """)
            )
        elif level == "ok":
            return ui.div(
                ui.HTML(f"""
                    <div class="alert alert-success d-flex align-items-center mb-2 py-1" role="alert">
                        <i class="bi bi-check-circle-fill me-2"></i>
                        <div>{message}</div>
                    </div>
                """)
            )
        return ui.div()

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
            friendly_name = get_blockchain_service().get_friendly_name(selection)

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

        today = datetime.now(timezone.utc).date()
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

        today = datetime.now(timezone.utc).date()
        yesterday = today - timedelta(days=1)

        today_count = len(df[df['timestamp'].dt.date == today])
        yesterday_count = len(df[df['timestamp'].dt.date == yesterday])

        if yesterday_count > 0:
            change = ((today_count - yesterday_count) / yesterday_count) * 100
            if change > 0:
                return ui.HTML(f'<i class="bi bi-arrow-up-circle-fill text-success"></i> +{change:.1f}% vs yesterday')
            else:
                return ui.HTML(f'<i class="bi bi-arrow-down-circle-fill" style="color: #475569;"></i> {change:.1f}% vs yesterday')
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

        time_ago = datetime.now(timezone.utc) - last_time

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
            if get_blockchain_service().is_connected():
                return ui.HTML("""
                    <span class="badge bg-success">
                        <i class="bi bi-arrow-repeat"></i> Live WebSocket
                    </span>
                """)
            elif get_blockchain_service().is_infura_connected():
                return ui.HTML("""
                    <span class="badge bg-success">
                        <i class="bi bi-arrow-repeat"></i> Infura (15s)
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
                    <i class="bi bi-hourglass-split"></i> Initializing...
                </span>
            """)
        elif status == "loading":
            return ui.HTML("""
                <span class="badge bg-info">
                    <i class="bi bi-arrow-repeat spin-animation"></i> Loading transactions...
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

        # Only apply filters if the filter panel is shown and inputs exist
        # This prevents blocking the table render when filters are hidden
        try:
            filters_shown = input.show_filters() if hasattr(input, 'show_filters') else False
        except:
            filters_shown = False

        if not filters_shown:
            # Return unfiltered data when filters are hidden
            return df

        # Apply transaction type filter
        if hasattr(input, 'tx_type_filter'):
            try:
                if input.tx_type_filter() != "all":
                    filter_type = input.tx_type_filter()
                    if filter_type == "in":
                        df = df[df['type'] == 'IN']
                    elif filter_type == "out":
                        df = df[df['type'] == 'OUT']
            except:
                pass

        # Apply token filter
        if hasattr(input, 'token_filter'):
            try:
                if input.token_filter() != "all":
                    token = input.token_filter()
                    if token == "eth":
                        df = df[df['token'] == 'ETH']
                    elif token == "erc20":
                        df = df[df['token'] != 'ETH']
                    elif token == "usdc":
                        df = df[df['token'].str.upper() == 'USDC']
                    elif token == "usdt":
                        df = df[df['token'].str.upper() == 'USDT']
            except:
                pass

        # Apply minimum value filter
        if hasattr(input, 'min_value'):
            try:
                if input.min_value() > 0:
                    df = df[df['amount'] >= input.min_value()]
            except:
                pass

        # Apply time range filter
        if hasattr(input, 'time_range'):
            try:
                if input.time_range() != "all":
                    time_range = input.time_range()
                    now = datetime.now(timezone.utc)

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
            except:
                pass

        return df

    # Main transaction table
    @output
    @render.data_frame
    def blockchain_transactions_table():
        # Use filtered transactions
        df = filtered_transactions()

        if df.empty:
            # Return empty DataFrame with proper columns
            display_df = pd.DataFrame(columns=['Decoded', 'Status', 'Type', 'Hash', 'Block', 'From', 'To', 'Amount', 'Gas', 'Time'])
            return render.DataGrid(display_df, width="100%", height="500px")

        # Format display columns
        display_data = []
        for _, row in df.iterrows():
            # Get decode status for ALL transactions
            tx_hash = row.get('hash', '')
            decode_icon = ""

            cached_decode = decoded_tx_cache.get().get(tx_hash)
            if cached_decode and cached_decode.get('status') == 'success':
                decode_icon = "[D]"  # Successfully decoded
            elif cached_decode and cached_decode.get('status') == 'error':
                decode_icon = "[!]"  # Decode error
            else:
                decode_icon = ""  # Not yet decoded or pending
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
                time_display = row.get('timestamp', datetime.now(timezone.utc)).strftime("%Y-%m-%d %H:%M:%S UTC")

            display_data.append({
                'Decoded': decode_icon,
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
        limit = int(input.transaction_limit()) if hasattr(input, 'transaction_limit') else 50
        display_df = display_df.head(limit)

        return render.DataGrid(
            display_df,
            selection_mode="row",
            filters=True,
            width="100%",
            height="500px"
        )

    # Initialize DecoderRegistry when fund wallets are available (with retry logic)
    @reactive.effect
    @reactive.event(selected_fund, registry_init_attempts, connection_initiated)
    def initialize_decoder_registry():
        """
        Initialize the multi-platform decoder registry with retry logic.

        Tries multiple Web3 sources and retries if connection fails.
        Resets retry counter on fund change or successful init.
        """
        # Only run if user has clicked Connect
        if not connection_initiated.get():
            return

        # Try to get DecoderRegistry class (lazy load)
        DecoderRegistryClass = get_decoder_registry()
        if DecoderRegistryClass is None:
            logger.info("DecoderRegistry not available (import failed), using legacy blur_auto_decoder")
            decoder_registry_status.set("error:DecoderRegistry import failed — check that all decoder dependencies are installed")
            return

        attempts = registry_init_attempts.get()
        if attempts >= MAX_REGISTRY_INIT_ATTEMPTS:
            logger.warning(f"DecoderRegistry init failed after {attempts} attempts, using legacy decoder")
            decoder_registry_status.set(f"error:Decoder init failed after {attempts} attempts — Web3 connection unavailable")
            return

        try:
            from ...s3_utils import load_WALLET_file

            wallet_df = load_WALLET_file()
            current_fund = selected_fund()

            # Reset attempts on fund change
            if attempts > 0 and decoder_registry.get() is not None:
                registry_init_attempts.set(0)
                return

            # Skip re-initialization if registry already exists with same fund
            # This prevents clearing decoded_cache on reactive re-runs
            existing_registry = decoder_registry.get()
            if existing_registry is not None:
                if existing_registry.fund_id == current_fund and len(existing_registry.fund_wallets) > 0:
                    logger.debug(f"Registry already initialized for fund {current_fund}, skipping re-creation")
                    return
                else:
                    logger.info(f"Fund changed from {existing_registry.fund_id} to {current_fund}, re-initializing registry")

            if wallet_df.empty:
                logger.warning("No wallet data available")
                decoder_registry_status.set("error:No wallet data — check S3 wallet mapper file")
                return

            # Load ALL wallets from mapper - decode any transaction involving our wallets
            fund_wallet_addresses = wallet_df['wallet_address'].str.strip().tolist()

            # CRITICAL: Also include currently monitored wallets to ensure they're decoded
            # This handles cases where monitored wallets aren't in the S3 mapper file
            try:
                monitored = get_monitored_wallets()
                if monitored:
                    for wallet in monitored:
                        if wallet and wallet.lower() not in [w.lower() for w in fund_wallet_addresses]:
                            fund_wallet_addresses.append(wallet)
                            logger.info(f"Added monitored wallet {wallet[:10]}... to decoder fund list")
            except Exception as e:
                logger.debug(f"Could not add monitored wallets: {e}")

            if not fund_wallet_addresses:
                logger.warning("No wallets found in wallet mapper")
                decoder_registry_status.set("error:No wallets found in wallet mapper")
                return

            # Log wallet addresses for debugging
            logger.info(f"Decoder will use {len(fund_wallet_addresses)} fund wallets:")
            for i, addr in enumerate(fund_wallet_addresses[:5]):  # Log first 5
                logger.info(f"  [{i+1}] {addr}")
            if len(fund_wallet_addresses) > 5:
                logger.info(f"  ... and {len(fund_wallet_addresses) - 5} more")

            # Try multiple Web3 sources
            w3 = None

            # Source 1: get_blockchain_service().infura.w3_http (primary)
            try:
                svc = get_blockchain_service()
                if (hasattr(svc, 'infura') and
                    svc.infura is not None and
                    hasattr(svc.infura, 'w3_http') and
                    svc.infura.w3_http is not None):
                    w3 = svc.infura.w3_http
                    logger.debug("Got Web3 from blockchain_service.infura.w3_http")
            except Exception as e:
                logger.debug(f"Could not get Web3 from infura: {e}")

            # Source 2: Direct Web3 from env vars (fallback)
            if w3 is None:
                try:
                    from web3 import Web3
                    from ...config.blockchain_config import INFURA_URL, INFURA_API_KEY
                    if INFURA_API_KEY and INFURA_URL:
                        fallback_w3 = Web3(Web3.HTTPProvider(INFURA_URL))
                        if fallback_w3.is_connected():
                            w3 = fallback_w3
                            logger.info("Got Web3 from direct Infura HTTP fallback")
                        else:
                            logger.debug("Direct Infura HTTP fallback failed to connect")
                except Exception as e:
                    logger.debug(f"Could not create direct Web3 fallback: {e}")

            if w3 is None:
                logger.warning(f"Web3 not available (attempt {attempts + 1}/{MAX_REGISTRY_INIT_ATTEMPTS}), will retry")
                # Schedule retry in 5 seconds
                if attempts + 1 < MAX_REGISTRY_INIT_ATTEMPTS:
                    decoder_registry_status.set(f"retrying:Web3 connection attempt {attempts + 1}/{MAX_REGISTRY_INIT_ATTEMPTS}...")
                    reactive.invalidate_later(5.0)
                    registry_init_attempts.set(attempts + 1)
                else:
                    decoder_registry_status.set("error:Web3 connection failed — check INFURA_API_KEY in .env")
                return

            # Verify connection with actual RPC call (is_connected() is unreliable for HTTP)
            try:
                chain_id = w3.eth.chain_id
                logger.info(f"Web3 connected to chain {chain_id}")
            except Exception as e:
                logger.warning(f"Web3 not connected (attempt {attempts + 1}/{MAX_REGISTRY_INIT_ATTEMPTS}): {e}")
                # Schedule retry
                if attempts + 1 < MAX_REGISTRY_INIT_ATTEMPTS:
                    decoder_registry_status.set(f"retrying:Web3 RPC verify attempt {attempts + 1}/{MAX_REGISTRY_INIT_ATTEMPTS}...")
                    reactive.invalidate_later(5.0)
                    registry_init_attempts.set(attempts + 1)
                else:
                    decoder_registry_status.set(f"error:Web3 RPC call failed — {e}")
                return

            # Create registry with fund_id for GL posting
            registry = DecoderRegistryClass(w3, fund_wallet_addresses, fund_id=current_fund)
            decoder_registry.set(registry)
            registry_init_attempts.set(0)  # Reset counter on success

            # Report decoder availability
            num_decoders = len(getattr(registry, '_decoder_classes', {}))
            if num_decoders > 0:
                decoder_registry_status.set(f"ok:{num_decoders} platform decoders active")
            else:
                decoder_registry_status.set("warning:Registry created but no platform decoders loaded")

            # Clear local decoded cache to force fresh decoding with new registry
            # This ensures old failed decodes (from before code fixes) are re-tried
            decoded_tx_cache.set({})

            logger.info(f"Initialized DecoderRegistry with {len(fund_wallet_addresses)} wallets, {num_decoders} decoders")

        except Exception as e:
            logger.error(f"Failed to initialize DecoderRegistry: {e}")
            decoder_registry_status.set(f"error:Decoder init failed — {e}")
            if attempts + 1 < MAX_REGISTRY_INIT_ATTEMPTS:
                registry_init_attempts.set(attempts + 1)

    # Auto-decode transactions in background (uses new registry when available)
    @reactive.effect
    def auto_decode_transactions():
        """Automatically decode transactions in background using multi-platform registry"""
        # Only run if connected
        if not connection_initiated.get():
            return

        df = transaction_data.get()

        if df.empty:
            return

        # Get fund wallets for decoding
        try:
            from ...s3_utils import load_WALLET_file
            wallet_df = load_WALLET_file()
            current_fund = selected_fund()

            if not wallet_df.empty:
                # Load ALL wallets - decode any transaction involving our wallets
                fund_wallet_addresses = wallet_df['wallet_address'].str.strip().tolist()
            else:
                fund_wallet_addresses = []
        except:
            fund_wallet_addresses = []

        # Check each transaction
        current_cache = decoded_tx_cache.get().copy()
        registry = decoder_registry.get()
        updated = False
        decode_count = 0
        tx_types = {}

        # Process transactions, starting from most recent
        for _, row in df.head(50).iterrows():
            tx_hash = row.get('hash', '')

            if not tx_hash or tx_hash in current_cache:
                continue

            try:
                # Use new DecoderRegistry if available
                if registry:
                    decoded = registry.decode_transaction(tx_hash)
                    result = decoded.to_dict()
                    tx_type = result.get('category', 'UNKNOWN')
                else:
                    # Fallback to legacy blur_auto_decoder
                    result = get_blur_auto_decoder().decode_transaction(
                        tx_hash,
                        fund_wallet_addresses,
                        wallet_metadata=None
                    )
                    tx_type = result.get('tx_type', 'UNKNOWN')

                current_cache[tx_hash] = result
                updated = True
                decode_count += 1
                tx_types[tx_type] = tx_types.get(tx_type, 0) + 1
            except Exception as e:
                logger.error(f"Failed to auto-decode {tx_hash[:10]}: {e}")
                current_cache[tx_hash] = {"status": "error", "error": str(e)}
                updated = True

        if updated:
            decoded_tx_cache.set(current_cache)
            # Trigger UI refresh for decoded transactions tab
            decoded_refresh_trigger.set(decoded_refresh_trigger.get() + 1)
            # Log summary
            if decode_count > 0:
                types_summary = ", ".join(f"{k}:{v}" for k, v in tx_types.items())
                logger.info(f"Auto-decoded {decode_count} transactions ({types_summary})")

    # Show decoder modal when user clicks decoded icon
    @reactive.effect
    @reactive.event(input.blockchain_transactions_table_selected_rows)
    def show_decoder_modal():
        """Show decoder modal when row with decoded icon is clicked"""
        selected = input.blockchain_transactions_table_selected_rows()

        if not selected or len(selected) == 0:
            return

        df = filtered_transactions()
        if df.empty or selected[0] >= len(df):
            return

        tx = df.iloc[selected[0]]
        tx_hash = tx.get('hash', '')

        # Check if this transaction is decoded
        if tx_hash in decoded_tx_cache.get():
            # Set current transaction for modal
            set_current_tx(tx_hash)

            # Show modal
            ui.modal_show(decoder_modal_ui(tx_hash))

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
        """Manually refresh transaction data and clear decoded caches"""
        # Only run if connected
        if not connection_initiated.get():
            return

        try:
            wallet = input.wallet_address() if hasattr(input, 'wallet_address') else get_blockchain_service().wallet_address
            limit = int(input.transaction_limit()) if hasattr(input, 'transaction_limit') else 50

            # Re-initialize if wallet changed
            if wallet != get_blockchain_service().wallet_address:
                get_blockchain_service().wallet_address = wallet

            # Clear decoded transaction caches on refresh
            decoded_tx_cache.set({})
            registry = decoder_registry.get()
            if registry and hasattr(registry, 'decoded_cache'):
                registry.decoded_cache.clear()
                logger.info("Cleared decoded transaction caches on refresh")

            # Fetch fresh data
            fresh_data = get_blockchain_service().fetch_historical_transactions(limit=limit)
            if not fresh_data.empty:
                transaction_data.set(fresh_data)
                last_refresh.set(datetime.now(timezone.utc))
                logger.info(f"Refreshed with {len(fresh_data)} transactions")

        except Exception as e:
            logger.error(f"Error refreshing transactions: {e}")
            error_message.set(f"Refresh error: {str(e)}")

    # Track the last wallet to detect changes
    last_wallet_selection = reactive.value(None)

    # Handle wallet address change - use @reactive.effect without @reactive.event
    # This pattern works better for dynamically rendered inputs
    @reactive.effect
    def wallet_changed():
        """Handle wallet address change"""
        # Only run if connected
        if not connection_initiated.get():
            return

        try:
            # Try to get the wallet address - may not exist yet if UI not rendered
            try:
                new_selection = input.wallet_address()
            except:
                return  # Input doesn't exist yet

            if not new_selection:
                return

            # Check if this is actually a change
            previous = last_wallet_selection.get()
            if new_selection == previous:
                return  # No change, skip

            # Update tracked value
            last_wallet_selection.set(new_selection)

            current_wallet = get_blockchain_service().wallet_address
            print(f"[WALLET] Selection changed: {new_selection[:10] if new_selection else None}... (current: {current_wallet[:10] if current_wallet else None}...)")
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
                    get_blockchain_service().wallet_address = wallets_to_monitor[0]
                else:
                    logger.warning("No wallets found for fund")
                    transaction_data.set(pd.DataFrame())
                    return
            else:
                # Single wallet selected
                if new_selection and len(new_selection) == 42 and new_selection.startswith('0x'):
                    get_blockchain_service().wallet_address = new_selection
                    logger.info(f"Switched to wallet: {new_selection}")
                else:
                    logger.warning(f"Invalid wallet address: {new_selection}")
                    return

            # Clear decoded transaction caches when switching wallets
            decoded_tx_cache.set({})
            registry = decoder_registry.get()
            if registry and hasattr(registry, 'decoded_cache'):
                registry.decoded_cache.clear()
                logger.info("Cleared decoded transaction caches for wallet switch")

            # NOTE: Don't trigger refresh here - let auto_decode_transactions() do it
            # after the new transactions are decoded. This prevents showing stale/empty state.

            # Fetch fresh data for the new wallet
            logger.info(f"Fetching transactions for: {get_blockchain_service().wallet_address}")
            fresh_data = get_blockchain_service().fetch_historical_transactions(limit=int(input.transaction_limit() if hasattr(input, 'transaction_limit') else 50))

            if not fresh_data.empty:
                transaction_data.set(fresh_data)
                initialization_status.set("active")
                last_refresh.set(datetime.now(timezone.utc))
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
        """Periodically refresh data - faster with Infura (15s) vs Etherscan (30s)"""
        # Only run if connected
        if not connection_initiated.get():
            return

        # Use faster refresh interval when Infura is connected
        refresh_interval = 15 if get_blockchain_service().is_infura_connected() else 30
        reactive.invalidate_later(refresh_interval)

        if initialization_status.get() == "active" and not get_blockchain_service().is_connected():
            try:
                # Get updated transactions using Infura primarily
                updated_data = get_blockchain_service().get_all_transactions()
                if not updated_data.empty:
                    # Only update if there are changes
                    current = transaction_data.get()
                    if current.empty or len(updated_data) != len(current):
                        transaction_data.set(updated_data)
                        last_refresh.set(datetime.now(timezone.utc))
                        source = "Infura" if get_blockchain_service().is_infura_connected() else "Etherscan"
                        logger.info(f"Auto-refreshed via {source}: {len(updated_data)} transactions")
            except Exception as e:
                logger.error(f"Error in periodic refresh: {e}")

    # S3 Sync Panel - only shown when orchestrator is enabled
    @output
    @render.ui
    def s3_sync_panel():
        """Render S3 sync panel when orchestrator is available"""
        if not USE_ORCHESTRATOR:
            return ui.div()  # Empty when orchestrator not enabled

        orch = orchestrator_instance.get()
        if not orch:
            return ui.div()

        sync_status = s3_sync_status.get()
        last_sync = sync_status.get("last_sync")
        last_sync_str = last_sync.strftime("%Y-%m-%d %H:%M:%S") if last_sync else "Never"

        return ui.card(
            ui.card_header(
                ui.layout_columns(
                    ui.h5("S3 Sync"),
                    ui.div(
                        ui.output_ui("orchestrator_status_badge"),
                        class_="text-end"
                    ),
                    col_widths=[6, 6]
                )
            ),
            ui.div(
                ui.layout_columns(
                    ui.div(
                        ui.tags.div("SYNCED", class_="text-muted small mb-1"),
                        ui.tags.strong(str(sync_status.get("synced", 0)), style="font-size: 1.5em;"),
                        class_="text-center"
                    ),
                    ui.div(
                        ui.tags.div("PENDING", class_="text-muted small mb-1"),
                        ui.tags.strong(str(sync_status.get("pending", 0)), style="font-size: 1.5em;"),
                        class_="text-center"
                    ),
                    ui.div(
                        ui.tags.div("LAST SYNC", class_="text-muted small mb-1"),
                        ui.tags.span(last_sync_str),
                        class_="text-center"
                    ),
                    ui.div(
                        ui.input_action_button(
                            "sync_to_s3",
                            ui.HTML('<i class="bi bi-cloud-arrow-up me-2"></i>Sync to S3'),
                            class_="btn btn-outline-primary"
                        ),
                        class_="text-center"
                    ),
                    col_widths=[3, 3, 3, 3]
                ),
                class_="p-3"
            ),
            class_="mt-4"
        )

    @output
    @render.ui
    def orchestrator_status_badge():
        """Show orchestrator connection status"""
        orch = orchestrator_instance.get()
        if not orch:
            return ui.HTML('<span class="badge bg-secondary">Not Initialized</span>')

        if orch.is_connected():
            status = orch.get_connection_status()
            mode = status.get("monitoring", {}).get("mode", "idle")
            if mode == "websocket" or mode == "hybrid":
                return ui.HTML('<span class="badge bg-success"><i class="bi bi-broadcast me-1"></i>Live (WebSocket)</span>')
            elif mode == "polling":
                return ui.HTML('<span class="badge bg-info"><i class="bi bi-arrow-repeat me-1"></i>Polling</span>')
            else:
                return ui.HTML('<span class="badge bg-success">Connected</span>')
        else:
            return ui.HTML('<span class="badge bg-warning">Disconnected</span>')

    # Handle S3 sync button click
    @reactive.effect
    @reactive.event(input.sync_to_s3)
    async def handle_s3_sync():
        """Sync decoded transactions to S3"""
        orch = orchestrator_instance.get()
        if not orch:
            logger.error("Cannot sync: orchestrator not initialized")
            return

        try:
            logger.info("Starting S3 sync...")
            result = await orch.sync_decoded_to_s3()

            if result:
                s3_sync_status.set({
                    "synced": result.synced,
                    "pending": 0,
                    "last_sync": datetime.now(timezone.utc)
                })
                logger.info(f"S3 sync complete: {result.synced} synced, {result.skipped} skipped")
            else:
                logger.warning("S3 sync returned no result")

        except Exception as e:
            logger.error(f"S3 sync failed: {e}")
            error_message.set(f"S3 sync failed: {str(e)}")

    # Initialize orchestrator when using new architecture
    @reactive.effect
    @reactive.event(connection_initiated, selected_fund)
    def initialize_orchestrator():
        """Initialize BlockchainOrchestrator when USE_ORCHESTRATOR=true"""
        if not USE_ORCHESTRATOR:
            return

        if not connection_initiated.get():
            return

        # Get wallets to monitor
        wallets = get_monitored_wallets()
        if not wallets:
            logger.warning("No wallets to monitor for orchestrator")
            return

        current_fund = selected_fund()

        # Check if we need to re-initialize
        existing = orchestrator_instance.get()
        if existing and hasattr(existing, 'fund_id') and existing.fund_id == current_fund:
            logger.debug("Orchestrator already initialized for this fund")
            return

        try:
            orch = get_orchestrator(fund_wallets=wallets, fund_id=current_fund)
            if orch:
                orchestrator_instance.set(orch)
                logger.info(f"Orchestrator initialized with {len(wallets)} wallets")

                # Update pending sync count
                registry = decoder_registry.get()
                if registry and hasattr(registry, 'decoded_cache'):
                    pending = len(registry.decoded_cache)
                    s3_sync_status.set({
                        "synced": 0,
                        "pending": pending,
                        "last_sync": None
                    })
        except Exception as e:
            logger.error(f"Failed to initialize orchestrator: {e}")