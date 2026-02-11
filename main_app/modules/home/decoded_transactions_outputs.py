"""
Decoded Transactions Server Outputs

Registers server-side outputs for the Decoded Transactions tab.
Handles filtering, card rendering, stats, and GL posting actions.
"""

from shiny import reactive, render, ui
from typing import Dict, List, Any, Optional
import logging
import pandas as pd

from .decoded_transactions_ui import (
    transaction_card_ui,
    empty_state_ui,
    error_card_ui,
)

logger = logging.getLogger(__name__)


# =========================================================================
# HELPER FUNCTIONS
# =========================================================================

def _generate_gl_row_key(row: Dict) -> str:
    """
    Generate deterministic unique key for GL row deduplication.

    Key components: hash + account + type + debit + credit
    This ensures the same journal entry line can't be posted twice.
    """
    return (
        f"{row.get('hash', '')}:"
        f"{row.get('account_name', '')}:"
        f"{row.get('transaction_type', '')}:"
        f"{row.get('debit_crypto', 0)}:"
        f"{row.get('credit_crypto', 0)}"
    )


def _get_unified_registry(decoder_registry_value, decoded_tx_cache_value):
    """
    Get registry or adapter, ensuring consistent interface.

    Returns DecoderRegistry if available, otherwise LegacyRegistryAdapter
    wrapping the legacy cache, or None if nothing available.
    """
    registry = decoder_registry_value.get()
    if registry and registry.decoded_cache:
        return registry

    if decoded_tx_cache_value:
        try:
            from ...services.decoders.adapter import LegacyRegistryAdapter
            legacy_cache = decoded_tx_cache_value.get()
            if legacy_cache:
                return LegacyRegistryAdapter(decoded_tx_cache_value.get)
        except ImportError:
            logger.debug("LegacyRegistryAdapter not available")

    return None


def register_decoded_transactions_outputs(output, input, session, decoder_registry_value, decoded_tx_cache_value=None, refresh_trigger_value=None):
    """
    Register server outputs for the Decoded Transactions tab.

    Args:
        output: Shiny output object
        input: Shiny input object
        session: Shiny session object
        decoder_registry_value: Reactive value containing DecoderRegistry instance
        decoded_tx_cache_value: Reactive value containing legacy decoded transactions cache (fallback)
        refresh_trigger_value: Reactive value that triggers UI refresh when incremented
    """

    # =========================================================================
    # REACTIVE CALCULATIONS
    # =========================================================================

    @reactive.calc
    def filtered_decoded_transactions() -> List[Dict[str, Any]]:
        """Get filtered decoded transactions based on user selections"""
        transactions = []

        # Read refresh trigger to establish dependency (forces re-calc when trigger changes)
        if refresh_trigger_value:
            _ = refresh_trigger_value.get()

        # Try to get from registry first
        registry = decoder_registry_value.get()
        if registry and registry.decoded_cache:
            logger.info(f"[DEBUG] Registry has {len(registry.decoded_cache)} cached transactions")
            transactions = [tx.to_dict() for tx in registry.decoded_cache.values()]
            if transactions:
                # Debug first transaction
                first_tx = transactions[0]
                logger.info(f"[DEBUG] First tx keys: {list(first_tx.keys())}")
                logger.info(f"[DEBUG] First tx value: {first_tx.get('value')}, eth_price: {first_tx.get('eth_price')}")
                logger.info(f"[DEBUG] First tx platform: {first_tx.get('platform')}, category: {first_tx.get('category')}")
                logger.info(f"[DEBUG] First tx journal_entries count: {len(first_tx.get('journal_entries', []))}")
                if first_tx.get('journal_entries'):
                    je = first_tx['journal_entries'][0]
                    logger.info(f"[DEBUG] First JE: {je}")
        else:
            logger.info(f"[DEBUG] Registry not available or empty cache")

        # Fallback to legacy cache if no registry or empty
        if not transactions and decoded_tx_cache_value:
            legacy_cache = decoded_tx_cache_value.get()
            if legacy_cache:
                logger.info(f"[DEBUG] Using legacy cache with {len(legacy_cache)} transactions")
                # Legacy cache is already dict format
                transactions = [
                    tx if isinstance(tx, dict) else tx.to_dict()
                    for tx in legacy_cache.values()
                    if tx.get('status') != 'error'  # Filter out errors
                ]

        if not transactions:
            logger.info("[DEBUG] No transactions found in either cache")
            return []

        # Apply filters - wrap in try/except since inputs may not exist yet
        try:
            platform_filter = input.decoded_platform_filter()
        except Exception:
            platform_filter = "all"

        try:
            category_filter = input.decoded_category_filter()
        except Exception:
            category_filter = "all"

        try:
            status_filter = input.decoded_status_filter()
        except Exception:
            status_filter = "all"

        logger.debug(f"[FILTER] platform={platform_filter}, category={category_filter}, status={status_filter}")
        logger.debug(f"[FILTER] Before filtering: {len(transactions)} transactions")

        # Platform filter (case-insensitive)
        if platform_filter and platform_filter != "all":
            before_count = len(transactions)
            transactions = [
                tx for tx in transactions
                if tx.get('platform', '').lower() == platform_filter.lower()
            ]
            logger.debug(f"[FILTER] Platform filter '{platform_filter}': {before_count} -> {len(transactions)}")

        # Category filter
        if category_filter and category_filter != "all":
            before_count = len(transactions)
            transactions = [
                tx for tx in transactions
                if tx.get('category') == category_filter
            ]
            logger.debug(f"[FILTER] Category filter '{category_filter}': {before_count} -> {len(transactions)}")

        # Status filter - use transaction-level posting_status
        if status_filter and status_filter != "all":
            before_count = len(transactions)
            transactions = [
                tx for tx in transactions
                if tx.get('posting_status') == status_filter
            ]
            logger.debug(f"[FILTER] Status filter '{status_filter}': {before_count} -> {len(transactions)}")

        # Sort by timestamp descending
        transactions.sort(key=lambda x: x.get('timestamp', ''), reverse=True)

        return transactions

    @reactive.calc
    def decoded_stats() -> Dict[str, Any]:
        """Calculate statistics for decoded transactions"""
        # Try registry stats first
        registry = decoder_registry_value.get()
        if registry and registry.decoded_cache:
            return registry.stats

        # Fallback to legacy cache stats
        if decoded_tx_cache_value:
            legacy_cache = decoded_tx_cache_value.get()
            if legacy_cache:
                total = len(legacy_cache)
                success_count = sum(1 for tx in legacy_cache.values() if tx.get('status') != 'error')
                platforms = {}
                for tx in legacy_cache.values():
                    platform = tx.get('platform', tx.get('decoder_type', 'unknown'))
                    platforms[platform] = platforms.get(platform, 0) + 1

                return {
                    "total_decoded": total,
                    "success_count": success_count,
                    "auto_post_ready": 0,  # Legacy decoder doesn't track this
                    "review_queue": success_count,  # All go to review queue
                    "platforms": platforms,
                }

        return {
            "total_decoded": 0,
            "auto_post_ready": 0,
            "review_queue": 0,
            "platforms": {},
        }

    # =========================================================================
    # STAT OUTPUTS
    # =========================================================================

    @output
    @render.text
    def decoded_total_count():
        """Total decoded transactions count"""
        stats = decoded_stats()
        return str(stats.get('total_decoded', 0))

    @output
    @render.text
    def decoded_auto_post_count():
        """Auto-post ready count"""
        stats = decoded_stats()
        return str(stats.get('auto_post_ready', 0))

    @output
    @render.text
    def decoded_review_queue_count():
        """Review queue count"""
        stats = decoded_stats()
        return str(stats.get('review_queue', 0))

    @output
    @render.text
    def decoded_platforms_count():
        """Number of platforms with decoded transactions"""
        stats = decoded_stats()
        platforms = stats.get('platforms', {})
        return str(len([k for k, v in platforms.items() if v > 0]))

    @output
    @render.text
    def decoded_spam_count():
        """Spam filtered count"""
        stats = decoded_stats()
        return str(stats.get('spam_filtered', 0))

    @output
    @render.text
    def decoded_posted_count():
        """Already posted count"""
        stats = decoded_stats()
        return str(stats.get('posted', 0))

    # =========================================================================
    # CARD RENDERING
    # =========================================================================

    @output
    @render.ui
    def decoded_transaction_cards():
        """Render decoded transaction cards"""
        transactions = filtered_decoded_transactions()

        if not transactions:
            return empty_state_ui()

        cards = []
        for tx in transactions[:50]:  # Limit to 50 for performance
            if tx.get('status') == 'error':
                cards.append(error_card_ui(tx.get('tx_hash', ''), tx.get('error', 'Unknown error')))
            else:
                cards.append(transaction_card_ui(tx))

        return ui.div(*cards)

    # =========================================================================
    # MODAL CONTENT
    # =========================================================================

    # Track selected transaction hash for modal
    selected_tx_hash = reactive.value("")

    # Cache wallet names for friendly display
    _wallet_names_cache = {}

    def _load_wallet_names():
        """Load wallet name mappings from S3"""
        nonlocal _wallet_names_cache
        if _wallet_names_cache:
            return _wallet_names_cache
        try:
            from ...s3_utils import load_WALLET_file
            wallet_df = load_WALLET_file()
            if wallet_df is not None and not wallet_df.empty:
                for _, row in wallet_df.iterrows():
                    addr = str(row.get('wallet_address', '')).strip().lower()
                    name = str(row.get('friendly_name', '')).strip()
                    if addr and name:
                        _wallet_names_cache[addr] = name
        except Exception as e:
            logger.debug(f"Could not load wallet names: {e}")
        return _wallet_names_cache

    def _get_friendly_name(address: str) -> str:
        """Get friendly name for an address, fallback to shortened address"""
        if not address:
            return ""
        wallet_names = _load_wallet_names()
        friendly = wallet_names.get(address.lower())
        if friendly:
            return friendly
        # Fallback to shortened address
        return f"{address[:8]}...{address[-6:]}" if len(address) > 16 else address

    @output
    @render.ui
    def decoder_modal_content():
        """Render full details for expanded transaction in modal"""
        tx_hash = selected_tx_hash.get()
        if not tx_hash:
            return ui.div("No transaction selected", class_="text-muted")

        # Find the transaction in the cache (keep both dict and raw object)
        registry = decoder_registry_value.get()
        decoded = None
        decoded_tx_obj = None  # Raw DecodedTransaction for formatted_journal_df access

        if registry and registry.decoded_cache:
            for cached_tx in registry.decoded_cache.values():
                if cached_tx.tx_hash == tx_hash:
                    decoded = cached_tx.to_dict()
                    decoded_tx_obj = cached_tx
                    break

        # Fallback to legacy cache
        if not decoded and decoded_tx_cache_value:
            legacy_cache = decoded_tx_cache_value.get()
            if legacy_cache and tx_hash in legacy_cache:
                cached = legacy_cache[tx_hash]
                decoded = cached if isinstance(cached, dict) else cached.to_dict()

        if not decoded:
            return ui.div(f"Transaction {tx_hash[:16]}... not found in cache", class_="text-danger")

        # Build detailed modal content
        journal_entries = decoded.get('journal_entries', [])
        events = decoded.get('events', [])

        # Get friendly names for addresses
        from_address = decoded.get('from_address', '')
        to_address = decoded.get('to_address', '')
        from_friendly = _get_friendly_name(from_address)
        to_friendly = _get_friendly_name(to_address)

        # Etherscan URL - ensure 0x prefix
        tx_hash = decoded.get('tx_hash', '')
        if tx_hash and not tx_hash.startswith('0x'):
            tx_hash = f"0x{tx_hash}"
        etherscan_url = f"https://etherscan.io/tx/{tx_hash}"

        return ui.div(
            # Transaction Summary
            ui.card(
                ui.card_header(
                    ui.div(
                        ui.span("Transaction Summary", class_="fw-semibold"),
                        ui.div(
                            ui.a(
                                ui.span(class_="bi bi-box-arrow-up-right me-1"),
                                "View on Etherscan",
                                href=etherscan_url,
                                target="_blank",
                                class_="btn btn-sm btn-outline-primary"
                            ),
                            ui.input_action_button(
                                "ask_claude_code_btn",
                                ui.span(ui.HTML('<i class="bi bi-terminal"></i>'), " Ask Claude Code"),
                                class_="btn btn-outline-info btn-sm ms-2"
                            ),
                            class_="ms-auto"
                        ),
                        class_="d-flex align-items-center w-100"
                    )
                ),
                ui.layout_columns(
                    ui.div(
                        ui.strong("Platform: "),
                        ui.span(decoded.get('platform', 'unknown').upper(), class_="badge bg-primary"),
                    ),
                    ui.div(
                        ui.strong("Category: "),
                        ui.span(decoded.get('category', 'UNKNOWN')),
                    ),
                    ui.div(
                        ui.strong("Status: "),
                        ui.span(decoded.get('status', 'unknown')),
                    ),
                    col_widths=[4, 4, 4]
                ),
                ui.hr(),
                ui.layout_columns(
                    ui.div(
                        ui.strong("Block: "),
                        ui.span(str(decoded.get('block', ''))),
                    ),
                    ui.div(
                        ui.strong("Timestamp: "),
                        ui.span(str(decoded.get('timestamp', ''))[:19]),
                    ),
                    ui.div(
                        ui.strong("ETH Price: "),
                        ui.span(f"${decoded.get('eth_price', 0):,.2f}"),
                    ),
                    col_widths=[4, 4, 4]
                ),
                ui.hr(),
                ui.div(
                    ui.strong("TX Hash: "),
                    ui.code(decoded.get('tx_hash', ''), class_="user-select-all small"),
                ),
                ui.div(
                    ui.strong("Function: "),
                    ui.code(decoded.get('function_name', 'unknown')),
                ),
                ui.div(
                    ui.strong("From: "),
                    ui.span(from_friendly, class_="fw-semibold text-primary me-2") if from_friendly != from_address else None,
                    ui.code(from_address, class_="small"),
                ),
                ui.div(
                    ui.strong("To: "),
                    ui.span(to_friendly, class_="fw-semibold text-primary me-2") if to_friendly != to_address else None,
                    ui.code(to_address, class_="small"),
                ),
                ui.div(
                    ui.strong("Gas Fee: "),
                    ui.span(f"{decoded.get('gas_fee', 0):.6f} ETH"),
                ),
                class_="mb-3"
            ),

            # Journal Entries - use formatted_journal_df (37-column) if available
            _render_formatted_journal_table(decoded_tx_obj)
            if decoded_tx_obj and decoded_tx_obj.formatted_journal_df is not None and not decoded_tx_obj.formatted_journal_df.empty
            else ui.card(
                ui.card_header(f"Journal Entries ({len(journal_entries)})"),
                *[_render_journal_entry_ui(je, i) for i, je in enumerate(journal_entries)] if journal_entries else [
                    ui.div("No journal entries generated", class_="text-muted p-3")
                ],
                class_="mb-3"
            ),

            # Decoded Events
            ui.card(
                ui.card_header(f"Decoded Events ({len(events)})"),
                *[_render_event_ui(evt, i) for i, evt in enumerate(events)] if events else [
                    ui.div("No events decoded", class_="text-muted p-3")
                ],
            ),
        )

    def _render_formatted_journal_table(decoded_tx_obj):
        """Render the 37-column formatted journal DataFrame as a rich table matching single TX tab."""
        df = decoded_tx_obj.formatted_journal_df
        display_cols = [
            ('account_name', 'Account'),
            ('debit_crypto', 'Debit'),
            ('credit_crypto', 'Credit'),
            ('cryptocurrency', 'Currency'),
            ('debit_USD', 'Debit USD'),
            ('credit_USD', 'Credit USD'),
            ('event', 'Event'),
            ('loan_id', 'Loan ID'),
            ('fund_role', 'Role'),
            ('lender', 'Lender'),
            ('borrower', 'Borrower'),
        ]

        header_cells = [ui.tags.th(label, class_="small") for _, label in display_cols]

        rows = []
        for _, row in df.iterrows():
            cells = []
            for col, _ in display_cols:
                val = row.get(col, '')
                if col in ('debit_crypto', 'credit_crypto'):
                    val_float = float(val) if val and str(val) != '' else 0
                    cells.append(ui.tags.td(
                        f"{val_float:.6f}" if val_float > 0 else '',
                        class_="text-end small"
                    ))
                elif col in ('debit_USD', 'credit_USD'):
                    val_float = float(val) if val and str(val) != '' else 0
                    cells.append(ui.tags.td(
                        f"${val_float:,.2f}" if val_float > 0 else '',
                        class_="text-end small"
                    ))
                elif col in ('lender', 'borrower'):
                    addr = str(val) if val else ''
                    display = _get_friendly_name(addr) if addr else ''
                    cells.append(ui.tags.td(display, class_="small"))
                else:
                    cells.append(ui.tags.td(str(val) if val and str(val) != 'nan' else '', class_="small"))
            rows.append(ui.tags.tr(*cells))

        return ui.card(
            ui.card_header(f"Journal Entries ({len(df)} rows) - 37-Column Format"),
            ui.card_body(
                ui.div(
                    ui.tags.table(
                        ui.tags.thead(ui.tags.tr(*header_cells)),
                        ui.tags.tbody(*rows),
                        class_="table table-sm table-striped"
                    ),
                    style="overflow-x: auto; max-height: 400px; overflow-y: auto;"
                )
            ),
            class_="mb-3"
        )

    def _render_journal_entry_ui(je: Dict, index: int):
        """Helper to render a single journal entry"""
        entries = je.get('entries', [])
        return ui.div(
            ui.div(
                ui.strong(f"Entry {index + 1}: "),
                ui.span(je.get('description', 'N/A')[:100]),
                ui.span(
                    "Balanced" if je.get('is_balanced') else "IMBALANCED",
                    class_=f"badge ms-2 bg-{'success' if je.get('is_balanced') else 'warning'}"
                ),
            ),
            ui.div(
                ui.tags.table(
                    ui.tags.thead(
                        ui.tags.tr(
                            ui.tags.th("Type"),
                            ui.tags.th("Account"),
                            ui.tags.th("Amount"),
                            ui.tags.th("Asset"),
                        )
                    ),
                    ui.tags.tbody(
                        *[ui.tags.tr(
                            ui.tags.td(
                                ui.span(e.get('type', ''), class_=f"badge bg-{'success' if e.get('type') == 'DEBIT' else 'primary'}")
                            ),
                            ui.tags.td(e.get('account', '')),
                            ui.tags.td(f"{float(e.get('amount', 0)):.6f}"),
                            ui.tags.td(e.get('asset', 'ETH')),
                        ) for e in entries]
                    ),
                    class_="table table-sm table-striped"
                ) if entries else ui.div("No line items", class_="text-muted"),
            ),
            class_="border-bottom pb-2 mb-2"
        )

    def _render_event_ui(evt: Dict, index: int):
        """Helper to render a single decoded event"""
        args = evt.get('args', {})
        return ui.div(
            ui.div(
                ui.strong(f"Event {index + 1}: "),
                ui.code(evt.get('name', 'Unknown')),
                ui.span(f" @ log index {evt.get('log_index', -1)}", class_="text-muted small"),
            ),
            ui.div(
                *[ui.div(
                    ui.span(f"{k}: ", class_="fw-semibold"),
                    ui.code(str(v)[:64] + ('...' if len(str(v)) > 64 else '')),
                    class_="small"
                ) for k, v in args.items()] if args else [ui.div("No args", class_="text-muted")],
                class_="ps-3 border-start"
            ),
            class_="border-bottom pb-2 mb-2"
        )

    # =========================================================================
    # EVENT HANDLERS
    # =========================================================================

    @reactive.effect
    @reactive.event(input.refresh_decoded)
    def refresh_decoded_transactions():
        """Refresh button handler"""
        # Trigger re-render by invalidating the registry
        registry = decoder_registry_value.get()
        if registry:
            # Force reactive update
            decoder_registry_value.set(registry)
        ui.notification_show("Decoded transactions refreshed", type="message", duration=2)

    @reactive.effect
    @reactive.event(input.clear_decoded_cache)
    def clear_decoded_cache():
        """Clear cache button handler"""
        registry = decoder_registry_value.get()
        if registry:
            registry.clear_cache()
            decoder_registry_value.set(registry)
            ui.notification_show("Decoded transactions cache cleared", type="message", duration=2)

    @reactive.effect
    @reactive.event(input.expand_decoded_tx)
    def handle_expand_decoded_tx():
        """Handle clicking expand on a decoded transaction"""
        tx_hash = input.expand_decoded_tx()
        if tx_hash:
            logger.info(f"Expand decoded tx: {tx_hash}")
            # Set the selected transaction hash to trigger modal content render
            selected_tx_hash.set(tx_hash)
            # Show the modal
            ui.modal_show(
                ui.modal(
                    ui.output_ui("decoder_modal_content"),
                    title=f"Transaction: {tx_hash[:16]}...",
                    size="xl",
                    easy_close=True,
                )
            )

    @reactive.effect
    @reactive.event(input.ask_claude_code_btn)
    def handle_ask_claude_code():
        """Launch Claude Code with transaction context for AI-assisted analysis"""
        tx_hash = selected_tx_hash.get()
        if not tx_hash:
            ui.notification_show("No transaction selected", type="warning")
            return

        # Get transaction from registry
        registry = decoder_registry_value.get()
        tx_data = None

        if registry and registry.decoded_cache:
            for cached_tx in registry.decoded_cache.values():
                if cached_tx.tx_hash == tx_hash:
                    tx_data = cached_tx.to_dict()
                    break

        # Fallback to legacy cache
        if not tx_data and decoded_tx_cache_value:
            legacy_cache = decoded_tx_cache_value.get()
            if legacy_cache and tx_hash in legacy_cache:
                cached = legacy_cache[tx_hash]
                tx_data = cached if isinstance(cached, dict) else cached.to_dict()

        if not tx_data:
            ui.notification_show("Transaction not found in cache", type="error")
            return

        # Launch Claude Code
        try:
            from ...services.claude_code_launcher import launch_claude_code_analysis

            success = launch_claude_code_analysis(tx_data, tx_hash)

            if success:
                ui.notification_show(
                    "Claude Code launched! Check your terminal window.",
                    type="success",
                    duration=5
                )
            else:
                ui.notification_show(
                    "Failed to launch Claude Code. Is it installed?",
                    type="error"
                )
        except Exception as e:
            logger.error(f"Error launching Claude Code: {e}")
            ui.notification_show(f"Error: {str(e)}", type="error")

    @reactive.effect
    @reactive.event(input.post_all_auto)
    def post_all_auto_ready():
        """
        Post all auto-ready transactions to General Ledger (idempotent).

        Features:
        - Generates deterministic row keys for deduplication
        - Skips entries already in GL
        - Uses optimistic status update with rollback on failure
        - Generates daily interest accruals for loan repayments
        """
        registry = decoder_registry_value.get()
        if not registry:
            ui.notification_show("No decoder registry available", type="error")
            return

        auto_ready = registry.get_auto_post_ready()

        if not auto_ready:
            ui.notification_show("No transactions ready for auto-posting", type="warning")
            return

        # Collect entries and mark as POSTING (optimistic update)
        all_entries = []
        accrual_entries = []  # Separate list for interest accruals
        original_statuses = {}  # For rollback on failure

        from ...services.decoders import PostingStatus
        from ...services.decoders.base import generate_daily_interest_accruals
        from decimal import Decimal

        # Load wallet mapping for fund_id lookup
        try:
            from ...s3_utils import load_WALLET_file, load_COA_file
            wallet_df = load_WALLET_file()
            wallet_to_fund_map = {}
            if wallet_df is not None and not wallet_df.empty:
                for _, row in wallet_df.iterrows():
                    addr = str(row.get('wallet_address', '')).strip().lower()
                    fund = str(row.get('fund_id', '')).strip()
                    if addr and fund:
                        wallet_to_fund_map[addr] = fund
            logger.info(f"Loaded {len(wallet_to_fund_map)} wallet-to-fund mappings")
        except Exception as e:
            logger.warning(f"Could not load wallet mappings: {e}")
            wallet_to_fund_map = {}

        # Load COA for GL account number lookup
        try:
            coa_df = load_COA_file()
            coa_map = {}
            if coa_df is not None and not coa_df.empty:
                for _, row in coa_df.iterrows():
                    try:
                        acct_num = int(row.get('GL_Acct_Number', 0))
                        acct_name = str(row.get('GL_Acct_Name', '')).strip()
                        if acct_num and acct_name:
                            # Map by GL_Acct_Name for lookup
                            coa_map[acct_name] = (acct_num, acct_name)
                            # Also map by lowercase for flexible matching
                            coa_map[acct_name.lower()] = (acct_num, acct_name)
                    except:
                        continue
            logger.info(f"Loaded {len(coa_map)} COA account mappings")
        except Exception as e:
            logger.warning(f"Could not load COA: {e}")
            coa_map = {}

        for tx in auto_ready:
            # Save original statuses for rollback
            for entry in tx.journal_entries:
                original_statuses[entry.entry_id] = entry.posting_status
                entry.posting_status = PostingStatus.POSTED

            # Prefer formatted_journal_df (37-column) over JournalEntry.to_gl_records()
            if tx.formatted_journal_df is not None and not tx.formatted_journal_df.empty:
                for _, row in tx.formatted_journal_df.iterrows():
                    row_dict = row.to_dict()
                    row_dict['row_key'] = _generate_gl_row_key(row_dict)
                    all_entries.append(row_dict)
                logger.info(f"Using formatted_journal_df ({len(tx.formatted_journal_df)} rows) for tx {tx.tx_hash[:16]}")
            else:
                # Fallback: use JournalEntry.to_gl_records() for non-Gondi platforms
                for entry in tx.journal_entries:
                    all_entries.extend(entry.to_gl_records(
                        wallet_to_fund_map=wallet_to_fund_map,
                        coa_map=coa_map
                    ))

            # Generate interest accruals for loan repayments
            if tx.category.value == 'LOAN_REPAYMENT' and tx.positions:
                for lien_id, position in tx.positions.items():
                    try:
                        # Extract loan details from position
                        principal = Decimal(str(position.principal)) if hasattr(position, 'principal') else Decimal(0)
                        rate_bips = int(position.rate) if hasattr(position, 'rate') else 0
                        start_time = position.start_time if hasattr(position, 'start_time') else None

                        if principal > 0 and rate_bips > 0 and start_time:
                            # Get timestamps
                            start_timestamp = int(start_time.timestamp()) if hasattr(start_time, 'timestamp') else int(start_time)
                            end_timestamp = int(tx.timestamp.timestamp()) if hasattr(tx.timestamp, 'timestamp') else int(tx.timestamp)

                            # Determine if fund is lender or borrower
                            is_lender = False
                            fund_wallets = getattr(registry, 'fund_wallets', []) or []
                            fund_wallets_lower = [w.lower() for w in fund_wallets]

                            # Check wallet roles
                            for addr, role in tx.wallet_roles.items():
                                if addr.lower() in fund_wallets_lower and role == 'lender':
                                    is_lender = True
                                    break

                            # Fallback: check position lender/borrower
                            if not is_lender and hasattr(position, 'lender') and fund_wallets_lower:
                                is_lender = position.lender.lower() in fund_wallets_lower

                            # Get platform string for account selection
                            platform_str = tx.platform.value if hasattr(tx.platform, 'value') else str(tx.platform)

                            # Get wallet address and look up fund_id from mapping
                            wallet_addr = tx.journal_entries[0].wallet_address if tx.journal_entries else ''
                            accrual_fund_id = tx.journal_entries[0].fund_id if tx.journal_entries else ''
                            # If no fund_id set, look up from wallet mapping
                            if not accrual_fund_id and wallet_addr and wallet_to_fund_map:
                                accrual_fund_id = wallet_to_fund_map.get(wallet_addr.lower(), '')

                            # Common metadata for accruals - matches parquet schema
                            common_metadata = {
                                'tx_hash': tx.tx_hash,
                                'fund_id': accrual_fund_id,
                                'wallet_id': wallet_addr,
                                'cryptocurrency': 'ETH',
                                'eth_usd_price': float(tx.eth_price),
                                # Loan details
                                'loan_id': str(lien_id) if lien_id else None,
                                'lender': getattr(position, 'lender', None),
                                'borrower': getattr(position, 'borrower', None),
                                'principal_crypto': float(principal),
                                'principal_USD': float(principal * tx.eth_price),
                                'annual_interest_rate': float(Decimal(rate_bips) / Decimal(10000)),
                                'contract_address': getattr(position, 'collection', None),
                                'token_id': getattr(position, 'token_id', None),
                            }

                            # Generate daily accruals
                            accruals = generate_daily_interest_accruals(
                                start_timestamp=start_timestamp,
                                end_timestamp=end_timestamp,
                                principal=principal,
                                rate_bips=rate_bips,
                                is_lender=is_lender,
                                common_metadata=common_metadata,
                                platform=platform_str,
                            )

                            if accruals:
                                accrual_entries.extend(accruals)
                                logger.info(f"Generated {len(accruals)} interest accrual entries for loan {lien_id}")

                    except Exception as accrual_err:
                        logger.warning(f"Could not generate accruals for loan {lien_id}: {accrual_err}")
                        # Don't fail the whole posting for accrual errors

        if not all_entries and not accrual_entries:
            ui.notification_show("No journal entries to post", type="warning")
            return

        # Combine regular entries with accrual entries
        combined_entries = all_entries + accrual_entries
        accrual_count = len(accrual_entries)

        try:
            from ...s3_utils import (
                save_GL_file, load_GL_file,
                save_GL2_file, load_GL2_file, clear_GL2_cache
            )

            # Create DataFrame with dedup keys
            df_new = pd.DataFrame(combined_entries)
            # Use existing row_key if present (from interest accruals), otherwise generate one
            df_new['_row_key'] = df_new.apply(
                lambda row: row.get('row_key') if pd.notna(row.get('row_key')) and row.get('row_key')
                           else _generate_gl_row_key(row.to_dict()),
                axis=1
            )

            # Get fresh GL data
            load_GL_file.cache_clear()
            existing_gl = load_GL_file()

            # Add dedup keys to existing GL
            existing_gl['_row_key'] = existing_gl.apply(
                lambda row: _generate_gl_row_key(row.to_dict()), axis=1
            )
            existing_keys = set(existing_gl['_row_key'].tolist())

            # Filter out duplicates
            df_to_add = df_new[~df_new['_row_key'].isin(existing_keys)]
            duplicates_count = len(df_new) - len(df_to_add)

            if df_to_add.empty:
                ui.notification_show(
                    f"All {len(all_entries)} entries already posted (skipped)",
                    type="info"
                )
                return

            # Remove helper column before save
            df_to_add = df_to_add.drop(columns=['_row_key'])
            existing_gl = existing_gl.drop(columns=['_row_key'])

            # Combine and save to existing GL
            combined_gl = pd.concat([existing_gl, df_to_add], ignore_index=True)
            save_GL_file(combined_gl)

            # Also save to GL2 (37-column format, same as CSV export)
            try:
                from ...s3_utils import get_gl2_schema_columns

                # df_to_add is already in 37-column format — just add row_key and pass through
                gl2_new_df = df_to_add.copy()
                if 'row_key' not in gl2_new_df.columns:
                    gl2_new_df['row_key'] = gl2_new_df.apply(
                        lambda r: _generate_gl_row_key(r.to_dict()), axis=1
                    )

                # Ensure all GL2 schema columns exist
                for col in get_gl2_schema_columns():
                    if col not in gl2_new_df.columns:
                        gl2_new_df[col] = ''

                clear_GL2_cache()
                existing_gl2 = load_GL2_file()

                # Deduplicate by row_key
                existing_gl2_keys = set(existing_gl2['row_key'].tolist()) if 'row_key' in existing_gl2.columns and not existing_gl2.empty else set()
                gl2_to_add = gl2_new_df[~gl2_new_df['row_key'].isin(existing_gl2_keys)]

                if not gl2_to_add.empty:
                    combined_gl2 = pd.concat([existing_gl2, gl2_to_add], ignore_index=True)
                    save_GL2_file(combined_gl2)
                    logger.info(f"Also posted {len(gl2_to_add)} entries to GL2")
            except Exception as gl2_err:
                logger.warning(f"Could not post to GL2: {gl2_err}")
                # Don't fail the whole operation if GL2 fails

            # Clear cache so other parts of app see updated data
            load_GL_file.cache_clear()

            # Force registry update to reflect new status
            decoder_registry_value.set(registry)

            # Success message with duplicate and accrual info
            msg = f"Posted {len(df_to_add)} new entries to GL"
            if accrual_count > 0:
                msg += f" (including {accrual_count} interest accruals)"
            if duplicates_count > 0:
                msg += f" ({duplicates_count} duplicates skipped)"
            ui.notification_show(msg, type="success", duration=5)

            logger.info(f"GL posting complete: {len(df_to_add)} new, {duplicates_count} skipped, {accrual_count} accruals")

        except Exception as e:
            # Rollback posting status on failure
            for tx in auto_ready:
                for entry in tx.journal_entries:
                    if entry.entry_id in original_statuses:
                        entry.posting_status = original_statuses[entry.entry_id]

            logger.error(f"Failed to post entries to GL: {e}")
            import traceback
            traceback.print_exc()
            ui.notification_show(f"Posting failed: {str(e)}", type="error")

    @render.download(filename="journal_entries_export.csv")
    def download_decoded_csv():
        """Download journal entries from decoded transactions as CSV (37-column format)"""
        from ...services.decoders.gondi_decoder import CSV_COLUMNS

        transactions = filtered_decoded_transactions()

        if not transactions:
            yield ','.join(CSV_COLUMNS) + '\n'
            return

        try:
            # Access raw DecodedTransaction objects from registry for formatted_journal_df
            registry = decoder_registry_value.get()
            all_formatted_dfs = []
            fallback_rows = []

            for tx_dict in transactions:
                tx_hash = tx_dict.get('tx_hash', '')

                # Try to get formatted_journal_df from the cached DecodedTransaction object
                decoded_tx = None
                if registry and registry.decoded_cache:
                    decoded_tx = registry.decoded_cache.get(tx_hash)

                if decoded_tx and decoded_tx.formatted_journal_df is not None and not decoded_tx.formatted_journal_df.empty:
                    all_formatted_dfs.append(decoded_tx.formatted_journal_df)
                else:
                    # Fallback for non-Gondi or older transactions without formatted_journal_df
                    _export_legacy_journal_entries(tx_dict, fallback_rows)

            if all_formatted_dfs or fallback_rows:
                parts = []
                if all_formatted_dfs:
                    parts.append(pd.concat(all_formatted_dfs, ignore_index=True))
                if fallback_rows:
                    fallback_df = pd.DataFrame(fallback_rows)
                    for col in CSV_COLUMNS:
                        if col not in fallback_df.columns:
                            fallback_df[col] = ''
                    fallback_df = fallback_df[CSV_COLUMNS]
                    parts.append(fallback_df)

                combined = pd.concat(parts, ignore_index=True) if len(parts) > 1 else parts[0]
                yield combined.to_csv(index=False)
                logger.info(f"Exported {len(combined)} journal entry rows (37-column format)")
            else:
                yield ','.join(CSV_COLUMNS) + '\n'

        except Exception as e:
            logger.error(f"Failed to export CSV: {e}", exc_info=True)
            yield f"Error exporting: {str(e)}\n"

    def _export_legacy_journal_entries(tx_dict: Dict, rows: List):
        """Fallback export for transactions without formatted_journal_df (non-Gondi platforms)"""
        tx_hash = tx_dict.get('tx_hash', '')
        platform = tx_dict.get('platform', 'unknown')
        category = tx_dict.get('category', 'UNKNOWN')
        timestamp = tx_dict.get('timestamp', '')
        eth_price = float(tx_dict.get('eth_price', 0))

        stablecoins = {'USDC', 'USDT', 'DAI', 'FRAX', 'LUSD'}

        for je in tx_dict.get('journal_entries', []):
            for entry in je.get('entries', []):
                account_key = entry.get('account', '')
                amount = float(entry.get('amount', 0))
                asset = entry.get('asset', 'ETH')
                entry_type = entry.get('type', '')

                amount_usd = amount if asset.upper() in stablecoins else amount * eth_price

                rows.append({
                    'date': timestamp,
                    'transaction_type': category,
                    'platform': platform,
                    'hash': tx_hash,
                    'account_name': account_key,
                    'debit_crypto': amount if entry_type == 'DEBIT' else 0,
                    'credit_crypto': amount if entry_type == 'CREDIT' else 0,
                    'cryptocurrency': asset,
                    'eth_usd_price': eth_price,
                    'debit_USD': amount_usd if entry_type == 'DEBIT' else 0,
                    'credit_USD': amount_usd if entry_type == 'CREDIT' else 0,
                })

    @render.download(filename="raw_transactions_export.csv")
    def download_raw_transactions_csv():
        """Download raw decoded transactions as CSV (transaction-level summary)"""
        transactions = filtered_decoded_transactions()

        if not transactions:
            yield "tx_hash,platform,category,timestamp,block,from_address,to_address,value_eth,eth_price,value_usd,gas_fee,function_name,posting_status,journal_entries_count,status\n"
            return

        try:
            rows = []
            for tx in transactions:
                eth_price = float(tx.get('eth_price', 0))
                value_eth = float(tx.get('value', 0))

                # Calculate total value from journal entries if available
                je_count = len(tx.get('journal_entries', []))
                posting_status = tx.get('posting_status', 'review_queue')

                rows.append({
                    'tx_hash': tx.get('tx_hash', ''),
                    'platform': tx.get('platform', 'unknown'),
                    'category': tx.get('category', 'UNKNOWN'),
                    'timestamp': tx.get('timestamp', ''),
                    'block': tx.get('block', ''),
                    'from_address': tx.get('from_address', ''),
                    'to_address': tx.get('to_address', ''),
                    'value_eth': value_eth,
                    'eth_price': eth_price,
                    'value_usd': value_eth * eth_price,
                    'gas_fee': tx.get('gas_fee', 0),
                    'function_name': tx.get('function_name', ''),
                    'posting_status': posting_status,
                    'journal_entries_count': je_count,
                    'status': tx.get('status', ''),
                })

            df = pd.DataFrame(rows)
            yield df.to_csv(index=False)
            logger.info(f"Exported {len(rows)} raw transactions to CSV")

        except Exception as e:
            logger.error(f"Failed to export raw transactions CSV: {e}")
            import traceback
            traceback.print_exc()
            yield f"Error exporting: {str(e)}\n"

    # Return a cleanup function (optional)
    return None
