"""
Decoded Transactions Tab UI

Displays decoded blockchain transactions with compact card layout.
Features:
- Stats row (Total Decoded, Auto-Post Ready, Review Queue, Platforms)
- Filter controls
- Compact transaction cards with expand functionality
- Bulk action buttons
"""

from shiny import ui
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

# Cache for wallet friendly names
_wallet_names_cache = None

def _load_wallet_names() -> Dict[str, str]:
    """Load wallet name mappings from S3"""
    global _wallet_names_cache
    if _wallet_names_cache is not None:
        return _wallet_names_cache

    _wallet_names_cache = {}
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

def get_friendly_name(address: str) -> str:
    """Get friendly name for an address, fallback to shortened address"""
    if not address:
        return ""
    wallet_names = _load_wallet_names()
    friendly = wallet_names.get(address.lower())
    if friendly:
        return friendly
    # Fallback to shortened address
    return f"{address[:6]}...{address[-4:]}" if len(address) > 12 else address


# Platform badge colors
PLATFORM_COLORS = {
    "blur": "primary",
    "arcade": "success",
    "nftfi": "info",
    "gondi": "warning",
    "zharta": "danger",
    "generic": "secondary",
    "unknown": "dark",
}

# Category display names
CATEGORY_NAMES = {
    "LOAN_ORIGINATION": "Loan Originated",
    "LOAN_REPAYMENT": "Loan Repaid",
    "LOAN_REFINANCE": "Refinanced",
    "LOAN_AUCTION": "Auction Started",
    "COLLATERAL_SEIZURE": "Collateral Seized",
    "ETH_TRANSFER": "ETH Transfer",
    "ERC20_TRANSFER": "Token Transfer",
    "WETH_WRAP": "WETH Wrap",
    "WETH_UNWRAP": "WETH Unwrap",
    "CONTRACT_CALL": "Contract Call",
    "UNKNOWN": "Unknown",
}


def decoded_transactions_ui():
    """Decoded Transactions tab with compact card grid"""
    return ui.div(
        # Stats row
        ui.layout_columns(
            ui.value_box(
                "Total Decoded",
                ui.output_text("decoded_total_count"),
                showcase=ui.span(class_="bi bi-check-circle-fill"),
                theme="primary",
            ),
            ui.value_box(
                "Auto-Post Ready",
                ui.output_text("decoded_auto_post_count"),
                showcase=ui.span(class_="bi bi-lightning-fill"),
                theme="success",
            ),
            ui.value_box(
                "Review Queue",
                ui.output_text("decoded_review_queue_count"),
                showcase=ui.span(class_="bi bi-hourglass-split"),
                theme="warning",
            ),
            ui.value_box(
                "Platforms",
                ui.output_text("decoded_platforms_count"),
                showcase=ui.span(class_="bi bi-grid-fill"),
                theme="info",
            ),
            col_widths=[3, 3, 3, 3]
        ),

        # Filter controls
        ui.card(
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
        ),

        # Transaction cards container
        ui.div(
            ui.output_ui("decoded_transaction_cards"),
            class_="decoded-cards-container",
            style="max-height: 600px; overflow-y: auto;"
        ),

        # Bulk actions
        ui.div(
            ui.input_action_button(
                "post_all_auto",
                "Post All Auto-Ready",
                class_="btn-success me-2",
                icon=ui.span(class_="bi bi-check2-all me-1")
            ),
            ui.input_action_button(
                "export_decoded_csv",
                "Export to CSV",
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
        ),

        # Custom CSS
        ui.tags.style("""
            .decoded-card {
                border: 1px solid #dee2e6;
                border-radius: 12px;
                padding: 16px;
                margin-bottom: 10px;
                background: white;
                cursor: pointer;
                transition: all 0.2s ease-out;
                position: relative;
                overflow: hidden;
            }
            .decoded-card::before {
                content: '';
                position: absolute;
                top: 0;
                left: 0;
                right: 0;
                bottom: 0;
                background: linear-gradient(135deg, rgba(99, 102, 241, 0.03) 0%, rgba(168, 85, 247, 0.03) 100%);
                opacity: 0;
                transition: opacity 0.2s ease-out;
                pointer-events: none;
            }
            .decoded-card:hover {
                transform: translateY(-2px);
                box-shadow: 0 8px 25px rgba(0, 0, 0, 0.1);
                border-color: #a5b4fc;
            }
            .decoded-card:hover::before {
                opacity: 1;
            }
            .decoded-card:active {
                transform: translateY(0px);
                box-shadow: 0 4px 15px rgba(0, 0, 0, 0.08);
            }
            .decoded-card .click-hint {
                position: absolute;
                right: 12px;
                top: 50%;
                transform: translateY(-50%);
                opacity: 0;
                transition: all 0.2s ease-out;
                color: #6366f1;
                font-size: 1.2rem;
            }
            .decoded-card:hover .click-hint {
                opacity: 1;
                transform: translateY(-50%) translateX(-4px);
            }
            .decoded-card .platform-badge {
                font-size: 0.7rem;
                font-weight: 600;
            }
            .decoded-card .category-text {
                font-size: 0.8rem;
                color: #6c757d;
            }
            .decoded-card .amount-display {
                font-weight: 600;
                font-size: 1.1rem;
                color: #1f2937;
            }
            .decoded-card .usd-amount {
                font-size: 0.8rem;
                color: #6c757d;
            }
            .decoded-card .function-name {
                font-family: monospace;
                font-size: 0.85rem;
                color: #4b5563;
            }
            .decoded-card .address-display {
                font-family: monospace;
                font-size: 0.75rem;
                color: #9ca3af;
            }
            .decoded-card .timestamp-display {
                font-size: 0.75rem;
                color: #9ca3af;
            }
            .posting-badge {
                font-size: 0.7rem;
                padding: 3px 8px;
                border-radius: 4px;
            }
            .posting-badge.auto-post {
                background-color: #d1fae5;
                color: #065f46;
            }
            .posting-badge.review-queue {
                background-color: #fef3c7;
                color: #92400e;
            }
            .posting-badge.posted {
                background-color: #dbeafe;
                color: #1e40af;
            }
        """),

        class_="p-4"
    )


def transaction_card_ui(decoded: Dict[str, Any]) -> ui.Tag:
    """
    Render a compact card for a decoded transaction.

    Args:
        decoded: DecodedTransaction.to_dict() result

    Returns:
        Shiny UI tag for the card
    """
    tx_hash = decoded.get('tx_hash', '')
    platform = decoded.get('platform', 'unknown')
    category = decoded.get('category', 'UNKNOWN')
    tx_value = decoded.get('value', 0)  # Native ETH sent (often 0 for contract calls)
    eth_price = decoded.get('eth_price', 0)
    function_name = decoded.get('function_name', 'unknown')
    to_address = decoded.get('to_address', '')
    timestamp = decoded.get('timestamp', '')
    status = decoded.get('status', 'error')

    # Determine posting status
    journal_entries = decoded.get('journal_entries', [])
    if journal_entries:
        posting_status = journal_entries[0].get('posting_status', 'review_queue')
    else:
        posting_status = 'review_queue'

    # Calculate display value from journal entries (actual amounts, not tx.value)
    # Sum up all debit amounts from journal entries as the transaction amount
    display_value = 0.0
    if journal_entries:
        for je in journal_entries:
            entries = je.get('entries', [])
            for entry in entries:
                if entry.get('type') == 'DEBIT':
                    display_value += float(entry.get('amount', 0))

    # Fall back to tx.value if no journal entries
    if display_value == 0.0:
        display_value = float(tx_value)

    # Calculate USD value
    usd_value = display_value * float(eth_price) if eth_price else 0
    value = display_value  # Use display_value for the card

    # Platform badge color
    badge_color = PLATFORM_COLORS.get(platform, 'secondary')

    # Category display name
    category_display = CATEGORY_NAMES.get(category, category.replace('_', ' ').title())

    # Posting status badge
    status_class = posting_status.replace('_', '-')
    status_text = posting_status.replace('_', ' ').title()

    # Format timestamp
    if isinstance(timestamp, str):
        timestamp_display = timestamp[:16].replace('T', ' ')
    else:
        timestamp_display = str(timestamp)[:16]

    return ui.div(
        # Click hint arrow (appears on hover)
        ui.span(class_="bi bi-chevron-right click-hint"),

        # Header row: platform badge + category
        ui.div(
            ui.span(
                platform.upper(),
                class_=f"badge bg-{badge_color} platform-badge me-2"
            ),
            ui.span(category_display, class_="category-text"),
            class_="d-flex align-items-center mb-2"
        ),

        # Content row
        ui.layout_columns(
            # Amount column
            ui.div(
                ui.div(f"{value:.4f} ETH", class_="amount-display"),
                ui.div(f"${usd_value:,.2f}", class_="usd-amount"),
            ),
            # Function/Address column
            ui.div(
                ui.span(function_name, class_="function-name"),
                ui.div(
                    get_friendly_name(to_address) if to_address else "",
                    class_="address-display"
                ),
            ),
            # Status column
            ui.div(
                ui.span(
                    status_text,
                    class_=f"badge posting-badge {status_class}"
                ),
            ),
            col_widths=[4, 5, 3]
        ),

        # Footer row: timestamp only (expand is now full card click)
        ui.div(
            ui.span(timestamp_display, class_="timestamp-display"),
            class_="d-flex align-items-center mt-2 pt-2 border-top"
        ),

        class_="decoded-card",
        id=f"card_{tx_hash[:10]}",
        onclick=f"Shiny.setInputValue('expand_decoded_tx', '{tx_hash}', {{priority: 'event'}});"
    )


def empty_state_ui() -> ui.Tag:
    """Render empty state when no decoded transactions"""
    return ui.div(
        ui.div(
            ui.span(class_="bi bi-inbox display-4 text-muted"),
            class_="text-center mb-3"
        ),
        ui.h5("No Decoded Transactions", class_="text-muted text-center"),
        ui.p(
            "Transactions will appear here as they are decoded by the blockchain listener.",
            class_="text-muted text-center small"
        ),
        class_="py-5"
    )


def error_card_ui(tx_hash: str, error: str) -> ui.Tag:
    """Render error card for failed decoding"""
    return ui.div(
        ui.div(
            ui.span("ERROR", class_="badge bg-danger platform-badge me-2"),
            ui.span("Decoding Failed", class_="category-text"),
            class_="d-flex align-items-center mb-2"
        ),
        ui.div(
            ui.span(f"TX: {tx_hash[:16]}...", class_="address-display"),
            ui.div(error[:100], class_="text-danger small mt-1"),
        ),
        class_="decoded-card border-danger"
    )
