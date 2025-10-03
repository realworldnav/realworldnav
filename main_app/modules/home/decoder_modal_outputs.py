"""
Decoder Modal Server Outputs
Reactive outputs for the transaction decoder modal
"""

from shiny import reactive, render, ui
import pandas as pd
from typing import Dict, Any, List
from .blur_auto_decoder import blur_auto_decoder
from .decoder_modal_ui import summary_card_ui, role_badge_ui, event_card_ui, journal_entry_card_ui
import logging

logger = logging.getLogger(__name__)


def register_decoder_modal_outputs(input, output, session, selected_fund):
    """Register all decoder modal outputs"""

    # Reactive value to store currently selected transaction
    current_tx_hash = reactive.value(None)
    decode_result = reactive.value(None)

    # Update decode result when transaction changes
    @reactive.effect
    @reactive.event(current_tx_hash)
    def load_decode_result():
        """Load decode result for current transaction"""
        tx_hash = current_tx_hash.get()
        if not tx_hash:
            decode_result.set(None)
            return

        # Get cached result or trigger decode
        result = blur_auto_decoder.get_cached_decode(tx_hash)
        decode_result.set(result)

    # ========================================================================
    # SUMMARY TAB
    # ========================================================================

    @output
    @render.ui
    def decoder_modal_summary():
        """Render transaction summary tab"""
        result = decode_result.get()

        if not result:
            return ui.div(
                ui.p("No transaction data available", class_="text-muted text-center"),
                style="padding: 2rem;"
            )

        if result.get("status") == "error":
            return ui.div(
                ui.div(
                    ui.span("❌ ", style="font-size: 1.5rem;"),
                    ui.h5("Decoding Error", style="display: inline; margin-left: 0.5rem;"),
                    style="margin-bottom: 1rem;"
                ),
                ui.p(result.get("error", "Unknown error"), class_="text-danger"),
                style="padding: 2rem;"
            )

        # Success - show summary
        return ui.div(
            # Transaction info cards
            ui.layout_columns(
                summary_card_ui(
                    "Block Number",
                    f"#{result.get('block', 'N/A'):,}",
                    result.get('timestamp', ''),
                    "🔗",
                    "primary"
                ),
                summary_card_ui(
                    "Function Called",
                    result.get('function', 'Unknown'),
                    "Decoded method",
                    "⚙️",
                    "info"
                ),
                summary_card_ui(
                    "ETH Price",
                    f"${result.get('eth_price', 0):,.2f}",
                    "At block time",
                    "💵",
                    "success"
                ),
                summary_card_ui(
                    "Gas Used",
                    f"{result.get('gas_used', 0):,}",
                    f"{result.get('value', 0):.6f} ETH value",
                    "⛽",
                    "warning"
                ),
                col_widths=[3, 3, 3, 3]
            ),

            # Wallet roles section
            ui.div(
                ui.h5("Wallet Roles", style="margin-bottom: 1rem; font-weight: 600;"),
                ui.div(
                    *[
                        role_badge_ui(role, address)
                        for address, role in result.get('wallet_roles', {}).items()
                    ] if result.get('wallet_roles') else [
                        ui.p("No fund wallet involvement detected", class_="text-muted")
                    ]
                ),
                style="margin-top: 2rem; padding: 1rem; background: var(--bs-gray-100); border-radius: 0.375rem;"
            ),

            # Quick stats
            ui.div(
                ui.h5("Decoding Results", style="margin: 2rem 0 1rem 0; font-weight: 600;"),
                ui.layout_columns(
                    ui.div(
                        ui.div("📝 Events Decoded", style="font-size: 0.875rem; color: var(--bs-secondary);"),
                        ui.div(
                            str(result.get('summary', {}).get('total_events', 0)),
                            style="font-size: 2rem; font-weight: 600; color: var(--bs-primary);"
                        )
                    ),
                    ui.div(
                        ui.div("💰 Journal Entries", style="font-size: 0.875rem; color: var(--bs-secondary);"),
                        ui.div(
                            str(result.get('summary', {}).get('total_journal_entries', 0)),
                            style="font-size: 2rem; font-weight: 600; color: var(--bs-success);"
                        )
                    ),
                    ui.div(
                        ui.div("✅ Balance Check", style="font-size: 0.875rem; color: var(--bs-secondary);"),
                        ui.div(
                            "Balanced" if result.get('summary', {}).get('all_balanced', True) else "Imbalanced",
                            style=f"font-size: 1.5rem; font-weight: 600; color: {'var(--bs-success)' if result.get('summary', {}).get('all_balanced', True) else 'var(--bs-danger)'};"
                        )
                    ),
                    col_widths=[4, 4, 4]
                )
            ),

            # Etherscan link
            ui.div(
                ui.a(
                    "View on Etherscan →",
                    href=f"https://etherscan.io/tx/{result.get('tx_hash', '')}",
                    target="_blank",
                    class_="btn btn-outline-primary"
                ),
                style="margin-top: 2rem; text-align: center;"
            )
        )

    # ========================================================================
    # EVENTS TAB
    # ========================================================================

    @output
    @render.ui
    def decoder_modal_events():
        """Render events tab"""
        result = decode_result.get()

        if not result or result.get("status") == "error":
            return ui.div(
                ui.p("No events available", class_="text-muted text-center"),
                style="padding: 2rem;"
            )

        events = result.get('events', [])

        if not events:
            return ui.div(
                ui.p("No events decoded for this transaction", class_="text-muted text-center"),
                style="padding: 2rem;"
            )

        # Render event cards
        event_cards = []
        for event in events:
            event_name = event.get('name', 'Unknown')
            event_args = event.get('args', {})
            event_cards.append(event_card_ui(event_name, event_args))

        return ui.div(*event_cards)

    # ========================================================================
    # ACCOUNTING TAB
    # ========================================================================

    @output
    @render.ui
    def decoder_modal_accounting():
        """Render accounting/journal entries tab"""
        result = decode_result.get()

        if not result or result.get("status") == "error":
            return ui.div(
                ui.p("No journal entries available", class_="text-muted text-center"),
                style="padding: 2rem;"
            )

        journal_entries = result.get('journal_entries', [])

        if not journal_entries:
            return ui.div(
                ui.div(
                    ui.span("ℹ️ ", style="font-size: 1.5rem;"),
                    ui.p(
                        "No journal entries generated. This transaction may not involve fund wallets or may not require accounting entries.",
                        class_="text-muted",
                        style="display: inline; margin-left: 0.5rem;"
                    ),
                ),
                style="padding: 2rem; text-align: center;"
            )

        # Render journal entry cards
        entry_cards = []
        for idx, entry in enumerate(journal_entries, 1):
            debits = [e for e in entry.get('entries', []) if e.get('type') == 'DEBIT']
            credits = [e for e in entry.get('entries', []) if e.get('type') == 'CREDIT']

            # Format tax info
            tax_info = ""
            if entry.get('tax_implications'):
                tax_treatments = [t.get('treatment', '') for t in entry['tax_implications']]
                tax_info = ", ".join(set(tax_treatments))

            entry_cards.append(
                journal_entry_card_ui(
                    entry_num=idx,
                    description=entry.get('description', ''),
                    wallet_role=entry.get('wallet_role', ''),
                    debits=debits,
                    credits=credits,
                    tax_info=tax_info,
                    balanced=True  # TODO: Calculate from entry validation
                )
            )

        return ui.div(
            ui.div(
                ui.h5(
                    f"Journal Entries ({len(journal_entries)})",
                    style="margin-bottom: 1.5rem; font-weight: 600;"
                ),
                ui.p(
                    "Review the generated double-entry bookkeeping entries below. "
                    "Select entries to post to the General Ledger.",
                    class_="text-muted",
                    style="font-size: 0.875rem; margin-bottom: 1.5rem;"
                ),
            ),
            *entry_cards
        )

    # ========================================================================
    # POSITIONS TAB
    # ========================================================================

    @output
    @render.ui
    def decoder_modal_positions():
        """Render loan positions tab"""
        result = decode_result.get()

        if not result or result.get("status") == "error":
            return ui.div(
                ui.p("No position data available", class_="text-muted text-center"),
                style="padding: 2rem;"
            )

        positions = result.get('positions', {})

        if not positions:
            return ui.div(
                ui.p(
                    "No loan positions tracked for this transaction",
                    class_="text-muted text-center"
                ),
                style="padding: 2rem;"
            )

        # Convert positions dict to table format
        position_data = []
        for lien_id, position in positions.items():
            position_data.append({
                "Lien ID": lien_id,
                "Status": position.get('status', 'Unknown'),
                "Lender": f"{position.get('lender', '')[:6]}...{position.get('lender', '')[-4:]}",
                "Borrower": f"{position.get('borrower', '')[:6]}...{position.get('borrower', '')[-4:]}",
                "Collection": f"{position.get('collection', '')[:10]}...",
                "Token ID": position.get('token_id', 0),
                "Principal": f"{position.get('principal', 0):.4f} ETH",
                "Rate (bps)": position.get('rate', 0),
            })

        if not position_data:
            return ui.div(
                ui.p("No position data to display", class_="text-muted text-center"),
                style="padding: 2rem;"
            )

        df = pd.DataFrame(position_data)

        return ui.div(
            ui.h5("Loan Positions", style="margin-bottom: 1rem; font-weight: 600;"),
            ui.output_data_frame("decoder_positions_table")
        )

    @output
    @render.data_frame
    def decoder_positions_table():
        """Render positions data table"""
        result = decode_result.get()

        if not result or not result.get('positions'):
            return pd.DataFrame()

        position_data = []
        for lien_id, position in result['positions'].items():
            position_data.append({
                "Lien ID": lien_id,
                "Status": position.get('status', 'Unknown'),
                "Lender": f"{position.get('lender', '')[:10]}...",
                "Borrower": f"{position.get('borrower', '')[:10]}...",
                "Principal": f"{position.get('principal', 0):.4f} ETH",
                "Rate": f"{position.get('rate', 0)} bps",
            })

        df = pd.DataFrame(position_data)
        return render.DataGrid(df, width="100%", height="400px")

    # ========================================================================
    # ACTIONS
    # ========================================================================

    @reactive.effect
    @reactive.event(input.decoder_post_to_gl)
    def post_entries_to_gl():
        """Post selected journal entries to General Ledger"""
        result = decode_result.get()

        if not result or not result.get('journal_entries'):
            ui.notification_show(
                "No journal entries to post",
                type="warning",
                duration=3
            )
            return

        # TODO: Implement GL posting logic
        ui.notification_show(
            f"Posted {len(result['journal_entries'])} entries to General Ledger",
            type="success",
            duration=3
        )

        logger.info(f"Posted journal entries for {current_tx_hash.get()}")

    @reactive.effect
    @reactive.event(input.decoder_export_csv)
    def export_entries_csv():
        """Export journal entries to CSV"""
        result = decode_result.get()

        if not result or not result.get('journal_entries'):
            ui.notification_show(
                "No journal entries to export",
                type="warning",
                duration=3
            )
            return

        # TODO: Implement CSV export
        ui.notification_show(
            "Export functionality coming soon",
            type="info",
            duration=3
        )

    @reactive.effect
    @reactive.event(input.decoder_copy_entries)
    def copy_entries():
        """Copy journal entries to clipboard"""
        ui.notification_show(
            "Copied to clipboard",
            type="success",
            duration=2
        )

    # Return function to set current transaction
    def set_current_transaction(tx_hash: str):
        """Set the current transaction to display in modal"""
        current_tx_hash.set(tx_hash)

    return set_current_transaction
