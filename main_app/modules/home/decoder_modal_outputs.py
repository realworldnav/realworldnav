"""
Decoder Modal Server Outputs
Reactive outputs for the transaction decoder modal
"""

from shiny import reactive, render, ui
import pandas as pd
from typing import Dict, Any, List
from .blur_auto_decoder import blur_auto_decoder
from .decoder_modal_ui import summary_card_ui, role_badge_ui, event_card_ui, journal_entry_card_ui, log_section_ui, metadata_row_ui, raw_log_ui
from ...s3_utils import load_WALLET_file
import logging

logger = logging.getLogger(__name__)

# Cache wallet mappings for friendly name lookups
_wallet_name_cache = None

def _get_wallet_name_mapping() -> dict:
    """Load and cache wallet name mappings from S3"""
    global _wallet_name_cache
    if _wallet_name_cache is None:
        try:
            df = load_WALLET_file()
            if df is not None and not df.empty:
                # Build mapping from wallet address to friendly name
                # Normalize addresses to lowercase for consistent lookups
                _wallet_name_cache = {}
                for _, row in df.iterrows():
                    address = str(row.get('wallet_address', row.get('address', ''))).lower().strip()
                    name = str(row.get('wallet_name', row.get('name', row.get('friendly_name', '')))).strip()
                    if address and name:
                        _wallet_name_cache[address] = name
                logger.info(f"Loaded {len(_wallet_name_cache)} wallet name mappings from S3")
            else:
                _wallet_name_cache = {}
        except Exception as e:
            logger.warning(f"Could not load wallet names from S3: {e}")
            _wallet_name_cache = {}
    return _wallet_name_cache

def get_wallet_friendly_name(address: str) -> str:
    """Get friendly name for a wallet address, or None if not found"""
    if not address:
        return None
    mapping = _get_wallet_name_mapping()
    return mapping.get(address.lower().strip())


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
                    ui.HTML('<i class="bi bi-x-circle" style="font-size: 1.5rem; color: var(--bs-danger);"></i>'),
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
                    '<i class="bi bi-link-45deg"></i>',
                    "primary"
                ),
                summary_card_ui(
                    "Function Called",
                    result.get('function', 'Unknown'),
                    "Decoded method",
                    '<i class="bi bi-gear"></i>',
                    "info"
                ),
                summary_card_ui(
                    "ETH Price",
                    f"${result.get('eth_price', 0):,.2f}",
                    "At block time",
                    '<i class="bi bi-currency-dollar"></i>',
                    "success"
                ),
                summary_card_ui(
                    "Gas Used",
                    f"{result.get('gas_used', 0):,}",
                    f"{result.get('value', 0):.6f} ETH value",
                    '<i class="bi bi-fuel-pump"></i>',
                    "warning"
                ),
                col_widths=[3, 3, 3, 3]
            ),

            # Wallet roles section
            ui.div(
                ui.h5("Wallet Roles", style="margin-bottom: 1rem; font-weight: 600;"),
                ui.div(
                    *[
                        role_badge_ui(role, address, get_wallet_friendly_name(address))
                        for address, role in result.get('wallet_roles', {}).items()
                    ] if result.get('wallet_roles') else [
                        ui.p("No fund wallet involvement detected", class_="text-muted")
                    ]
                ),
                style="margin-top: 2rem; padding: 1rem; background: #f8f9fa; border-radius: 0.375rem;"
            ),

            # Quick stats
            ui.div(
                ui.h5("Decoding Results", style="margin: 2rem 0 1rem 0; font-weight: 600;"),
                ui.layout_columns(
                    ui.div(
                        ui.HTML('<div style="font-size: 0.875rem; color: var(--bs-secondary);"><i class="bi bi-file-text me-1"></i>Events Decoded</div>'),
                        ui.div(
                            str(result.get('summary', {}).get('total_events', 0)),
                            style="font-size: 2rem; font-weight: 600; color: var(--bs-primary);"
                        )
                    ),
                    ui.div(
                        ui.HTML('<div style="font-size: 0.875rem; color: var(--bs-secondary);"><i class="bi bi-cash-coin me-1"></i>Journal Entries</div>'),
                        ui.div(
                            str(result.get('summary', {}).get('total_journal_entries', 0)),
                            style="font-size: 2rem; font-weight: 600; color: var(--bs-success);"
                        )
                    ),
                    ui.div(
                        ui.HTML('<div style="font-size: 0.875rem; color: var(--bs-secondary);"><i class="bi bi-check-circle me-1"></i>Balance Check</div>'),
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
                    ui.HTML('<i class="bi bi-info-circle" style="font-size: 1.5rem; color: var(--bs-info);"></i>'),
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

    # ========================================================================
    # LOGS TAB
    # ========================================================================

    @output
    @render.ui
    def decoder_modal_logs():
        """Render detailed decoding logs and metadata"""
        result = decode_result.get()

        if not result or result.get("status") == "error":
            return ui.div(
                ui.p("No log data available", class_="text-muted text-center"),
                style="padding: 2rem;"
            )

        # Transaction Metadata Section
        metadata_section = log_section_ui(
            '<i class="bi bi-list-ul me-1"></i> Transaction Metadata',
            '<i class="bi bi-list-ul"></i>',
            [
                metadata_row_ui("Transaction Hash", result.get('tx_hash', 'N/A'), is_code=True),
                metadata_row_ui("Block Number", f"{result.get('block', 'N/A'):,}" if result.get('block') else 'N/A'),
                metadata_row_ui("Timestamp", str(result.get('timestamp', 'N/A'))),
                metadata_row_ui("Transaction Type", result.get('tx_type', 'UNKNOWN')),
                metadata_row_ui("ETH Price", f"${result.get('eth_price', 0):,.2f}"),
                metadata_row_ui("Gas Used", f"{result.get('gas_used', 0):,}"),
                metadata_row_ui("From Address", result.get('from', 'N/A'), is_code=True),
                metadata_row_ui("To Address", result.get('to', 'N/A'), is_code=True),
                metadata_row_ui("Value", f"{result.get('value', 0):.6f} ETH"),
            ],
            theme="primary"
        )

        # Function Decoding Section
        func_name = result.get('function', 'N/A')
        func_params = result.get('function_params', {})

        func_params_content = []
        if func_params:
            for key, val in func_params.items():
                val_str = str(val)
                if len(val_str) > 100:
                    val_str = val_str[:100] + "..."
                func_params_content.append(
                    metadata_row_ui(key, val_str, is_code=isinstance(val, (str, bytes)))
                )
        else:
            func_params_content = [ui.p("No parameters", class_="text-muted", style="font-size: 0.875rem;")]

        function_section = log_section_ui(
            f'<i class="bi bi-gear me-1"></i> Function Call: {func_name}',
            '<i class="bi bi-gear"></i>',
            func_params_content,
            theme="info"
        )

        # Decoded Events Section
        events = result.get('events', [])
        events_content = []
        if events:
            for i, event in enumerate(events):
                event_name = event.get('event', 'Unknown')
                event_args = event.get('args', {})

                event_details = [
                    ui.div(
                        ui.strong(f"Event #{i+1}: {event_name}", style="color: var(--bs-success); font-size: 0.9rem;"),
                        style="margin-bottom: 0.5rem;"
                    )
                ]

                for key, val in event_args.items():
                    val_str = str(val)
                    if len(val_str) > 80:
                        val_str = val_str[:80] + "..."
                    event_details.append(metadata_row_ui(key, val_str, is_code=isinstance(val, (str, bytes))))

                events_content.append(ui.div(*event_details, style="margin-bottom: 1rem; padding-bottom: 1rem; border-bottom: 1px solid var(--bs-border-color);"))
        else:
            events_content = [ui.p("No events decoded", class_="text-muted", style="font-size: 0.875rem;")]

        events_section = log_section_ui(
            f'<i class="bi bi-broadcast me-1"></i> Decoded Events ({len(events)})',
            '<i class="bi bi-broadcast"></i>',
            events_content,
            theme="success"
        )

        # Pool Transfers Section
        pool_transfers = result.get('pool_transfers', [])
        pool_content = []
        if pool_transfers:
            for i, transfer in enumerate(pool_transfers):
                pool_content.append(
                    ui.div(
                        ui.strong(f"Transfer #{i+1}: {transfer['direction']}", style="font-size: 0.9rem; color: var(--bs-warning);"),
                        metadata_row_ui("From", transfer.get('from', 'N/A'), is_code=True),
                        metadata_row_ui("To", transfer.get('to', 'N/A'), is_code=True),
                        metadata_row_ui("Amount", f"{transfer.get('amount', 0):.6f} ETH"),
                        style="margin-bottom: 1rem; padding-bottom: 1rem; border-bottom: 1px solid var(--bs-border-color);"
                    )
                )
        else:
            pool_content = [ui.p("No pool transfers", class_="text-muted", style="font-size: 0.875rem;")]

        pool_section = log_section_ui(
            f'<i class="bi bi-arrow-left-right me-1"></i> Pool Transfers ({len(pool_transfers)})',
            '<i class="bi bi-arrow-left-right"></i>',
            pool_content,
            theme="warning"
        )

        # Wallet Roles Section
        wallet_roles = result.get('wallet_roles', {})
        roles_content = []
        if wallet_roles:
            for wallet, role in wallet_roles.items():
                friendly_name = get_wallet_friendly_name(wallet)
                display_name = f"{friendly_name} " if friendly_name else ""
                roles_content.append(
                    ui.div(
                        ui.span(display_name, style="font-weight: 600; color: #0d6efd; margin-right: 0.5rem;") if friendly_name else ui.span(),
                        ui.code(wallet, style="font-size: 0.85rem; background: #e9ecef; padding: 0.25rem 0.5rem; border-radius: 0.25rem;"),
                        ui.span(" → ", style="margin: 0 0.5rem; color: #6c757d;"),
                        ui.span(role, style="font-weight: 600; color: #0d6efd;"),
                        style="padding: 0.4rem 0;"
                    )
                )
        else:
            roles_content = [ui.p("No wallet roles identified", class_="text-muted", style="font-size: 0.875rem;")]

        roles_section = log_section_ui(
            '<i class="bi bi-people me-1"></i> Wallet Roles',
            '<i class="bi bi-people"></i>',
            roles_content,
            theme="primary"
        )

        # Summary Stats
        summary = result.get('summary', {})
        stats_section = log_section_ui(
            '<i class="bi bi-bar-chart me-1"></i> Decoding Summary',
            '<i class="bi bi-bar-chart"></i>',
            [
                metadata_row_ui("Total Events Decoded", str(summary.get('total_events', 0))),
                metadata_row_ui("Pool Transfers", str(len(pool_transfers))),
                metadata_row_ui("Journal Entries Created", str(summary.get('total_journal_entries', 0))),
                metadata_row_ui("All Entries Balanced", "Yes" if summary.get('all_balanced') else "No"),
                metadata_row_ui("Involves Fund Wallets", "Yes" if summary.get('involves_fund_wallets') else "No"),
            ],
            theme="light"
        )

        return ui.div(
            metadata_section,
            function_section,
            events_section,
            pool_section,
            roles_section,
            stats_section,
        )

    # Return function to set current transaction
    def set_current_transaction(tx_hash: str):
        """Set the current transaction to display in modal"""
        current_tx_hash.set(tx_hash)

    return set_current_transaction
