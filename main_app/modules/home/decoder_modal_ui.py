"""
Decoder Modal UI Components
Clean, elegant modal/drawer for displaying decoded Blur transactions
"""

from shiny import ui
from typing import List, Dict, Any


def decoder_modal_ui(tx_hash: str):
    """Create modal UI for decoded transaction details"""
    return ui.modal(
        # Modal Header
        ui.div(
            ui.h4(
                ui.span("🔍 ", style="opacity: 0.7;"),
                "Transaction Decoded",
                style="margin: 0; font-weight: 600;"
            ),
            ui.p(
                f"Hash: {tx_hash[:10]}...{tx_hash[-8:]}",
                style="margin: 0.5rem 0 0 0; color: var(--bs-secondary); font-family: monospace; font-size: 0.875rem;"
            ),
            style="padding-bottom: 1rem; border-bottom: 1px solid var(--bs-border-color);"
        ),

        # Tabbed Content
        ui.navset_tab(
            ui.nav_panel(
                "📊 Summary",
                ui.div(
                    ui.output_ui("decoder_modal_summary"),
                    style="padding: 1.5rem 0;"
                )
            ),
            ui.nav_panel(
                "📝 Events",
                ui.div(
                    ui.output_ui("decoder_modal_events"),
                    style="padding: 1.5rem 0;"
                )
            ),
            ui.nav_panel(
                "💰 Accounting",
                ui.div(
                    ui.output_ui("decoder_modal_accounting"),
                    style="padding: 1.5rem 0;"
                )
            ),
            ui.nav_panel(
                "🔄 Positions",
                ui.div(
                    ui.output_ui("decoder_modal_positions"),
                    style="padding: 1.5rem 0;"
                )
            ),
            id="decoder_modal_tabs"
        ),

        # Footer Actions
        ui.div(
            ui.layout_columns(
                ui.div(
                    ui.input_action_button(
                        "decoder_copy_entries",
                        "📋 Copy Entries",
                        class_="btn btn-outline-secondary"
                    ),
                    ui.input_action_button(
                        "decoder_export_csv",
                        "⬇️ Export CSV",
                        class_="btn btn-outline-primary"
                    ),
                ),
                ui.div(
                    ui.input_action_button(
                        "decoder_post_to_gl",
                        "✓ Post to General Ledger",
                        class_="btn btn-success"
                    ),
                    style="text-align: right;"
                ),
                col_widths=[6, 6]
            ),
            style="margin-top: 1.5rem; padding-top: 1rem; border-top: 1px solid var(--bs-border-color);"
        ),

        # Modal Settings
        title="Transaction Details",
        size="xl",
        easy_close=True,
        footer=None
    )


def summary_card_ui(title: str, value: str, subtitle: str = "", icon: str = "", theme: str = "light"):
    """Create a summary card component"""
    theme_colors = {
        "primary": "var(--bs-primary)",
        "success": "var(--bs-success)",
        "info": "var(--bs-info)",
        "warning": "var(--bs-warning)",
        "danger": "var(--bs-danger)",
        "light": "var(--bs-gray-600)"
    }

    color = theme_colors.get(theme, theme_colors["light"])

    return ui.div(
        ui.div(
            ui.span(icon + " " if icon else "", style=f"color: {color}; font-size: 1.2rem;"),
            ui.span(title, style="font-size: 0.875rem; color: var(--bs-secondary); font-weight: 500;"),
            style="margin-bottom: 0.25rem;"
        ),
        ui.div(
            value,
            style=f"font-size: 1.25rem; font-weight: 600; color: {color};"
        ),
        ui.div(
            subtitle,
            style="font-size: 0.75rem; color: var(--bs-secondary); margin-top: 0.25rem;"
        ) if subtitle else ui.div(),
        style="padding: 1rem; background: var(--bs-gray-100); border-radius: 0.375rem; margin-bottom: 1rem;"
    )


def role_badge_ui(role: str, address: str):
    """Create a wallet role badge"""
    role_themes = {
        "LENDER": ("primary", "🏦"),
        "BORROWER": ("info", "👤"),
        "NEW_LENDER": ("success", "🆕"),
        "OLD_LENDER": ("warning", "📤"),
        "BORROWER_REPAYING": ("info", "💰"),
        "LENDER_RECEIVING": ("success", "✅"),
        "LIQUIDATOR": ("danger", "⚡"),
        "GAS_PAYER": ("secondary", "⛽")
    }

    theme, icon = role_themes.get(role, ("secondary", "❓"))

    return ui.div(
        ui.span(
            icon + " ",
            style="margin-right: 0.25rem;"
        ),
        ui.span(
            role.replace("_", " ").title(),
            class_=f"badge bg-{theme}",
            style="font-size: 0.75rem; padding: 0.35em 0.65em; margin-right: 0.5rem;"
        ),
        ui.code(
            f"{address[:6]}...{address[-4:]}",
            style="font-size: 0.75rem; background: var(--bs-gray-200); padding: 0.25rem 0.5rem; border-radius: 0.25rem;"
        ),
        style="margin-bottom: 0.5rem; display: flex; align-items: center;"
    )


def journal_entry_card_ui(entry_num: int, description: str, wallet_role: str,
                          debits: List[Dict], credits: List[Dict],
                          tax_info: str = "", balanced: bool = True):
    """Create a journal entry card for display"""
    return ui.card(
        ui.card_header(
            ui.div(
                ui.strong(f"Entry {entry_num}: {description}"),
                ui.span(
                    wallet_role.replace("_", " ").title(),
                    class_="badge bg-primary ms-2",
                    style="font-size: 0.75rem;"
                ),
                style="display: flex; align-items: center; justify-content: space-between;"
            )
        ),
        ui.layout_columns(
            # Debits Column
            ui.div(
                ui.h6("DEBITS", style="color: var(--bs-success); margin-bottom: 1rem; font-size: 0.875rem;"),
                ui.div(
                    *[
                        ui.div(
                            ui.div(debit["account"], style="font-weight: 500; font-size: 0.9rem;"),
                            ui.div(
                                f"{debit['amount']:.6f} {debit['asset']}",
                                style="color: var(--bs-success); font-weight: 600; font-size: 1rem;"
                            ),
                            style="padding: 0.5rem; background: var(--bs-success-bg-subtle); border-radius: 0.25rem; margin-bottom: 0.5rem;"
                        )
                        for debit in debits
                    ]
                ),
            ),
            # Credits Column
            ui.div(
                ui.h6("CREDITS", style="color: var(--bs-primary); margin-bottom: 1rem; font-size: 0.875rem;"),
                ui.div(
                    *[
                        ui.div(
                            ui.div(credit["account"], style="font-weight: 500; font-size: 0.9rem;"),
                            ui.div(
                                f"{credit['amount']:.6f} {credit['asset']}",
                                style="color: var(--bs-primary); font-weight: 600; font-size: 1rem;"
                            ),
                            style="padding: 0.5rem; background: var(--bs-primary-bg-subtle); border-radius: 0.25rem; margin-bottom: 0.5rem;"
                        )
                        for credit in credits
                    ]
                ),
            ),
            col_widths=[6, 6]
        ),
        # Footer with tax and balance info
        ui.div(
            ui.div(
                ui.span(
                    f"Tax: {tax_info}" if tax_info else "Tax: Not applicable",
                    style="font-size: 0.8rem; color: var(--bs-secondary);"
                ),
                ui.span(
                    "✅ Balanced" if balanced else "❌ Imbalanced",
                    style=f"font-size: 0.8rem; font-weight: 600; color: {'var(--bs-success)' if balanced else 'var(--bs-danger)'};"
                ),
                style="display: flex; justify-content: space-between; align-items: center;"
            ),
            style="margin-top: 1rem; padding-top: 1rem; border-top: 1px solid var(--bs-border-color);"
        ),
        style="margin-bottom: 1rem;"
    )


def event_card_ui(event_name: str, event_args: Dict[str, Any]):
    """Create an event card for display"""
    # Event icons mapping
    event_icons = {
        "LoanOfferTaken": "📝",
        "Repay": "💰",
        "Refinance": "🔄",
        "StartAuction": "🔨",
        "Seize": "⚡",
        "BuyLocked": "🛒",
        "Transfer": "➡️"
    }

    icon = event_icons.get(event_name, "📋")

    return ui.card(
        ui.card_header(
            ui.div(
                ui.span(icon + " ", style="margin-right: 0.5rem; font-size: 1.2rem;"),
                ui.strong(event_name),
                style="display: flex; align-items: center;"
            )
        ),
        ui.div(
            *[
                ui.div(
                    ui.span(f"{key}: ", style="color: var(--bs-secondary); font-size: 0.875rem;"),
                    ui.span(str(value), style="font-family: monospace; font-size: 0.875rem;"),
                    style="padding: 0.25rem 0;"
                )
                for key, value in event_args.items()
            ]
        ),
        style="margin-bottom: 1rem;"
    )


# Custom CSS for modal styling
decoder_modal_styles = ui.tags.style("""
    /* Modal animations */
    .modal.fade .modal-dialog {
        transition: transform 0.3s ease-out;
    }

    /* Summary cards */
    .decoder-summary-card {
        background: linear-gradient(135deg, var(--bs-primary-bg-subtle) 0%, var(--bs-light) 100%);
        border: 1px solid var(--bs-border-color);
        border-radius: 0.5rem;
        padding: 1.25rem;
        margin-bottom: 1rem;
    }

    /* Entry balance indicator */
    .entry-balanced {
        color: var(--bs-success);
        font-weight: 600;
    }

    .entry-imbalanced {
        color: var(--bs-danger);
        font-weight: 600;
    }

    /* Responsive tables */
    .decoder-table {
        font-size: 0.875rem;
    }

    .decoder-table th {
        background: var(--bs-gray-100);
        font-weight: 600;
        padding: 0.75rem 0.5rem;
    }

    .decoder-table td {
        padding: 0.5rem;
        border-bottom: 1px solid var(--bs-border-color);
    }

    /* Action buttons */
    .decoder-action-buttons {
        display: flex;
        gap: 0.5rem;
        justify-content: flex-end;
        margin-top: 1rem;
    }

    /* Hover effects */
    .decoder-entry-card:hover {
        box-shadow: 0 0.25rem 0.5rem rgba(0, 0, 0, 0.1);
        transition: box-shadow 0.2s ease;
    }
""")
