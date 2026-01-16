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
                ui.HTML('<i class="bi bi-search me-2" style="opacity: 0.7;"></i>'),
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
                ui.HTML('<i class="bi bi-bar-chart me-1"></i> Summary'),
                ui.div(
                    ui.output_ui("decoder_modal_summary"),
                    style="padding: 1.5rem 0;"
                )
            ),
            ui.nav_panel(
                ui.HTML('<i class="bi bi-list-ul me-1"></i> Events'),
                ui.div(
                    ui.output_ui("decoder_modal_events"),
                    style="padding: 1.5rem 0;"
                )
            ),
            ui.nav_panel(
                ui.HTML('<i class="bi bi-currency-dollar me-1"></i> Accounting'),
                ui.div(
                    ui.output_ui("decoder_modal_accounting"),
                    style="padding: 1.5rem 0;"
                )
            ),
            ui.nav_panel(
                ui.HTML('<i class="bi bi-arrow-repeat me-1"></i> Positions'),
                ui.div(
                    ui.output_ui("decoder_modal_positions"),
                    style="padding: 1.5rem 0;"
                )
            ),
            ui.nav_panel(
                ui.HTML('<i class="bi bi-terminal me-1"></i> Logs'),
                ui.div(
                    ui.output_ui("decoder_modal_logs"),
                    style="padding: 1.5rem 0; max-height: 600px; overflow-y: auto;"
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
                        ui.HTML('<i class="bi bi-clipboard me-1"></i> Copy Entries'),
                        class_="btn btn-outline-secondary"
                    ),
                    ui.input_action_button(
                        "decoder_export_csv",
                        ui.HTML('<i class="bi bi-download me-1"></i> Export CSV'),
                        class_="btn btn-outline-primary"
                    ),
                ),
                ui.div(
                    ui.input_action_button(
                        "decoder_post_to_gl",
                        ui.HTML('<i class="bi bi-check-lg me-1"></i> Post to General Ledger'),
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
        "primary": "#0d6efd",
        "success": "#198754",
        "info": "#0dcaf0",
        "warning": "#ffc107",
        "danger": "#dc3545",
        "light": "#6c757d"
    }

    color = theme_colors.get(theme, theme_colors["light"])

    # Handle icon as HTML if it contains '<' (Bootstrap icon markup)
    icon_element = ui.HTML(f'<span style="color: {color}; font-size: 1.2rem; margin-right: 0.5rem;">{icon}</span>') if icon else ui.span()

    return ui.div(
        ui.div(
            icon_element,
            ui.span(title, style="font-size: 0.875rem; color: #6c757d; font-weight: 500;"),
            style="margin-bottom: 0.25rem; display: flex; align-items: center;"
        ),
        ui.div(
            value,
            style=f"font-size: 1.25rem; font-weight: 600; color: #212529;"
        ),
        ui.div(
            subtitle,
            style="font-size: 0.75rem; color: #6c757d; margin-top: 0.25rem;"
        ) if subtitle else ui.div(),
        style="padding: 1rem; background: #f8f9fa; border-radius: 0.375rem; margin-bottom: 1rem; border: 1px solid #dee2e6;"
    )


def role_badge_ui(role: str, address: str, friendly_name: str = None):
    """Create a wallet role badge with optional friendly name"""
    role_themes = {
        "LENDER": ("primary", '<i class="bi bi-bank"></i>'),
        "BORROWER": ("info", '<i class="bi bi-person"></i>'),
        "NEW_LENDER": ("success", '<i class="bi bi-plus-circle"></i>'),
        "OLD_LENDER": ("warning", '<i class="bi bi-box-arrow-up"></i>'),
        "BORROWER_REPAYING": ("info", '<i class="bi bi-cash-coin"></i>'),
        "LENDER_RECEIVING": ("success", '<i class="bi bi-check-circle"></i>'),
        "LIQUIDATOR": ("danger", '<i class="bi bi-lightning"></i>'),
        "GAS_PAYER": ("secondary", '<i class="bi bi-fuel-pump"></i>'),
        "SENDER": ("warning", '<i class="bi bi-box-arrow-up-right"></i>'),
        "RECEIVER": ("success", '<i class="bi bi-box-arrow-in-down"></i>'),
        "ETH_SENDER": ("warning", '<i class="bi bi-box-arrow-up-right"></i>'),
        "ETH_RECEIVER": ("success", '<i class="bi bi-box-arrow-in-down"></i>'),
        "NFT_SENDER": ("warning", '<i class="bi bi-image"></i>'),
        "NFT_RECEIVER": ("success", '<i class="bi bi-image"></i>'),
        "TX_SENDER": ("secondary", '<i class="bi bi-send"></i>'),
        "DEPOSITOR": ("success", '<i class="bi bi-box-arrow-in-down"></i>'),
        "WITHDRAWER": ("warning", '<i class="bi bi-box-arrow-up"></i>'),
        "POOL_DEPOSITOR": ("success", '<i class="bi bi-box-arrow-in-down"></i>'),
        "POOL_WITHDRAWER": ("warning", '<i class="bi bi-box-arrow-up"></i>')
    }

    theme, icon = role_themes.get(role, ("secondary", '<i class="bi bi-question-circle"></i>'))

    # Display friendly name if available, otherwise show shortened address
    if friendly_name:
        display_name = friendly_name
        address_display = f"{address[:6]}...{address[-4:]}"
    else:
        display_name = None
        address_display = f"{address[:6]}...{address[-4:]}"

    return ui.div(
        ui.HTML(f'{icon} '),
        ui.span(
            role.replace("_", " ").title(),
            class_=f"badge bg-{theme}",
            style="font-size: 0.75rem; padding: 0.35em 0.65em; margin-right: 0.5rem;"
        ),
        ui.span(
            friendly_name if friendly_name else "",
            style="font-weight: 600; margin-right: 0.5rem; color: #212529;"
        ) if friendly_name else ui.span(),
        ui.code(
            address_display,
            style="font-size: 0.75rem; background: #e9ecef; padding: 0.25rem 0.5rem; border-radius: 0.25rem; color: #495057;"
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
                ui.HTML(
                    f'<span style="font-size: 0.8rem; font-weight: 600; color: {"var(--bs-success)" if balanced else "var(--bs-danger)"};">'
                    f'<i class="bi bi-{"check-circle" if balanced else "x-circle"} me-1"></i>'
                    f'{"Balanced" if balanced else "Imbalanced"}</span>'
                ),
                style="display: flex; justify-content: space-between; align-items: center;"
            ),
            style="margin-top: 1rem; padding-top: 1rem; border-top: 1px solid var(--bs-border-color);"
        ),
        style="margin-bottom: 1rem;"
    )


def event_card_ui(event_name: str, event_args: Dict[str, Any]):
    """Create an event card for display"""
    # Event icons mapping (Bootstrap Icons)
    event_icons = {
        "LoanOfferTaken": '<i class="bi bi-file-earmark-text"></i>',
        "Repay": '<i class="bi bi-cash-coin"></i>',
        "Refinance": '<i class="bi bi-arrow-repeat"></i>',
        "StartAuction": '<i class="bi bi-hammer"></i>',
        "Seize": '<i class="bi bi-lightning"></i>',
        "BuyLocked": '<i class="bi bi-cart"></i>',
        "Transfer": '<i class="bi bi-arrow-right"></i>'
    }

    icon = event_icons.get(event_name, '<i class="bi bi-list-ul"></i>')

    return ui.card(
        ui.card_header(
            ui.div(
                ui.HTML(f'{icon} '),
                ui.strong(event_name),
                style="display: flex; align-items: center; gap: 0.5rem;"
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


def log_section_ui(title: str, icon: str, content: List, theme: str = "light"):
    """Create a log section with expandable content"""
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
            ui.HTML(f'<span style="font-size: 1.1rem; margin-right: 0.5rem; color: {color};">{icon}</span>'),
            ui.strong(title, style=f"color: {color}; font-size: 0.95rem;"),
            style="margin-bottom: 0.75rem; display: flex; align-items: center;"
        ),
        ui.div(
            *content,
            style="background: var(--bs-gray-50); border-left: 3px solid " + color + "; padding: 1rem; border-radius: 0.25rem; margin-bottom: 1.5rem;"
        )
    )


def metadata_row_ui(label: str, value: str, is_code: bool = False):
    """Create a metadata row for the logs tab"""
    value_content = ui.code(
        value,
        style="font-size: 0.85rem; background: var(--bs-gray-200); padding: 0.25rem 0.5rem; border-radius: 0.25rem; word-break: break-all;"
    ) if is_code else ui.span(value, style="font-weight: 500; color: var(--bs-body-color);")

    return ui.div(
        ui.div(
            ui.span(label + ":", style="color: var(--bs-secondary); font-size: 0.875rem; min-width: 140px; display: inline-block;"),
            value_content,
            style="display: flex; align-items: center; gap: 0.5rem; padding: 0.4rem 0;"
        )
    )


def raw_log_ui(log_index: int, address: str, topics: List[str], data: str):
    """Create a raw log display card"""
    return ui.div(
        ui.div(
            ui.strong(f"Log #{log_index}", style="color: var(--bs-primary); font-size: 0.9rem;"),
            style="margin-bottom: 0.5rem;"
        ),
        ui.div(
            metadata_row_ui("Address", address, is_code=True),
            metadata_row_ui("Topics", f"{len(topics)} topics"),
            *[
                ui.div(
                    ui.span(f"  Topic[{i}]:", style="color: var(--bs-secondary); font-size: 0.75rem; font-family: monospace; margin-left: 1rem;"),
                    ui.code(topic, style="font-size: 0.75rem; background: var(--bs-gray-200); padding: 0.2rem 0.4rem; margin-left: 0.5rem; word-break: break-all;"),
                    style="padding: 0.2rem 0;"
                )
                for i, topic in enumerate(topics)
            ],
            metadata_row_ui("Data", data[:66] + "..." if len(data) > 66 else data, is_code=True),
        ),
        style="background: var(--bs-light); border: 1px solid var(--bs-border-color); border-radius: 0.25rem; padding: 0.75rem; margin-bottom: 0.75rem; font-size: 0.875rem;"
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
