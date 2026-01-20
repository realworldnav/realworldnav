from shiny import ui
from datetime import datetime
from .decoded_transactions_ui import decoded_transactions_ui


def quick_dashboard_ui():
    """Quick loading dashboard - no blockchain dependencies"""
    return ui.page_fluid(
        ui.h2("RealWorldNAV Dashboard"),
        ui.p("Fund accounting and blockchain transaction management", class_="text-muted mb-3"),

        # Quick Stats Row - uses output_ui for lazy loading
        ui.layout_columns(
            ui.value_box(
                "Fund Status",
                ui.output_ui("dashboard_fund_status"),
                showcase=ui.HTML('<i class="bi bi-graph-up" style="font-size: 2rem;"></i>'),
                theme="primary"
            ),
            ui.value_box(
                "GL Entries",
                ui.output_ui("dashboard_gl_count"),
                showcase=ui.HTML('<i class="bi bi-journal-text" style="font-size: 2rem;"></i>'),
                theme="success"
            ),
            ui.value_box(
                "Pending Review",
                ui.output_ui("dashboard_pending_count"),
                showcase=ui.HTML('<i class="bi bi-hourglass-split" style="font-size: 2rem;"></i>'),
                theme="warning"
            ),
            col_widths=[4, 4, 4]
        ),

        # Quick Actions
        ui.card(
            ui.card_header("Quick Actions"),
            ui.div(
                ui.layout_columns(
                    ui.div(
                        ui.input_action_button(
                            "go_to_listener",
                            ui.HTML('<i class="bi bi-broadcast me-2"></i>Blockchain Listener'),
                            class_="btn btn-outline-primary w-100 mb-2"
                        ),
                        ui.p("Monitor live blockchain transactions", class_="text-muted small")
                    ),
                    ui.div(
                        ui.input_action_button(
                            "go_to_decoded",
                            ui.HTML('<i class="bi bi-code-square me-2"></i>Decoded Transactions'),
                            class_="btn btn-outline-success w-100 mb-2"
                        ),
                        ui.p("Review and post decoded transactions to GL", class_="text-muted small")
                    ),
                    ui.div(
                        ui.input_action_button(
                            "go_to_gl2",
                            ui.HTML('<i class="bi bi-book me-2"></i>General Ledger'),
                            class_="btn btn-outline-info w-100 mb-2"
                        ),
                        ui.p("View journal entries and trial balance", class_="text-muted small")
                    ),
                    col_widths=[4, 4, 4]
                ),
                class_="p-3"
            )
        ),

        # Recent Activity - lightweight
        ui.card(
            ui.card_header("Recent Activity"),
            ui.output_ui("dashboard_recent_activity"),
            class_="mt-3"
        ),

        ui.tags.style("""
            .value-box { min-height: 120px; }
            .btn-outline-primary:hover, .btn-outline-success:hover, .btn-outline-info:hover {
                transform: translateY(-2px);
                transition: transform 0.2s;
            }
        """)
    )


def blockchain_listener_ui():
    """Blockchain listener UI - heavy initialization deferred until tab clicked"""
    return ui.page_fluid(
        ui.h2("Blockchain Transaction Monitor"),
        ui.p("Live monitoring of wallet transactions on the blockchain", class_="text-muted mb-3"),

        # Combined Monitor Settings & Filters
        ui.card(
            ui.card_header("Monitor Settings"),
            ui.div(
                # Main controls row
                ui.layout_columns(
                    ui.div(
                        ui.output_ui("wallet_selector_ui"),
                    ),
                    ui.div(
                        ui.input_select(
                            "transaction_limit",
                            "Display Limit:",
                            {
                                "50": "Last 50",
                                "100": "Last 100",
                                "200": "Last 200",
                            },
                            selected="100",
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
                # Advanced filters toggle
                ui.div(
                    ui.layout_columns(
                        ui.div(
                            ui.input_switch("show_filters", "Advanced Filters", value=False),
                            class_="d-flex align-items-center"
                        ),
                        col_widths=[12]
                    ),
                    class_="mt-3 pt-3 border-top"
                ),
                # Filter controls (conditionally shown)
                ui.output_ui("filter_controls"),
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

        # Transaction Table
        ui.card(
            ui.card_header(
                ui.layout_columns(
                    ui.h5("Recent Transactions"),
                    ui.div(
                        ui.output_ui("auto_refresh_indicator"),
                        class_="text-end"
                    ),
                    col_widths=[6, 6]
                )
            ),
            ui.div(
                ui.output_data_frame("blockchain_transactions_table"),
                class_="transaction-table-container"
            ),
            full_screen=True,
            class_="mt-4"
        ),

        # Transaction Details Panel
        ui.card(
            ui.card_header("Transaction Details"),
            ui.div(
                ui.output_ui("transaction_details_panel"),
                class_="p-3"
            ),
            class_="mt-4"
        ),

        # Add custom CSS for the listener
        ui.tags.style("""
            .transaction-table-container {
                max-height: 600px;
                overflow-y: auto;
            }

            /* Decoded icon styling */
            .decoded-icon {
                cursor: pointer;
                font-size: 1.2rem;
                transition: transform 0.2s ease;
            }

            .decoded-icon:hover {
                transform: scale(1.2);
            }

            /* Loading spinner animation for pending decodes */
            @keyframes spin {
                0% { transform: rotate(0deg); }
                100% { transform: rotate(360deg); }
            }

            .decoding-pending {
                animation: spin 2s linear infinite;
            }

            .spin-animation {
                animation: spin 1s linear infinite;
                display: inline-block;
            }

            .status-badge {
                padding: 4px 8px;
                border-radius: 4px;
                font-size: 0.85em;
                font-weight: 500;
            }

            .status-pending {
                background-color: #ffc107;
                color: #000;
            }

            .status-confirmed {
                background-color: #28a745;
                color: #fff;
            }

            .status-failed {
                background-color: #dc3545;
                color: #fff;
            }

            .connection-active {
                color: #28a745;
                animation: pulse 2s infinite;
            }

            .connection-inactive {
                color: #dc3545;
            }

            @keyframes pulse {
                0% { opacity: 1; }
                50% { opacity: 0.5; }
                100% { opacity: 1; }
            }

            .transaction-hash {
                font-family: monospace;
                font-size: 0.9em;
            }

            .address-text {
                font-family: monospace;
                font-size: 0.85em;
                color: #0066cc;
            }

            .amount-text {
                font-weight: 600;
                color: #000;
            }

            .block-number {
                background-color: #e9ecef;
                padding: 2px 6px;
                border-radius: 3px;
                font-size: 0.85em;
            }

            .address-text.small {
                font-size: 0.75em;
                opacity: 0.8;
            }
        """)
    )

def enhanced_home_ui():
    """Enhanced Home section with sub-navigation tabs - Dashboard loads instantly"""
    return ui.navset_tab(
        ui.nav_panel(
            ui.HTML('<i class="bi bi-speedometer2 me-1"></i> Dashboard'),
            quick_dashboard_ui()
        ),
        ui.nav_panel(
            ui.HTML('<i class="bi bi-broadcast me-1"></i> Blockchain Listener'),
            blockchain_listener_ui()
        ),
        ui.nav_panel(
            ui.HTML('<i class="bi bi-code-square me-1"></i> Decoded Transactions'),
            decoded_transactions_ui()
        ),
        id="home_tabs"
    )