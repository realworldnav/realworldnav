from shiny import ui
from datetime import datetime

def home_dashboard_ui():
    """Home Dashboard with blockchain transaction monitoring"""
    return ui.page_fluid(
        ui.h2("Blockchain Transaction Monitor"),
        ui.p("Live monitoring of wallet transactions on the blockchain", class_="text-muted mb-4"),

        # Control Panel
        ui.card(
            ui.card_header("Monitor Settings"),
            ui.layout_columns(
                ui.div(
                    ui.output_ui("wallet_selector_ui"),
                ),
                ui.div(
                    ui.input_select(
                        "transaction_limit",
                        "Display Limit:",
                        {
                            "30": "Last 30 transactions",
                            "50": "Last 50 transactions",
                            "100": "Last 100 transactions",
                            "150": "Last 150 transactions",
                            "500": "Last 500 transactions"
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
                            "42161": "Arbitrum One",
                            "10": "Optimism",
                            "137": "Polygon",
                            "8453": "Base"
                        },
                        selected="1",
                        width="100%"
                    ),
                ),
                ui.div(
                    ui.input_action_button(
                        "refresh_data",
                        "Refresh Now",
                        class_="btn btn-primary w-100"
                    ),
                ),
                col_widths=[5, 3, 2, 2]
            ),
            class_="mb-4"
        ),

        # Advanced Filters Panel
        ui.card(
            ui.card_header(
                ui.layout_columns(
                    ui.h5("Transaction Filters"),
                    ui.input_switch("show_filters", "Show Filters", value=False),
                    col_widths=[6, 6]
                )
            ),
            ui.output_ui("filter_controls"),
            class_="mb-4"
        ),

        # Status Indicators
        ui.layout_column_wrap(
            ui.value_box(
                "Connection Status",
                ui.output_ui("connection_status"),
                ui.output_ui("connection_indicator"),
                theme="primary"
            ),
            ui.value_box(
                "Active Wallet",
                ui.output_ui("active_wallet_display"),
                "Monitoring address",
                theme="info"
            ),
            ui.value_box(
                "Transactions Today",
                ui.output_ui("transactions_today_count"),
                ui.output_ui("transactions_change"),
                theme="success"
            ),
            ui.value_box(
                "Last Transaction",
                ui.output_ui("last_transaction_time"),
                "Most recent activity",
                theme="warning"
            ),
            fill=False,
            width="25%"
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

        # Add custom CSS for the dashboard
        ui.tags.style("""
            .transaction-table-container {
                max-height: 600px;
                overflow-y: auto;
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
    """Enhanced Home section with sub-navigation tabs"""
    return ui.navset_tab(
        ui.nav_panel("Dashboard", home_dashboard_ui()),
        ui.nav_panel("Analytics", ui.div(
            ui.h3("Transaction Analytics"),
            ui.p("Coming soon: Detailed analytics and visualizations of blockchain activity"),
            class_="p-4"
        )),
        ui.nav_panel("Settings", ui.div(
            ui.h3("Listener Settings"),
            ui.p("Coming soon: Configure blockchain endpoints, filters, and alerts"),
            class_="p-4"
        )),
        id="home_tabs"
    )