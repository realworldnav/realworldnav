"""
General Ledger 2 UI Module

Professional GL interface with full accounting functionality:
1. Journal Entries - View, filter, edit, delete entries
2. Account Ledger - Account-specific view with running balances
3. Trial Balance - Standard TB format with account groupings
4. New Entry - Manual journal entry creation connected to COA
"""

from shiny import ui
from datetime import date, timedelta


def general_ledger_v2_ui():
    """Main GL2 UI with professional accounting interface."""
    return ui.page_fluid(
        # Custom CSS for professional look
        ui.tags.style("""
            .gl2-header {
                background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
                color: white;
                padding: 20px;
                border-radius: 8px 8px 0 0;
                margin-bottom: 0;
            }
            .gl2-header h2,
            .gl2-header h3,
            .gl2-header h4,
            .gl2-header p,
            .gl2-header span {
                color: white !important;
            }
            .gl2-header .opacity-75 {
                color: rgba(255,255,255,0.75) !important;
            }
            .gl2-stat-box {
                text-align: center;
                padding: 10px;
            }
            .gl2-stat-value {
                font-size: 1.5rem;
                font-weight: 700;
                color: white;
            }
            .gl2-stat-label {
                font-size: 0.75rem;
                opacity: 0.8;
                text-transform: uppercase;
                color: rgba(255,255,255,0.85);
            }
            /* Bright colors for dark header background */
            .gl2-header .gl2-success { color: #4ade80 !important; }
            .gl2-header .gl2-danger { color: #f87171 !important; }
            .gl2-header .gl2-warning { color: #fbbf24 !important; }
            .gl2-header .gl2-info { color: #38bdf8 !important; }
            /* Standard colors for light backgrounds */
            .gl2-success { color: #16a34a; }
            .gl2-danger { color: #dc2626; }
            .gl2-warning { color: #d97706; }
            .gl2-info { color: #0284c7; }
            .gl2-toolbar {
                background: #f8f9fa;
                padding: 12px 20px;
                border-bottom: 1px solid #dee2e6;
                display: flex;
                gap: 15px;
                align-items: center;
                flex-wrap: wrap;
            }
            .gl2-quick-filter {
                flex: 1;
                min-width: 200px;
                max-width: 350px;
            }
            .gl2-actions {
                display: flex;
                gap: 8px;
            }
            .entry-row-debit {
                background-color: rgba(40, 167, 69, 0.05);
            }
            .entry-row-credit {
                background-color: rgba(220, 53, 69, 0.05);
            }
            .balance-positive { color: #16a34a; font-weight: 600; }
            .balance-negative { color: #dc2626; font-weight: 600; }
            .tb-category-header {
                background-color: #e9ecef;
                font-weight: 600;
            }
            .tb-total-row {
                background-color: #343a40;
                color: white;
                font-weight: 700;
            }
            /* Fix horizontal and vertical scrolling for tables */
            .gl2-table-wrapper {
                overflow-x: auto;
                overflow-y: auto;
                max-width: 100%;
                max-height: 800px;
            }
            .gl2-table-wrapper .shiny-data-grid {
                min-width: 1200px;
            }
            /* Enable mouse wheel scrolling */
            .gl2-table-wrapper .shiny-data-grid,
            .gl2-table-wrapper .shiny-data-grid-grid {
                overflow-y: auto !important;
                -webkit-overflow-scrolling: touch;
            }
            /* Make the table body scrollable with mouse wheel */
            .shiny-data-grid-grid {
                overflow: auto !important;
                scroll-behavior: smooth;
            }
            /* Edit form styling */
            .gl2-edit-form {
                background: #f8f9fa;
                border: 1px solid #dee2e6;
                border-radius: 8px;
                padding: 20px;
            }
            .gl2-edit-form-header {
                background: #1a1a2e;
                color: white;
                padding: 10px 15px;
                border-radius: 8px 8px 0 0;
                margin: -20px -20px 15px -20px;
            }
            .gl2-field-label {
                font-weight: 600;
                font-size: 0.85rem;
                color: #555;
                margin-bottom: 4px;
            }
            .gl2-field-value {
                padding: 8px 12px;
                background: white;
                border: 1px solid #ced4da;
                border-radius: 4px;
            }
        """),

        # Header with stats
        ui.div(
            ui.layout_columns(
                ui.div(
                    ui.h2("General Ledger 2", class_="mb-1"),
                    ui.p("Professional Accounting Interface", class_="mb-0 opacity-75", style="font-size: 0.9rem;"),
                ),
                ui.div(
                    ui.layout_columns(
                        ui.div(
                            ui.div(ui.output_text("gl2_header_entries"), class_="gl2-stat-value"),
                            ui.div("Entries", class_="gl2-stat-label"),
                            class_="gl2-stat-box"
                        ),
                        ui.div(
                            ui.div(ui.output_text("gl2_header_accounts"), class_="gl2-stat-value"),
                            ui.div("Accounts", class_="gl2-stat-label"),
                            class_="gl2-stat-box"
                        ),
                        ui.div(
                            ui.div(ui.output_text("gl2_header_debits"), class_="gl2-stat-value gl2-success"),
                            ui.div("Total Debits", class_="gl2-stat-label"),
                            class_="gl2-stat-box"
                        ),
                        ui.div(
                            ui.div(ui.output_text("gl2_header_credits"), class_="gl2-stat-value gl2-danger"),
                            ui.div("Total Credits", class_="gl2-stat-label"),
                            class_="gl2-stat-box"
                        ),
                        ui.div(
                            ui.div(ui.output_text("gl2_header_balance"), class_="gl2-stat-value"),
                            ui.div("Balance Check", class_="gl2-stat-label"),
                            class_="gl2-stat-box"
                        ),
                        col_widths=[2, 2, 3, 3, 2]
                    ),
                ),
                col_widths=[4, 8]
            ),
            class_="gl2-header"
        ),

        # Navigation tabs
        ui.navset_card_tab(
            ui.nav_panel(
                ui.HTML('<i class="bi bi-journal-text"></i> Journal Entries'),
                journal_entries_tab()
            ),
            ui.nav_panel(
                ui.HTML('<i class="bi bi-list-columns"></i> Account Ledger'),
                account_ledger_tab()
            ),
            ui.nav_panel(
                ui.HTML('<i class="bi bi-calculator"></i> Trial Balance'),
                trial_balance_tab()
            ),
            ui.nav_panel(
                ui.HTML('<i class="bi bi-plus-circle"></i> New Entry'),
                new_entry_tab()
            ),
            id="gl2_tabs"
        ),
        class_="px-0"
    )


def journal_entries_tab():
    """Journal Entries tab - view, filter, edit, delete entries."""
    return ui.div(
        # Toolbar
        ui.div(
            # Quick filter
            ui.div(
                ui.input_text(
                    "gl2_quick_search",
                    None,
                    placeholder="Quick search (hash, description, account...)",
                    width="100%"
                ),
                class_="gl2-quick-filter"
            ),

            # Date range
            ui.input_date_range(
                "gl2_date_range",
                None,
                start=date.today() - timedelta(days=365),
                end=date.today()
            ),

            # Account filter
            ui.div(
                ui.input_selectize(
                    "gl2_account_filter",
                    None,
                    choices={"": "All Accounts"},
                    selected="",
                    width="180px"
                ),
            ),

            # Category filter
            ui.div(
                ui.input_selectize(
                    "gl2_category_filter",
                    None,
                    choices={"": "All Categories"},
                    selected="",
                    width="150px"
                ),
            ),

            # Rows per page selector
            ui.div(
                ui.input_select(
                    "gl2_rows_per_page",
                    None,
                    choices={
                        "50": "50 rows",
                        "100": "100 rows",
                        "200": "200 rows",
                        "300": "300 rows"
                    },
                    selected="100",
                    width="110px"
                ),
            ),

            # Spacer
            ui.div(style="flex: 1;"),

            # Action buttons
            ui.div(
                ui.input_action_button(
                    "gl2_refresh",
                    ui.HTML('<i class="bi bi-arrow-clockwise"></i> Refresh'),
                    class_="btn-outline-secondary btn-sm"
                ),
                ui.input_action_button(
                    "gl2_clear_filters",
                    ui.HTML('<i class="bi bi-x-circle"></i> Clear'),
                    class_="btn-outline-secondary btn-sm"
                ),
                ui.download_button(
                    "gl2_download_je",
                    ui.HTML('<i class="bi bi-download"></i> Export'),
                    class_="btn-outline-success btn-sm"
                ),
                class_="gl2-actions"
            ),
            class_="gl2-toolbar"
        ),

        # Journal entries table with selection and editing
        ui.card(
            ui.card_header(
                ui.div(
                    ui.span("Journal Entries", class_="fw-bold"),
                    ui.div(
                        ui.input_action_button(
                            "gl2_delete_selected",
                            ui.HTML('<i class="bi bi-trash"></i> Delete Selected'),
                            class_="btn-outline-danger btn-sm me-2"
                        ),
                        ui.input_action_button(
                            "gl2_reverse_selected",
                            ui.HTML('<i class="bi bi-arrow-counterclockwise"></i> Reverse'),
                            class_="btn-outline-warning btn-sm"
                        ),
                        class_="d-flex"
                    ),
                    class_="d-flex justify-content-between align-items-center"
                )
            ),
            ui.div(
                ui.output_data_frame("gl2_journal_entries_table"),
                class_="gl2-table-wrapper"
            ),
        ),

        # Entry detail panel (shown when entry selected)
        ui.output_ui("gl2_entry_detail_panel"),

        class_="p-0"
    )


def account_ledger_tab():
    """Account Ledger tab - account-specific view with running balance."""
    return ui.div(
        # Account selector toolbar
        ui.div(
            ui.div(
                ui.input_selectize(
                    "gl2_ledger_account",
                    "Account",
                    choices={"": "Select an account..."},
                    selected="",
                    width="350px"
                ),
            ),
            ui.input_date_range(
                "gl2_ledger_date_range",
                "Date Range",
                start=date.today() - timedelta(days=365),
                end=date.today()
            ),
            ui.input_action_button(
                "gl2_load_ledger",
                ui.HTML('<i class="bi bi-play-fill"></i> Load Ledger'),
                class_="btn-primary mt-4"
            ),
            ui.div(style="flex: 1;"),
            ui.download_button(
                "gl2_download_ledger",
                ui.HTML('<i class="bi bi-download"></i> Export'),
                class_="btn-outline-success mt-4"
            ),
            class_="gl2-toolbar"
        ),

        # Account summary cards
        ui.output_ui("gl2_account_summary"),

        # Account ledger table
        ui.card(
            ui.card_header(
                ui.div(
                    ui.output_text("gl2_ledger_account_name"),
                    ui.span(id="gl2_ledger_entry_count", class_="badge bg-secondary ms-2"),
                    class_="fw-bold"
                )
            ),
            ui.div(
                ui.output_data_frame("gl2_account_ledger_table"),
                class_="gl2-table-wrapper"
            ),
        ),

        class_="p-0"
    )


def trial_balance_tab():
    """Trial Balance tab - beautiful professional TB format."""
    return ui.div(
        # Custom CSS for beautiful Trial Balance
        ui.tags.style("""
            .tb-container {
                background: #fff;
                border-radius: 12px;
                box-shadow: 0 2px 12px rgba(0,0,0,0.08);
                overflow: hidden;
                margin: 20px;
            }
            .tb-header {
                background: linear-gradient(135deg, #1e3a5f 0%, #2d5a87 100%);
                color: white;
                padding: 24px 32px;
            }
            .tb-header h2,
            .tb-header h3,
            .tb-header p,
            .tb-header span,
            .tb-header div {
                color: white !important;
            }
            .tb-header h2 {
                margin: 0 0 4px 0;
                font-weight: 600;
                font-size: 1.5rem;
            }
            .tb-header .tb-subtitle {
                opacity: 0.85;
                font-size: 0.9rem;
                color: rgba(255,255,255,0.85) !important;
            }
            .tb-controls {
                background: #f8fafc;
                padding: 16px 32px;
                border-bottom: 1px solid #e2e8f0;
                display: flex;
                gap: 24px;
                align-items: flex-end;
                flex-wrap: wrap;
            }
            .tb-control-group {
                display: flex;
                flex-direction: column;
                gap: 4px;
            }
            .tb-control-label {
                font-size: 0.75rem;
                font-weight: 600;
                color: #64748b;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }
            .tb-summary {
                display: grid;
                grid-template-columns: repeat(3, 1fr);
                gap: 0;
                border-bottom: 2px solid #e2e8f0;
            }
            .tb-summary-item {
                padding: 20px 32px;
                text-align: center;
                border-right: 1px solid #e2e8f0;
            }
            .tb-summary-item:last-child {
                border-right: none;
            }
            .tb-summary-label {
                font-size: 0.75rem;
                font-weight: 600;
                color: #64748b;
                text-transform: uppercase;
                letter-spacing: 0.5px;
                margin-bottom: 4px;
            }
            .tb-summary-value {
                font-size: 1.5rem;
                font-weight: 700;
            }
            .tb-summary-value.debit { color: #059669; }
            .tb-summary-value.credit { color: #dc2626; }
            .tb-summary-value.balanced { color: #059669; }
            .tb-summary-value.unbalanced { color: #dc2626; }
            .tb-table-container {
                padding: 0;
            }
            .tb-table {
                width: 100%;
                border-collapse: collapse;
                font-size: 0.9rem;
            }
            .tb-table thead {
                background: #1e3a5f;
                color: white;
            }
            .tb-table thead th {
                padding: 14px 20px;
                font-weight: 600;
                text-transform: uppercase;
                font-size: 0.75rem;
                letter-spacing: 0.5px;
            }
            .tb-table thead th.text-right {
                text-align: right;
            }
            .tb-table tbody tr {
                border-bottom: 1px solid #e2e8f0;
                transition: background 0.15s ease;
            }
            .tb-table tbody tr:hover {
                background: #f8fafc;
            }
            .tb-table tbody td {
                padding: 12px 20px;
            }
            .tb-table tbody td.text-right {
                text-align: right;
                font-family: 'SF Mono', 'Consolas', monospace;
            }
            .tb-category-row {
                background: linear-gradient(90deg, #f1f5f9 0%, #fff 100%);
            }
            .tb-category-row td {
                font-weight: 700;
                color: #1e3a5f;
                padding: 14px 20px !important;
                font-size: 0.85rem;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }
            .tb-account-row td.acct-num {
                color: #64748b;
                font-family: 'SF Mono', 'Consolas', monospace;
                padding-left: 40px;
            }
            .tb-account-row td.acct-name {
                color: #334155;
            }
            .tb-account-row td.debit-value {
                color: #059669;
                font-weight: 500;
            }
            .tb-account-row td.credit-value {
                color: #dc2626;
                font-weight: 500;
            }
            .tb-total-row {
                background: linear-gradient(135deg, #1e3a5f 0%, #2d5a87 100%);
                color: white;
            }
            .tb-total-row td {
                padding: 16px 20px !important;
                font-weight: 700;
                font-size: 1rem;
            }
            .tb-total-row td.text-right {
                font-family: 'SF Mono', 'Consolas', monospace;
            }
            .tb-empty-cell {
                color: #cbd5e1;
            }
            .tb-footer {
                background: #f8fafc;
                padding: 16px 32px;
                display: flex;
                justify-content: space-between;
                align-items: center;
                border-top: 1px solid #e2e8f0;
            }
            .tb-footer-info {
                font-size: 0.8rem;
                color: #64748b;
            }
            .tb-balanced-badge {
                display: inline-flex;
                align-items: center;
                gap: 6px;
                padding: 6px 12px;
                border-radius: 20px;
                font-size: 0.8rem;
                font-weight: 600;
            }
            .tb-balanced-badge.balanced {
                background: #d1fae5;
                color: #059669;
            }
            .tb-balanced-badge.unbalanced {
                background: #fee2e2;
                color: #dc2626;
            }
        """),

        # Main Trial Balance container
        ui.div(
            # Header
            ui.div(
                ui.tags.h2("Trial Balance"),
                ui.div(ui.output_text("gl2_tb_date_display", inline=True), class_="tb-subtitle"),
                class_="tb-header"
            ),

            # Controls
            ui.div(
                ui.div(
                    ui.span("As of Date", class_="tb-control-label"),
                    ui.input_date("gl2_tb_as_of_date", None, value=date.today()),
                    class_="tb-control-group"
                ),
                ui.div(
                    ui.span("Group By", class_="tb-control-label"),
                    ui.input_select("gl2_tb_grouping", None, choices={
                        "account": "Individual Accounts",
                        "category": "Account Category"
                    }, selected="account", width="180px"),
                    class_="tb-control-group"
                ),
                ui.div(
                    ui.span("Currency", class_="tb-control-label"),
                    ui.input_select("gl2_tb_currency", None, choices={
                        "crypto": "Crypto Units",
                        "usd": "USD"
                    }, selected="crypto", width="140px"),
                    class_="tb-control-group"
                ),
                ui.div(style="flex: 1;"),
                ui.download_button(
                    "gl2_download_tb",
                    ui.HTML('<i class="bi bi-file-earmark-excel me-1"></i> Export'),
                    class_="btn-outline-primary"
                ),
                class_="tb-controls"
            ),

            # Summary row
            ui.div(
                ui.div(
                    ui.div("Total Debits", class_="tb-summary-label"),
                    ui.div(ui.output_text("gl2_tb_total_debits", inline=True), class_="tb-summary-value debit"),
                    class_="tb-summary-item"
                ),
                ui.div(
                    ui.div("Total Credits", class_="tb-summary-label"),
                    ui.div(ui.output_text("gl2_tb_total_credits", inline=True), class_="tb-summary-value credit"),
                    class_="tb-summary-item"
                ),
                ui.div(
                    ui.div("Balance Status", class_="tb-summary-label"),
                    ui.output_ui("gl2_tb_balance_status"),
                    class_="tb-summary-item"
                ),
                class_="tb-summary"
            ),

            # Table
            ui.div(
                ui.output_ui("gl2_trial_balance_display"),
                class_="tb-table-container"
            ),

            # Footer
            ui.div(
                ui.div(
                    ui.HTML('<i class="bi bi-info-circle me-1"></i>'),
                    "Accounts with zero balance are included for completeness",
                    class_="tb-footer-info"
                ),
                ui.output_ui("gl2_tb_balance_badge"),
                class_="tb-footer"
            ),

            class_="tb-container"
        ),

        class_="p-0 bg-light"
    )


def new_entry_tab():
    """New Entry tab - manual journal entry creation."""
    return ui.div(
        ui.layout_columns(
            # Entry form
            ui.card(
                ui.card_header(
                    ui.HTML('<i class="bi bi-plus-circle"></i> Create Manual Journal Entry')
                ),
                ui.card_body(
                    # Entry header
                    ui.layout_columns(
                        ui.input_date(
                            "gl2_new_entry_date",
                            "Entry Date",
                            value=date.today()
                        ),
                        ui.input_text(
                            "gl2_new_entry_description",
                            "Description",
                            placeholder="Enter journal entry description...",
                            width="100%"
                        ),
                        ui.input_select(
                            "gl2_new_entry_category",
                            "Category",
                            choices={
                                "manual_adjustment": "Manual Adjustment",
                                "accrual": "Accrual",
                                "reclassification": "Reclassification",
                                "correction": "Correction",
                                "closing_entry": "Closing Entry",
                                "adjusting_entry": "Adjusting Entry",
                                "other": "Other"
                            },
                            selected="manual_adjustment"
                        ),
                        col_widths=[3, 6, 3]
                    ),

                    ui.hr(),

                    # Entry lines header
                    ui.div(
                        ui.h6("Entry Lines", class_="mb-0"),
                        ui.tags.small("Debits must equal credits", class_="text-muted"),
                        class_="d-flex justify-content-between align-items-center mb-3"
                    ),

                    # Entry lines container
                    ui.output_ui("gl2_entry_lines_container"),

                    # Add line button
                    ui.input_action_button(
                        "gl2_add_entry_line",
                        ui.HTML('<i class="bi bi-plus-lg"></i> Add Line'),
                        class_="btn-outline-primary btn-sm mt-2"
                    ),

                    ui.hr(),

                    # Entry totals
                    ui.layout_columns(
                        ui.div(
                            ui.tags.small("Total Debits", class_="text-muted d-block"),
                            ui.span(class_="fs-4 fw-bold gl2-success"),
                            ui.output_text("gl2_new_entry_total_debits", inline=True),
                            class_="text-center"
                        ),
                        ui.div(
                            ui.tags.small("Total Credits", class_="text-muted d-block"),
                            ui.span(class_="fs-4 fw-bold gl2-danger"),
                            ui.output_text("gl2_new_entry_total_credits", inline=True),
                            class_="text-center"
                        ),
                        ui.div(
                            ui.tags.small("Difference", class_="text-muted d-block"),
                            ui.span(class_="fs-4 fw-bold"),
                            ui.output_text("gl2_new_entry_difference", inline=True),
                            class_="text-center"
                        ),
                        col_widths=[4, 4, 4],
                        class_="py-3 bg-light rounded"
                    ),

                    # Validation message
                    ui.output_ui("gl2_entry_validation_message"),

                    ui.hr(),

                    # Action buttons
                    ui.div(
                        ui.input_action_button(
                            "gl2_post_entry",
                            ui.HTML('<i class="bi bi-check-circle"></i> Post Entry'),
                            class_="btn-success me-2"
                        ),
                        ui.input_action_button(
                            "gl2_clear_entry",
                            ui.HTML('<i class="bi bi-x-circle"></i> Clear Form'),
                            class_="btn-outline-secondary"
                        ),
                        class_="d-flex"
                    ),
                )
            ),

            # Recent manual entries
            ui.card(
                ui.card_header(
                    ui.HTML('<i class="bi bi-clock-history"></i> Recent Manual Entries')
                ),
                ui.output_data_frame("gl2_recent_manual_entries"),
            ),
            col_widths=[8, 4]
        ),
        class_="p-3"
    )
