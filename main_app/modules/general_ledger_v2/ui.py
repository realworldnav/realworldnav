"""
General Ledger 2 UI Module

Provides a fresh GL interface with 4 tabs:
1. Journal Entries - View and filter all posted entries
2. Account Ledger - Account-specific view with running balances
3. Trial Balance - Standard TB format with account groupings
4. New Entry - Manual journal entry creation connected to COA
"""

from shiny import ui
from datetime import date, timedelta


def general_ledger_v2_ui():
    """Main GL2 UI with 4 tabs."""
    return ui.page_fluid(
        ui.h2("General Ledger 2", class_="mt-3"),
        ui.p("Fresh implementation with full accounting functionality", class_="text-muted"),

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
        )
    )


def journal_entries_tab():
    """Journal Entries tab - view and filter all posted entries."""
    return ui.div(
        # Filters row
        ui.layout_columns(
            ui.card(
                ui.card_header("Filters"),
                ui.layout_columns(
                    ui.input_date_range(
                        "gl2_date_range",
                        "Date Range",
                        start=date.today() - timedelta(days=365),
                        end=date.today()
                    ),
                    ui.input_selectize(
                        "gl2_account_filter",
                        "Account",
                        choices={"": "All Accounts"},
                        selected=""
                    ),
                    ui.input_selectize(
                        "gl2_category_filter",
                        "Category",
                        choices={
                            "": "All Categories",
                            "nft_lending": "NFT Lending",
                            "token_swap": "Token Swap",
                            "fee": "Fees",
                            "transfer": "Transfers"
                        },
                        selected=""
                    ),
                    ui.input_text(
                        "gl2_search",
                        "Search",
                        placeholder="TX hash or description..."
                    ),
                    col_widths=[3, 3, 3, 3]
                ),
                ui.layout_columns(
                    ui.input_action_button(
                        "gl2_apply_filters",
                        "Apply Filters",
                        class_="btn-primary"
                    ),
                    ui.input_action_button(
                        "gl2_clear_filters",
                        "Clear Filters",
                        class_="btn-outline-secondary"
                    ),
                    ui.input_action_button(
                        "gl2_refresh",
                        "Refresh Data",
                        class_="btn-outline-primary"
                    ),
                    col_widths=[2, 2, 2]
                ),
                class_="mb-3"
            ),
            col_widths=[12]
        ),

        # Summary cards
        ui.layout_columns(
            ui.value_box(
                title="Total Entries",
                value=ui.output_text("gl2_total_entries"),
                showcase=ui.HTML('<i class="bi bi-journal-text"></i>'),
                theme="primary"
            ),
            ui.value_box(
                title="Total Debits",
                value=ui.output_text("gl2_total_debits"),
                showcase=ui.HTML('<i class="bi bi-arrow-up-circle"></i>'),
                theme="success"
            ),
            ui.value_box(
                title="Total Credits",
                value=ui.output_text("gl2_total_credits"),
                showcase=ui.HTML('<i class="bi bi-arrow-down-circle"></i>'),
                theme="info"
            ),
            ui.value_box(
                title="Balance Check",
                value=ui.output_text("gl2_balance_check"),
                showcase=ui.HTML('<i class="bi bi-check-circle"></i>'),
                theme="secondary"
            ),
            col_widths=[3, 3, 3, 3]
        ),

        # Journal entries table
        ui.card(
            ui.card_header(
                ui.div(
                    "Journal Entries",
                    ui.download_button(
                        "gl2_download_je",
                        "Export to Excel",
                        class_="btn-sm btn-outline-success float-end"
                    ),
                    class_="d-flex justify-content-between align-items-center"
                )
            ),
            ui.output_data_frame("gl2_journal_entries_table"),
            class_="mt-3"
        ),

        # Dynamic dropdown updates
        ui.output_ui("gl2_update_account_dropdown"),
        ui.output_ui("gl2_update_category_dropdown"),

        class_="p-3"
    )


def account_ledger_tab():
    """Account Ledger tab - account-specific view with running balance."""
    return ui.div(
        # Account selector
        ui.card(
            ui.card_header("Select Account"),
            ui.layout_columns(
                ui.input_selectize(
                    "gl2_ledger_account",
                    "Account",
                    choices={"": "Select an account..."},
                    selected=""
                ),
                ui.input_date_range(
                    "gl2_ledger_date_range",
                    "Date Range",
                    start=date.today() - timedelta(days=365),
                    end=date.today()
                ),
                ui.input_action_button(
                    "gl2_load_ledger",
                    "Load Ledger",
                    class_="btn-primary mt-4"
                ),
                col_widths=[5, 5, 2]
            ),
            class_="mb-3"
        ),

        # Account summary
        ui.output_ui("gl2_account_summary"),

        # Account ledger table with running balance
        ui.card(
            ui.card_header(
                ui.div(
                    ui.output_text("gl2_ledger_account_name"),
                    ui.download_button(
                        "gl2_download_ledger",
                        "Export",
                        class_="btn-sm btn-outline-success float-end"
                    ),
                    class_="d-flex justify-content-between align-items-center"
                )
            ),
            ui.output_data_frame("gl2_account_ledger_table")
        ),

        # Dynamic dropdown update
        ui.output_ui("gl2_update_ledger_account_dropdown"),

        class_="p-3"
    )


def trial_balance_tab():
    """Trial Balance tab - standard TB format with account groupings."""
    return ui.div(
        # TB controls
        ui.card(
            ui.card_header("Trial Balance Settings"),
            ui.layout_columns(
                ui.input_date(
                    "gl2_tb_as_of_date",
                    "As of Date",
                    value=date.today()
                ),
                ui.input_select(
                    "gl2_tb_grouping",
                    "Group By",
                    choices={
                        "category": "Account Category",
                        "account": "Individual Account"
                    },
                    selected="category"
                ),
                ui.input_action_button(
                    "gl2_generate_tb",
                    "Generate Trial Balance",
                    class_="btn-primary mt-4"
                ),
                col_widths=[4, 4, 4]
            ),
            class_="mb-3"
        ),

        # TB summary
        ui.layout_columns(
            ui.value_box(
                title="Total Debits",
                value=ui.output_text("gl2_tb_total_debits"),
                showcase=ui.HTML('<i class="bi bi-arrow-up-circle"></i>'),
                theme="success"
            ),
            ui.value_box(
                title="Total Credits",
                value=ui.output_text("gl2_tb_total_credits"),
                showcase=ui.HTML('<i class="bi bi-arrow-down-circle"></i>'),
                theme="info"
            ),
            ui.value_box(
                title="Difference",
                value=ui.output_text("gl2_tb_difference"),
                showcase=ui.HTML('<i class="bi bi-calculator"></i>'),
                theme="secondary"
            ),
            col_widths=[4, 4, 4]
        ),

        # Trial balance table
        ui.card(
            ui.card_header(
                ui.div(
                    "Trial Balance",
                    ui.download_button(
                        "gl2_download_tb",
                        "Export to Excel",
                        class_="btn-sm btn-outline-success float-end"
                    ),
                    class_="d-flex justify-content-between align-items-center"
                )
            ),
            ui.output_data_frame("gl2_trial_balance_table"),
            class_="mt-3"
        ),

        class_="p-3"
    )


def new_entry_tab():
    """New Entry tab - manual journal entry creation."""
    return ui.div(
        ui.card(
            ui.card_header("Create Manual Journal Entry"),

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
                    placeholder="Enter journal entry description..."
                ),
                ui.input_select(
                    "gl2_new_entry_category",
                    "Category",
                    choices={
                        "manual_adjustment": "Manual Adjustment",
                        "accrual": "Accrual",
                        "reclassification": "Reclassification",
                        "correction": "Correction",
                        "other": "Other"
                    },
                    selected="manual_adjustment"
                ),
                col_widths=[3, 6, 3]
            ),

            ui.hr(),

            # Entry lines header
            ui.h5("Entry Lines"),
            ui.p("Add debit and credit lines. Total debits must equal total credits.", class_="text-muted"),

            # Entry lines container (dynamic)
            ui.output_ui("gl2_entry_lines_container"),

            # Add line button
            ui.input_action_button(
                "gl2_add_entry_line",
                "Add Line",
                class_="btn-outline-primary mt-2"
            ),

            ui.hr(),

            # Entry totals
            ui.layout_columns(
                ui.div(
                    ui.h6("Total Debits"),
                    ui.output_text("gl2_new_entry_total_debits", inline=True),
                    class_="text-end"
                ),
                ui.div(
                    ui.h6("Total Credits"),
                    ui.output_text("gl2_new_entry_total_credits", inline=True),
                    class_="text-end"
                ),
                ui.div(
                    ui.h6("Difference"),
                    ui.output_text("gl2_new_entry_difference", inline=True),
                    class_="text-end"
                ),
                col_widths=[4, 4, 4]
            ),

            # Validation message
            ui.output_ui("gl2_entry_validation_message"),

            ui.hr(),

            # Action buttons
            ui.layout_columns(
                ui.input_action_button(
                    "gl2_post_entry",
                    "Post Entry",
                    class_="btn-success"
                ),
                ui.input_action_button(
                    "gl2_clear_entry",
                    "Clear Form",
                    class_="btn-outline-secondary"
                ),
                col_widths=[2, 2]
            ),

            class_="p-3"
        ),

        # Recent manual entries
        ui.card(
            ui.card_header("Recent Manual Entries"),
            ui.output_data_frame("gl2_recent_manual_entries"),
            class_="mt-3"
        ),

        class_="p-3"
    )
