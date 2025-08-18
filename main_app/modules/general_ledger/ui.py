from shiny import ui, reactive, render

def crypto_tracker_tab_content():
    """Crypto tracker interface with Overview, FIFO, and Fetcher tabs"""
    # Import here to avoid circular imports
    from .crypto_tracker import crypto_tracker_ui
    
    return crypto_tracker_ui()

def general_ledger_ui():
    print("DEBUG — general_ledger_ui() function called")

    return ui.page_fluid(
        ui.h2("General Ledger", class_="mt-3"),
        
        # Navigation tabs for different GL modules
        ui.navset_card_tab(
            ui.nav_panel(
                ui.HTML('<i class="fas fa-book"></i> Journal Entries'),
                journal_entries_content()
            ),
            ui.nav_panel(
                ui.HTML('<i class="fas fa-coins"></i> Crypto Tracker'),
                crypto_tracker_tab_content()
            ),
            ui.nav_panel(
                ui.HTML('<i class="fas fa-chart-bar"></i> GL Analytics'), 
                gl_analytics_content()
            )
        )
    )

def journal_entries_content():
    """Traditional journal entries interface"""
    return ui.div(
        # --- Filters: Account & Wallet ---
        ui.layout_columns(
            ui.input_selectize(
                "gl_account_filter",
                "Filter by GL Account Name",
                choices=[],
                selected="All Accounts"
            ),
            ui.input_selectize(
                "wallet_filter",
                "Filter by Wallet",
                choices=[],
                selected="All Wallets"
            ),
        ),

        # --- Column Selector + Buttons ---
        ui.layout_columns(
            ui.input_selectize(
                "gl_column_selector",
                "Show Columns",
                choices=[],       # filled dynamically
                selected=[],      # defaults set server-side
                multiple=True
            ),
            ui.input_action_button("apply_columns", "Apply Columns", class_="mt-4"),
            ui.input_action_button("reset_columns", "Reset Columns", class_="mt-4")
        ),

        # --- Dynamic Dropdown Injection ---
        ui.output_ui("update_gl_dropdown"),
        ui.output_ui("update_wallet_dropdown"),
        ui.output_ui("update_gl_column_selector"),

        # --- Add New Entry Management ---
        ui.output_ui("add_gl_trigger"),

        # --- GL Table Output ---
        ui.card(
            ui.card_header("Preview of General Ledger"),
            ui.output_ui("gl_view_router"),
        ),

        # --- Action Buttons ---
        ui.input_switch("edit_mode", "Edit Journal Entries", False),
        ui.input_action_button("save_gl_changes", "Save Changes", class_="mt-4 btn-success"),
        ui.input_action_button("undo_gl_changes", "Undo Changes", class_="mt-4 btn-warning"),

        # --- Selected Transaction Editor ---
        ui.card(
            ui.card_header("Edit Selected Transaction"),
            ui.output_ui("selected_transaction_editor")
        ),

        # Apple-inspired CSS styling matching Chart of Accounts
    )

def gl_analytics_content():
    """GL Analytics and reporting interface"""
    # Import here to avoid circular imports
    from .gl_analytics import gl_analytics_ui
    
    return gl_analytics_ui()
