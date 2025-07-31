from shiny import ui, reactive, render
def general_ledger_ui():
    print("DEBUG — general_ledger_ui() function called")

    return ui.page_fluid(
        ui.h2("📘 General Ledger", class_="mt-3"),

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

        # --- GL Table Output ---
        ui.card(
            ui.card_header("Preview of General Ledger"),
            ui.output_ui("gl_view_router"),
        ),

        # --- Action Buttons ---
        ui.input_switch("edit_mode", "📝 Edit Journal Entries", False),
        ui.input_action_button("save_gl_changes", "💾 Save Changes", class_="mt-4 btn-success"),
        ui.input_action_button("undo_gl_changes", "↩️ Undo Changes", class_="mt-4 btn-warning"),

        # --- Selected Transaction Editor ---
        ui.card(
            ui.card_header("🛠️ Edit Selected Transaction"),
            ui.output_ui("selected_transaction_editor")
        )
    )
