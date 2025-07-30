from shiny import reactive, render, req, ui
import pandas as pd
from shinywidgets import render_plotly  # ← optional: force shinywidgets plugin
from ...s3_utils import load_GL_file, load_WALLET_file, load_COA_file, save_GL_file



@reactive.calc
def coa_choices():
    print("DEBUG — Loading COA...")
    df = load_COA_file()
    choices = sorted(df["GL_Acct_Name"].dropna().astype(str).unique().tolist())
    return ["All Accounts"] + choices

def register_outputs(output, input, session, selected_fund):
    @reactive.calc
    def wallet_choices():
        wallet_df = load_WALLET_file()
        fund_id = selected_fund()
        print(f"DEBUG — wallet_choices for fund_id: {fund_id}")

        wallet_df["wallet_address"] = wallet_df["wallet_address"].str.lower().str.strip()
        wallet_df = wallet_df[wallet_df["fund_id"] == fund_id]

        wallet_df["friendly_name"] = wallet_df["friendly_name"].fillna(wallet_df["wallet_address"])
        choices = sorted(wallet_df["friendly_name"].dropna().unique().tolist())
        return ["All Wallets"] + choices


    @reactive.calc
    def df_gl():
        print("DEBUG — Loading GL and Wallet metadata...")
        df = load_GL_file()
        wallet_df = load_WALLET_file()
        coa_df = load_COA_file()

        # === Normalize and filter wallet_df by selected fund ===
        fund_id = selected_fund()
        wallet_df["wallet_address"] = wallet_df["wallet_address"].str.lower().str.strip()
        wallet_df = wallet_df[wallet_df["fund_id"] == fund_id]

        wallet_df["friendly_name"] = wallet_df["friendly_name"].fillna(wallet_df["wallet_address"])

        # DEBUG
        print(f"DEBUG — Fund {fund_id} has {len(wallet_df)} wallets")

        # === Merge wallet metadata into GL ===
        df["wallet_id"] = df["wallet_id"].str.lower().str.strip()
        df = df.merge(wallet_df.rename(columns={"wallet_address": "wallet_id"}), on="wallet_id", how="inner")

        # Always scope GL to fund's wallets (already done by merge)
        df["wallet_id"] = df["friendly_name"].fillna(df["wallet_id"])
        df.drop(columns=["friendly_name"], inplace=True)

        # === Apply GL Account Name Filter ===
        gl_selected = input.gl_account_filter()
        if gl_selected != "All Accounts":
            matching_names = coa_df[coa_df["GL_Acct_Name"] == gl_selected]["account_name"].dropna().unique().tolist()
            print(f"DEBUG — Matching GL accounts: {matching_names}")
            if matching_names:
                df = df[df["account_name"].isin(matching_names)]
            else:
                df = df.iloc[0:0]

        # === Apply Wallet Filter ===
        wallet_selected = input.wallet_filter()
        if wallet_selected != "All Wallets":
            df = df[df["wallet_id"] == wallet_selected]

        return df
    
    original_df = reactive.value(None)
    @reactive.effect
    @reactive.event(df_gl)
    def cache_original():
        df = df_gl().copy()
        print("DEBUG — Caching original GL preview")
        edited_df_store.set(df.head(100).copy())


    DEFAULT_COLUMNS = ["date", "wallet_id", "account_name", "net_debit_credit_crypto"]

    @reactive.calc
    def selected_columns():
        apply_clicks = input.apply_columns()
        reset_clicks = input.reset_columns()

        # Case 1: Reset button clicked more recently
        if reset_clicks > apply_clicks:
            print("DEBUG — Reset clicked, using default columns")
            return DEFAULT_COLUMNS

        # Case 2: First load (neither clicked yet)
        if apply_clicks == 0 and reset_clicks == 0:
            print("DEBUG — First load, applying default columns")
            return DEFAULT_COLUMNS

        # Case 3: Apply clicked
        cols = input.gl_column_selector()
        print(f"DEBUG — Apply clicked, using: {cols}")
        return cols if cols else DEFAULT_COLUMNS
    
    @output
    @render.data_frame
    def gl_view():
        df = edited_df_store.get()

        # Fallback if reactive store is not yet set
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            print("⚠️ edited_df_store is empty — falling back to df_gl()")
            df = df_gl().copy()
            if df.empty:
                print("⚠️ df_gl() is also empty")
                return pd.DataFrame(columns=DEFAULT_COLUMNS)

        # Get selected columns safely
        cols_to_show = selected_columns()
        if not isinstance(cols_to_show, (list, tuple)) or not all(isinstance(c, str) for c in cols_to_show):
            cols_to_show = DEFAULT_COLUMNS

        cols_to_show = [col for col in cols_to_show if col in df.columns]

        # Ensure those columns are all strings for display
        for col in cols_to_show:
            df[col] = df[col].astype(str)

        preview = df[cols_to_show].head(100)

        return render.DataGrid(
            preview,
            width="100%",
            height="500px",
            filters=True,
            editable=True,
            selection_mode="rows"
        )

    @output
    @render.ui
    def update_gl_dropdown():
        return ui.update_selectize("gl_account_filter", choices=coa_choices())
    @output
    @render.ui
    def update_wallet_dropdown():
        return ui.update_selectize("wallet_filter", choices=wallet_choices())
    @output
    @render.ui
    def update_gl_column_selector():
        df = df_gl()
        req(df is not None and not df.empty)
        all_cols = df.columns.tolist()
        current_selection = selected_columns()
        return ui.update_selectize(
            "gl_column_selector",
            choices=all_cols,
            selected=current_selection
        )
    
    edited_df_store = reactive.value(None)
    
    @reactive.effect
    @reactive.event(input.save_gl_changes)
    def save_gl_edits():
        print("🟡 Save button clicked")
        edited_rows = input.gl_view()
        print(f"DEBUG — edited_rows type: {type(edited_rows)}")

        if not edited_rows or not isinstance(edited_rows, list):
            print("⚠️ Nothing to save — grid is empty or invalid")
            return

        df_edited = pd.DataFrame(edited_rows)
        print(f"✅ Edited DataFrame shape: {df_edited.shape}")
        print(df_edited.head(3))

        save_GL_file(df_edited)

        ui.notification_show("✅ General Ledger saved to S3!", duration=4000, type="message")

    @reactive.effect
    @reactive.event(input.undo_gl_changes)
    def undo_changes():
        df = df_gl().copy()
        print("🔁 Reverting GL to original snapshot")
        edited_df_store.set(df.head(100).copy())
        ui.notification_show("↩️ Changes reverted", duration=3000)

# === UI ===
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
            ui.output_data_frame("gl_view")
        ),
        ui.input_action_button("save_gl_changes", "💾 Save Changes", class_="mt-4 btn-success"),
        ui.input_action_button("undo_gl_changes", "↩️ Undo Changes", class_="mt-4 btn-warning"),


    )
