from shiny import reactive, render, req, ui
import pandas as pd
from shinywidgets import render_plotly, output_widget
from shiny.render import DataGrid
from ...s3_utils import load_GL_file, load_WALLET_file, load_COA_file, save_GL_file
from plotly.graph_objects import Table, Figure

DEFAULT_COLUMNS = [
    'transaction_id', "date", "transaction_type", "wallet_id", "cryptocurrency",
    "account_name", "debit_crypto", "credit_crypto", "eth_usd_price",
    "debit_USD", "credit_USD", "hash"
]

@reactive.calc
def coa_choices():
    print("🔃 Loading COA choices")
    df = load_COA_file()
    return ["All Accounts"] + sorted(df["GL_Acct_Name"].dropna().astype(str).unique())

def register_outputs(output, input, session, selected_fund):
    print("🚀 register_outputs() called")
    edited_df_store = reactive.value(None)
    selected_row_store = reactive.value(None)

    @reactive.calc
    def wallet_choices():
        print("🔃 Loading wallet choices")
        df = load_WALLET_file()
        fund_id = selected_fund()
        df = df[df["fund_id"] == fund_id].copy()
        df["wallet_address"] = df["wallet_address"].str.lower().str.strip()
        df["friendly_name"] = df["friendly_name"].fillna(df["wallet_address"])
        return ["All Wallets"] + sorted(df["friendly_name"].dropna().unique())
    @reactive.calc
    def df_gl():
        print("📦 Loading GL from S3")
        df = load_GL_file()


        print("🔍 Sample transaction_id values:\n", df["transaction_id"].head().tolist())
        print("📦 Loading GL from S3")
        df = load_GL_file()
        wallet_df = load_WALLET_file()
        coa_df = load_COA_file()
        fund_id = selected_fund()

        wallet_df = wallet_df[wallet_df["fund_id"] == fund_id].copy()
        wallet_df["wallet_address"] = wallet_df["wallet_address"].str.lower().str.strip()
        wallet_df["friendly_name"] = wallet_df["friendly_name"].fillna(wallet_df["wallet_address"])

        df["wallet_id"] = df["wallet_id"].str.lower().str.strip()
        df = df.merge(wallet_df.rename(columns={"wallet_address": "wallet_id"}), on="wallet_id", how="inner")
        df["wallet_id"] = df["friendly_name"].fillna(df["wallet_id"])
        df.drop(columns=["friendly_name"], inplace=True)
        if input.gl_account_filter() != "All Accounts":
            print(f"🔍 Filtering by GL account: {input.gl_account_filter()}")
            matching = coa_df[coa_df["GL_Acct_Name"] == input.gl_account_filter()]["account_name"].dropna().unique()
            df = df[df["account_name"].isin(matching)] if len(matching) else df.iloc[0:0]

        if input.wallet_filter() != "All Wallets":
            print(f"🔍 Filtering by wallet: {input.wallet_filter()}")
            df = df[df["wallet_id"] == input.wallet_filter()]

        print("✅ Final GL shape:", df.shape)
        return df

    @reactive.effect
    @reactive.event(df_gl)
    def cache_original():
        print("📤 Caching original GL DataFrame")
        edited_df_store.set(df_gl().copy().head(100))

    @reactive.calc
    def selected_columns():
        print("🔀 Resolving selected columns")
        if input.reset_columns() > input.apply_columns():
            return DEFAULT_COLUMNS
        if input.apply_columns() == 0 and input.reset_columns() == 0:
            return DEFAULT_COLUMNS
        return input.gl_column_selector() or DEFAULT_COLUMNS

    @output
    @render_plotly
    def gl_view_plotly():
        print("📊 Rendering Plotly GL view")
        df = edited_df_store.get()
        if df is None or df.empty:
            df = df_gl().copy().head(100)
        df = df[list(selected_columns())].astype(str)

        fig = Figure(
            data=[Table(
                columnwidth=[1] * len(df.columns),
                header=dict(values=[f"<b>{col}</b>" for col in df.columns],
                            fill_color="#f2f2f7", font=dict(color="#000", size=13), align="left"),
                cells=dict(values=[df[col].tolist() for col in df.columns],
                           fill_color="#fff", font=dict(color="#000", size=12), align="left")
            )],
            layout=dict(margin=dict(l=0, r=0, t=0, b=0), height=500)
        )
        return fig
    @reactive.effect
    def capture_selected_row():
        selection = gl_view_editable.cell_selection()
        print("📥 DataGrid selection:", selection)

        if not isinstance(selection, dict) or "rows" not in selection or not selection["rows"]:
            selected_row_store.set(None)
            print("⚠️ No row selected")
            return

        row_idx = selection["rows"][0]
        df = edited_df_store.get()
        if df is None or row_idx >= len(df):
            selected_row_store.set(None)
            print("⚠️ Invalid row index")
            return

        selected_row = df.iloc[row_idx].to_dict()
        print("🧩 selected_row_data():", selected_row)
        selected_row_store.set(selected_row)


    @output
    @render.data_frame
    def gl_view_editable():
        print("🧾 Rendering DataGrid")
        df = edited_df_store.get()
        if df is None or df.empty:
            df = df_gl().copy().head(100)
        df = df[list(selected_columns())].astype(str)
        return DataGrid(df, editable=False, filters=True, selection_mode="row")
    
    @output
    @render.ui
    def selected_transaction_editor():
        row = selected_row_store.get()
        print("🧩 selected_row_data():", row)
        if not row:
            return ui.p("⚠️ Select a transaction to edit.")

        elements = []
        for col, val in row.items():
            input_id = f"edit_{col}"
            val_str = str(val)
            try:
                if col.lower() in ("amount", "net_debit_credit_crypto") and val_str.replace(".", "", 1).isdigit():
                    elements.append(ui.input_numeric(input_id, col, float(val)))
                elif "date" in col.lower():
                    # Don't strip the time — preserve full timestamp
                    elements.append(ui.input_text(input_id, col, value=val_str))

                elif col.lower() == "account_name":
                    choices = sorted(load_COA_file()["GL_Acct_Name"].dropna().unique())
                    elements.append(ui.input_selectize(input_id, col, choices=choices, selected=val_str))
                else:
                    elements.append(ui.input_text(input_id, col, val_str))
            except Exception as e:
                print(f"❌ Failed rendering input for {col}:", e)
                elements.append(ui.input_text(input_id, col, val_str))

        elements.append(ui.input_action_button("submit_edit", "✅ Save Edit", class_="btn-success mt-3"))
        return ui.layout_column_wrap(*elements, width="400px")

    @reactive.effect
    @reactive.event(input.submit_edit)
    def on_save_edit():
        print("💾 Save button clicked")
        print("🔍 selected_row_store:", selected_row_store.get())   
        df = edited_df_store.get()
        selected = selected_row_store.get()

        if df is None or df.empty or not selected:
            print("⚠️ Nothing to update.")
            return

        txn_id = selected.get("transaction_id")
        if not txn_id:
            print("❌ No transaction_id found in selected row")
            return

        match = df[df["transaction_id"] == txn_id]
        if match.empty:
            print(f"❌ No match for transaction_id {txn_id} in edited_df_store")
            return

        idx_match = df[df["transaction_id"] == txn_id]

        if idx_match.empty:
            print(f"❌ No match for transaction_id {txn_id}")
            return

        idx = idx_match.index[0]

        
        for col in selected:
            shiny_id = f"edit_{col}"
            if hasattr(input, shiny_id):
                try:
                    new_val = getattr(input, shiny_id)()
                    df.at[idx, col] = new_val
                    print(f"  🔧 {col} → {new_val}")
                except Exception as e:
                    print(f"❌ Failed updating {col}:", e)

        edited_df_store.set(df.copy())
        selected_row_store.set(None)
        ui.notification_show("✅ Entry updated locally", duration=3000)


    @reactive.effect
    @reactive.event(input.save_gl_changes)
    def save_changes_to_s3():
        print("☁️ Uploading GL to S3")
        try:
            df = edited_df_store.get()
            if df is not None and not df.empty:
                save_GL_file(df)
                ui.notification_show("✅ GL saved to S3", duration=3000)
                print("✅ Saved to S3")
            else:
                print("⚠️ Nothing to save")
                ui.notification_show("⚠️ Nothing to save", duration=3000)
        except Exception as e:
            print("❌ Failed to save:", e)
            ui.notification_show(f"❌ Failed to save: {e}", duration=5000)

    @output
    @render.ui
    def gl_view_router():
        print("🔁 Routing GL view")
        return ui.output_data_frame("gl_view_editable") if input.edit_mode() else output_widget("gl_view_plotly")

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
        return ui.update_selectize("gl_column_selector", choices=df.columns.tolist(), selected=selected_columns())

    @reactive.effect
    @reactive.event(input.undo_gl_changes)
    def undo_changes():
        print("↩️ Undoing GL edits")
        edited_df_store.set(df_gl().copy().head(100))
        ui.notification_show("↩️ Changes reverted", duration=3000)
