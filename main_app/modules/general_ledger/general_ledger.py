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
    edited_df_store = reactive.value(None)
    selected_row_store = reactive.value(None)
    refresh_trigger = reactive.Value(0)  # Counter to trigger data refresh

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
        # Add dependency on refresh trigger to force refresh after adds/deletes
        refresh_trigger()
        df = load_GL_file()


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
            return ui.p("Select a transaction to edit.")

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
                    elements.append(ui.div(ui.input_selectize(input_id, col, choices=choices, selected=val_str), class_="custom-dropdown"))
                else:
                    elements.append(ui.input_text(input_id, col, val_str))
            except Exception as e:
                print(f"❌ Failed rendering input for {col}:", e)
                elements.append(ui.input_text(input_id, col, val_str))

        elements.append(ui.input_action_button("submit_edit", "Save Edit", class_="btn-success mt-3 me-2"))
        elements.append(ui.input_action_button("delete_gl_entry", "Delete Entry", class_="btn-danger mt-3"))
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
        ui.notification_show("Entry updated locally", duration=3000)


    @reactive.effect
    @reactive.event(input.save_gl_changes)
    def save_changes_to_s3():
        print("☁️ Uploading GL to S3")
        try:
            df = edited_df_store.get()
            if df is not None and not df.empty:
                save_GL_file(df)
                ui.notification_show("GL saved to S3", duration=3000)
                print("✅ Saved to S3")
            else:
                print("⚠️ Nothing to save")
                ui.notification_show("Nothing to save", duration=3000)
        except Exception as e:
            print("❌ Failed to save:", e)
            ui.notification_show(f"Failed to save: {e}", duration=5000)

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
        ui.notification_show("Changes reverted", duration=3000)

    # === Add New GL Entry Modal Logic ===
    @reactive.effect
    @reactive.event(input.show_add_gl_modal)
    def show_add_gl_modal():
        """Show the add new GL entry modal"""
        # Get available choices for dropdowns
        coa_df = load_COA_file()
        wallet_df = load_WALLET_file()
        fund_id = selected_fund()
        
        # Filter wallets by fund
        fund_wallets = wallet_df[wallet_df["fund_id"] == fund_id].copy()
        fund_wallets["wallet_address"] = fund_wallets["wallet_address"].str.lower().str.strip()
        fund_wallets["friendly_name"] = fund_wallets["friendly_name"].fillna(fund_wallets["wallet_address"])
        wallet_choices = {row["wallet_address"]: row["friendly_name"] for _, row in fund_wallets.iterrows()}
        
        # Get account choices
        account_choices = {name: name for name in sorted(coa_df["account_name"].dropna().unique())}
        
        m = ui.modal(
            ui.div(
                ui.h4(ui.HTML('<i class="bi bi-plus-lg me-2"></i>Add New Journal Entry'), class_="mb-4"),
                
                # Basic Transaction Information Section
                ui.h6("Basic Transaction Information", class_="mt-3 mb-2", style="color: #6c757d; border-bottom: 1px solid #eee; padding-bottom: 0.5rem;"),
                ui.layout_columns(
                    ui.input_date(
                        "new_date",
                        "Date:",
                        value=None
                    ),
                    ui.input_text(
                        "new_transaction_type",
                        "Transaction Type:",
                        value=""
                    ),
                    col_widths=[6, 6]
                ),
                ui.layout_columns(
                    ui.input_text(
                        "new_fund_id",
                        "Fund ID:",
                        value=selected_fund()
                    ),
                    ui.div(
                        ui.input_selectize(
                            "new_wallet_id",
                            "Wallet:",
                            wallet_choices,
                            selected=list(wallet_choices.keys())[0] if wallet_choices else ""
                        ),
                        class_="custom-dropdown"
                    ),
                    col_widths=[6, 6]
                ),
                ui.layout_columns(
                    ui.input_text(
                        "new_cryptocurrency",
                        "Cryptocurrency:",
                        value="ETH"
                    ),
                    ui.div(
                        ui.input_selectize(
                            "new_account_name",
                            "Account Name:",
                            account_choices,
                            selected=list(account_choices.keys())[0] if account_choices else ""
                        ),
                        class_="custom-dropdown"
                    ),
                    col_widths=[6, 6]
                ),
                
                # Crypto and USD Amounts Section
                ui.h6("Crypto and USD Amounts", class_="mt-4 mb-2", style="color: #6c757d; border-bottom: 1px solid #eee; padding-bottom: 0.5rem;"),
                ui.layout_columns(
                    ui.input_numeric(
                        "new_debit_crypto",
                        "Debit Crypto:",
                        value=0.0,
                        min=0
                    ),
                    ui.input_numeric(
                        "new_credit_crypto",
                        "Credit Crypto:",
                        value=0.0,
                        min=0
                    ),
                    col_widths=[6, 6]
                ),
                ui.layout_columns(
                    ui.input_numeric(
                        "new_eth_usd_price",
                        "ETH USD Price:",
                        value=0.0,
                        min=0
                    ),
                    ui.input_numeric(
                        "new_net_debit_credit_crypto",
                        "Net Debit Credit Crypto:",
                        value=0.0
                    ),
                    col_widths=[6, 6]
                ),
                ui.layout_columns(
                    ui.input_numeric(
                        "new_debit_usd",
                        "Debit USD:",
                        value=0.0,
                        min=0
                    ),
                    ui.input_numeric(
                        "new_credit_usd",
                        "Credit USD:",
                        value=0.0,
                        min=0
                    ),
                    col_widths=[6, 6]
                ),
                ui.div(
                    ui.input_numeric(
                        "new_net_debit_credit_usd",
                        "Net Debit Credit USD:",
                        value=0.0
                    ),
                    style="width: 50%;"
                ),
                
                # Transaction Details Section
                ui.h6("Transaction Details", class_="mt-4 mb-2", style="color: #6c757d; border-bottom: 1px solid #eee; padding-bottom: 0.5rem;"),
                ui.layout_columns(
                    ui.input_text(
                        "new_hash",
                        "Transaction Hash:",
                        value=""
                    ),
                    ui.input_text(
                        "new_contract_address",
                        "Contract Address:",
                        value=""
                    ),
                    col_widths=[6, 6]
                ),
                ui.layout_columns(
                    ui.input_text(
                        "new_from",
                        "From:",
                        value=""
                    ),
                    ui.input_text(
                        "new_to",
                        "To:",
                        value=""
                    ),
                    col_widths=[6, 6]
                ),
                ui.layout_columns(
                    ui.input_text(
                        "new_event",
                        "Event:",
                        value=""
                    ),
                    ui.input_text(
                        "new_function",
                        "Function:",
                        value=""
                    ),
                    col_widths=[6, 6]
                ),
                
                # Partner and Source Information Section
                ui.h6("Partner and Source Information", class_="mt-4 mb-2", style="color: #6c757d; border-bottom: 1px solid #eee; padding-bottom: 0.5rem;"),
                ui.layout_columns(
                    ui.input_text(
                        "new_limited_partner_id",
                        "Limited Partner ID:",
                        value=""
                    ),
                    ui.input_text(
                        "new_source_file",
                        "Source File:",
                        value=""
                    ),
                    col_widths=[6, 6]
                ),
                ui.layout_columns(
                    ui.input_text(
                        "new_platform",
                        "Platform:",
                        value=""
                    ),
                    ui.input_text(
                        "new_counterparty_fund_id",
                        "Counterparty Fund ID:",
                        value=""
                    ),
                    col_widths=[6, 6]
                ),
                
                # Loan Information Section
                ui.h6("Loan Information", class_="mt-4 mb-2", style="color: #6c757d; border-bottom: 1px solid #eee; padding-bottom: 0.5rem;"),
                ui.layout_columns(
                    ui.input_text(
                        "new_loan_id",
                        "Loan ID:",
                        value=""
                    ),
                    ui.input_text(
                        "new_lender",
                        "Lender:",
                        value=""
                    ),
                    col_widths=[6, 6]
                ),
                ui.layout_columns(
                    ui.input_text(
                        "new_borrower",
                        "Borrower:",
                        value=""
                    ),
                    ui.input_text(
                        "new_payable_currency",
                        "Payable Currency:",
                        value=""
                    ),
                    col_widths=[6, 6]
                ),
                ui.layout_columns(
                    ui.input_text(
                        "new_collateral_address",
                        "Collateral Address:",
                        value=""
                    ),
                    ui.input_text(
                        "new_token_id",
                        "Token ID:",
                        value=""
                    ),
                    col_widths=[6, 6]
                ),
                ui.layout_columns(
                    ui.input_text(
                        "new_collateral_id",
                        "Collateral ID:",
                        value=""
                    ),
                    ui.input_numeric(
                        "new_annual_interest_rate",
                        "Annual Interest Rate:",
                        value=0.0,
                        min=0
                    ),
                    col_widths=[6, 6]
                ),
                ui.input_date(
                    "new_loan_due_date",
                    "Loan Due Date:",
                    value=None
                ),
                
                # Principal and Interest Section
                ui.h6("Principal and Interest", class_="mt-4 mb-2", style="color: #6c757d; border-bottom: 1px solid #eee; padding-bottom: 0.5rem;"),
                ui.layout_columns(
                    ui.input_numeric(
                        "new_principal_crypto",
                        "Principal Crypto:",
                        value=0.0,
                        min=0
                    ),
                    ui.input_numeric(
                        "new_principal_usd",
                        "Principal USD:",
                        value=0.0,
                        min=0
                    ),
                    col_widths=[6, 6]
                ),
                ui.layout_columns(
                    ui.input_numeric(
                        "new_interest_rec_crypto",
                        "Interest Received Crypto:",
                        value=0.0,
                        min=0
                    ),
                    ui.input_numeric(
                        "new_interest_rec_usd",
                        "Interest Received USD:",
                        value=0.0,
                        min=0
                    ),
                    col_widths=[6, 6]
                ),
                ui.layout_columns(
                    ui.input_numeric(
                        "new_payoff_amount_crypto",
                        "Payoff Amount Crypto:",
                        value=0.0,
                        min=0
                    ),
                    ui.input_numeric(
                        "new_payoff_amount_usd",
                        "Payoff Amount USD:",
                        value=0.0,
                        min=0
                    ),
                    col_widths=[6, 6]
                ),
                
                # Additional Hashes and Flags Section
                ui.h6("Additional Information", class_="mt-4 mb-2", style="color: #6c757d; border-bottom: 1px solid #eee; padding-bottom: 0.5rem;"),
                ui.layout_columns(
                    ui.input_text(
                        "new_hash_origination",
                        "Hash Origination:",
                        value=""
                    ),
                    ui.input_text(
                        "new_hash_foreclosure",
                        "Hash Foreclosure:",
                        value=""
                    ),
                    col_widths=[6, 6]
                ),
                ui.layout_columns(
                    ui.input_text(
                        "new_hash_monetize",
                        "Hash Monetize:",
                        value=""
                    ),
                    ui.div(
                        ui.input_selectize(
                            "new_intercompany",
                            "Intercompany:",
                            {"": "", "Yes": "Yes", "No": "No"},
                            selected=""
                        ),
                        class_="custom-dropdown"
                    ),
                    col_widths=[6, 6]
                ),
                ui.layout_columns(
                    ui.input_date(
                        "new_date_only",
                        "Date Only:",
                        value=None
                    ),
                    ui.div(),  # Empty column for spacing
                    col_widths=[6, 6]
                ),
                ui.div(
                    ui.input_text_area(
                        "new_notes",
                        "Notes:",
                        value="",
                        rows=3
                    ),
                    style="width: 100%;"
                ),
                
                # Footer with buttons
                ui.div(
                    ui.input_action_button("cancel_add_gl", "Cancel", class_="btn btn-secondary me-2"),
                    ui.input_action_button("add_new_gl_entry", "Add Entry", class_="btn btn-primary"),
                    style="text-align: right; margin-top: 2rem; padding-top: 1rem; border-top: 1px solid #eee;"
                ),
                
                style="padding: 1.5rem; max-height: 80vh; overflow-y: auto;"
            ),
            title="Add New Journal Entry - All Fields",
            size="xl"
        )
        ui.modal_show(m)

    @reactive.effect
    @reactive.event(input.cancel_add_gl)
    def cancel_add_gl():
        """Hide the add GL entry modal"""
        ui.modal_remove()

    @reactive.effect
    @reactive.event(input.add_new_gl_entry)
    def handle_add_new_gl_entry():
        """Handle adding a new GL entry"""
        try:
            
            # Get all form values - Basic Transaction Information
            new_date = input.new_date()
            new_transaction_type = input.new_transaction_type()
            new_fund_id = input.new_fund_id()
            new_wallet_id = input.new_wallet_id()
            new_cryptocurrency = input.new_cryptocurrency()
            new_account_name = input.new_account_name()
            
            # Crypto and USD Amounts
            new_debit_crypto = input.new_debit_crypto() or 0.0
            new_credit_crypto = input.new_credit_crypto() or 0.0
            new_eth_usd_price = input.new_eth_usd_price() or 0.0
            new_net_debit_credit_crypto = input.new_net_debit_credit_crypto() or 0.0
            new_debit_usd = input.new_debit_usd() or 0.0
            new_credit_usd = input.new_credit_usd() or 0.0
            new_net_debit_credit_usd = input.new_net_debit_credit_usd() or 0.0
            
            # Transaction Details
            new_hash = input.new_hash()
            new_contract_address = input.new_contract_address()
            new_from = input.new_from()
            new_to = input.new_to()
            new_event = input.new_event()
            new_function = input.new_function()
            
            # Partner and Source Information
            new_limited_partner_id = input.new_limited_partner_id()
            new_source_file = input.new_source_file()
            new_platform = input.new_platform()
            new_counterparty_fund_id = input.new_counterparty_fund_id()
            
            # Loan Information
            new_loan_id = input.new_loan_id()
            new_lender = input.new_lender()
            new_borrower = input.new_borrower()
            new_payable_currency = input.new_payable_currency()
            new_collateral_address = input.new_collateral_address()
            new_token_id = input.new_token_id()
            new_collateral_id = input.new_collateral_id()
            new_annual_interest_rate = input.new_annual_interest_rate() or 0.0
            new_loan_due_date = input.new_loan_due_date()
            
            # Principal and Interest
            new_principal_crypto = input.new_principal_crypto() or 0.0
            new_principal_usd = input.new_principal_usd() or 0.0
            new_interest_rec_crypto = input.new_interest_rec_crypto() or 0.0
            new_interest_rec_usd = input.new_interest_rec_usd() or 0.0
            new_payoff_amount_crypto = input.new_payoff_amount_crypto() or 0.0
            new_payoff_amount_usd = input.new_payoff_amount_usd() or 0.0
            
            # Additional Information
            new_hash_origination = input.new_hash_origination()
            new_hash_foreclosure = input.new_hash_foreclosure()
            new_hash_monetize = input.new_hash_monetize()
            new_intercompany = input.new_intercompany()
            new_date_only = input.new_date_only()
            new_notes = input.new_notes()
            
            
            if not all([new_date, new_transaction_type, new_account_name]):
                ui.notification_show("Missing required fields: Date, Transaction Type, and Account Name", duration=5000)
                return
            
            # Load current GL data
            gl_df = load_GL_file()
            
            # Generate new transaction ID
            if gl_df.empty:
                new_transaction_id = "0"
            else:
                # Find the maximum transaction_id and increment
                max_id = gl_df["transaction_id"].astype(str).str.extract(r'(\d+)').astype(float).max().iloc[0]
                new_transaction_id = str(int(max_id + 1) if not pd.isna(max_id) else len(gl_df))
            
            # Create new row with all form values - all 43 columns
            new_row = pd.DataFrame({
                "transaction_id": [new_transaction_id],
                "date": [str(new_date)],
                "transaction_type": [str(new_transaction_type)],
                "fund_id": [str(new_fund_id) if new_fund_id else ""],
                "wallet_id": [str(new_wallet_id)],
                "cryptocurrency": [str(new_cryptocurrency)],
                "account_name": [str(new_account_name)],
                "debit_crypto": [float(new_debit_crypto)],
                "credit_crypto": [float(new_credit_crypto)],
                "eth_usd_price": [float(new_eth_usd_price)],
                "debit_USD": [float(new_debit_usd)],
                "credit_USD": [float(new_credit_usd)],
                "hash": [str(new_hash) if new_hash else ""],
                "contract_address": [str(new_contract_address) if new_contract_address else ""],
                "from": [str(new_from) if new_from else ""],
                "to": [str(new_to) if new_to else ""],
                "event": [str(new_event) if new_event else ""],
                "function": [str(new_function) if new_function else ""],
                "limited_partner_ID": [str(new_limited_partner_id) if new_limited_partner_id else ""],
                "source_file": [str(new_source_file) if new_source_file else ""],
                "platform": [str(new_platform) if new_platform else ""],
                "counterparty_fund_id": [str(new_counterparty_fund_id) if new_counterparty_fund_id else ""],
                "loan_id": [str(new_loan_id) if new_loan_id else ""],
                "lender": [str(new_lender) if new_lender else ""],
                "borrower": [str(new_borrower) if new_borrower else ""],
                "payable_currency": [str(new_payable_currency) if new_payable_currency else ""],
                "collateral_address": [str(new_collateral_address) if new_collateral_address else ""],
                "token_id": [str(new_token_id) if new_token_id else ""],
                "principal_crypto": [float(new_principal_crypto)],
                "principal_USD": [float(new_principal_usd)],
                "interest_rec_crypto": [float(new_interest_rec_crypto)],
                "interest_rec_USD": [float(new_interest_rec_usd)],
                "hash_origination": [str(new_hash_origination) if new_hash_origination else ""],
                "hash_foreclosure": [str(new_hash_foreclosure) if new_hash_foreclosure else ""],
                "hash_monetize": [str(new_hash_monetize) if new_hash_monetize else ""],
                "collateral_id": [str(new_collateral_id) if new_collateral_id else ""],
                "notes": [str(new_notes) if new_notes else ""],
                "annual_interest_rate": [float(new_annual_interest_rate)],
                "payoff_amount_crypto": [float(new_payoff_amount_crypto)],
                "payoff_amount_USD": [float(new_payoff_amount_usd)],
                "loan_due_date": [str(new_loan_due_date) if new_loan_due_date else ""],
                "intercompany": [str(new_intercompany) if new_intercompany else ""],
                "date_only": [str(new_date_only) if new_date_only else ""],
                "net_debit_credit_crypto": [float(new_net_debit_credit_crypto)],
                "net_debit_credit_USD": [float(new_net_debit_credit_usd)]
            })
            
            # Add any other columns from the original GL to maintain structure
            for col in gl_df.columns:
                if col not in new_row.columns:
                    new_row[col] = [""]  # Add empty value for missing columns
            
            # Append to existing data
            if gl_df.empty:
                updated_gl = new_row
            else:
                # Ensure all columns match before concatenation
                for col in new_row.columns:
                    if col not in gl_df.columns:
                        gl_df[col] = ""
                updated_gl = pd.concat([gl_df, new_row], ignore_index=True)
            
            # Save back to S3
            try:
                save_GL_file(updated_gl)
                
                # Clear cache and refresh data
                load_GL_file.cache_clear()
                
                # Clear all form inputs
                ui.update_date("new_date", value=None)
                ui.update_text("new_transaction_type", value="")
                ui.update_text("new_fund_id", value=selected_fund())
                ui.update_text("new_cryptocurrency", value="ETH")
                
                # Clear amounts
                ui.update_numeric("new_debit_crypto", value=0.0)
                ui.update_numeric("new_credit_crypto", value=0.0)
                ui.update_numeric("new_eth_usd_price", value=0.0)
                ui.update_numeric("new_net_debit_credit_crypto", value=0.0)
                ui.update_numeric("new_debit_usd", value=0.0)
                ui.update_numeric("new_credit_usd", value=0.0)
                ui.update_numeric("new_net_debit_credit_usd", value=0.0)
                
                # Clear transaction details
                ui.update_text("new_hash", value="")
                ui.update_text("new_contract_address", value="")
                ui.update_text("new_from", value="")
                ui.update_text("new_to", value="")
                ui.update_text("new_event", value="")
                ui.update_text("new_function", value="")
                
                # Clear partner and source info
                ui.update_text("new_limited_partner_id", value="")
                ui.update_text("new_source_file", value="")
                ui.update_text("new_platform", value="")
                ui.update_text("new_counterparty_fund_id", value="")
                
                # Clear loan information
                ui.update_text("new_loan_id", value="")
                ui.update_text("new_lender", value="")
                ui.update_text("new_borrower", value="")
                ui.update_text("new_payable_currency", value="")
                ui.update_text("new_collateral_address", value="")
                ui.update_text("new_token_id", value="")
                ui.update_text("new_collateral_id", value="")
                ui.update_numeric("new_annual_interest_rate", value=0.0)
                ui.update_date("new_loan_due_date", value=None)
                
                # Clear principal and interest
                ui.update_numeric("new_principal_crypto", value=0.0)
                ui.update_numeric("new_principal_usd", value=0.0)
                ui.update_numeric("new_interest_rec_crypto", value=0.0)
                ui.update_numeric("new_interest_rec_usd", value=0.0)
                ui.update_numeric("new_payoff_amount_crypto", value=0.0)
                ui.update_numeric("new_payoff_amount_usd", value=0.0)
                
                # Clear additional info
                ui.update_text("new_hash_origination", value="")
                ui.update_text("new_hash_foreclosure", value="")
                ui.update_text("new_hash_monetize", value="")
                ui.update_selectize("new_intercompany", selected="")
                ui.update_date("new_date_only", value=None)
                ui.update_text_area("new_notes", value="")
                
                # Close the modal
                ui.modal_remove()
                
                # Trigger a refresh by incrementing the trigger counter
                current_count = refresh_trigger.get()
                refresh_trigger.set(current_count + 1)
                
                # Refresh edited_df_store with new data
                edited_df_store.set(load_GL_file().copy().head(100))
                
                ui.notification_show(f"New journal entry {new_transaction_id} added successfully!", duration=5000)
            except Exception as save_error:
                print(f"ERROR - GL: Failed to save new entry to S3: {save_error}")
                ui.notification_show(f"Failed to save entry: {save_error}", duration=5000)
                
        except Exception as e:
            print(f"ERROR in handle_add_new_gl_entry: {e}")
            import traceback
            traceback.print_exc()
            ui.notification_show(f"Error adding entry: {e}", duration=5000)

    # === Delete GL Entry Logic ===
    @reactive.effect
    @reactive.event(input.delete_gl_entry)
    def handle_delete_gl_entry():
        """Handle deleting a GL entry with confirmation"""
        try:
            
            # Get current selected row data
            row = selected_row_store.get()
            if not row:
                ui.notification_show("Please select an entry to delete", duration=5000)
                return
            
            transaction_id = row.get("transaction_id")
            date = row.get("date", "")
            account_name = row.get("account_name", "")
            transaction_type = row.get("transaction_type", "")
            
            # Show confirmation modal
            confirm_modal = ui.modal(
                ui.div(
                    ui.h4(ui.HTML('<i class="bi bi-exclamation-triangle me-2"></i>Confirm Entry Deletion'), class_="mb-4 text-danger"),
                    ui.p(f"Are you sure you want to delete the following journal entry?", class_="mb-3"),
                    ui.div(
                        ui.strong(f"Transaction ID: {transaction_id}"),
                        ui.br(),
                        ui.strong(f"Date: {date}"),
                        ui.br(),
                        ui.strong(f"Type: {transaction_type}"),
                        ui.br(),
                        ui.strong(f"Account: {account_name}"),
                        class_="mb-3 p-3",
                        style="background-color: #f8d7da; border: 1px solid #f5c6cb; border-radius: 5px;"
                    ),
                    ui.div(
                        ui.tags.strong(ui.HTML('<i class="bi bi-exclamation-triangle me-1"></i>Warning: '), class_="text-danger"),
                        "This action cannot be undone. The journal entry will be permanently removed from your General Ledger.",
                        class_="alert alert-warning"
                    ),
                    
                    # Confirmation buttons
                    ui.div(
                        ui.input_action_button("confirm_delete_gl", "Yes, Delete Entry", class_="btn btn-danger me-3"),
                        ui.input_action_button("cancel_delete_gl", "Cancel", class_="btn btn-secondary"),
                        style="text-align: right; margin-top: 2rem;"
                    ),
                    
                    style="padding: 1.5rem;"
                ),
                title="Delete Journal Entry",
                size="lg"
            )
            ui.modal_show(confirm_modal)
                
        except Exception as e:
            print(f"ERROR in handle_delete_gl_entry: {e}")
            import traceback
            traceback.print_exc()
            ui.notification_show(f"Error: {e}", duration=5000)

    @reactive.effect
    @reactive.event(input.confirm_delete_gl)
    def handle_confirm_delete_gl():
        """Handle confirmed GL entry deletion"""
        try:
            
            # Get current selected row data
            row = selected_row_store.get()
            if not row:
                return
            
            transaction_id = row.get("transaction_id")
            account_name = row.get("account_name", "")
            
            # Load current GL data
            gl_df = load_GL_file()
            if gl_df.empty:
                return
            
            # Find and remove the row
            mask = gl_df["transaction_id"] == transaction_id
            
            if not mask.any():
                ui.notification_show(f"Transaction {transaction_id} not found", duration=5000)
                return
            
            # Remove the entry
            updated_gl = gl_df[~mask].copy()
            
            # Save back to S3
            try:
                save_GL_file(updated_gl)
                
                # Clear selection and close modal
                selected_row_store.set(None)
                ui.modal_remove()
                
                # Clear cache and trigger refresh
                load_GL_file.cache_clear()
                current_count = refresh_trigger.get()
                refresh_trigger.set(current_count + 1)
                
                # Refresh edited_df_store with new data
                edited_df_store.set(load_GL_file().copy().head(100))
                
                ui.notification_show(f"Journal entry {transaction_id} deleted successfully!", duration=5000)
            except Exception as save_error:
                print(f"ERROR - GL: Failed to delete entry from S3: {save_error}")
                ui.notification_show(f"Failed to delete entry: {save_error}", duration=5000)
                
        except Exception as e:
            print(f"ERROR in handle_confirm_delete_gl: {e}")
            import traceback
            traceback.print_exc()
            ui.notification_show(f"Error deleting entry: {e}", duration=5000)

    @reactive.effect
    @reactive.event(input.cancel_delete_gl)
    def handle_cancel_delete_gl():
        """Handle canceling GL entry deletion"""
        try:
            ui.modal_remove()
        except Exception as e:
            print(f"Error in handle_cancel_delete_gl: {e}")

    @output
    @render.ui
    def add_gl_trigger():
        """Button to trigger Add GL Entry modal"""
        return ui.card(
            ui.card_header(ui.HTML('<i class="bi bi-plus-lg me-2"></i>Entry Management')),
            ui.div(
                ui.p("Create new Journal Entries with all required transaction details.", class_="text-muted mb-3"),
                ui.input_action_button("show_add_gl_modal", ui.HTML('<i class="bi bi-plus-lg me-1"></i> Add New Entry'), class_="btn btn-primary btn-lg"),
                style="text-align: center; padding: 2rem;"
            )
        )
    
    # Note: crypto tracker outputs are registered in main server.py
