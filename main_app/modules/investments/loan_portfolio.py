from shiny import ui, reactive, render, req
from shiny import App, Inputs, Outputs, Session
from shiny.render import DataGrid

from ...s3_utils import load_GL_file
from .nft_collateral import get_nft_metadata_with_fallback, get_nft_collection_info, safe_str

import pandas as pd
from decimal import Decimal, InvalidOperation

# === Helpers ===
def safe_decimal(val):
    try:
        return Decimal(str(val))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")

def clean_token_id(x):
    try:
        if pd.isna(x) or x is None:
            return ""
        return str(int(float(x)))
    except Exception:
        return str(x) if pd.notna(x) else ""

def safe_str(v):
    return "" if pd.isna(v) or v is None else str(v)



# === Server ===
def register_outputs(output: Outputs, input: Inputs, session: Session, selected_fund):
    selected_row_store = reactive.Value(None)

    @reactive.calc
    def raw_loans():
        try:
            gl = load_GL_file()
            fund_id = selected_fund()
            gl = gl[gl["fund_id"] == fund_id]

            loans = gl[gl["account_name"].str.contains(r"loan|interest_receivable|bad debt", case=False, na=False)].copy()
            if loans.empty:
                return pd.DataFrame()

            loans = loans[loans["loan_id"].notna()]
            loans["loan_id"] = loans["loan_id"].astype(str)

            loans["crypto_amount"] = loans.apply(
                lambda r:  safe_decimal(r["debit_crypto"]) - safe_decimal(r["credit_crypto"]), axis=1
            )

            loans["date"] = pd.to_datetime(loans["date"], utc=True, errors="coerce")
            return loans
        except Exception as e:
            # Silently handle loan loading errors - usually missing fund data
            return pd.DataFrame()

    @reactive.calc
    def date_bounds():
        df = raw_loans()
        if df.empty or "date" not in df.columns:
            return None, None
        return df["date"].min(), df["date"].max()

    @reactive.effect
    def _sync_inputs():
        try:
            df = raw_loans()
            if df.empty:
                return
            
            ui.update_selectize("loan_select", choices=["All Loans"] + sorted(df["loan_id"].dropna().astype(str).unique()))
            ui.update_selectize("event_filter", choices=["All Events"] + sorted(df["event"].dropna().astype(str).unique()))
            ui.update_selectize("function_filter", choices=["All Functions"] + sorted(df["function"].dropna().astype(str).unique()))
            platforms = ["All Platforms"] + sorted(df["platform"].dropna().astype(str).unique()) if "platform" in df.columns else ["All Platforms"]
            ui.update_selectize("platform_filter", choices=platforms, selected="All Platforms")
        except Exception as e:
            print(f"Error in _sync_inputs: {e}")
    
    @output
    @render.ui
    def loan_date_range_ui():
        """Generate date range input with auto-populated min/max dates from GL data"""
        df = raw_loans()
        if not df.empty and "date" in df.columns:
            min_date = df["date"].min().date()  # Convert to date object
            max_date = df["date"].max().date()  # Convert to date object
            print(f"DEBUG - Setting loan date range defaults: {min_date} to {max_date}")
        else:
            # Fallback dates if no data
            min_date = None
            max_date = None
            print("DEBUG - No loan data available, using None for date range")
        
        return ui.input_date_range(
            "loan_date_range", 
            "Date Range:", 
            start=min_date, 
            end=max_date
        )

    @reactive.calc
    def filtered_loans():
        try:
            df = raw_loans()
            loan_id = input.loan_select()
            event_filter = input.event_filter()
            function_filter = input.function_filter()
            platform_filter = input.platform_filter()
            start_date, end_date = input.loan_date_range()

            if start_date and end_date:
                df = df[df["date"].notna()]
                df = df[(df["date"] >= pd.to_datetime(start_date).tz_localize("UTC")) &
                        (df["date"] <= pd.to_datetime(end_date).tz_localize("UTC"))]

            if loan_id and loan_id != "All Loans":
                df = df[df["loan_id"] == str(loan_id)]

            if event_filter and event_filter != "All Events":
                df = df[df["event"] == event_filter]

            if function_filter and function_filter != "All Functions":
                df = df[df["function"] == function_filter]

            if platform_filter and platform_filter != "All Platforms" and "platform" in df.columns:
                df = df[df["platform"] == platform_filter]

            return df
        except Exception as e:
            print(f"Error in filtered_loans: {e}")
            return pd.DataFrame()

    @reactive.calc
    def loan_summary_df():
        try:
            df = filtered_loans()
            if df.empty:
                return pd.DataFrame()

            df = df[~df["account_name"].str.contains("usdc|usd", case=False, na=False)]
            df = df.copy()

            df["account_role"] = df["account_name"].apply(
                lambda x: "bad_debt_expense" if any(kw in x.lower() for kw in ["bad debt", "provision"])
                else "loan_receivable" if "loan_receivable" in x.lower()
                else "interest_receivable" if "interest_receivable" in x.lower()
                else x
            )

            summary = (
                df.groupby(["loan_id", "account_role"])
                ["crypto_amount"]
                .sum()
                .unstack(fill_value=Decimal("0"))
            )

            preferred_order = ["loan_receivable", "interest_receivable", "bad_debt_expense"]
            summary = summary[[col for col in preferred_order if col in summary.columns] + 
                              [col for col in summary.columns if col not in preferred_order]]

            meta = (
                df[["loan_id", "collateral_address", "token_id", "hash", "function", "event", "date"]]
                .drop_duplicates(subset=["loan_id"])
                .set_index("loan_id")
                .copy()
            )

            meta["function"] = meta["function"].astype(str).str.slice(0, 60)
            meta["event"] = meta["event"].astype(str).str.slice(0, 60)

            result = summary.join(meta, how="left").reset_index()
            result["token_id"] = result["token_id"].apply(clean_token_id)

            return result
        except Exception as e:
            print(f"Error in loan_summary_df: {e}")
            return pd.DataFrame()

    @output
    @render.data_frame
    def loan_portfolio_table():
        df = loan_summary_df()
        print(f"DEBUG - Loan table rendering with {len(df)} rows")
        if df.empty:
            return pd.DataFrame({"Message": ["No loan data available"]})
        
        # Ensure proper data types for rendering
        display_df = df.copy()
        
        return DataGrid(
            display_df, 
            selection_mode="row",
            row_selection_mode="single",
            filters=True,
            summary=False,
            height="350px"  # Fixed height might help with selection
        )

    @reactive.effect
    def capture_selected_loan_row():
        selection = loan_portfolio_table.cell_selection()
        df = loan_summary_df()

        print(f"DEBUG - Loan selection event: {selection}")
        
        if not selection or "rows" not in selection or not selection["rows"]:
            print(f"DEBUG - No valid row selection")
            selected_row_store.set(None)
            return

        row_idx = selection["rows"][0]
        print(f"DEBUG - Selected row index: {row_idx}")
        
        if df is None or row_idx >= len(df):
            print(f"DEBUG - Invalid row index or empty dataframe")
            selected_row_store.set(None)
            return

        row = df.iloc[row_idx].to_dict()
        print(f"DEBUG - Selected row data: {row.get('loan_id', 'N/A')}")
        selected_row_store.set(row)

    @reactive.calc
    def selected_loan_id():
        row = selected_row_store.get()
        if row is None:
            return None
        return row.get("loan_id")





    @output
    @render.ui
    def loan_row_inspector():
        """Display detailed loan information with clean design"""
        row = selected_row_store.get()
        if not row:
            return ui.tags.div(
                ui.tags.h5("Select a Loan", class_="mb-2"),
                ui.tags.p("Click on a loan row to view details", class_="text-muted"),
                class_="empty-state",
                style="padding: 2rem; text-align: center; background-color: var(--bs-light);"
            )

        try:
            elements = []
            
            # Basic loan information
            elements.append(ui.tags.h5("Loan Details", class_="mb-3", style="color: var(--bs-primary); font-weight: 600;"))
            
            # Create detail items
            detail_items = []
            if 'loan_id' in row:
                detail_items.append(("Loan ID", safe_str(row.get('loan_id'))))
            if 'loan_receivable' in row:
                amount = row.get('loan_receivable', 0)
                detail_items.append(("Principal Amount", f"{float(amount):,.4f} ETH"))
            if 'interest_receivable' in row:
                amount = row.get('interest_receivable', 0)
                detail_items.append(("Interest Receivable", f"{float(amount):,.4f} ETH"))
            if 'bad_debt_expense' in row:
                amount = row.get('bad_debt_expense', 0)
                if float(amount) != 0:
                    detail_items.append(("Bad Debt Expense", f"{float(amount):,.4f} ETH"))
            if 'date' in row:
                detail_items.append(("Last Activity", safe_str(row.get('date'))))
            if 'function' in row:
                detail_items.append(("Last Function", safe_str(row.get('function'))))
            if 'event' in row:
                detail_items.append(("Last Event", safe_str(row.get('event'))))
            
            # Add detail items to elements with better styling
            for label, value in detail_items:
                elements.append(ui.tags.div(
                    ui.tags.div(
                        ui.tags.strong(label, style="color: var(--bs-dark);"),
                        style="margin-bottom: 0.25rem;"
                    ),
                    ui.tags.div(
                        value,
                        style="color: var(--bs-secondary); font-size: 0.95rem; margin-bottom: 1rem;"
                    ),
                    class_="detail-item"
                ))

            return ui.tags.div(
                *elements,
                style="padding: 1.5rem; background-color: var(--bs-light); border-radius: 8px;"
            )

        except Exception as e:
            print(f"Error displaying loan details: {e}")
            return ui.tags.div(
                ui.tags.h5("Error Loading Details", class_="mb-2"),
                ui.tags.p(f"Unable to load loan details: {e}", class_="text-muted"),
                class_="empty-state",
                style="padding: 2rem; text-align: center; background-color: var(--bs-light);"
            )

    @output
    @render.ui
    def nft_collateral_display():
        """Display NFT collateral information with clean design"""
        row = selected_row_store.get()
        if not row:
            return ui.tags.div(
                ui.tags.h5("Select a Loan", class_="mb-2"),
                ui.tags.p("Click on a loan row to view NFT collateral", class_="text-muted"),
                class_="empty-state",
                style="padding: 2rem; text-align: center; background-color: var(--bs-light);"
            )

        try:
            contract_address = row.get("collateral_address", "")
            token_id = row.get("token_id", "")

            if not contract_address or not token_id:
                return ui.tags.div(
                    ui.tags.h5("No NFT Collateral", class_="mb-2"),
                    ui.tags.p("This loan has no associated NFT collateral", class_="text-muted"),
                    class_="empty-state",
                    style="padding: 2rem; text-align: center; background-color: var(--bs-light);"
                )

            # Fetch NFT metadata
            metadata = get_nft_metadata_with_fallback(contract_address, token_id)
            collection_info = get_nft_collection_info(contract_address)

            # Create content
            content = []
            
            # NFT Information Header
            content.append(ui.tags.h5("NFT Collateral", class_="mb-3", style="color: var(--bs-primary); font-weight: 600;"))
            
            # NFT Image (if available)
            if metadata and "image" in metadata and metadata["image"]:
                image_url = metadata["image"]
                content.append(ui.tags.div(
                    ui.tags.img(
                        src=image_url,
                        alt=f"NFT {token_id}",
                        style="width: 100%; max-width: 200px; height: 200px; object-fit: cover; border-radius: 8px; margin: 0 auto 1rem auto; display: block;",
                        class_="img-fluid"
                    ),
                    style="text-align: center;"
                ))
            
            # Basic NFT details with clean styling
            detail_items = [
                ("Token ID", token_id),
                ("Contract", contract_address[:10] + "..." if len(contract_address) > 10 else contract_address)
            ]
            
            if metadata:
                name = metadata.get("name", "Unknown")
                if name != "Unknown":
                    detail_items.insert(0, ("Name", name))
            
            if collection_info:
                collection_name = collection_info.get("name", "Unknown Collection")
                if collection_name != "Unknown Collection":
                    detail_items.append(("Collection", collection_name))
            
            # Add detail items with better styling
            for label, value in detail_items:
                content.append(ui.tags.div(
                    ui.tags.div(
                        ui.tags.strong(label, style="color: var(--bs-dark);"),
                        style="margin-bottom: 0.25rem;"
                    ),
                    ui.tags.div(
                        value,
                        style="color: var(--bs-secondary); font-size: 0.95rem; margin-bottom: 1rem;"
                    )
                ))
            
            # External links
            nftscan_url = f"https://nftscan.com/{contract_address}/{token_id}"
            opensea_url = f"https://opensea.io/assets/{contract_address}/{token_id}"

            content.append(ui.tags.div(
                ui.tags.a("NFTScan", href=nftscan_url, target="_blank", 
                         class_="btn btn-outline-primary btn-sm me-2",
                         style="text-decoration: none;"),
                ui.tags.a("OpenSea", href=opensea_url, target="_blank", 
                         class_="btn btn-outline-primary btn-sm",
                         style="text-decoration: none;"),
                class_="mt-2"
            ))

            return ui.tags.div(
                *content,
                style="padding: 1.5rem; background-color: var(--bs-light); border-radius: 8px;"
            )

        except Exception as e:
            print(f"Error displaying NFT collateral: {e}")
            return ui.tags.div(
                ui.tags.h5("Error Loading NFT Data", class_="mb-2"),
                ui.tags.p(f"Unable to load NFT collateral: {e}", class_="text-muted"),
                class_="empty-state",
                style="padding: 2rem; text-align: center; background-color: var(--bs-light);"
            )

    # KPI Output Functions
    @output
    @render.ui
    def loan_portfolio_value():
        """Calculate total portfolio value from loan receivables"""
        try:
            df = loan_summary_df()
            if df.empty or 'loan_receivable' not in df.columns:
                return "0.00 ETH"
            
            # Sum all positive loan receivables
            total_value = float(df[df['loan_receivable'] > 0]['loan_receivable'].sum())
            return f"{total_value:,.4f} ETH"
        except Exception as e:
            print(f"Error calculating portfolio value: {e}")
            return "Error"

    @output
    @render.ui  
    def active_loans_count():
        """Count active loans (those with positive balances)"""
        try:
            df = loan_summary_df()
            if df.empty or 'loan_receivable' not in df.columns:
                return "0"
            
            # Count loans with positive balances
            active_count = len(df[df['loan_receivable'] > 0])
            return str(active_count)
        except Exception as e:
            print(f"Error counting active loans: {e}")
            return "Error"

    @output
    @render.ui
    def nft_collateral_count():
        """Count NFTs used as collateral"""
        try:
            df = loan_summary_df()
            if df.empty:
                return "0"
            
            # Count loans with collateral address and token ID
            nft_count = len(df[
                df['collateral_address'].notna() & 
                df['token_id'].notna() &
                (df['collateral_address'] != '') &
                (df['token_id'] != '')
            ])
            return str(nft_count)
        except Exception as e:
            print(f"Error counting NFT collateral: {e}")
            return "Error"

    @output
    @render.ui
    def portfolio_performance():
        """Calculate portfolio performance metrics"""
        try:
            df = loan_summary_df()
            if df.empty:
                return "N/A"
            
            # Calculate performance based on interest vs principal
            if 'loan_receivable' in df.columns and 'interest_receivable' in df.columns:
                total_principal = float(df[df['loan_receivable'] > 0]['loan_receivable'].sum())
                total_interest = float(df['interest_receivable'].sum())
                
                if total_principal > 0:
                    yield_rate = (total_interest / total_principal) * 100
                    return f"{yield_rate:.2f}%"
            
            return "N/A"
        except Exception as e:
            print(f"Error calculating performance: {e}")
            return "Error"


