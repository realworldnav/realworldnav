from shiny import ui, reactive, render, req
from shiny import App, Inputs, Outputs, Session
from shiny.render import DataGrid

from ...s3_utils import get_current_nft_holdings, load_NFT_LEDGER_file
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
def register_nft_portfolio_outputs(output: Outputs, input: Inputs, session: Session, selected_fund):
    selected_nft_store = reactive.Value(None)

    @reactive.calc
    def date_bounds():
        """Get date bounds for NFT transactions"""
        try:
            fund_id = selected_fund()
            if not fund_id:
                return None, None
                
            ledger_df = load_NFT_LEDGER_file()
            if ledger_df.empty:
                return None, None
                
            fund_data = ledger_df[ledger_df["fund_id"] == fund_id]
            if fund_data.empty or "date" not in fund_data.columns:
                return None, None
                
            return fund_data["date"].min(), fund_data["date"].max()
        except Exception as e:
            print(f"Error in date_bounds: {e}")
            return None, None

    @output
    @render.ui
    def nft_date_range_ui():
        """Generate date range input with auto-populated min/max dates from NFT ledger"""
        min_date, max_date = date_bounds()
        
        if min_date is not None and max_date is not None:
            min_date = min_date.date()
            max_date = max_date.date()
        else:
            min_date = None
            max_date = None
        
        return ui.input_date_range(
            "nft_date_range", 
            "Date Range:", 
            start=min_date, 
            end=max_date
        )

    @reactive.effect
    def _sync_nft_filters():
        """Update filter choices based on available data"""
        try:
            df = nft_portfolio_data()
            if df.empty:
                return
            
            # Update acquisition type filter
            if "acquisition_type" in df.columns:
                acquisition_types = ["All Types"] + sorted(df["acquisition_type"].dropna().unique())
                ui.update_selectize("nft_acquisition_filter", choices=acquisition_types, selected="All Types")
            
            # Update collection filter (based on asset name if available)
            if "asset" in df.columns:
                collections = ["All Collections"] + sorted(df["asset"].dropna().unique())
                ui.update_selectize("nft_collection_filter", choices=collections, selected="All Collections")
                
        except Exception as e:
            print(f"Error in _sync_nft_filters: {e}")

    @reactive.calc
    def nft_portfolio_data():
        """Get filtered NFT holdings from the NFT ledger based on date range and filters"""
        try:
            fund_id = selected_fund()
            
            if not fund_id:
                return pd.DataFrame()
            
            # Get the full ledger data for filtering
            ledger_df = load_NFT_LEDGER_file()
            if ledger_df.empty:
                return pd.DataFrame()
            
            # Filter by fund
            fund_data = ledger_df[ledger_df["fund_id"] == fund_id]
            
            # Apply date range filter if set
            try:
                if hasattr(input, 'nft_date_range') and input.nft_date_range():
                    start_date, end_date = input.nft_date_range()
                    if start_date and end_date:
                        fund_data = fund_data[
                            (fund_data["date"] >= pd.to_datetime(start_date).tz_localize("UTC")) &
                            (fund_data["date"] <= pd.to_datetime(end_date).tz_localize("UTC"))
                        ]
            except Exception as e:
                # Date range not available yet
                pass
            
            # Get NFTs owned during this period (remaining_qty > 0)
            owned_nfts = fund_data[fund_data['remaining_qty'] > 0].copy()
            
            if owned_nfts.empty:
                return pd.DataFrame()
            
            # Group by NFT and get the most recent transaction for each
            current_holdings = (
                owned_nfts.sort_values('date', ascending=False)
                .groupby(['collateral_address', 'token_id'])
                .first()
                .reset_index()
            )
            
            
            # Clean token_id formatting
            current_holdings["token_id"] = current_holdings["token_id"].apply(clean_token_id)
            
            # Add computed columns for better display
            current_holdings["nft_value"] = current_holdings["remaining_cost_basis_eth"].abs()
            current_holdings["status"] = "Owned"
            
            # Map acquisition type based on account_name
            current_holdings["acquisition_type"] = current_holdings["account_name"].apply(
                lambda x: "Seized Collateral" if "seized" in str(x).lower() or "collateral" in str(x).lower()
                else "Direct Investment"
            )
            
            # Apply additional filters
            try:
                if hasattr(input, 'nft_acquisition_filter') and input.nft_acquisition_filter():
                    acq_filter = input.nft_acquisition_filter()
                    if acq_filter and acq_filter != "All Types":
                        current_holdings = current_holdings[current_holdings["acquisition_type"] == acq_filter]
                        
                if hasattr(input, 'nft_collection_filter') and input.nft_collection_filter():
                    coll_filter = input.nft_collection_filter()
                    if coll_filter and coll_filter != "All Collections" and "asset" in current_holdings.columns:
                        current_holdings = current_holdings[current_holdings["asset"] == coll_filter]
            except Exception as e:
                # Ignore filter errors
                pass
            
            return current_holdings

        except Exception as e:
            # Silently handle NFT portfolio errors - usually missing fund data
            return pd.DataFrame()

    @output
    @render.data_frame
    def nft_portfolio_table():
        """Display owned NFT portfolio table"""
        df = nft_portfolio_data()
        if df.empty:
            return pd.DataFrame({"Message": ["No owned NFTs found in ledger"]})
        
        # Select relevant columns for display
        display_columns = []
        column_mapping = {}
        
        if "collateral_address" in df.columns:
            display_columns.append("collateral_address")
            column_mapping["collateral_address"] = "Contract Address"
            
        if "token_id" in df.columns:
            display_columns.append("token_id") 
            column_mapping["token_id"] = "Token ID"
            
        if "acquisition_type" in df.columns:
            display_columns.append("acquisition_type")
            column_mapping["acquisition_type"] = "Acquisition Type"
            
        if "nft_value" in df.columns:
            display_columns.append("nft_value")
            column_mapping["nft_value"] = "Value (ETH)"
            
        if "status" in df.columns:
            display_columns.append("status")
            column_mapping["status"] = "Status"
            
        if "date" in df.columns:
            display_columns.append("date")
            column_mapping["date"] = "Date"
        
        if not display_columns:
            return pd.DataFrame({"Message": ["No valid columns found in NFT data"]})
        
        display_df = df[display_columns].copy()
        display_df.columns = [column_mapping[col] for col in display_columns]
        
        # Format the display
        if "Value (ETH)" in display_df.columns:
            display_df["Value (ETH)"] = display_df["Value (ETH)"].apply(lambda x: f"{float(x):,.4f}" if pd.notna(x) else "0.0000")
            
        if "Date" in display_df.columns:
            display_df["Date"] = pd.to_datetime(display_df["Date"], errors='coerce')
            display_df["Date"] = display_df["Date"].dt.strftime("%Y-%m-%d") if not display_df["Date"].isna().all() else display_df["Date"]
        
        return DataGrid(
            display_df, 
            selection_mode="row",
            row_selection_mode="single",
            filters=True,
            summary=False
        )

    @reactive.effect
    def capture_selected_nft():
        """Capture selected NFT row"""
        selection = nft_portfolio_table.cell_selection()
        df = nft_portfolio_data()

        if not selection or "rows" not in selection or not selection["rows"]:
            selected_nft_store.set(None)
            return

        row_idx = selection["rows"][0]
        if df is None or row_idx >= len(df):
            selected_nft_store.set(None)
            return

        row = df.iloc[row_idx].to_dict()
        selected_nft_store.set(row)

    @output
    @render.ui
    def nft_portfolio_summary():
        """Display enhanced NFT portfolio summary with larger text and variety"""
        try:
            df = nft_portfolio_data()
            if df.empty:
                return ui.div(
                    ui.h4("No NFTs Found", style="font-size: 1.5rem; color: var(--bs-secondary); margin-bottom: 0.5rem; text-align: center;"),
                    ui.p("No NFTs match your current filters", style="font-size: 1rem; color: var(--bs-muted); text-align: center; margin: 0;"),
                    style="padding: 2rem;"
                )
            
            # Calculate summary metrics
            total_nfts = len(df)
            total_portfolio_value = float(df["nft_value"].sum())
            unique_collections = df["collateral_address"].nunique()
            seized_nfts = len(df[df["acquisition_type"] == "Seized Collateral"]) if "acquisition_type" in df.columns else 0
            direct_nfts = len(df[df["acquisition_type"] == "Direct Investment"]) if "acquisition_type" in df.columns else 0
            
            # Calculate average value and get highest value NFT
            avg_value = total_portfolio_value / total_nfts if total_nfts > 0 else 0
            highest_value_nft = df.loc[df["nft_value"].idxmax()] if not df.empty else None
            
            # Get date range info
            min_date = df["date"].min() if "date" in df.columns and not df["date"].isna().all() else None
            max_date = df["date"].max() if "date" in df.columns and not df["date"].isna().all() else None
            
            return ui.div(
                # Compact metrics in a single efficient row
                ui.div(
                    ui.div(
                        ui.h3(f"{total_nfts}", style="font-size: 2rem; font-weight: 600; color: var(--bs-primary); margin: 0; text-align: center;"),
                        ui.p("NFTs Owned", style="font-size: 0.9rem; color: var(--bs-secondary); margin: 0; text-align: center;"),
                        style="flex: 1; padding: 0.75rem;"
                    ),
                    ui.div(
                        ui.h3(f"{total_portfolio_value:,.3f}", style="font-size: 2rem; font-weight: 600; color: var(--bs-success); margin: 0; text-align: center;"),
                        ui.p("ETH Total Value", style="font-size: 0.9rem; color: var(--bs-secondary); margin: 0; text-align: center;"),
                        style="flex: 1; padding: 0.75rem; border-left: 1px solid var(--bs-border-color);"
                    ),
                    ui.div(
                        ui.h3(f"{unique_collections}", style="font-size: 2rem; font-weight: 600; color: var(--bs-info); margin: 0; text-align: center;"),
                        ui.p("Collections", style="font-size: 0.9rem; color: var(--bs-secondary); margin: 0; text-align: center;"),
                        style="flex: 1; padding: 0.75rem; border-left: 1px solid var(--bs-border-color);"
                    ),
                    ui.div(
                        ui.h3(f"{avg_value:.3f}", style="font-size: 2rem; font-weight: 600; color: var(--bs-warning); margin: 0; text-align: center;"),
                        ui.p("Avg Value ETH", style="font-size: 0.9rem; color: var(--bs-secondary); margin: 0; text-align: center;"),
                        style="flex: 1; padding: 0.75rem; border-left: 1px solid var(--bs-border-color);"
                    ),
                    style="display: flex; border: 1px solid var(--bs-border-color); border-radius: 6px; margin-bottom: 1rem;"
                ),
                
                # Compact secondary metrics row
                ui.div(
                    ui.div(
                        ui.span("Seized: ", style="color: var(--bs-secondary); font-size: 0.8rem;"),
                        ui.span(f"{seized_nfts}", style="font-size: 1.2rem; color: var(--bs-warning);"),
                        style="flex: 1; text-align: center; padding: 0.5rem;"
                    ),
                    ui.div(
                        ui.span("Direct: ", style="color: var(--bs-secondary); font-size: 0.8rem;"),
                        ui.span(f"{direct_nfts}", style="font-size: 1.2rem; color: var(--bs-primary);"),
                        style="flex: 1; text-align: center; padding: 0.5rem; border-left: 1px solid var(--bs-border-color);"
                    ),
                    ui.div(
                        ui.span("Highest: ", style="color: var(--bs-secondary); font-size: 0.8rem;"),
                        ui.span(f"{float(highest_value_nft['nft_value']):.3f} ETH" if highest_value_nft is not None else "0.000 ETH", 
                               style="font-size: 1.2rem; color: var(--bs-success);"),
                        style="flex: 1; text-align: center; padding: 0.5rem; border-left: 1px solid var(--bs-border-color);"
                    ),
                    ui.div(
                        ui.div(
                            ui.span("Period", style="font-size: 0.8rem; color: var(--bs-secondary); display: block;"),
                            ui.span(
                                f"{min_date.strftime('%m/%d/%Y') if min_date else 'Unknown'} - {max_date.strftime('%m/%d/%Y') if max_date else 'Unknown'}" if min_date and max_date 
                                else "No date range",
                                style="font-size: 0.9rem; color: var(--bs-dark);"
                            ),
                        ),
                        style="flex: 1; text-align: center; padding: 0.5rem; border-left: 1px solid var(--bs-border-color);"
                    ),
                    style="display: flex; border: 1px solid var(--bs-border-color); border-radius: 6px;"
                )
            )
            
        except Exception as e:
            print(f"Error in NFT portfolio summary: {e}")
            import traceback
            traceback.print_exc()
            return ui.div(
                ui.h4("Error Loading Portfolio", style="font-size: 1.3rem; color: var(--bs-danger); margin-bottom: 0.5rem; text-align: center;"),
                ui.p("Unable to load NFT portfolio data", style="font-size: 1rem; color: var(--bs-muted); text-align: center; margin: 0;"),
                style="padding: 1.5rem;"
            )

    @output
    @render.ui
    def nft_detail_display():
        """Display detailed NFT information"""
        row = selected_nft_store.get()
        if not row:
            return ui.div(
                ui.h5("Select an NFT", style="color: var(--bs-secondary); margin-bottom: 0.5rem; text-align: center;"),
                ui.p("Click on an NFT row to view details", style="color: var(--bs-muted); text-align: center; margin: 0;"),
                style="padding: 2rem;"
            )

        try:
            contract_address = row.get("collateral_address", "")
            token_id = row.get("token_id", "")

            # Fetch NFT metadata
            metadata = get_nft_metadata_with_fallback(contract_address, token_id)
            collection_info = get_nft_collection_info(contract_address)

            content = []
            
            # NFT Information Header
            content.append(ui.h5("NFT Details", style="color: var(--bs-primary); margin-bottom: 1rem;"))
            
            # Basic NFT details in a clean list format
            detail_items = [
                ("Contract Address", contract_address[:20] + "..." if len(contract_address) > 20 else contract_address),
                ("Token ID", token_id),
                ("Acquisition Type", safe_str(row.get("acquisition_type"))),
                ("NFT Value", f"{float(row.get('nft_value', 0)):,.4f} ETH"),
                ("Status", safe_str(row.get("status")))
            ]

            if metadata:
                name = metadata.get("name", "Unknown")
                description = metadata.get("description", "No description available")
                detail_items.insert(2, ("Name", name))
                if description and description != "No description available":
                    detail_items.append(("Description", description[:80] + "..." if len(description) > 80 else description))

            if collection_info:
                collection_name = collection_info.get("name", "Unknown Collection")
                if collection_name != "Unknown Collection":
                    detail_items.append(("Collection", collection_name))
            
            # Add detail items with clean formatting
            for label, value in detail_items:
                content.append(ui.div(
                    ui.span(f"{label}: ", style="color: var(--bs-secondary); font-size: 0.9rem;"),
                    ui.span(value, style="color: var(--bs-dark); font-size: 0.9rem;"),
                    style="margin-bottom: 0.5rem;"
                ))

            # External links
            content.append(ui.h6("External Links", style="color: var(--bs-primary); margin-top: 1rem; margin-bottom: 0.5rem;"))
            nftscan_url = f"https://nftscan.com/{contract_address}/{token_id}"
            opensea_url = f"https://opensea.io/assets/{contract_address}/{token_id}"

            content.append(ui.div(
                ui.a("NFTScan", href=nftscan_url, target="_blank", 
                     style="color: var(--bs-primary); text-decoration: none; margin-right: 1rem; font-size: 0.9rem;"),
                ui.a("OpenSea", href=opensea_url, target="_blank", 
                     style="color: var(--bs-primary); text-decoration: none; font-size: 0.9rem;"),
                style="margin-bottom: 1rem;"
            ))

            # NFT image preview below the text
            if metadata and "image" in metadata and metadata["image"]:
                image_url = metadata["image"]
                content.append(ui.div(
                    ui.h6("NFT Preview", style="color: var(--bs-primary); margin-bottom: 0.5rem;"),
                    ui.img(
                        src=image_url,
                        alt=f"NFT {token_id}",
                        style="width: 100%; max-width: 250px; height: auto; border-radius: 8px; display: block; margin: 0 auto;",
                        class_="img-fluid"
                    ),
                    style="text-align: center; margin-top: 1rem;"
                ))
            else:
                content.append(ui.div(
                    ui.h6("NFT Preview", style="color: var(--bs-secondary); margin-bottom: 0.5rem;"),
                    ui.p("Image not available", style="color: var(--bs-muted); font-size: 0.9rem; text-align: center; margin: 0;"),
                    style="text-align: center; margin-top: 1rem; padding: 1rem; background: var(--bs-light); border-radius: 6px;"
                ))

            return ui.div(*content)

        except Exception as e:
            print(f"Error displaying NFT details: {e}")
            return ui.div(
                ui.h5("Error Loading NFT Data", style="color: var(--bs-danger); margin-bottom: 0.5rem; text-align: center;"),
                ui.p(f"Unable to load NFT details", style="color: var(--bs-muted); text-align: center; margin: 0;"),
                style="padding: 2rem;"
            )
