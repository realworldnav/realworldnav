from shiny import ui, reactive, render, req
from shiny import App, Inputs, Outputs, Session
from shiny.render import DataGrid

from ...s3_utils import load_GL_file
import pandas as pd
from decimal import Decimal, InvalidOperation
import re
import requests
from typing import Dict, Optional
from functools import lru_cache

# === Helpers ===
def safe_decimal(val):
    try:
        return Decimal(str(val))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")

def safe_str(v):
    return "" if pd.isna(v) or v is None else str(v)

def extract_crypto_type(account_name: str) -> str:
    """Extract cryptocurrency type from account name like 'digital_assets_weth' -> 'WETH'"""
    if not account_name or not account_name.startswith('digital_assets_'):
        return "Unknown"
    
    # Split by underscore and get the last part
    parts = account_name.split('_')
    if len(parts) >= 3:  # digital_assets_[type]
        crypto_type = parts[2].upper()
        return crypto_type
    return "Unknown"

def format_crypto_amount(amount, crypto_type):
    """Format crypto amounts with proper styling"""
    try:
        if pd.isna(amount) or amount is None:
            return f"0.0000 {crypto_type}"
        return f"{float(amount):,.6f} {crypto_type}"
    except:
        return f"0.0000 {crypto_type}"

def get_crypto_display_name(crypto_type: str) -> str:
    """Get human-readable cryptocurrency names"""
    crypto_map = {
        "WETH": "Wrapped Ethereum",
        "ETH": "Ethereum", 
        "USDC": "USD Coin",
        "USDT": "Tether USD",
        "DAI": "Dai Stablecoin",
        "WBTC": "Wrapped Bitcoin",
        "BTC": "Bitcoin",
        "LINK": "Chainlink",
        "UNI": "Uniswap",
        "AAVE": "Aave",
        "COMP": "Compound"
    }
    return crypto_map.get(crypto_type.upper(), crypto_type)

@lru_cache(maxsize=500)
def get_coingecko_coin_id(crypto_symbol: str) -> str:
    """Map cryptocurrency symbols to CoinGecko IDs"""
    symbol_to_id_map = {
        "ETH": "ethereum",
        "WETH": "weth", 
        "BTC": "bitcoin",
        "WBTC": "wrapped-bitcoin",
        "USDC": "usd-coin",
        "USDT": "tether",
        "DAI": "dai",
        "LINK": "chainlink",
        "UNI": "uniswap",
        "AAVE": "aave",
        "COMP": "compound",
        "MATIC": "matic-network",
        "ADA": "cardano",
        "DOT": "polkadot",
        "SOL": "solana",
        "AVAX": "avalanche-2",
        "ATOM": "cosmos",
        "FTM": "fantom",
        "NEAR": "near",
        "ALGO": "algorand",
        "VET": "vechain",
        "ICP": "internet-computer",
        "FLOW": "flow",
        "XTZ": "tezos",
        "EGLD": "elrond-erd-2"
    }
    return symbol_to_id_map.get(crypto_symbol.upper(), crypto_symbol.lower())

@lru_cache(maxsize=500)
def get_crypto_logo_url(crypto_symbol: str) -> str:
    """Get cryptocurrency logo URL from CoinGecko API or direct URLs"""
    try:
        # Direct URLs for common cryptocurrencies - more reliable than API calls
        direct_urls = {
            "ETH": "https://assets.coingecko.com/coins/images/279/large/ethereum.png",
            "WETH": "https://assets.coingecko.com/coins/images/2518/large/weth.png",
            "BTC": "https://assets.coingecko.com/coins/images/1/large/bitcoin.png",
            "WBTC": "https://assets.coingecko.com/coins/images/7598/large/wrapped_bitcoin_wbtc.png",
            "USDC": "https://assets.coingecko.com/coins/images/6319/large/USD_Coin_icon.png",
            "USDT": "https://assets.coingecko.com/coins/images/325/large/Tether.png",
            "DAI": "https://assets.coingecko.com/coins/images/9956/large/4943.png",
            "LINK": "https://assets.coingecko.com/coins/images/877/large/chainlink-new-logo.png",
            "UNI": "https://assets.coingecko.com/coins/images/12504/large/uni.jpg",
            "AAVE": "https://assets.coingecko.com/coins/images/12645/large/AAVE.png",
            "COMP": "https://assets.coingecko.com/coins/images/10775/large/COMP.png"
        }
        
        symbol_upper = crypto_symbol.upper()
        if symbol_upper in direct_urls:
            return direct_urls[symbol_upper]
        
        # Fallback to CoinGecko API for other coins
        coin_id = get_coingecko_coin_id(crypto_symbol)
        
        url = f"https://api.coingecko.com/api/v3/coins/{coin_id}"
        headers = {
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        response = requests.get(url, headers=headers, timeout=5)
        if response.status_code == 200:
            data = response.json()
            if "image" in data and "large" in data["image"]:
                return data["image"]["large"]
            elif "image" in data and "small" in data["image"]:
                return data["image"]["small"]
        
        # Final fallback to constructed URL
        return f"https://assets.coingecko.com/coins/images/279/large/ethereum.png"  # Default to ETH
        
    except Exception as e:
        print(f"Error fetching logo for {crypto_symbol}: {e}")
        # Default to ETH logo if error
        return "https://assets.coingecko.com/coins/images/279/large/ethereum.png"

# === Server ===
def register_cryptocurrency_portfolio_outputs(output: Outputs, input: Inputs, session: Session, selected_fund):
    selected_crypto_store = reactive.Value(None)

    @reactive.calc
    def cryptocurrency_portfolio_data():
        """Extract cryptocurrency data from digital_assets accounts"""
        try:
            gl = load_GL_file()
            fund_id = selected_fund()
            
            # Debug: Print selected fund for verification
            print(f"DEBUG - Crypto Portfolio: Selected fund = {fund_id}")
            
            if not fund_id:
                print("DEBUG - Crypto Portfolio: No fund selected, returning empty DataFrame")
                return pd.DataFrame()
            
            gl = gl[gl["fund_id"] == fund_id]
            print(f"DEBUG - Crypto Portfolio: Found {len(gl)} GL entries for fund {fund_id}")

            # Get cryptocurrency assets from digital_assets accounts
            crypto_assets = gl[
                gl["account_name"].str.contains(r"digital_assets", case=False, na=False)
            ].copy()
            
            print(f"DEBUG - Crypto Portfolio: Found {len(crypto_assets)} digital assets entries")
            
            if crypto_assets.empty:
                print("DEBUG - Crypto Portfolio: No digital assets entries found")
                return pd.DataFrame()

            # Calculate net amounts for each crypto entry
            crypto_assets["crypto_amount"] = crypto_assets.apply(
                lambda r: safe_decimal(r["debit_crypto"]) - safe_decimal(r["credit_crypto"]), axis=1
            )

            # Extract cryptocurrency type from account name
            crypto_assets["crypto_type"] = crypto_assets["account_name"].apply(extract_crypto_type)
            crypto_assets["crypto_display_name"] = crypto_assets["crypto_type"].apply(get_crypto_display_name)

            # Group by account name to get portfolio summary with proper aggregation
            crypto_summary = crypto_assets.groupby(["account_name", "crypto_type"]).agg({
                "crypto_amount": "sum",
                "date": "max",  # Use most recent date
                "function": "last", 
                "event": "last",
                "crypto_display_name": "first"
            }).reset_index()

            crypto_summary["date"] = pd.to_datetime(crypto_summary["date"], utc=True, errors="coerce")

            # Add computed columns for better display
            crypto_summary["balance"] = crypto_summary["crypto_amount"]
            crypto_summary["status"] = crypto_summary["crypto_amount"].apply(
                lambda x: "Positive Balance" if x > 0 else "Zero/Negative Balance" if x <= 0 else "Unknown"
            )

            # Filter out zero balances except for ETH (always include ETH)
            crypto_summary = crypto_summary[
                (crypto_summary["balance"] != 0) | 
                (crypto_summary["crypto_type"].str.upper().isin(["ETH", "WETH"]))
            ]

            print(f"DEBUG - Crypto Portfolio: Final summary has {len(crypto_summary)} crypto types")

            return crypto_summary

        except Exception as e:
            print(f"Error in cryptocurrency_portfolio_data: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    @output
    @render.data_frame
    def cryptocurrency_portfolio_table():
        """Display cryptocurrency portfolio table"""
        df = cryptocurrency_portfolio_data()
        if df.empty:
            return pd.DataFrame({"Message": ["No cryptocurrency assets found in digital_assets accounts"]})
        
        # Sort by balance descending and select relevant columns for display
        df_sorted = df.sort_values('balance', ascending=False)
        display_df = df_sorted[["crypto_type", "crypto_display_name", "balance", "status", "date"]].copy()
        display_df.columns = ["Symbol", "Cryptocurrency", "Balance", "Status", "Date"]
        
        # Format the display
        display_df["Balance"] = display_df.apply(
            lambda row: f"{float(row['Balance']):,.6f}" if pd.notna(row['Balance']) else "0.000000", 
            axis=1
        )
        display_df["Date"] = display_df["Date"].dt.strftime("%Y-%m-%d") if not display_df["Date"].isna().all() else display_df["Date"]
        
        return DataGrid(
            display_df, 
            selection_mode="row",
            row_selection_mode="single",
            filters=True,
            summary=False,
            height="420px"
        )

    @reactive.effect
    def capture_selected_crypto():
        """Capture selected cryptocurrency row"""
        selection = cryptocurrency_portfolio_table.cell_selection()
        df = cryptocurrency_portfolio_data()

        print(f"DEBUG - Crypto selection event: {selection}")

        if not selection or "rows" not in selection or not selection["rows"]:
            print(f"DEBUG - No valid crypto row selection")
            selected_crypto_store.set(None)
            return

        row_idx = selection["rows"][0]
        print(f"DEBUG - Selected crypto row index: {row_idx}")
        
        if df.empty or row_idx >= len(df):
            print(f"DEBUG - Invalid crypto row index or empty dataframe")
            selected_crypto_store.set(None)
            return

        # Use the sorted DataFrame to match table order
        df_sorted = df.sort_values('balance', ascending=False)
        if row_idx >= len(df_sorted):
            print(f"DEBUG - Row index out of bounds for sorted dataframe")
            selected_crypto_store.set(None)
            return

        row = df_sorted.iloc[row_idx].to_dict()
        print(f"DEBUG - Selected crypto row data: {row.get('crypto_type', 'N/A')}")
        selected_crypto_store.set(row)

    @output
    @render.ui
    def cryptocurrency_portfolio_summary():
        """Display cryptocurrency portfolio summary"""
        try:
            df = cryptocurrency_portfolio_data()
            if df.empty:
                return ui.tags.div(
                    ui.tags.div("₿", class_="empty-icon"),
                    ui.tags.h5("No Cryptocurrency Assets", class_="mb-2"),
                    ui.tags.p("No cryptocurrency assets found in digital_assets accounts", class_="text-muted"),
                    class_="empty-state"
                )
            
            # Calculate summary metrics
            total_cryptos = len(df)
            positive_balances = len(df[df["balance"] > 0])
            unique_types = df["crypto_type"].nunique()
            total_accounts = df["account_name"].nunique()
            
            return ui.tags.div(
                ui.tags.div(
                    ui.tags.div(f"{total_cryptos}", class_="stat-number"),
                    ui.tags.div("Total Holdings", class_="stat-label"),
                    class_="stat-card"
                ),
                ui.tags.div(
                    ui.tags.div(f"{positive_balances}", class_="stat-number"),
                    ui.tags.div("Positive Balances", class_="stat-label"),
                    class_="stat-card"
                ),
                ui.tags.div(
                    ui.tags.div(f"{unique_types}", class_="stat-number"),
                    ui.tags.div("Crypto Types", class_="stat-label"),
                    class_="stat-card"
                ),
                ui.tags.div(
                    ui.tags.div(f"{total_accounts}", class_="stat-number"),
                    ui.tags.div("Asset Accounts", class_="stat-label"),
                    class_="stat-card"
                ),
                class_="stats-grid"
            )
            
        except Exception as e:
            print(f"Error in cryptocurrency portfolio summary: {e}")
            return ui.tags.div(
                ui.tags.div("⚠️", class_="empty-icon"),
                ui.tags.h5("Error Loading Summary", class_="mb-2"),
                ui.tags.p("Unable to load cryptocurrency portfolio data", class_="text-muted"),
                class_="empty-state"
            )

    @output
    @render.ui
    def cryptocurrency_detail_display():
        """Display detailed cryptocurrency information"""
        row = selected_crypto_store.get()
        if not row:
            return ui.tags.div(
                ui.tags.h5("Select a Cryptocurrency", class_="mb-2"),
                ui.tags.p("Click on a cryptocurrency row to view details", class_="text-muted"),
                class_="empty-state",
                style="padding: 2rem; text-align: center; background-color: var(--bs-light); border-radius: 8px;"
            )

        try:
            crypto_type = row.get("crypto_type", "")
            crypto_display_name = row.get("crypto_display_name", "")
            balance = row.get("balance", 0)
            account_name = row.get("account_name", "")

            # Create clean detail content
            content = []
            
            # Header with token image
            logo_url = get_crypto_logo_url(crypto_type) if crypto_type else ""
            
            header_content = [
                ui.tags.h5("Asset Details", class_="mb-3", style="color: var(--bs-primary); font-weight: 600;")
            ]
            
            if logo_url and crypto_type:
                header_content.insert(0, ui.div(
                    ui.img(
                        src=logo_url,
                        alt=f"{crypto_type} logo",
                        style="width: 48px; height: 48px; margin-bottom: 1rem; border-radius: 50%; display: block; margin-left: auto; margin-right: auto;"
                    ),
                    style="text-align: center;"
                ))
            
            content.extend(header_content)
            
            # Asset info with clean formatting
            detail_items = [
                ("Symbol", crypto_type),
                ("Name", crypto_display_name),
                ("Balance", f"{float(balance):,.6f} {crypto_type}"),
                ("Account", account_name),
                ("Status", row.get("status", "Active")),
                ("Last Activity", safe_str(row.get("date", "")))
            ]
            
            for label, value in detail_items:
                if value and value != "":  # Only show non-empty values
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
            if crypto_type.upper() in ["ETH", "WETH", "BTC", "WBTC"]:
                content.append(ui.tags.div(
                    ui.tags.strong("External Links", style="color: var(--bs-dark);"),
                    style="margin-bottom: 0.5rem; margin-top: 1rem;"
                ))
                
                if crypto_type.upper() in ["ETH", "WETH"]:
                    content.append(ui.tags.div(
                        ui.tags.a("Etherscan", href="https://etherscan.io/", target="_blank", 
                                 class_="btn btn-outline-primary btn-sm",
                                 style="text-decoration: none;")
                    ))
                elif crypto_type.upper() in ["BTC", "WBTC"]:
                    content.append(ui.tags.div(
                        ui.tags.a("Blockstream", href="https://blockstream.info/", target="_blank", 
                                 class_="btn btn-outline-primary btn-sm",
                                 style="text-decoration: none;")
                    ))

            return ui.tags.div(
                *content,
                style="padding: 1.5rem; background-color: var(--bs-light); border-radius: 8px;"
            )

        except Exception as e:
            print(f"Error displaying cryptocurrency details: {e}")
            return ui.tags.div(
                ui.tags.h5("Error Loading Crypto Data", class_="mb-2"),
                ui.tags.p(f"Unable to load cryptocurrency details: {e}", class_="text-muted"),
                class_="empty-state",
                style="padding: 2rem; text-align: center; background-color: var(--bs-light);"
            )

    # New KPI Functions for Enhanced UI
    @output
    @render.ui
    def crypto_total_value():
        """Calculate total cryptocurrency portfolio value"""
        try:
            df = cryptocurrency_portfolio_data()
            if df.empty:
                return "0.000000 ETH"
            
            # Sum all positive balances (treat as ETH equivalent)
            total_value = float(df[df['balance'] > 0]['balance'].sum())
            return f"{total_value:,.6f} ETH"
        except Exception as e:
            print(f"Error calculating crypto total value: {e}")
            return "Error"

    @output
    @render.ui
    def crypto_asset_count():
        """Count total cryptocurrency assets"""
        try:
            df = cryptocurrency_portfolio_data()
            if df.empty:
                return "0"
            return str(len(df[df['balance'] > 0]))
        except Exception as e:
            print(f"Error counting crypto assets: {e}")
            return "Error"

    @output
    @render.ui
    def crypto_largest_holding():
        """Find largest cryptocurrency holding"""
        try:
            df = cryptocurrency_portfolio_data()
            if df.empty:
                return "None"
            
            largest = df.loc[df['balance'].idxmax()]
            crypto_symbol = largest['crypto_type']
            balance = float(largest['balance'])
            
            return f"{crypto_symbol}: {balance:,.4f}"
        except Exception as e:
            print(f"Error finding largest holding: {e}")
            return "Error"

    @output
    @render.ui
    def crypto_largest_holding_image():
        """Get largest cryptocurrency holding token image"""
        try:
            df = cryptocurrency_portfolio_data()
            if df.empty:
                return ui.div()
            
            largest = df.loc[df['balance'].idxmax()]
            crypto_symbol = largest['crypto_type']
            logo_url = get_crypto_logo_url(crypto_symbol)
            
            return ui.img(
                src=logo_url,
                alt=f"{crypto_symbol} logo",
                style="width: 32px; height: 32px; border-radius: 50%;"
            )
        except Exception as e:
            print(f"Error finding largest holding image: {e}")
            return ui.div()

    @output
    @render.ui
    def crypto_portfolio_health():
        """Assess portfolio health"""
        try:
            df = cryptocurrency_portfolio_data()
            if df.empty:
                return "No Data"
            
            positive_count = len(df[df['balance'] > 0])
            total_count = len(df)
            health_pct = (positive_count / total_count) * 100 if total_count > 0 else 0
            
            if health_pct >= 80:
                return "Excellent"
            elif health_pct >= 60:
                return "Good"
            elif health_pct >= 40:
                return "Fair"
            else:
                return "Needs Attention"
        except Exception as e:
            print(f"Error calculating portfolio health: {e}")
            return "Error"

    @output
    @render.ui
    def crypto_portfolio_composition():
        """Show portfolio composition breakdown"""
        try:
            df = cryptocurrency_portfolio_data()
            if df.empty:
                return ui.tags.div(
                    ui.tags.h6("No Data Available", class_="text-muted text-center"),
                    style="padding: 2rem;"
                )
            
            # Calculate composition percentages and sort by balance
            positive_df = df[df['balance'] > 0].copy()
            positive_df = positive_df.sort_values('balance', ascending=False)  # Sort largest first
            total_value = positive_df['balance'].sum()
            
            composition_items = []
            for _, row in positive_df.head(5).iterrows():  # Top 5 holdings
                percentage = (row['balance'] / total_value) * 100 if total_value > 0 else 0
                composition_items.append(ui.tags.div(
                    ui.tags.div(
                        ui.tags.strong(row['crypto_type'], style="color: var(--bs-primary);"),
                        ui.tags.span(f"{percentage:.1f}%", style="float: right; color: var(--bs-secondary);")
                    ),
                    ui.tags.div(
                        f"{float(row['balance']):,.6f}",
                        style="font-size: 0.9rem; color: var(--bs-muted); margin-bottom: 0.5rem;"
                    )
                ))
            
            return ui.tags.div(
                *composition_items if composition_items else [ui.tags.p("No positive balances", class_="text-muted")],
                style="padding: 1rem;"
            )
            
        except Exception as e:
            print(f"Error in portfolio composition: {e}")
            return ui.tags.div(
                ui.tags.p("Error loading composition", class_="text-danger"),
                style="padding: 1rem;"
            )

    @output
    @render.ui
    def crypto_recent_activity():
        """Show recent cryptocurrency activity"""
        try:
            df = cryptocurrency_portfolio_data()
            if df.empty:
                return ui.tags.div(
                    ui.tags.h6("No Recent Activity", class_="text-muted text-center"),
                    style="padding: 2rem;"
                )
            
            # Sort by date and get recent entries
            recent_df = df.sort_values('date', ascending=False).head(5)
            
            activity_items = []
            for _, row in recent_df.iterrows():
                activity_items.append(ui.tags.div(
                    ui.tags.div(
                        ui.tags.strong(row['crypto_type']),
                        ui.tags.span(row['date'].strftime("%m/%d"), style="float: right; font-size: 0.85rem; color: var(--bs-secondary);")
                    ),
                    ui.tags.div(
                        f"Balance: {float(row['balance']):,.6f}",
                        style="font-size: 0.9rem; color: var(--bs-muted); margin-bottom: 0.5rem;"
                    )
                ))
            
            return ui.tags.div(
                *activity_items,
                style="padding: 1rem;"
            )
            
        except Exception as e:
            print(f"Error in recent activity: {e}")
            return ui.tags.div(
                ui.tags.p("Error loading activity", class_="text-danger"),
                style="padding: 1rem;"
            )

    @output
    @render.ui
    def crypto_holdings_chart():
        """Holdings distribution analytics"""
        try:
            from faicons import icon_svg
            
            df = cryptocurrency_portfolio_data()
            if df.empty:
                return ui.tags.div(
                    icon_svg("chart-pie", style="font-size: 2.5rem; opacity: 0.3; margin-bottom: 1rem;"),
                    ui.tags.h6("No Holdings Data", class_="mb-2 text-muted"),
                    ui.tags.p("Analytics will appear with cryptocurrency data", class_="text-muted", style="font-size: 0.9rem;"),
                    style="text-align: center; padding: 2rem;"
                )
            
            # Simple text-based distribution for now
            positive_df = df[df['balance'] > 0]
            total_types = len(positive_df['crypto_type'].unique())
            largest_holding = positive_df.loc[positive_df['balance'].idxmax()] if not positive_df.empty else None
            
            if largest_holding is not None:
                return ui.tags.div(
                    ui.tags.h6(f"Portfolio Overview", class_="mb-2", style="color: var(--bs-primary);"),
                    ui.tags.div(
                        ui.tags.div(f"{total_types}", style="font-size: 1.5rem; font-weight: 600; color: var(--bs-success);"),
                        ui.tags.div("Asset Types", style="font-size: 0.85rem; color: var(--bs-muted);"),
                        style="text-align: center; margin-bottom: 1rem;"
                    ),
                    ui.tags.div(
                        ui.tags.strong("Top Asset: "),
                        largest_holding['crypto_type'],
                        style="font-size: 0.9rem; margin-bottom: 0.5rem;"
                    ),
                    ui.tags.div(
                        ui.tags.strong("Active Holdings: "),
                        str(len(positive_df)),
                        style="font-size: 0.9rem;"
                    ),
                    style="padding: 1rem;"
                )
            else:
                return ui.tags.div(
                    ui.tags.h6("No Active Holdings", class_="text-muted"),
                    style="text-align: center; padding: 1rem;"
                )
                
        except Exception as e:
            print(f"Error in holdings chart: {e}")
            return ui.tags.div(
                ui.tags.p("Error loading chart", class_="text-danger"),
                style="padding: 2rem;"
            )

    @output
    @render.ui
    def crypto_balance_trends():
        """Balance trends and statistics"""
        try:
            from faicons import icon_svg
            
            df = cryptocurrency_portfolio_data()
            if df.empty:
                return ui.tags.div(
                    icon_svg("chart-line", style="font-size: 2.5rem; opacity: 0.3; margin-bottom: 1rem;"),
                    ui.tags.h6("No Balance Data", class_="mb-2 text-muted"),
                    ui.tags.p("Balance analysis will appear with historical data", class_="text-muted", style="font-size: 0.9rem;"),
                    style="text-align: center; padding: 2rem;"
                )
            
            # Simple trends summary for now
            positive_balances = df[df['balance'] > 0]
            if not positive_balances.empty:
                avg_balance = positive_balances['balance'].mean()
                max_balance = positive_balances['balance'].max()
                min_balance = positive_balances[positive_balances['balance'] > 0]['balance'].min()
                
                return ui.tags.div(
                    ui.tags.h6("Balance Analysis", class_="mb-2", style="color: var(--bs-primary);"),
                    ui.tags.div(
                        ui.tags.div(
                            ui.tags.div(f"{avg_balance:.4f}", style="font-size: 1.2rem; font-weight: 600; color: var(--bs-primary);"),
                            ui.tags.div("Average Balance", style="font-size: 0.8rem; color: var(--bs-muted);"),
                            style="text-align: center; margin-bottom: 0.75rem;"
                        ),
                        ui.tags.div(
                            ui.tags.span("High: ", style="font-size: 0.85rem; color: var(--bs-success);"),
                            ui.tags.span(f"{max_balance:.4f}", style="font-size: 0.85rem;"),
                            style="margin-bottom: 0.25rem;"
                        ),
                        ui.tags.div(
                            ui.tags.span("Low: ", style="font-size: 0.85rem; color: var(--bs-info);"),
                            ui.tags.span(f"{min_balance:.4f}", style="font-size: 0.85rem;"),
                        )
                    ),
                    style="padding: 1rem;"
                )
            else:
                return ui.tags.div(
                    ui.tags.h6("No Balance Data", class_="text-muted"),
                    style="text-align: center; padding: 2rem;"
                )
                
        except Exception as e:
            print(f"Error in balance trends: {e}")
            return ui.tags.div(
                ui.tags.p("Error loading trends", class_="text-danger"),
                style="padding: 2rem;"
            )