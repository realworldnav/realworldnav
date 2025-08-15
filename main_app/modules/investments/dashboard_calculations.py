from shiny import reactive
from ...s3_utils import load_GL_file
import pandas as pd
from decimal import Decimal, InvalidOperation
from typing import Dict, Optional

# === Helpers ===
def safe_decimal(val):
    try:
        return Decimal(str(val))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")

def safe_str(v):
    return "" if pd.isna(v) or v is None else str(v)

# === Dashboard Data Functions ===
def create_investment_dashboard_calculations(selected_fund):
    """Create reactive calculations for investment dashboard"""
    
    @reactive.calc
    def dashboard_loans_data():
        """Get loan portfolio data for dashboard"""
        try:
            gl = load_GL_file()
            fund_id = selected_fund()
            
            if not fund_id:
                return pd.DataFrame()
            
            gl = gl[gl["fund_id"] == fund_id]
            
            # Get loan accounts
            loans = gl[gl["account_name"].str.contains(r"loan|interest_receivable|bad debt", case=False, na=False)].copy()
            if loans.empty:
                return pd.DataFrame()
            
            loans = loans[loans["loan_id"].notna()]
            loans["loan_id"] = loans["loan_id"].astype(str)
            
            # Calculate loan amounts
            loans["crypto_amount"] = loans.apply(
                lambda r: safe_decimal(r["debit_crypto"]) - safe_decimal(r["credit_crypto"]), axis=1
            )
            
            print(f"DEBUG - Dashboard: Found {len(loans)} loan entries")
            return loans
            
        except Exception as e:
            print(f"Error in dashboard_loans_data: {e}")
            return pd.DataFrame()
    
    @reactive.calc
    def dashboard_nft_data():
        """Get NFT portfolio data for dashboard"""
        try:
            gl = load_GL_file()
            fund_id = selected_fund()
            
            if not fund_id:
                return pd.DataFrame()
            
            gl = gl[gl["fund_id"] == fund_id]
            
            # Get NFTs from owned investment accounts
            owned_nfts = gl[
                gl["account_name"].isin([
                    "investments_nfts_seized_collateral", 
                    "investments_nfts"
                ])
            ].copy()
            
            if owned_nfts.empty:
                return pd.DataFrame()
            
            # Filter for rows with NFT data
            nft_rows = owned_nfts[
                (owned_nfts["collateral_address"].notna()) & 
                (owned_nfts["token_id"].notna()) &
                (owned_nfts["collateral_address"] != "") &
                (owned_nfts["token_id"] != "")
            ].copy()
            
            if not nft_rows.empty:
                nft_rows["crypto_amount"] = nft_rows.apply(
                    lambda r: safe_decimal(r["debit_crypto"]) - safe_decimal(r["credit_crypto"]), axis=1
                )
            
            print(f"DEBUG - Dashboard: Found {len(nft_rows)} NFT entries")
            return nft_rows
            
        except Exception as e:
            print(f"Error in dashboard_nft_data: {e}")
            return pd.DataFrame()
    
    @reactive.calc 
    def dashboard_crypto_data():
        """Get cryptocurrency portfolio data for dashboard"""
        try:
            gl = load_GL_file()
            fund_id = selected_fund()
            
            if not fund_id:
                return pd.DataFrame()
            
            gl = gl[gl["fund_id"] == fund_id]
            
            # Get cryptocurrency assets from digital_assets accounts
            crypto_assets = gl[
                gl["account_name"].str.contains(r"digital_assets", case=False, na=False)
            ].copy()
            
            if crypto_assets.empty:
                return pd.DataFrame()
            
            # Calculate crypto amounts
            crypto_assets["crypto_amount"] = crypto_assets.apply(
                lambda r: safe_decimal(r["debit_crypto"]) - safe_decimal(r["credit_crypto"]), axis=1
            )
            
            print(f"DEBUG - Dashboard: Found {len(crypto_assets)} crypto entries")
            return crypto_assets
            
        except Exception as e:
            print(f"Error in dashboard_crypto_data: {e}")
            return pd.DataFrame()
    
    @reactive.calc
    def dashboard_portfolio_summary():
        """Calculate portfolio summary metrics"""
        try:
            loans_df = dashboard_loans_data()
            nft_df = dashboard_nft_data()
            crypto_df = dashboard_crypto_data()
            
            summary = {
                "total_portfolio_value": 0.0,
                "active_loans": 0,
                "nft_count": 0,
                "crypto_count": 0,
                "loan_value": 0.0,
                "nft_value": 0.0,
                "crypto_value": 0.0
            }
            
            # Calculate loan metrics
            if not loans_df.empty:
                # Count unique active loans
                unique_loans = loans_df.groupby("loan_id")["crypto_amount"].sum()
                active_loans = unique_loans[unique_loans > 0]
                summary["active_loans"] = len(active_loans)
                summary["loan_value"] = float(active_loans.sum())
            
            # Calculate NFT metrics
            if not nft_df.empty:
                # Count unique NFTs
                unique_nfts = nft_df.groupby(["collateral_address", "token_id"])["crypto_amount"].sum()
                summary["nft_count"] = len(unique_nfts)
                summary["nft_value"] = float(unique_nfts.abs().sum())
            
            # Calculate crypto metrics
            if not crypto_df.empty:
                # Count crypto types with positive balances
                crypto_summary = crypto_df.groupby("account_name")["crypto_amount"].sum()
                positive_crypto = crypto_summary[crypto_summary > 0]
                summary["crypto_count"] = len(positive_crypto)
                summary["crypto_value"] = float(positive_crypto.sum())
            
            # Calculate total portfolio value
            summary["total_portfolio_value"] = (
                summary["loan_value"] + 
                summary["nft_value"] + 
                summary["crypto_value"]
            )
            
            print(f"DEBUG - Dashboard Summary: {summary}")
            return summary
            
        except Exception as e:
            print(f"Error in dashboard_portfolio_summary: {e}")
            return {
                "total_portfolio_value": 0.0,
                "active_loans": 0,
                "nft_count": 0,
                "crypto_count": 0,
                "loan_value": 0.0,
                "nft_value": 0.0,
                "crypto_value": 0.0
            }
    
    @reactive.calc
    def dashboard_portfolio_allocation():
        """Calculate portfolio allocation breakdown"""
        try:
            summary = dashboard_portfolio_summary()
            
            total_value = summary["total_portfolio_value"]
            if total_value <= 0:
                return []
            
            allocation = []
            
            if summary["loan_value"] > 0:
                allocation.append({
                    "category": "Loans",
                    "value": summary["loan_value"],
                    "percentage": (summary["loan_value"] / total_value) * 100,
                    "color": "#007bff"
                })
            
            if summary["nft_value"] > 0:
                allocation.append({
                    "category": "NFTs",
                    "value": summary["nft_value"],
                    "percentage": (summary["nft_value"] / total_value) * 100,
                    "color": "#28a745"
                })
            
            if summary["crypto_value"] > 0:
                allocation.append({
                    "category": "Crypto",
                    "value": summary["crypto_value"],
                    "percentage": (summary["crypto_value"] / total_value) * 100,
                    "color": "#ffc107"
                })
            
            return allocation
            
        except Exception as e:
            print(f"Error in dashboard_portfolio_allocation: {e}")
            return []
    
    @reactive.calc
    def dashboard_top_assets():
        """Get top performing/largest assets"""
        try:
            loans_df = dashboard_loans_data()
            nft_df = dashboard_nft_data()
            crypto_df = dashboard_crypto_data()
            
            top_assets = []
            
            # Top loans
            if not loans_df.empty:
                loan_summary = loans_df.groupby("loan_id")["crypto_amount"].sum().sort_values(ascending=False)
                for loan_id, amount in loan_summary.head(3).items():
                    if amount > 0:
                        top_assets.append({
                            "name": f"Loan {loan_id}",
                            "type": "Loan",
                            "value": float(amount),
                            "color": "#007bff"
                        })
            
            # Top NFTs
            if not nft_df.empty:
                nft_summary = nft_df.groupby(["collateral_address", "token_id"])["crypto_amount"].sum()
                nft_summary = nft_summary.abs().sort_values(ascending=False)
                
                for (address, token_id), amount in nft_summary.head(2).items():
                    if amount > 0:
                        # Truncate address for display
                        short_address = f"{address[:6]}...{address[-4:]}" if len(address) > 10 else address
                        top_assets.append({
                            "name": f"NFT {short_address}#{token_id}",
                            "type": "NFT", 
                            "value": float(amount),
                            "color": "#28a745"
                        })
            
            # Top crypto holdings
            if not crypto_df.empty:
                crypto_summary = crypto_df.groupby("account_name")["crypto_amount"].sum()
                crypto_summary = crypto_summary[crypto_summary > 0].sort_values(ascending=False)
                
                for account_name, amount in crypto_summary.head(2).items():
                    # Extract crypto type from account name
                    crypto_type = account_name.split("_")[-1].upper() if "_" in account_name else account_name
                    top_assets.append({
                        "name": crypto_type,
                        "type": "Crypto",
                        "value": float(amount),
                        "color": "#ffc107"
                    })
            
            # Sort by value and return top 5
            top_assets.sort(key=lambda x: x["value"], reverse=True)
            return top_assets[:5]
            
        except Exception as e:
            print(f"Error in dashboard_top_assets: {e}")
            return []
    
    return {
        "portfolio_summary": dashboard_portfolio_summary,
        "portfolio_allocation": dashboard_portfolio_allocation,
        "top_assets": dashboard_top_assets,
        "loans_data": dashboard_loans_data,
        "nft_data": dashboard_nft_data,
        "crypto_data": dashboard_crypto_data
    }