# -*- coding: utf-8 -*-
"""
General Ledger Journal Builder

Creates journal entries for cryptocurrency transactions based on FIFO calculations.
Adapted from master_fifo.py build_full_journal function.
"""

import pandas as pd
from decimal import Decimal, InvalidOperation
from datetime import datetime, date
from typing import Dict, List, Optional, Any
import logging
import os

logger = logging.getLogger(__name__)


def normalize_asset_name(asset: str) -> str:
    """
    Normalize asset name for GL account naming.
    
    Args:
        asset: Asset symbol (e.g., 'ETH', 'USDC')
        
    Returns:
        Normalized asset name for account naming
    """
    return asset.strip().lower().replace(" ", "_")


def build_crypto_journal_entries(df_fifo: pd.DataFrame, 
                                wallet_metadata: Optional[Dict] = None) -> pd.DataFrame:
    """
    Build journal entries from FIFO processed cryptocurrency transactions.
    
    Args:
        df_fifo: DataFrame from FIFO processing with columns:
                fund_id, wallet_address, asset, date, hash, side, qty,
                proceeds_eth, cost_basis_sold_eth, realized_gain_eth,
                proceeds_usd, cost_basis_sold_usd, realized_gain_usd
        wallet_metadata: Optional wallet metadata for fund grouping
        
    Returns:
        DataFrame with journal entries ready for GL posting
    """
    if df_fifo.empty:
        return pd.DataFrame()
    
    # Default wallet metadata if not provided
    if wallet_metadata is None:
        wallet_metadata = {}
    
    # Allowed GL accounts - will be expanded as needed
    ALLOWED_ACCOUNTS = {
        "deemed_cash_usd", 
        "realized_gain_loss",
        "digital_assets_eth",
        "digital_assets_usdc", 
        "digital_assets_usdt",
        "digital_assets_weth",
        "digital_assets_blur_pool",
        "digital_assets_meme",
        "digital_assets_variabledebtethusdc",
        "digital_assets_metastreet_pool_staking_ppg_5",
        "digital_assets_aethweth",
        "digital_assets_aave_yield_token_weth",
        "digital_assets_wsteth"
    }
    
    journal_entries = []
    
    for _, row in df_fifo.iterrows():
        try:
            # Extract transaction details
            asset = str(row["asset"]).upper()
            side = str(row["side"]).lower()
            # Use token_amount for token quantity (display)
            token_qty = Decimal(str(row.get("token_amount", row.get("qty", 0))))
            # Use eth_value for ETH value (cost basis)
            eth_value = Decimal(str(row.get("eth_value", 0)))
            date_val = row["date"]
            tx_hash = row["hash"]
            fund_id = row.get("fund_id", "")
            wallet_address = row.get("wallet_address", "")
            
            # Get financial amounts (ETH-based cost basis, USD for GL entries)
            eth_usd_rate = Decimal(str(row.get("eth_usd_rate", 0)))
            
            # ETH-based values (primary cost basis)
            proceeds_eth = Decimal(str(row.get("proceeds_eth", 0)))
            cost_basis_eth = Decimal(str(row.get("cost_basis_sold_eth", 0)))
            gain_eth = Decimal(str(row.get("realized_gain_eth", 0)))
            
            # USD values for GL entries (converted from ETH if needed)
            proceeds_usd = Decimal(str(row.get("proceeds_usd", 0)))
            cost_basis_usd = Decimal(str(row.get("cost_basis_sold_usd", 0)))
            gain_usd = Decimal(str(row.get("realized_gain_usd", 0)))
            
            # If USD values are missing, convert from ETH
            if proceeds_usd == 0 and proceeds_eth > 0 and eth_usd_rate > 0:
                proceeds_usd = proceeds_eth * eth_usd_rate
            if cost_basis_usd == 0 and cost_basis_eth > 0 and eth_usd_rate > 0:
                cost_basis_usd = cost_basis_eth * eth_usd_rate
            if gain_usd == 0 and gain_eth != 0 and eth_usd_rate > 0:
                gain_usd = gain_eth * eth_usd_rate
            
            # Create GL account names
            asset_account = f"digital_assets_{normalize_asset_name(asset)}"
            cash_account = "deemed_cash_usd"
            gain_account = "realized_gain_loss"
            
            # Add new accounts to allowed set
            ALLOWED_ACCOUNTS.add(asset_account)
            
            # Determine if this is an intercompany transaction
            # For now, default to False - can be enhanced with wallet metadata
            intercompany = False
            
            # Common fields for all journal entries
            common_fields = {
                "date": date_val,
                "transaction_type": "cryptocurrency_trades",
                "fund_id": fund_id,
                "counterparty_fund_id": "",
                "wallet_id": wallet_address,
                "cryptocurrency": asset,
                "hash": tx_hash,
                "from": "",  # Can be populated from transaction metadata
                "to": "",    # Can be populated from transaction metadata
                "contract_address": "",  # Can be populated from transaction metadata
                "intercompany": intercompany,
                "eth_usd_rate": float(eth_usd_rate),
                "units": float(token_qty),  # Token units for reference
                "eth_value": float(eth_value),  # ETH value for this transaction
                # Include both ETH and USD values for audit trail
                "proceeds_eth": float(proceeds_eth),
                "cost_basis_eth": float(cost_basis_eth),
                "realized_gain_eth": float(gain_eth),
            }
            
            if side == "buy":
                # Buy transaction: Dr. Digital Asset, Cr. Cash
                # Use USD value for GL entries, based on ETH cost basis
                transaction_value_usd = token_qty * (proceeds_usd / token_qty if token_qty != 0 else 0)
                
                # Debit: Digital Asset Account
                journal_entries.append({
                    **common_fields,
                    "account": asset_account,
                    "debit_USD": float(transaction_value_usd),
                    "credit_USD": 0.0,
                    "description": f"Purchase of {token_qty:.6f} {asset} tokens (ETH value: {eth_value:.6f}, cost basis: {proceeds_eth:.6f})",
                })
                
                # Credit: Cash Account
                journal_entries.append({
                    **common_fields,
                    "account": cash_account,
                    "debit_USD": 0.0,
                    "credit_USD": float(transaction_value_usd),
                    "description": f"Cash payment for {token_qty:.6f} {asset} tokens (ETH: {eth_value:.6f})",
                })
                
            elif side == "sell":
                # Sell transaction: Dr. Cash, Cr. Digital Asset, Dr/Cr. Gain/Loss
                
                # Debit: Cash Account (proceeds in USD)
                journal_entries.append({
                    **common_fields,
                    "account": cash_account,
                    "debit_USD": float(proceeds_usd),
                    "credit_USD": 0.0,
                    "description": f"Cash received from sale of {token_qty:.6f} {asset} tokens (ETH value: {eth_value:.6f}, proceeds: {proceeds_eth:.6f})",
                })
                
                # Credit: Digital Asset Account (cost basis in USD)
                journal_entries.append({
                    **common_fields,
                    "account": asset_account,
                    "debit_USD": 0.0,
                    "credit_USD": float(cost_basis_usd),
                    "description": f"Cost basis of {token_qty:.6f} {asset} tokens sold (ETH cost basis: {cost_basis_eth:.6f})",
                })
                
                # Realized Gain/Loss entry (if there's a gain or loss)
                if gain_usd != 0:
                    if gain_usd > 0:
                        # Gain: Credit gain account
                        journal_entries.append({
                            **common_fields,
                            "account": gain_account,
                            "debit_USD": 0.0,
                            "credit_USD": float(gain_usd),
                            "description": f"Realized gain on sale of {token_qty:.6f} {asset} tokens (ETH gain: {gain_eth:.6f})",
                        })
                    else:
                        # Loss: Debit gain account
                        journal_entries.append({
                            **common_fields,
                            "account": gain_account,
                            "debit_USD": float(abs(gain_usd)),
                            "credit_USD": 0.0,
                            "description": f"Realized loss on sale of {token_qty:.6f} {asset} tokens (ETH loss: {abs(gain_eth):.6f})",
                        })
                        
        except Exception as e:
            logger.error(f"Error creating journal entry for transaction {row.get('hash', 'unknown')}: {e}")
            continue
    
    # Convert to DataFrame
    journal_df = pd.DataFrame(journal_entries)
    
    if not journal_df.empty:
        # Ensure date column is datetime
        journal_df["date"] = pd.to_datetime(journal_df["date"])
        
        # Sort by date and hash for consistent ordering
        journal_df = journal_df.sort_values(["date", "hash"])
        
        # Reset index
        journal_df = journal_df.reset_index(drop=True)
    
    return journal_df


def export_journal_entries_by_month(journal_df: pd.DataFrame, 
                                   output_dir: str = "journal_exports") -> List[str]:
    """
    Export journal entries to CSV files grouped by month.
    
    Args:
        journal_df: DataFrame with journal entries
        output_dir: Directory to save CSV files
        
    Returns:
        List of created file paths
    """
    if journal_df.empty:
        logger.warning("No journal entries to export")
        return []
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Ensure date column is datetime
    journal_df["date"] = pd.to_datetime(journal_df["date"])
    
    created_files = []
    
    # Group by year and month
    for (year, month), group in journal_df.groupby([
        journal_df["date"].dt.year,
        journal_df["date"].dt.month
    ]):
        # Create filename with month end date
        from calendar import monthrange
        last_day = monthrange(year, month)[1]
        month_end_str = f"{year}{month:02d}{last_day:02d}"
        
        # Add timestamp for uniqueness
        timestamp = datetime.now().strftime("%H%M%S")
        filename = f"{month_end_str}_crypto_trades_{timestamp}.csv"
        
        # Full file path
        file_path = os.path.join(output_dir, filename)
        
        # Export to CSV
        group.to_csv(file_path, index=False)
        created_files.append(file_path)
        
        logger.info(f"Exported {len(group)} journal entries to {file_path}")
    
    return created_files


def validate_journal_entries(journal_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate journal entries for accounting accuracy.
    
    Args:
        journal_df: DataFrame with journal entries
        
    Returns:
        Dictionary with validation results
    """
    validation_results = {
        "is_balanced": True,
        "total_debits": 0.0,
        "total_credits": 0.0,
        "unbalanced_transactions": [],
        "errors": []
    }
    
    if journal_df.empty:
        validation_results["errors"].append("No journal entries to validate")
        return validation_results
    
    try:
        # Calculate totals
        total_debits = journal_df["debit_USD"].sum()
        total_credits = journal_df["credit_USD"].sum()
        
        validation_results["total_debits"] = float(total_debits)
        validation_results["total_credits"] = float(total_credits)
        
        # Check if debits equal credits (within rounding tolerance)
        tolerance = 0.01  # 1 cent tolerance
        is_balanced = abs(total_debits - total_credits) <= tolerance
        validation_results["is_balanced"] = is_balanced
        
        if not is_balanced:
            difference = total_debits - total_credits
            validation_results["errors"].append(
                f"Journal entries not balanced. Difference: ${difference:.2f}"
            )
        
        # Check individual transactions for balance
        transaction_groups = journal_df.groupby("hash")
        for tx_hash, group in transaction_groups:
            tx_debits = group["debit_USD"].sum()
            tx_credits = group["credit_USD"].sum()
            
            if abs(tx_debits - tx_credits) > tolerance:
                validation_results["unbalanced_transactions"].append({
                    "hash": tx_hash,
                    "debits": float(tx_debits),
                    "credits": float(tx_credits),
                    "difference": float(tx_debits - tx_credits)
                })
        
        logger.info(f"Validation complete. Balanced: {is_balanced}, "
                   f"Debits: ${total_debits:.2f}, Credits: ${total_credits:.2f}")
        
    except Exception as e:
        validation_results["errors"].append(f"Validation error: {str(e)}")
        validation_results["is_balanced"] = False
        logger.error(f"Journal validation error: {e}")
    
    return validation_results


def create_summary_report(journal_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a summary report of journal entries by asset and transaction type.
    
    Args:
        journal_df: DataFrame with journal entries
        
    Returns:
        DataFrame with summary statistics
    """
    if journal_df.empty:
        return pd.DataFrame()
    
    summary_data = []
    
    # Group by cryptocurrency and transaction type
    crypto_groups = journal_df.groupby(["cryptocurrency", "transaction_type"])
    
    for (crypto, tx_type), group in crypto_groups:
        # Calculate buy/sell statistics
        buy_entries = group[group["account"].str.contains("digital_assets")]
        sell_entries = group[group["account"] == "deemed_cash_usd"]
        
        buy_amount = buy_entries["debit_USD"].sum()
        sell_amount = sell_entries["debit_USD"].sum()
        
        # Calculate realized gains/losses
        gain_entries = group[group["account"] == "realized_gain_loss"]
        gains = gain_entries["credit_USD"].sum()
        losses = gain_entries["debit_USD"].sum()
        net_gain_loss = gains - losses
        
        summary_data.append({
            "cryptocurrency": crypto,
            "transaction_type": tx_type,
            "total_buy_amount": float(buy_amount),
            "total_sell_amount": float(sell_amount),
            "realized_gains": float(gains),
            "realized_losses": float(losses),
            "net_gain_loss": float(net_gain_loss),
            "transaction_count": len(group["hash"].unique())
        })
    
    return pd.DataFrame(summary_data)