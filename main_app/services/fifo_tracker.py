# -*- coding: utf-8 -*-
"""
FIFO (First In, First Out) Tracker Service

Provides cost basis tracking for cryptocurrency transactions using FIFO methodology.
Adapted from master_fifo.py reference implementation.
"""

import pandas as pd
from collections import deque
from decimal import Decimal, InvalidOperation
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
import logging

logger = logging.getLogger(__name__)


class FIFOTracker:
    """
    FIFO cost basis tracker for cryptocurrency transactions.
    
    Maintains separate lot queues for each (fund_id, wallet, asset) combination
    and processes buy/sell transactions to calculate realized gains/losses.
    """
    
    def __init__(self):
        """Initialize FIFO tracker with empty lots and logs."""
        # Key: (fund_id, wallet, asset) -> deque of [qty, price]
        self.lots: Dict[Tuple[str, str, str], deque] = {}
        # List of processed transaction logs
        self.logs: List[Dict[str, Any]] = []
    
    def process(self, fund_id: str, wallet: str, asset: str, side: str, 
                token_amount: float, eth_value: float, date: datetime, tx_hash: str, 
                log: bool = True, price_usd: Optional[float] = None, 
                eth_usd_rate: Optional[float] = None) -> None:
        """
        Process a single transaction through FIFO methodology using ETH values for cost basis.
        
        Args:
            fund_id: Fund identifier
            wallet: Wallet address
            asset: Asset symbol (e.g., 'ETH', 'USDC', 'MEME')
            side: 'buy' or 'sell'
            token_amount: Number of tokens (for display/reference)
            eth_value: ETH value of the transaction (for FIFO cost basis)
            date: Transaction date
            tx_hash: Transaction hash
            log: Whether to record this transaction in logs
            price_usd: USD value of transaction (for GL entries)
            eth_usd_rate: ETH to USD exchange rate at transaction time
        """
        try:
            # Convert to Decimal for precision
            token_qty = Decimal(str(token_amount))  # Token quantity for reference
            eth_val = Decimal(str(eth_value))  # ETH value for FIFO calculations
            usd_val = Decimal(str(price_usd)) if price_usd else Decimal("0")
            eth_rate = Decimal(str(eth_usd_rate)) if eth_usd_rate else Decimal("0")
        except (InvalidOperation, TypeError) as e:
            logger.error(f"Invalid amounts for {tx_hash}: {e}")
            return
        
        # Create key for this asset/wallet/fund combination
        key = (fund_id, wallet, asset)
        
        # Get or create deque for this combination
        dq = self.lots.setdefault(key, deque())
        
        # Initialize tracking variables (ETH values for cost basis)
        proceeds_eth = cost_basis_eth = gain_eth = Decimal("0")
        proceeds_usd = cost_basis_usd = gain_usd = Decimal("0")
        remaining_eth_value = eth_val
        remaining_token_amount = token_qty
        
        if str(side).lower() == "buy":
            # For buys: add new lot with ETH value and token metadata
            # Lot structure: [eth_value, usd_value, token_amount, asset_symbol]
            dq.append([eth_val, usd_val, token_qty, asset])
            remaining_eth_value = eth_val
            remaining_token_amount = token_qty
            
        elif str(side).lower() == "sell":
            # For sells: consume ETH value from oldest lots (FIFO)
            eth_to_sell = abs(eth_val)  # ETH value to sell
            tokens_sold = Decimal("0")  # Track token amounts sold
            
            while eth_to_sell > 0 and dq:
                # Get oldest lot: [eth_value, usd_value, token_amount, asset_symbol]
                lot = dq[0]
                if len(lot) >= 4:
                    lot_eth_value, lot_usd_value, lot_token_amount, lot_asset = lot
                else:
                    # Legacy format handling (should not happen with new data)
                    lot_eth_value = lot[0]
                    lot_usd_value = lot[1] if len(lot) > 1 else Decimal("0")
                    lot_token_amount = lot[2] if len(lot) > 2 else Decimal("0")
                    lot_asset = asset
                
                if lot_eth_value <= eth_to_sell:
                    # Consume entire lot
                    consume_eth = lot_eth_value
                    consume_tokens = lot_token_amount
                    consume_usd = lot_usd_value
                    dq.popleft()  # Remove consumed lot
                else:
                    # Partial consumption - proportional to ETH value
                    consume_eth = eth_to_sell
                    proportion = consume_eth / lot_eth_value
                    consume_tokens = lot_token_amount * proportion
                    consume_usd = lot_usd_value * proportion
                    
                    # Update remaining lot
                    dq[0][0] = lot_eth_value - consume_eth  # ETH value
                    dq[0][1] = lot_usd_value - consume_usd  # USD value  
                    dq[0][2] = lot_token_amount - consume_tokens  # Token amount
                
                # Calculate proceeds and cost basis (ETH-based)
                portion_proceeds_eth = consume_eth  # Sale proceeds in ETH
                portion_cost_basis_eth = consume_eth  # Cost basis = ETH value of lot consumed
                portion_gain_eth = (eth_val / abs(eth_val)) * consume_eth - consume_eth  # Gain based on sale price vs cost
                
                # For gain calculation, we need to compare sale ETH rate vs buy ETH rate
                # Sale proceeds = sell_eth_value, Cost basis = consumed lot ETH value
                actual_sale_proceeds = (eth_val / abs(eth_val)) * consume_eth  # Proportional sale proceeds
                portion_gain_eth = actual_sale_proceeds - consume_eth
                
                # USD equivalents
                portion_proceeds_usd = (usd_val / abs(eth_val)) * consume_eth if eth_val != 0 else consume_usd
                portion_cost_basis_usd = consume_usd
                portion_gain_usd = portion_proceeds_usd - portion_cost_basis_usd
                
                # Accumulate totals
                proceeds_eth += portion_proceeds_eth
                cost_basis_eth += portion_cost_basis_eth
                gain_eth += portion_gain_eth
                
                proceeds_usd += portion_proceeds_usd
                cost_basis_usd += portion_cost_basis_usd
                gain_usd += portion_gain_usd
                
                tokens_sold += consume_tokens
                
                # Reduce remaining ETH to sell
                eth_to_sell -= consume_eth
            
            # Calculate remaining position after sale
            remaining_eth_value = sum(lot[0] for lot in dq)
            remaining_token_amount = sum(lot[2] if len(lot) > 2 else Decimal("0") for lot in dq)
            
        # Log transaction if requested
        if log:
            # Ensure date is timezone-naive for consistent handling
            if hasattr(date, 'tz_localize'):
                log_date = date.tz_localize(None) if date.tzinfo is not None else date
            elif hasattr(date, 'replace'):
                log_date = date.replace(tzinfo=None) if date.tzinfo is not None else date
            else:
                log_date = date
                
            self.logs.append({
                "fund_id": fund_id,
                "wallet_address": wallet,
                "asset": asset,
                "date": log_date,
                "hash": tx_hash,
                "side": str(side).lower(),
                # Token information (for display)
                "token_amount": float(token_qty),
                "eth_value": float(eth_val),
                # ETH-based cost basis (primary)
                "proceeds_eth": float(proceeds_eth),
                "cost_basis_sold_eth": float(cost_basis_eth),
                "realized_gain_eth": float(gain_eth),
                "remaining_eth_value": float(remaining_eth_value),
                # USD values (for GL entries)
                "usd_value": float(usd_val),
                "proceeds_usd": float(proceeds_usd),
                "cost_basis_sold_usd": float(cost_basis_usd),
                "realized_gain_usd": float(gain_usd),
                # Position tracking
                "remaining_token_amount": float(remaining_token_amount),
                "eth_usd_rate": float(eth_rate),
            })
    
    def to_dataframe(self) -> pd.DataFrame:
        """
        Convert logged transactions to DataFrame.
        
        Returns:
            DataFrame containing all processed transactions with FIFO calculations
        """
        return pd.DataFrame(self.logs)
    
    def get_current_position(self, fund_id: str, wallet: str, asset: str) -> Dict[str, float]:
        """
        Get current position for a specific asset/wallet/fund combination.
        
        Args:
            fund_id: Fund identifier
            wallet: Wallet address
            asset: Asset symbol
            
        Returns:
            Dictionary with current token amount, ETH value, and cost basis
        """
        key = (fund_id, wallet, asset)
        dq = self.lots.get(key, deque())
        
        # New lot structure: [eth_value, usd_value, token_amount, asset_symbol]
        total_eth_value = sum(lot[0] for lot in dq)  # Total ETH value
        total_usd_value = sum(lot[1] if len(lot) > 1 else Decimal("0") for lot in dq)  # Total USD value
        total_token_amount = sum(lot[2] if len(lot) > 2 else Decimal("0") for lot in dq)  # Total tokens
        
        # Average prices
        avg_eth_per_token = float(total_eth_value / total_token_amount) if total_token_amount > 0 else 0.0
        avg_usd_per_token = float(total_usd_value / total_token_amount) if total_token_amount > 0 else 0.0
        
        return {
            "asset": asset,
            "token_amount": float(total_token_amount),
            "eth_value": float(total_eth_value),
            "usd_value": float(total_usd_value),
            "cost_basis_eth": float(total_eth_value),  # Cost basis = current ETH value of lots
            "cost_basis_usd": float(total_usd_value),  # Cost basis = current USD value of lots
            "average_price_eth": avg_eth_per_token,
            "average_price_usd": avg_usd_per_token,
            "lot_count": len(dq)
        }
    
    def get_all_positions(self) -> pd.DataFrame:
        """
        Get all current positions as DataFrame.
        
        Returns:
            DataFrame with current positions for all assets
        """
        positions = []
        for (fund_id, wallet, asset), dq in self.lots.items():
            if dq:  # Only include non-empty positions
                position = self.get_current_position(fund_id, wallet, asset)
                positions.append({
                    "fund_id": fund_id,
                    "wallet_address": wallet,
                    "asset": asset,
                    **position
                })
        
        return pd.DataFrame(positions)


def build_fifo_ledger(df_input: pd.DataFrame, price_column: str = "price_eth") -> pd.DataFrame:
    """
    Build FIFO ledger from transaction data using ETH values for cost basis.
    
    Args:
        df_input: DataFrame with transaction data including token_amount and eth_value columns
        price_column: Column name containing price data (for backward compatibility)
        
    Returns:
        DataFrame with FIFO calculations applied
    """
    tracker = FIFOTracker()
    
    # Process each transaction
    for i, row in enumerate(df_input.itertuples(index=False)):
        try:
            side_val = getattr(row, 'side', '')
            print(f"  Processing transaction {i+1}: side='{side_val}' (type: {type(side_val)})")
            
            # Calculate token amount and ETH value from input data
            token_qty = getattr(row, 'qty', 0)
            
            # Calculate ETH value: qty * price_eth gives total ETH value for this transaction
            price_eth = getattr(row, price_column, 0)
            eth_value = token_qty * price_eth
            
            tracker.process(
                fund_id=str(getattr(row, 'fund_id', '')),
                wallet=str(getattr(row, 'wallet_address', '')),
                asset=str(getattr(row, 'asset', '')),
                side=str(getattr(row, 'side', '')),
                token_amount=token_qty,  # Token quantity for display
                eth_value=eth_value,     # ETH value for FIFO cost basis
                date=getattr(row, 'date'),
                tx_hash=str(getattr(row, 'hash', '')),
                log=True,
                price_usd=getattr(row, 'price_usd', 0),
                eth_usd_rate=getattr(row, 'eth_usd_rate', None)
            )
        except Exception as e:
            print(f"  Error processing transaction {i+1}: {e}")
            print(f"  Row data: {row}")
            raise
    
    # Return processed DataFrame
    df_result = tracker.to_dataframe()
    
    # Filter out zero token amount transactions
    df_result = df_result[df_result["token_amount"] != 0]
    
    return df_result


def convert_crypto_fetch_to_fifo_format(df_transactions: pd.DataFrame) -> pd.DataFrame:
    """
    Convert crypto_fetch transaction format to FIFO input format with ETH-based cost basis.
    
    Args:
        df_transactions: DataFrame from crypto_token_fetch with columns:
                        date, tx_hash, direction, token_name, token_amount, 
                        token_value_eth, token_value_usd, from_address, to_address
    
    Returns:
        DataFrame formatted for FIFO processing with ETH cost basis
    """
    fifo_df = pd.DataFrame()
    
    if df_transactions.empty:
        return fifo_df
    
    print("Converting transactions to FIFO format with ETH-based cost basis:")
    print(f"  Input columns: {list(df_transactions.columns)}")
    print(f"  Input dtypes: {df_transactions.dtypes.to_dict()}")
    print(f"  Sample direction values: {df_transactions['direction'].head().tolist()}")
    print(f"  Sample token_name values: {df_transactions['token_name'].head().tolist()}")
    print(f"  Sample token_amount values: {df_transactions['token_amount'].head().tolist()}")
    print(f"  Sample token_value_eth values: {df_transactions['token_value_eth'].head().tolist()}")
    
    # Map crypto_fetch columns to FIFO format
    fifo_df['fund_id'] = 'fund_i_class_B_ETH'  # Default fund
    fifo_df['wallet_address'] = df_transactions.get('from_address', '')
    try:
        fifo_df['asset'] = df_transactions['token_name'].astype(str).str.upper()
        print("  Asset name conversion successful")
    except Exception as e:
        print(f"  Error in asset name conversion: {e}")
        print(f"  Token name values: {df_transactions['token_name'].tolist()}")
        raise
    
    # Handle timezone-aware dates properly
    date_series = pd.to_datetime(df_transactions['date'])
    if date_series.dt.tz is not None:
        fifo_df['date'] = date_series.dt.tz_localize(None)
    else:
        fifo_df['date'] = date_series
    fifo_df['hash'] = df_transactions['tx_hash']
    
    # Convert direction to side with proper type handling
    try:
        direction_series = df_transactions['direction'].astype(str).str.lower()
        fifo_df['side'] = direction_series.map({
            'incoming': 'buy',
            'outgoing': 'sell',
            'in': 'buy',
            'out': 'sell'
        })
        
        # Handle any unmapped values
        unmapped = fifo_df['side'].isna().sum()
        if unmapped > 0:
            print(f"  Warning: {unmapped} unmapped direction values")
            unique_directions = direction_series.unique()
            print(f"  Unique directions found: {unique_directions}")
            # Fill unmapped values based on common patterns
            fifo_df['side'] = fifo_df['side'].fillna('sell').infer_objects(copy=False)  # Default to sell for safety
        
        print("  Direction conversion successful")
    except Exception as e:
        print(f"  Error in direction conversion: {e}")
        print(f"  Direction values: {df_transactions['direction'].tolist()}")
        raise
    
    # Token quantities (for display and reference)
    fifo_df['qty'] = df_transactions['token_amount'].abs()  # Ensure positive
    
    # ETH-based cost basis calculation
    # We now calculate ETH value per token for consistent FIFO processing
    token_amounts = df_transactions['token_amount'].abs()
    eth_values = df_transactions['token_value_eth'].abs()
    
    # Avoid division by zero
    valid_amounts = token_amounts > 0
    fifo_df['price_eth'] = 0.0
    fifo_df.loc[valid_amounts, 'price_eth'] = (eth_values / token_amounts).loc[valid_amounts]
    
    # USD values (secondary, for GL entries)
    usd_values = df_transactions['token_value_usd'].abs()
    fifo_df['price_usd'] = 0.0
    fifo_df.loc[valid_amounts, 'price_usd'] = (usd_values / token_amounts).loc[valid_amounts]
    
    # ETH to USD exchange rate
    fifo_df['eth_usd_rate'] = df_transactions.get('eth_price_usd', 0)
    
    # Fill missing values and handle pandas FutureWarning
    fifo_df = fifo_df.fillna(0).infer_objects(copy=False)
    
    # Handle infinite values that might result from division by zero
    fifo_df = fifo_df.replace([float('inf'), float('-inf')], 0)
    
    print(f"  Conversion complete: {len(fifo_df)} transactions")
    print(f"  Sample price_eth values: {fifo_df['price_eth'].head().tolist()}")
    print(f"  Sample qty values: {fifo_df['qty'].head().tolist()}")
    
    return fifo_df