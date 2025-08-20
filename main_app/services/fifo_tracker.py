# -*- coding: utf-8 -*-
"""
FIFO (First In, First Out) Tracker Service

Provides cost basis tracking for cryptocurrency transactions using FIFO methodology.
Simplified ETH-based approach without asset categorization.
"""

import pandas as pd
from collections import deque
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
import logging

logger = logging.getLogger(__name__)

# Decimal precision settings
SCALE_CRYPTO = Decimal('0.000000000000000001')  # 18 decimals
SCALE_ETH = Decimal('0.000000000000000001')     # 18 decimals


class FIFOTracker:
    """
    FIFO cost basis tracker for cryptocurrency transactions.
    
    Tracks ETH-based cost basis uniformly for all assets.
    Maintains separate lot queues for each (fund_id, wallet, asset) combination.
    """
    
    def __init__(self):
        """Initialize FIFO tracker with empty lots and logs."""
        # Key: (fund_id, wallet, asset) -> deque of [qty, unit_price_eth]
        self.lots: Dict[Tuple[str, str, str], deque] = {}
        # List of processed transaction logs
        self.logs: List[Dict[str, Any]] = []
    
    def process(self, fund_id: str, wallet: str, asset: str, side: str, 
                qty: Decimal, unit_price_eth: Decimal, date: datetime, 
                tx_hash: str, price_eth: Optional[Decimal] = None,
                log: bool = True) -> None:
        """
        Process a single transaction through FIFO methodology using ETH-based cost basis.
        
        Args:
            fund_id: Fund identifier
            wallet: Wallet address
            asset: Asset symbol (e.g., 'ETH', 'USDC', 'WETH')
            side: 'buy' or 'sell'
            qty: Token quantity (positive for buy, negative for sell)
            unit_price_eth: ETH value per token (token_value_eth / token_amount)
            date: Transaction date
            tx_hash: Transaction hash
            price_eth: ETH/USD price at transaction time (optional)
            log: Whether to record this transaction in logs
        """
        try:
            # Ensure Decimal types
            qty = Decimal(str(qty))
            unit_price_eth = Decimal(str(unit_price_eth))
            price_eth = Decimal(str(price_eth)) if price_eth else Decimal("0")
        except (InvalidOperation, TypeError) as e:
            logger.error(f"Invalid amounts for {tx_hash}: {e}")
            return
        
        # Create key for this asset/wallet/fund combination
        key = (fund_id.lower(), wallet.lower(), asset.upper())
        
        # Get or create deque for this combination
        dq = self.lots.setdefault(key, deque())
        
        # Calculate total ETH value for this transaction
        amount_eth = abs(qty) * unit_price_eth
        
        # Initialize tracking variables
        proceeds_eth = Decimal("0")
        cost_basis_sold_eth = Decimal("0")
        realized_gain_eth = Decimal("0")
        
        if side.lower() == "buy":
            # For buys: add new lot with [qty, unit_price_eth]
            dq.append([abs(qty), unit_price_eth])
            logger.debug(f"Added buy lot: {abs(qty)} {asset} @ {unit_price_eth} ETH per token")
            
        elif side.lower() == "sell":
            # For sells: consume lots FIFO-style
            qty_to_sell = abs(qty)
            sell_price_eth = unit_price_eth
            
            # Total proceeds from the sale
            proceeds_eth = qty_to_sell * sell_price_eth
            
            while qty_to_sell > 0 and dq:
                # Get oldest lot
                lot_qty, lot_unit_price = dq[0]
                
                # Determine how much to take from this lot
                take = min(lot_qty, qty_to_sell)
                
                # Calculate cost basis from this lot
                lot_cost_basis = take * lot_unit_price
                cost_basis_sold_eth += lot_cost_basis
                
                # Update or remove lot
                if take < lot_qty:
                    # Partial consumption
                    dq[0][0] = lot_qty - take
                else:
                    # Full consumption
                    dq.popleft()
                
                qty_to_sell -= take
                logger.debug(f"Consumed {take} {asset} from lot @ {lot_unit_price} ETH per token")
            
            # Handle short sales (selling more than owned)
            if qty_to_sell > 0:
                # Create negative lot (short position)
                dq.appendleft([-qty_to_sell, sell_price_eth])
                # For shorts, cost basis = proceeds (no gain/loss until covered)
                short_cost_basis = qty_to_sell * sell_price_eth
                cost_basis_sold_eth += short_cost_basis
                logger.warning(f"Short sale: {qty_to_sell} {asset} @ {sell_price_eth} ETH per token")
            
            # Calculate realized gain
            realized_gain_eth = proceeds_eth - cost_basis_sold_eth
        
        # Calculate remaining position
        remaining_qty = sum(lot[0] for lot in dq)
        remaining_cost_basis_eth = sum(lot[0] * lot[1] for lot in dq if lot[0] > 0)  # Only positive lots
        
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
                "side": side.lower(),
                "qty": float(qty),
                "amount (eth)": float(amount_eth),
                "price_eth": float(price_eth),  # ETH/USD price
                "unit_price_eth": float(unit_price_eth),  # ETH per token
                "proceeds_eth": float(proceeds_eth),
                "cost_basis_sold_eth": float(cost_basis_sold_eth),
                "realized_gain_eth": float(realized_gain_eth),
                "remaining_qty": float(remaining_qty),
                "remaining_cost_basis_eth": float(remaining_cost_basis_eth),
            })
    
    def to_dataframe(self) -> pd.DataFrame:
        """
        Convert logged transactions to DataFrame with exact required column structure.
        
        Returns:
            DataFrame containing all processed transactions with FIFO calculations
            Columns: fund_id, wallet_address, asset, date, hash, side, qty, 
                    amount (eth), price_eth, unit_price_eth, proceeds_eth, 
                    cost_basis_sold_eth, realized_gain_eth, remaining_qty, 
                    remaining_cost_basis_eth
        """
        if not self.logs:
            return pd.DataFrame()
        
        df = pd.DataFrame(self.logs)
        
        # Ensure proper column order (exact specification from user)
        required_columns = [
            'fund_id', 'wallet_address', 'asset', 'date', 'hash', 'side', 
            'qty', 'amount (eth)', 'price_eth', 'unit_price_eth', 
            'proceeds_eth', 'cost_basis_sold_eth', 'realized_gain_eth', 
            'remaining_qty', 'remaining_cost_basis_eth'
        ]
        
        # Add missing columns with default values
        for col in required_columns:
            if col not in df.columns:
                if col in ['qty', 'amount (eth)', 'price_eth', 'unit_price_eth', 
                          'proceeds_eth', 'cost_basis_sold_eth', 'realized_gain_eth', 
                          'remaining_qty', 'remaining_cost_basis_eth']:
                    df[col] = 0.0
                else:
                    df[col] = ''
        
        # Reorder columns to match exact specification
        existing_cols = [col for col in required_columns if col in df.columns]
        extra_cols = [col for col in df.columns if col not in required_columns]
        df = df[existing_cols + extra_cols]
        
        return df
    
    def get_current_position(self, fund_id: str, wallet: str, asset: str) -> Dict[str, float]:
        """
        Get current position for a specific asset/wallet/fund combination.
        
        Args:
            fund_id: Fund identifier
            wallet: Wallet address
            asset: Asset symbol
            
        Returns:
            Dictionary with current position details
        """
        key = (fund_id.lower(), wallet.lower(), asset.upper())
        dq = self.lots.get(key, deque())
        
        total_qty = sum(lot[0] for lot in dq)
        total_cost_basis_eth = sum(lot[0] * lot[1] for lot in dq if lot[0] > 0)
        
        # Calculate average unit price
        positive_qty = sum(lot[0] for lot in dq if lot[0] > 0)
        avg_unit_price_eth = float(total_cost_basis_eth / positive_qty) if positive_qty > 0 else 0.0
        
        return {
            "asset": asset,
            "qty": float(total_qty),
            "cost_basis_eth": float(total_cost_basis_eth),
            "avg_unit_price_eth": avg_unit_price_eth,
            "lot_count": len(dq),
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
                    **position
                })
        
        return pd.DataFrame(positions)


def build_fifo_ledger(df_input: pd.DataFrame) -> pd.DataFrame:
    """
    Build FIFO ledger from transaction data using ETH-based cost basis.
    
    Expected input columns:
    - fund_id, wallet_address, asset, date, hash, side
    - token_amount, token_value_eth (from crypto_fetch)
    - OR qty, unit_price_eth (pre-calculated)
    
    Returns:
        DataFrame with FIFO calculations and required output columns
    """
    logger.info(f"Building FIFO ledger for {len(df_input)} transactions")
    
    # Ensure we have required columns
    required_cols = ['fund_id', 'wallet_address', 'asset', 'date', 'hash', 'side']
    missing_cols = [col for col in required_cols if col not in df_input.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Calculate unit_price_eth if not present
    if 'unit_price_eth' not in df_input.columns:
        if 'token_value_eth' in df_input.columns and 'token_amount' in df_input.columns:
            # Avoid division by zero
            valid_amounts = df_input['token_amount'].abs() > 0
            df_input['unit_price_eth'] = 0.0
            df_input.loc[valid_amounts, 'unit_price_eth'] = (
                df_input.loc[valid_amounts, 'token_value_eth'].abs() / 
                df_input.loc[valid_amounts, 'token_amount'].abs()
            )
        else:
            raise ValueError("Need either unit_price_eth or (token_value_eth, token_amount) columns")
    
    # Calculate qty with proper sign based on side
    if 'qty' not in df_input.columns:
        if 'token_amount' in df_input.columns:
            df_input['qty'] = df_input.apply(
                lambda row: abs(row['token_amount']) if row['side'].lower() == 'buy' 
                else -abs(row['token_amount']), 
                axis=1
            )
        else:
            raise ValueError("Need either qty or token_amount column")
    
    # Get ETH/USD price if available
    price_eth_col = 'price_eth' if 'price_eth' in df_input.columns else 'eth_usd_price'
    if price_eth_col not in df_input.columns:
        df_input['price_eth'] = 0.0  # Will need to fetch from blockchain if needed
    else:
        df_input['price_eth'] = df_input[price_eth_col]
    
    # Sort by date for chronological processing
    df_input = df_input.sort_values('date').reset_index(drop=True)
    
    # Initialize tracker
    tracker = FIFOTracker()
    
    # Process each transaction
    for idx, row in df_input.iterrows():
        try:
            # Convert to Decimal for precision
            qty = Decimal(str(abs(row['qty'])))  # Use absolute value
            unit_price_eth = Decimal(str(row['unit_price_eth']))
            price_eth = Decimal(str(row.get('price_eth', 0)))
            
            tracker.process(
                fund_id=str(row['fund_id']),
                wallet=str(row['wallet_address']),
                asset=str(row['asset']),
                side=str(row['side']),
                qty=qty,
                unit_price_eth=unit_price_eth,
                date=row['date'],
                tx_hash=str(row['hash']),
                price_eth=price_eth,
                log=True
            )
            
            if idx % 100 == 0:
                logger.debug(f"Processed {idx+1}/{len(df_input)} transactions")
                
        except Exception as e:
            logger.error(f"Error processing row {idx}: {e}")
            logger.error(f"Row data: {row.to_dict()}")
            raise
    
    # Get results
    df_result = tracker.to_dataframe()
    
    # Filter out zero quantity transactions
    df_result = df_result[df_result["qty"] != 0]
    
    logger.info(f"FIFO ledger complete: {len(df_result)} transactions processed")
    
    return df_result


def convert_crypto_fetch_to_fifo_format(df_transactions: pd.DataFrame) -> pd.DataFrame:
    """
    Convert crypto_fetch transaction format to FIFO input format.
    
    Args:
        df_transactions: DataFrame from crypto_token_fetch with columns:
                        date, tx_hash, direction, token_name, token_amount, 
                        token_value_eth, token_value_usd, from_address, to_address
    
    Returns:
        DataFrame formatted for FIFO processing with ETH-based cost basis
    """
    if df_transactions.empty:
        return pd.DataFrame()
    
    logger.info(f"Converting {len(df_transactions)} transactions to FIFO format")
    
    fifo_df = pd.DataFrame()
    
    # Map basic fields
    # Use fund_id from source data if available, otherwise default
    if 'Fund' in df_transactions.columns:
        fifo_df['fund_id'] = df_transactions['Fund']
    elif 'fund_id' in df_transactions.columns:
        fifo_df['fund_id'] = df_transactions['fund_id']
    else:
        fifo_df['fund_id'] = 'fund_i_class_B_ETH'  # Fallback default
    fifo_df['wallet_address'] = df_transactions.get('wallet_id', df_transactions.get('from_address', ''))
    fifo_df['asset'] = df_transactions['token_name'].astype(str).str.upper()
    fifo_df['hash'] = df_transactions['tx_hash']
    
    # Handle dates
    date_series = pd.to_datetime(df_transactions['date'])
    if date_series.dt.tz is not None:
        fifo_df['date'] = date_series.dt.tz_localize(None)
    else:
        fifo_df['date'] = date_series
    
    # Map direction to side
    direction_series = df_transactions['direction'].astype(str).str.lower()
    fifo_df['side'] = direction_series.map({
        'incoming': 'buy',
        'outgoing': 'sell',
        'in': 'buy',
        'out': 'sell'
    }).fillna('sell')  # Default to sell for unmapped
    
    # Set token amounts and ETH values
    fifo_df['token_amount'] = df_transactions['token_amount'].abs()
    fifo_df['token_value_eth'] = df_transactions['token_value_eth'].abs()
    
    # Calculate unit_price_eth
    valid_amounts = fifo_df['token_amount'] > 0
    fifo_df['unit_price_eth'] = 0.0
    fifo_df.loc[valid_amounts, 'unit_price_eth'] = (
        fifo_df.loc[valid_amounts, 'token_value_eth'] / 
        fifo_df.loc[valid_amounts, 'token_amount']
    )
    
    # Calculate signed qty based on side
    fifo_df['qty'] = fifo_df.apply(
        lambda row: row['token_amount'] if row['side'] == 'buy' 
        else -row['token_amount'], 
        axis=1
    )
    
    # Add ETH/USD price if available
    if 'eth_price_usd' in df_transactions.columns:
        fifo_df['price_eth'] = df_transactions['eth_price_usd']
    else:
        fifo_df['price_eth'] = 0.0  # Will need to fetch later
    
    # Clean up infinities and NaNs - preserve string columns
    fifo_df = fifo_df.replace([float('inf'), float('-inf')], 0)
    
    # Fill numeric columns with 0, string columns with appropriate defaults
    numeric_columns = ['token_amount', 'token_value_eth', 'unit_price_eth', 'qty', 'price_eth']
    for col in numeric_columns:
        if col in fifo_df.columns:
            fifo_df[col] = fifo_df[col].fillna(0)
    
    # Ensure string columns remain strings
    string_columns = ['fund_id', 'wallet_address', 'asset', 'hash', 'side']
    for col in string_columns:
        if col in fifo_df.columns:
            fifo_df[col] = fifo_df[col].fillna('').astype(str)
    
    logger.info(f"Conversion complete: {len(fifo_df)} transactions ready for FIFO")
    
    return fifo_df