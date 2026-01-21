# -*- coding: utf-8 -*-
"""
FIFO (First In, First Out) Tracker Service

Provides cost basis tracking for cryptocurrency transactions using FIFO methodology.
Simplified ETH-based approach without asset categorization.
"""

import pandas as pd
from collections import deque
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any
import logging

logger = logging.getLogger(__name__)


def _normalize_datetime(dt: datetime) -> datetime:
    """Normalize datetime to UTC for consistent comparisons.

    Converts naive datetimes to UTC and ensures all comparisons
    work regardless of input timezone awareness.
    """
    if dt.tzinfo is None:
        # Naive datetime - assume UTC
        return dt.replace(tzinfo=timezone.utc)
    else:
        # Already aware - convert to UTC
        return dt.astimezone(timezone.utc)

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


# ============================================================================
# ENHANCED COST BASIS TRACKER (from notebook Cell 216)
# Supports FIFO/LIFO/HIFO, USD-based tracking, tax lot management
# ============================================================================

from enum import Enum
from dataclasses import dataclass, field


class CostBasisMethod(Enum):
    """Cost basis calculation methods"""
    FIFO = "FIFO"  # First In, First Out
    LIFO = "LIFO"  # Last In, First Out
    HIFO = "HIFO"  # Highest In, First Out


class TaxTreatment(Enum):
    """Tax treatment classifications"""
    NON_TAXABLE = "NON_TAXABLE"
    TAXABLE_SALE = "TAXABLE_SALE"
    TAXABLE_INCOME = "TAXABLE_INCOME"
    CAPITAL_GAIN_SHORT = "CAPITAL_GAIN_SHORT"
    CAPITAL_GAIN_LONG = "CAPITAL_GAIN_LONG"
    CAPITAL_LOSS_SHORT = "CAPITAL_LOSS_SHORT"
    CAPITAL_LOSS_LONG = "CAPITAL_LOSS_LONG"


@dataclass
class TaxLot:
    """Individual tax lot for cost basis tracking"""
    lot_id: str
    asset: str
    amount: Decimal
    cost_basis_usd: Decimal
    cost_per_unit: Decimal
    acquisition_date: datetime
    acquisition_tx_hash: str
    wallet_id: str
    fund_id: str

    def to_dict(self) -> Dict:
        return {
            'lot_id': self.lot_id,
            'asset': self.asset,
            'amount': float(self.amount),
            'cost_basis_usd': float(self.cost_basis_usd),
            'cost_per_unit': float(self.cost_per_unit),
            'acquisition_date': self.acquisition_date.isoformat(),
            'acquisition_tx_hash': self.acquisition_tx_hash,
            'wallet_id': self.wallet_id,
            'fund_id': self.fund_id
        }


@dataclass
class DisposalEvent:
    """Record of asset disposal with gain/loss"""
    disposal_id: str
    disposal_date: datetime
    disposal_tx_hash: str
    asset: str
    amount_disposed: Decimal
    proceeds_usd: Decimal
    cost_basis_usd: Decimal
    gain_loss_usd: Decimal
    holding_days: int
    is_long_term: bool
    tax_treatment: str
    lots_used: List[Dict]
    wallet_id: str
    fund_id: str
    swap_pair_id: Optional[str] = None
    is_internal_transfer: bool = False

    def to_dict(self) -> Dict:
        return {
            'disposal_id': self.disposal_id,
            'disposal_date': self.disposal_date.isoformat(),
            'disposal_tx_hash': self.disposal_tx_hash,
            'asset': self.asset,
            'amount_disposed': float(self.amount_disposed),
            'proceeds_usd': float(self.proceeds_usd),
            'cost_basis_usd': float(self.cost_basis_usd),
            'gain_loss_usd': float(self.gain_loss_usd),
            'holding_days': self.holding_days,
            'is_long_term': self.is_long_term,
            'tax_treatment': self.tax_treatment,
            'lots_used': self.lots_used,
            'wallet_id': self.wallet_id,
            'fund_id': self.fund_id,
            'swap_pair_id': self.swap_pair_id,
            'is_internal_transfer': self.is_internal_transfer
        }


class CostBasisTracker:
    """
    Enhanced cost basis tracker with USD-based tracking and FIFO/LIFO/HIFO support.

    Use this for:
    - Currency swaps (aWETH -> WETH, ETH -> USDC)
    - Investment tracking with gain/loss calculation
    - Tax lot management with holding period tracking

    Example for Aave withdraw (aWETH -> WETH):
        tracker = CostBasisTracker(method=CostBasisMethod.FIFO)
        processor = SwapProcessor(tracker)

        result = processor.process_swap(
            from_asset="aWETH",
            from_amount=Decimal("10"),
            to_asset="WETH",
            to_amount=Decimal("10"),
            eth_price_usd=Decimal("3200"),
            date=datetime.now(),
            tx_hash="0x...",
            wallet_id="0x..."
        )
        print(f"Gain/Loss: ${result['gain_loss_usd']}")
    """

    def __init__(self, method: CostBasisMethod = CostBasisMethod.FIFO, fund_id: str = ""):
        self.method = method
        self.fund_id = fund_id
        self.lots: Dict[Tuple[str, str], deque] = {}  # (wallet_id, asset) -> deque[TaxLot]
        self.disposal_counter = 0
        self.lot_counter = 0

    def add_acquisition(self,
                       asset: str,
                       amount: Decimal,
                       cost_usd: Decimal,
                       date: datetime,
                       tx_hash: str,
                       wallet_id: str,
                       fund_id: Optional[str] = None) -> TaxLot:
        """Add an acquisition (buy, receive, deposit)"""
        if amount <= 0:
            raise ValueError(f"Amount must be positive, got {amount}")

        self.lot_counter += 1
        fund = fund_id or self.fund_id
        cost_per_unit = cost_usd / amount if amount > 0 else Decimal(0)

        lot = TaxLot(
            lot_id=f"LOT_{self.lot_counter:08d}",
            asset=asset,
            amount=amount,
            cost_basis_usd=cost_usd,
            cost_per_unit=cost_per_unit,
            acquisition_date=_normalize_datetime(date),
            acquisition_tx_hash=tx_hash,
            wallet_id=wallet_id.lower(),
            fund_id=fund
        )

        key = (wallet_id.lower(), asset)
        if key not in self.lots:
            self.lots[key] = deque()
        self.lots[key].append(lot)

        logger.debug(f"Added lot {lot.lot_id}: {amount} {asset} @ ${cost_per_unit}/unit")
        return lot

    def process_disposal(self,
                        asset: str,
                        amount: Decimal,
                        proceeds_usd: Decimal,
                        date: datetime,
                        tx_hash: str,
                        wallet_id: str,
                        fund_id: Optional[str] = None,
                        swap_pair_id: Optional[str] = None,
                        is_internal_transfer: bool = False) -> DisposalEvent:
        """Process a disposal (sell, swap out, withdraw)"""
        if amount <= 0:
            raise ValueError(f"Disposal amount must be positive, got {amount}")

        fund = fund_id or self.fund_id
        key = (wallet_id.lower(), asset)

        if key not in self.lots or not self.lots[key]:
            logger.warning(f"No lots found for {asset} in {wallet_id[:10]}...")
            self.disposal_counter += 1
            return DisposalEvent(
                disposal_id=f"DISP_{self.disposal_counter:08d}",
                disposal_date=date,
                disposal_tx_hash=tx_hash,
                asset=asset,
                amount_disposed=amount,
                proceeds_usd=proceeds_usd,
                cost_basis_usd=Decimal(0),
                gain_loss_usd=proceeds_usd,
                holding_days=0,
                is_long_term=False,
                tax_treatment=TaxTreatment.TAXABLE_SALE.value,
                lots_used=[],
                wallet_id=wallet_id.lower(),
                fund_id=fund,
                swap_pair_id=swap_pair_id,
                is_internal_transfer=is_internal_transfer
            )

        # Sort lots based on method
        lots_list = list(self.lots[key])
        if self.method == CostBasisMethod.LIFO:
            lots_list.reverse()
        elif self.method == CostBasisMethod.HIFO:
            lots_list.sort(key=lambda x: x.cost_per_unit, reverse=True)

        remaining = amount
        total_cost = Decimal(0)
        lots_used = []
        lots_to_remove = []

        for lot in lots_list:
            if remaining <= 0:
                break

            amount_from_lot = min(lot.amount, remaining)
            cost_from_lot = amount_from_lot * lot.cost_per_unit
            normalized_date = _normalize_datetime(date)
            holding_days = (normalized_date - lot.acquisition_date).days

            lots_used.append({
                'lot_id': lot.lot_id,
                'amount': float(amount_from_lot),
                'cost_basis': float(cost_from_lot),
                'cost_per_unit': float(lot.cost_per_unit),
                'acquisition_date': lot.acquisition_date.isoformat(),
                'holding_days': holding_days,
                'is_long_term': holding_days >= 365
            })

            total_cost += cost_from_lot
            remaining -= amount_from_lot

            if amount_from_lot >= lot.amount:
                lots_to_remove.append(lot.lot_id)
            else:
                lot.amount -= amount_from_lot
                lot.cost_basis_usd -= cost_from_lot

        self.lots[key] = deque([lot for lot in self.lots[key] if lot.lot_id not in lots_to_remove])

        gain_loss = proceeds_usd - total_cost

        if is_internal_transfer:
            tax_treatment = TaxTreatment.NON_TAXABLE.value
            avg_holding_days = 0
            is_long_term_result = False
        else:
            if lots_used:
                total_days = sum(lot['holding_days'] * lot['amount'] for lot in lots_used)
                disposed_amount = float(amount - remaining)
                avg_holding_days = total_days / disposed_amount if disposed_amount > 0 else 0
                is_long_term_result = avg_holding_days >= 365
            else:
                avg_holding_days = 0
                is_long_term_result = False

            if gain_loss >= 0:
                tax_treatment = TaxTreatment.CAPITAL_GAIN_LONG.value if is_long_term_result else TaxTreatment.CAPITAL_GAIN_SHORT.value
            else:
                tax_treatment = TaxTreatment.CAPITAL_LOSS_LONG.value if is_long_term_result else TaxTreatment.CAPITAL_LOSS_SHORT.value

        self.disposal_counter += 1

        return DisposalEvent(
            disposal_id=f"DISP_{self.disposal_counter:08d}",
            disposal_date=date,
            disposal_tx_hash=tx_hash,
            asset=asset,
            amount_disposed=amount - remaining,
            proceeds_usd=proceeds_usd,
            cost_basis_usd=total_cost,
            gain_loss_usd=gain_loss,
            holding_days=int(avg_holding_days) if lots_used else 0,
            is_long_term=is_long_term_result,
            tax_treatment=tax_treatment,
            lots_used=lots_used,
            wallet_id=wallet_id.lower(),
            fund_id=fund,
            swap_pair_id=swap_pair_id,
            is_internal_transfer=is_internal_transfer
        )

    def get_position(self, wallet_id: str, asset: str) -> Optional[Dict]:
        """Get current position for wallet/asset"""
        key = (wallet_id.lower(), asset)
        lots = self.lots.get(key)
        if not lots:
            return None

        total_amount = sum(lot.amount for lot in lots)
        total_cost = sum(lot.cost_basis_usd for lot in lots)
        avg_cost = total_cost / total_amount if total_amount > 0 else Decimal(0)

        return {
            'wallet_id': wallet_id.lower(),
            'asset': asset,
            'amount': float(total_amount),
            'cost_basis_usd': float(total_cost),
            'average_cost': float(avg_cost),
            'lot_count': len(lots)
        }

    def get_all_positions(self) -> pd.DataFrame:
        """Get all positions as DataFrame"""
        positions = []
        for (wallet_id, asset), lots in self.lots.items():
            if lots:
                pos = self.get_position(wallet_id, asset)
                if pos:
                    positions.append(pos)
        return pd.DataFrame(positions) if positions else pd.DataFrame()


class SwapProcessor:
    """
    Process asset swaps/conversions for FIFO tracking.

    A swap (WETH -> aWETH or aWETH -> WETH) is:
    1. Disposal of from_asset (find cost basis, calc gain/loss)
    2. Acquisition of to_asset (at fair market value)
    """

    # ETH-equivalent assets (use ETH price for valuation)
    ETH_EQUIVALENTS = {'ETH', 'WETH', 'aWETH', 'stETH', 'wstETH', 'cbETH', 'rETH'}

    def __init__(self, tracker: CostBasisTracker):
        self.tracker = tracker

    def process_swap(self,
                    from_asset: str,
                    from_amount: Decimal,
                    to_asset: str,
                    to_amount: Decimal,
                    eth_price_usd: Decimal,
                    date: datetime,
                    tx_hash: str,
                    wallet_id: str,
                    fund_id: Optional[str] = None) -> Dict:
        """
        Process an asset swap/conversion.

        Returns dict with disposal event, new lot, and gain/loss summary.
        """
        fund = fund_id or self.tracker.fund_id

        # Calculate USD values
        from_value_usd = from_amount * eth_price_usd if from_asset in self.ETH_EQUIVALENTS else from_amount * eth_price_usd
        to_value_usd = to_amount * eth_price_usd if to_asset in self.ETH_EQUIVALENTS else to_amount * eth_price_usd

        swap_pair_id = f"SWAP_{tx_hash[:16]}_{from_asset}_{to_asset}"

        # 1. Dispose from_asset
        disposal = self.tracker.process_disposal(
            asset=from_asset,
            amount=from_amount,
            proceeds_usd=from_value_usd,
            date=date,
            tx_hash=tx_hash,
            wallet_id=wallet_id,
            fund_id=fund,
            swap_pair_id=swap_pair_id
        )

        # 2. Acquire to_asset
        new_lot = self.tracker.add_acquisition(
            asset=to_asset,
            amount=to_amount,
            cost_usd=to_value_usd,
            date=date,
            tx_hash=tx_hash,
            wallet_id=wallet_id,
            fund_id=fund
        )

        return {
            'swap_pair_id': swap_pair_id,
            'disposal': disposal,
            'acquisition': new_lot,
            'from_asset': from_asset,
            'from_amount': float(from_amount),
            'to_asset': to_asset,
            'to_amount': float(to_amount),
            'gain_loss_usd': float(disposal.gain_loss_usd),
            'tax_treatment': disposal.tax_treatment
        }