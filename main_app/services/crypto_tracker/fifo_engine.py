"""
Enhanced FIFO Engine with S3 Persistence

Production-ready FIFO cost basis engine with persistent state management,
real-time updates, and duplicate prevention.
"""

import logging
from collections import deque
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Any, Deque
from dataclasses import dataclass, asdict
from decimal import Decimal, getcontext
import pandas as pd
import uuid

from .persistence_manager import PersistenceManager, TransactionRecord, FIFOLot
from .duplicate_detector import DuplicateDetector, DuplicateCheckResult
from .progress_tracker import ProgressTracker, ProgressContext

logger = logging.getLogger(__name__)

# Set decimal precision for financial calculations
getcontext().prec = 28


@dataclass
class FIFOResult:
    """Result of FIFO transaction processing."""
    transaction_id: str
    processed: bool
    duplicate_check: Optional[DuplicateCheckResult] = None
    realized_gain_eth: Decimal = Decimal('0')
    realized_gain_usd: Decimal = Decimal('0')
    remaining_lots: List[FIFOLot] = None
    error_message: Optional[str] = None
    
    def __post_init__(self):
        if self.remaining_lots is None:
            self.remaining_lots = []


@dataclass
class PortfolioSnapshot:
    """Portfolio state at a specific point in time."""
    fund_id: str
    as_of_date: datetime
    total_assets: Dict[str, Decimal]  # asset -> total_quantity
    total_cost_basis_eth: Dict[str, Decimal]  # asset -> total_cost_basis_eth
    total_cost_basis_usd: Dict[str, Decimal]  # asset -> total_cost_basis_usd
    unrealized_gains_eth: Dict[str, Decimal]  # asset -> unrealized_gain_eth
    unrealized_gains_usd: Dict[str, Decimal]  # asset -> unrealized_gain_usd
    active_lots: List[FIFOLot]
    transaction_count: int
    earliest_transaction: Optional[datetime] = None
    latest_transaction: Optional[datetime] = None


class FIFOEngine:
    """
    Enhanced FIFO engine with S3 persistence and real-time updates.
    
    Maintains accurate cost basis calculations with:
    - S3-backed persistence for reliability
    - Duplicate transaction prevention
    - Real-time progress tracking
    - Historical snapshot capabilities
    """
    
    def __init__(self, fund_id: str, auto_persist: bool = True):
        """Initialize FIFO engine for specific fund."""
        self.fund_id = fund_id
        self.auto_persist = auto_persist
        
        # Initialize components
        self.persistence = PersistenceManager(fund_id)
        self.duplicate_detector = DuplicateDetector(fund_id, self.persistence)
        self.progress_tracker = ProgressTracker()
        
        # FIFO state: key = (wallet_id, asset) -> deque of lots
        self.lots: Dict[Tuple[str, str], Deque[FIFOLot]] = {}
        
        # Transaction processing log
        self.processing_log: List[Dict[str, Any]] = []
        
        # Load existing state
        self._load_state()
        
        logger.info(f"Initialized FIFOEngine for fund {fund_id} with {len(self.lots)} asset pairs")
    
    def _load_state(self) -> bool:
        """Load FIFO state from S3."""
        try:
            logger.info(f"Loading FIFO state for fund {self.fund_id}")
            
            # Load FIFO lots
            lots_df = self.persistence.load_fifo_lots()
            
            if lots_df.empty:
                logger.info("No existing FIFO lots found")
                return True
            
            # Rebuild lots dictionary
            self.lots.clear()
            
            for _, row in lots_df.iterrows():
                if row['remaining_quantity'] <= 0:
                    continue  # Skip exhausted lots
                
                lot = FIFOLot(
                    lot_id=row['lot_id'],
                    fund_id=row['fund_id'],
                    wallet_id=row['wallet_id'],
                    asset=row['asset'],
                    purchase_date=pd.to_datetime(row['purchase_date']),
                    original_quantity=Decimal(str(row['original_quantity'])),
                    remaining_quantity=Decimal(str(row['remaining_quantity'])),
                    cost_basis_eth=Decimal(str(row['cost_basis_eth'])),
                    cost_basis_usd=Decimal(str(row['cost_basis_usd'])),
                    source_tx_hash=row['source_tx_hash']
                )
                
                key = (lot.wallet_id, lot.asset)
                if key not in self.lots:
                    self.lots[key] = deque()
                
                self.lots[key].append(lot)
            
            # Sort lots by purchase date to maintain FIFO order
            for key in self.lots:
                self.lots[key] = deque(sorted(self.lots[key], key=lambda x: x.purchase_date))
            
            total_lots = sum(len(lot_deque) for lot_deque in self.lots.values())
            logger.info(f"Loaded {total_lots} active FIFO lots for {len(self.lots)} asset pairs")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to load FIFO state: {e}")
            return False
    
    def _save_state(self) -> bool:
        """Save current FIFO state to S3."""
        if not self.auto_persist:
            return True
        
        try:
            # Convert lots to list
            all_lots = []
            for key, lot_deque in self.lots.items():
                all_lots.extend(list(lot_deque))
            
            if not all_lots:
                logger.info("No FIFO lots to save")
                return True
            
            return self.persistence.save_fifo_lots(all_lots)
            
        except Exception as e:
            logger.error(f"Failed to save FIFO state: {e}")
            return False
    
    def process_transaction(self, transaction: TransactionRecord) -> FIFOResult:
        """
        Process a single transaction through FIFO methodology.
        
        Includes duplicate checking and automatic persistence.
        """
        logger.debug(f"Processing transaction {transaction.tx_hash}")
        
        # Check for duplicates
        duplicate_check = self.duplicate_detector.check_duplicate(transaction)
        
        if duplicate_check.is_duplicate and duplicate_check.confidence >= 0.8:
            logger.warning(f"Skipping duplicate transaction {transaction.tx_hash}: {duplicate_check.reason}")
            return FIFOResult(
                transaction_id=transaction.tx_hash,
                processed=False,
                duplicate_check=duplicate_check,
                error_message=f"Duplicate detected: {duplicate_check.reason}"
            )
        
        try:
            # Process the transaction
            realized_gain_eth, realized_gain_usd = self._process_fifo_transaction(transaction)
            
            # Add to duplicate detection
            self.duplicate_detector.add_transaction_hash(transaction)
            
            # Get current lots for this asset
            key = (transaction.wallet_id, transaction.asset)
            remaining_lots = list(self.lots.get(key, deque()))
            
            # Save state if auto-persist is enabled
            if self.auto_persist:
                self._save_state()
            
            # Log the processing
            self.processing_log.append({
                'timestamp': datetime.now(timezone.utc),
                'transaction_hash': transaction.tx_hash,
                'fund_id': transaction.fund_id,
                'wallet_id': transaction.wallet_id,
                'asset': transaction.asset,
                'side': transaction.side,
                'realized_gain_eth': float(realized_gain_eth),
                'realized_gain_usd': float(realized_gain_usd),
                'lots_remaining': len(remaining_lots)
            })
            
            logger.info(
                f"Processed {transaction.side} transaction {transaction.tx_hash}: "
                f"Realized gain ETH: {realized_gain_eth}, USD: {realized_gain_usd}"
            )
            
            return FIFOResult(
                transaction_id=transaction.tx_hash,
                processed=True,
                duplicate_check=duplicate_check,
                realized_gain_eth=realized_gain_eth,
                realized_gain_usd=realized_gain_usd,
                remaining_lots=remaining_lots
            )
            
        except Exception as e:
            logger.error(f"Error processing transaction {transaction.tx_hash}: {e}")
            return FIFOResult(
                transaction_id=transaction.tx_hash,
                processed=False,
                error_message=str(e)
            )
    
    def _process_fifo_transaction(self, transaction: TransactionRecord) -> Tuple[Decimal, Decimal]:
        """Process transaction through FIFO methodology."""
        key = (transaction.wallet_id, transaction.asset)
        
        # Get or create lot deque for this asset/wallet combination
        lot_deque = self.lots.setdefault(key, deque())
        
        realized_gain_eth = Decimal('0')
        realized_gain_usd = Decimal('0')
        
        if transaction.side.lower() == 'buy':
            # Create new lot for buy transactions
            lot = FIFOLot(
                lot_id=str(uuid.uuid4()),
                fund_id=transaction.fund_id,
                wallet_id=transaction.wallet_id,
                asset=transaction.asset,
                purchase_date=transaction.date,
                original_quantity=transaction.token_amount,
                remaining_quantity=transaction.token_amount,
                cost_basis_eth=transaction.eth_value,
                cost_basis_usd=transaction.usd_value,
                source_tx_hash=transaction.tx_hash
            )
            
            lot_deque.append(lot)
            logger.debug(f"Added new FIFO lot: {transaction.token_amount} {transaction.asset}")
            
        elif transaction.side.lower() == 'sell':
            # Process sell using FIFO methodology
            tokens_to_sell = transaction.token_amount
            
            while tokens_to_sell > 0 and lot_deque:
                oldest_lot = lot_deque[0]
                
                if oldest_lot.remaining_quantity <= tokens_to_sell:
                    # Sell entire lot
                    tokens_sold = oldest_lot.remaining_quantity
                    
                    # Calculate realized gains
                    proportion = tokens_sold / oldest_lot.original_quantity
                    cost_basis_eth = oldest_lot.cost_basis_eth * proportion
                    cost_basis_usd = oldest_lot.cost_basis_usd * proportion
                    
                    # Calculate proportion of sale proceeds
                    sale_proportion = tokens_sold / transaction.token_amount
                    proceeds_eth = transaction.eth_value * sale_proportion
                    proceeds_usd = transaction.usd_value * sale_proportion
                    
                    realized_gain_eth += proceeds_eth - cost_basis_eth
                    realized_gain_usd += proceeds_usd - cost_basis_usd
                    
                    tokens_to_sell -= tokens_sold
                    
                    # Remove exhausted lot
                    lot_deque.popleft()
                    
                    logger.debug(f"Exhausted lot: sold {tokens_sold} {transaction.asset}")
                    
                else:
                    # Partial sale of lot
                    tokens_sold = tokens_to_sell
                    
                    # Calculate realized gains
                    proportion = tokens_sold / oldest_lot.original_quantity
                    cost_basis_eth = oldest_lot.cost_basis_eth * proportion
                    cost_basis_usd = oldest_lot.cost_basis_usd * proportion
                    
                    # Calculate proportion of sale proceeds
                    sale_proportion = tokens_sold / transaction.token_amount
                    proceeds_eth = transaction.eth_value * sale_proportion
                    proceeds_usd = transaction.usd_value * sale_proportion
                    
                    realized_gain_eth += proceeds_eth - cost_basis_eth
                    realized_gain_usd += proceeds_usd - cost_basis_usd
                    
                    # Update lot quantities and cost basis
                    oldest_lot.remaining_quantity -= tokens_sold
                    oldest_lot.cost_basis_eth -= cost_basis_eth
                    oldest_lot.cost_basis_usd -= cost_basis_usd
                    
                    tokens_to_sell = Decimal('0')
                    
                    logger.debug(f"Partial sale: sold {tokens_sold} {transaction.asset}, remaining {oldest_lot.remaining_quantity}")
            
            if tokens_to_sell > 0:
                logger.warning(f"Insufficient inventory for sale: missing {tokens_to_sell} {transaction.asset}")
        
        return realized_gain_eth, realized_gain_usd
    
    def process_transaction_batch(
        self,
        transactions: List[TransactionRecord],
        progress_callback: Optional[callable] = None
    ) -> List[FIFOResult]:
        """Process a batch of transactions with progress tracking."""
        
        with ProgressContext(
            operation_type="FIFO Batch Processing",
            total_steps=len(transactions),
            callback=progress_callback
        ) as operation_id:
            
            results = []
            
            # Sort transactions by date to ensure chronological processing
            sorted_transactions = sorted(transactions, key=lambda tx: tx.date)
            
            for i, transaction in enumerate(sorted_transactions):
                # Update progress
                progress_percent = (i / len(transactions)) * 100
                self.progress_tracker.update_progress(
                    operation_id=operation_id,
                    progress_percent=progress_percent,
                    current_step=f"Processing transaction {i+1}/{len(transactions)}: {transaction.tx_hash[:10]}...",
                    current_step_number=i + 1
                )
                
                # Process transaction
                result = self.process_transaction(transaction)
                results.append(result)
                
                # Stop processing if there's a critical error
                if result.error_message and "critical" in result.error_message.lower():
                    logger.error(f"Critical error in batch processing: {result.error_message}")
                    break
            
            # Final save
            if self.auto_persist:
                self._save_state()
            
            successful_count = sum(1 for result in results if result.processed)
            logger.info(f"Batch processing completed: {successful_count}/{len(transactions)} transactions processed")
            
            return results
    
    def recalculate_from_date(self, from_date: datetime) -> bool:
        """Recalculate FIFO from a specific date forward."""
        try:
            logger.info(f"Recalculating FIFO from {from_date} for fund {self.fund_id}")
            
            # Load all transactions
            transactions_df = self.persistence.load_transactions()
            
            if transactions_df.empty:
                logger.info("No transactions to recalculate")
                return True
            
            # Filter transactions from the specified date
            transactions_df['date'] = pd.to_datetime(transactions_df['date'])
            relevant_transactions = transactions_df[transactions_df['date'] >= from_date]
            
            if relevant_transactions.empty:
                logger.info(f"No transactions found from {from_date}")
                return True
            
            # Clear FIFO state for affected assets
            affected_assets = set()
            for _, row in relevant_transactions.iterrows():
                key = (row['wallet_id'], row['asset'])
                affected_assets.add(key)
            
            # Remove lots for affected assets that were created after from_date
            for key in affected_assets:
                if key in self.lots:
                    self.lots[key] = deque([
                        lot for lot in self.lots[key]
                        if lot.purchase_date < from_date
                    ])
            
            # Convert transactions to TransactionRecord objects
            transaction_records = []
            for _, row in relevant_transactions.iterrows():
                transaction = TransactionRecord(
                    tx_hash=row['tx_hash'],
                    block_number=int(row.get('block_number', 0)),
                    date=pd.to_datetime(row['date']),
                    fund_id=row['fund_id'],
                    wallet_id=row['wallet_id'],
                    asset=row['asset'],
                    side=row['side'],
                    token_amount=Decimal(str(row['token_amount'])),
                    eth_value=Decimal(str(row['eth_value'])),
                    usd_value=Decimal(str(row['usd_value']))
                )
                transaction_records.append(transaction)
            
            # Temporarily disable auto-persist to avoid constant saves
            original_auto_persist = self.auto_persist
            self.auto_persist = False
            
            try:
                # Reprocess transactions
                results = self.process_transaction_batch(transaction_records)
                
                # Final save
                self._save_state()
                
                successful_count = sum(1 for result in results if result.processed)
                logger.info(f"Recalculation completed: {successful_count}/{len(transaction_records)} transactions reprocessed")
                
                return True
                
            finally:
                self.auto_persist = original_auto_persist
            
        except Exception as e:
            logger.error(f"Error during recalculation: {e}")
            return False
    
    def get_portfolio_snapshot(self, as_of_date: Optional[datetime] = None) -> PortfolioSnapshot:
        """Get portfolio state at a specific date."""
        if as_of_date is None:
            as_of_date = datetime.now(timezone.utc)
        
        # Load transactions up to the specified date
        transactions_df = self.persistence.load_transactions()
        
        if transactions_df.empty:
            return PortfolioSnapshot(
                fund_id=self.fund_id,
                as_of_date=as_of_date,
                total_assets={},
                total_cost_basis_eth={},
                total_cost_basis_usd={},
                unrealized_gains_eth={},
                unrealized_gains_usd={},
                active_lots=[],
                transaction_count=0
            )
        
        # Filter transactions up to as_of_date
        transactions_df['date'] = pd.to_datetime(transactions_df['date'])
        historical_transactions = transactions_df[transactions_df['date'] <= as_of_date]
        
        # Create temporary FIFO engine for historical calculation
        temp_engine = FIFOEngine(fund_id=f"temp_{self.fund_id}", auto_persist=False)
        
        # Process historical transactions
        transaction_records = []
        for _, row in historical_transactions.iterrows():
            transaction = TransactionRecord(
                tx_hash=row['tx_hash'],
                block_number=int(row.get('block_number', 0)),
                date=pd.to_datetime(row['date']),
                fund_id=row['fund_id'],
                wallet_id=row['wallet_id'],
                asset=row['asset'],
                side=row['side'],
                token_amount=Decimal(str(row['token_amount'])),
                eth_value=Decimal(str(row['eth_value'])),
                usd_value=Decimal(str(row['usd_value']))
            )
            transaction_records.append(transaction)
        
        temp_engine.process_transaction_batch(transaction_records)
        
        # Calculate portfolio aggregates
        total_assets = {}
        total_cost_basis_eth = {}
        total_cost_basis_usd = {}
        active_lots = []
        
        for key, lot_deque in temp_engine.lots.items():
            wallet_id, asset = key
            
            asset_quantity = Decimal('0')
            asset_cost_basis_eth = Decimal('0')
            asset_cost_basis_usd = Decimal('0')
            
            for lot in lot_deque:
                if lot.remaining_quantity > 0:
                    asset_quantity += lot.remaining_quantity
                    asset_cost_basis_eth += lot.cost_basis_eth
                    asset_cost_basis_usd += lot.cost_basis_usd
                    active_lots.append(lot)
            
            if asset_quantity > 0:
                total_assets[asset] = total_assets.get(asset, Decimal('0')) + asset_quantity
                total_cost_basis_eth[asset] = total_cost_basis_eth.get(asset, Decimal('0')) + asset_cost_basis_eth
                total_cost_basis_usd[asset] = total_cost_basis_usd.get(asset, Decimal('0')) + asset_cost_basis_usd
        
        # Calculate unrealized gains (would need current market prices)
        unrealized_gains_eth = {asset: Decimal('0') for asset in total_assets}
        unrealized_gains_usd = {asset: Decimal('0') for asset in total_assets}
        
        # Get transaction date range
        earliest_transaction = None
        latest_transaction = None
        if not historical_transactions.empty:
            earliest_transaction = historical_transactions['date'].min()
            latest_transaction = historical_transactions['date'].max()
        
        return PortfolioSnapshot(
            fund_id=self.fund_id,
            as_of_date=as_of_date,
            total_assets=total_assets,
            total_cost_basis_eth=total_cost_basis_eth,
            total_cost_basis_usd=total_cost_basis_usd,
            unrealized_gains_eth=unrealized_gains_eth,
            unrealized_gains_usd=unrealized_gains_usd,
            active_lots=active_lots,
            transaction_count=len(historical_transactions),
            earliest_transaction=earliest_transaction,
            latest_transaction=latest_transaction
        )
    
    def get_asset_summary(self, asset: Optional[str] = None) -> Dict[str, Any]:
        """Get summary of FIFO lots by asset."""
        summary = {}
        
        for key, lot_deque in self.lots.items():
            wallet_id, asset_name = key
            
            if asset and asset_name != asset:
                continue
            
            if asset_name not in summary:
                summary[asset_name] = {
                    'total_quantity': Decimal('0'),
                    'total_cost_basis_eth': Decimal('0'),
                    'total_cost_basis_usd': Decimal('0'),
                    'lot_count': 0,
                    'wallets': set(),
                    'oldest_purchase': None,
                    'newest_purchase': None
                }
            
            for lot in lot_deque:
                if lot.remaining_quantity <= 0:
                    continue
                
                summary[asset_name]['total_quantity'] += lot.remaining_quantity
                summary[asset_name]['total_cost_basis_eth'] += lot.cost_basis_eth
                summary[asset_name]['total_cost_basis_usd'] += lot.cost_basis_usd
                summary[asset_name]['lot_count'] += 1
                summary[asset_name]['wallets'].add(wallet_id)
                
                if (summary[asset_name]['oldest_purchase'] is None or
                    lot.purchase_date < summary[asset_name]['oldest_purchase']):
                    summary[asset_name]['oldest_purchase'] = lot.purchase_date
                
                if (summary[asset_name]['newest_purchase'] is None or
                    lot.purchase_date > summary[asset_name]['newest_purchase']):
                    summary[asset_name]['newest_purchase'] = lot.purchase_date
        
        # Convert sets to lists for JSON serialization
        for asset_name in summary:
            summary[asset_name]['wallets'] = list(summary[asset_name]['wallets'])
        
        return summary
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get engine statistics."""
        total_lots = sum(len(lot_deque) for lot_deque in self.lots.values())
        active_lots = sum(
            len([lot for lot in lot_deque if lot.remaining_quantity > 0])
            for lot_deque in self.lots.values()
        )
        
        return {
            'fund_id': self.fund_id,
            'total_asset_pairs': len(self.lots),
            'total_lots': total_lots,
            'active_lots': active_lots,
            'processing_log_entries': len(self.processing_log),
            'auto_persist_enabled': self.auto_persist,
            'last_updated': datetime.now(timezone.utc).isoformat()
        }