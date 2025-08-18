"""
Transaction Duplicate Detector

Advanced duplicate detection system for cryptocurrency transactions using
multiple verification methods and conflict resolution.
"""

import hashlib
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Set, Optional, Tuple, Any
from dataclasses import dataclass
from decimal import Decimal
import pandas as pd

from .persistence_manager import PersistenceManager, TransactionRecord

logger = logging.getLogger(__name__)


@dataclass
class DuplicateCheckResult:
    """Result of duplicate detection check."""
    is_duplicate: bool
    confidence: float  # 0.0 to 1.0
    duplicate_type: str  # 'exact', 'probable', 'possible'
    conflicting_tx_hash: Optional[str] = None
    reason: str = ""
    suggested_action: str = ""


class DuplicateDetector:
    """
    Advanced duplicate detection for cryptocurrency transactions.
    
    Uses multiple detection methods:
    1. Exact hash matching
    2. Multi-field signature matching
    3. Amount and timing proximity matching
    4. Wallet and asset correlation matching
    """
    
    def __init__(self, fund_id: str, persistence_manager: Optional[PersistenceManager] = None):
        """Initialize duplicate detector."""
        self.fund_id = fund_id
        self.persistence = persistence_manager or PersistenceManager(fund_id)
        
        # Load existing hashes and transactions
        self.known_hashes = self.persistence.load_duplicate_hashes()
        self.existing_transactions = self.persistence.load_transactions()
        
        # Detection thresholds
        self.time_proximity_threshold = timedelta(minutes=5)  # Transactions within 5 minutes
        self.amount_tolerance = Decimal('0.001')  # 0.1% tolerance for amount matching
        
        logger.info(f"Initialized DuplicateDetector for fund {fund_id} with {len(self.known_hashes)} known hashes")
    
    def _generate_primary_hash(self, transaction: TransactionRecord) -> str:
        """Generate primary hash from transaction hash."""
        return hashlib.sha256(transaction.tx_hash.encode()).hexdigest()
    
    def _generate_signature_hash(self, transaction: TransactionRecord) -> str:
        """Generate signature hash from multiple transaction fields."""
        # Create signature from multiple fields for robustness
        signature_data = (
            f"{transaction.wallet_id}|"
            f"{transaction.asset}|"
            f"{transaction.side}|"
            f"{transaction.token_amount}|"
            f"{transaction.eth_value}|"
            f"{transaction.date.isoformat()}"
        )
        return hashlib.sha256(signature_data.encode()).hexdigest()
    
    def _generate_fuzzy_hash(self, transaction: TransactionRecord) -> str:
        """Generate fuzzy hash for approximate matching."""
        # Rounded amounts and date for fuzzy matching
        rounded_token = round(float(transaction.token_amount), 6)
        rounded_eth = round(float(transaction.eth_value), 6)
        date_rounded = transaction.date.replace(second=0, microsecond=0)
        
        fuzzy_data = (
            f"{transaction.wallet_id}|"
            f"{transaction.asset}|"
            f"{transaction.side}|"
            f"{rounded_token}|"
            f"{rounded_eth}|"
            f"{date_rounded.isoformat()}"
        )
        return hashlib.sha256(fuzzy_data.encode()).hexdigest()
    
    def _check_exact_duplicate(self, transaction: TransactionRecord) -> DuplicateCheckResult:
        """Check for exact duplicate using transaction hash."""
        primary_hash = self._generate_primary_hash(transaction)
        
        if primary_hash in self.known_hashes:
            return DuplicateCheckResult(
                is_duplicate=True,
                confidence=1.0,
                duplicate_type='exact',
                conflicting_tx_hash=transaction.tx_hash,
                reason="Exact transaction hash match found",
                suggested_action="Skip transaction - already processed"
            )
        
        # Check against existing transactions
        if not self.existing_transactions.empty:
            existing_match = self.existing_transactions[
                self.existing_transactions['tx_hash'] == transaction.tx_hash
            ]
            if not existing_match.empty:
                return DuplicateCheckResult(
                    is_duplicate=True,
                    confidence=1.0,
                    duplicate_type='exact',
                    conflicting_tx_hash=transaction.tx_hash,
                    reason="Transaction hash exists in database",
                    suggested_action="Skip transaction - already in database"
                )
        
        return DuplicateCheckResult(
            is_duplicate=False,
            confidence=0.0,
            duplicate_type='none'
        )
    
    def _check_signature_duplicate(self, transaction: TransactionRecord) -> DuplicateCheckResult:
        """Check for duplicate using multi-field signature."""
        signature_hash = self._generate_signature_hash(transaction)
        
        if not self.existing_transactions.empty:
            # Check for matching signature patterns
            potential_matches = self.existing_transactions[
                (self.existing_transactions['wallet_id'] == transaction.wallet_id) &
                (self.existing_transactions['asset'] == transaction.asset) &
                (self.existing_transactions['side'] == transaction.side)
            ]
            
            for _, existing_tx in potential_matches.iterrows():
                existing_signature = self._generate_signature_hash(
                    TransactionRecord(
                        tx_hash=existing_tx['tx_hash'],
                        block_number=int(existing_tx.get('block_number', 0)),
                        date=pd.to_datetime(existing_tx['date']),
                        fund_id=existing_tx['fund_id'],
                        wallet_id=existing_tx['wallet_id'],
                        asset=existing_tx['asset'],
                        side=existing_tx['side'],
                        token_amount=Decimal(str(existing_tx['token_amount'])),
                        eth_value=Decimal(str(existing_tx['eth_value'])),
                        usd_value=Decimal(str(existing_tx['usd_value']))
                    )
                )
                
                if signature_hash == existing_signature:
                    return DuplicateCheckResult(
                        is_duplicate=True,
                        confidence=0.9,
                        duplicate_type='probable',
                        conflicting_tx_hash=existing_tx['tx_hash'],
                        reason="Identical transaction signature (wallet, asset, side, amounts, date)",
                        suggested_action="Review transaction - likely duplicate with different hash"
                    )
        
        return DuplicateCheckResult(
            is_duplicate=False,
            confidence=0.0,
            duplicate_type='none'
        )
    
    def _check_proximity_duplicate(self, transaction: TransactionRecord) -> DuplicateCheckResult:
        """Check for duplicates based on time/amount proximity."""
        if self.existing_transactions.empty:
            return DuplicateCheckResult(is_duplicate=False, confidence=0.0, duplicate_type='none')
        
        # Define time window
        time_start = transaction.date - self.time_proximity_threshold
        time_end = transaction.date + self.time_proximity_threshold
        
        # Find transactions in same wallet and asset within time window
        proximity_matches = self.existing_transactions[
            (self.existing_transactions['wallet_id'] == transaction.wallet_id) &
            (self.existing_transactions['asset'] == transaction.asset) &
            (pd.to_datetime(self.existing_transactions['date']) >= time_start) &
            (pd.to_datetime(self.existing_transactions['date']) <= time_end)
        ]
        
        for _, existing_tx in proximity_matches.iterrows():
            # Check amount similarity
            existing_token_amount = Decimal(str(existing_tx['token_amount']))
            existing_eth_value = Decimal(str(existing_tx['eth_value']))
            
            token_diff = abs(transaction.token_amount - existing_token_amount)
            eth_diff = abs(transaction.eth_value - existing_eth_value)
            
            # Calculate relative differences
            token_rel_diff = token_diff / max(transaction.token_amount, existing_token_amount) if max(transaction.token_amount, existing_token_amount) > 0 else 0
            eth_rel_diff = eth_diff / max(transaction.eth_value, existing_eth_value) if max(transaction.eth_value, existing_eth_value) > 0 else 0
            
            if token_rel_diff <= self.amount_tolerance and eth_rel_diff <= self.amount_tolerance:
                confidence = 0.7 - float(token_rel_diff) - float(eth_rel_diff)
                
                return DuplicateCheckResult(
                    is_duplicate=True,
                    confidence=max(0.3, confidence),
                    duplicate_type='possible',
                    conflicting_tx_hash=existing_tx['tx_hash'],
                    reason=f"Similar transaction found within {self.time_proximity_threshold} (amounts within {self.amount_tolerance} tolerance)",
                    suggested_action="Manual review recommended - possible duplicate with minor differences"
                )
        
        return DuplicateCheckResult(
            is_duplicate=False,
            confidence=0.0,
            duplicate_type='none'
        )
    
    def check_duplicate(self, transaction: TransactionRecord) -> DuplicateCheckResult:
        """
        Comprehensive duplicate check using multiple methods.
        
        Returns the highest confidence duplicate result found.
        """
        logger.debug(f"Checking duplicate for transaction {transaction.tx_hash}")
        
        # Check exact duplicates first (highest confidence)
        exact_result = self._check_exact_duplicate(transaction)
        if exact_result.is_duplicate:
            logger.warning(f"Exact duplicate found: {exact_result.reason}")
            return exact_result
        
        # Check signature duplicates
        signature_result = self._check_signature_duplicate(transaction)
        if signature_result.is_duplicate and signature_result.confidence >= 0.8:
            logger.warning(f"High-confidence signature duplicate found: {signature_result.reason}")
            return signature_result
        
        # Check proximity duplicates
        proximity_result = self._check_proximity_duplicate(transaction)
        if proximity_result.is_duplicate:
            logger.warning(f"Proximity duplicate found: {proximity_result.reason}")
            
            # Return highest confidence result
            if signature_result.confidence > proximity_result.confidence:
                return signature_result
            else:
                return proximity_result
        
        # Return signature result if it has some confidence
        if signature_result.confidence > 0:
            return signature_result
        
        # No duplicates found
        logger.debug(f"No duplicates found for transaction {transaction.tx_hash}")
        return DuplicateCheckResult(
            is_duplicate=False,
            confidence=0.0,
            duplicate_type='none',
            reason="No duplicates detected",
            suggested_action="Process transaction normally"
        )
    
    def check_batch_duplicates(self, transactions: List[TransactionRecord]) -> Dict[str, DuplicateCheckResult]:
        """Check duplicates for a batch of transactions."""
        results = {}
        
        logger.info(f"Checking duplicates for batch of {len(transactions)} transactions")
        
        # Sort transactions by date to process in chronological order
        sorted_transactions = sorted(transactions, key=lambda tx: tx.date)
        
        for transaction in sorted_transactions:
            result = self.check_duplicate(transaction)
            results[transaction.tx_hash] = result
            
            # If not a duplicate, add to known transactions for subsequent checks
            if not result.is_duplicate:
                # Add to temporary transaction list for batch processing
                new_row = {
                    'tx_hash': transaction.tx_hash,
                    'date': transaction.date,
                    'fund_id': transaction.fund_id,
                    'wallet_id': transaction.wallet_id,
                    'asset': transaction.asset,
                    'side': transaction.side,
                    'token_amount': float(transaction.token_amount),
                    'eth_value': float(transaction.eth_value),
                    'usd_value': float(transaction.usd_value)
                }
                
                self.existing_transactions = pd.concat([
                    self.existing_transactions,
                    pd.DataFrame([new_row])
                ], ignore_index=True)
        
        duplicate_count = sum(1 for result in results.values() if result.is_duplicate)
        logger.info(f"Found {duplicate_count} duplicates in batch of {len(transactions)} transactions")
        
        return results
    
    def add_transaction_hash(self, transaction: TransactionRecord) -> bool:
        """Add transaction hash to known hashes set."""
        try:
            primary_hash = self._generate_primary_hash(transaction)
            signature_hash = self._generate_signature_hash(transaction)
            
            self.known_hashes.add(primary_hash)
            self.known_hashes.add(signature_hash)
            
            # Persist to S3
            return self.persistence.save_duplicate_hashes(self.known_hashes)
            
        except Exception as e:
            logger.error(f"Failed to add transaction hash: {e}")
            return False
    
    def remove_transaction_hash(self, transaction: TransactionRecord) -> bool:
        """Remove transaction hash from known hashes set."""
        try:
            primary_hash = self._generate_primary_hash(transaction)
            signature_hash = self._generate_signature_hash(transaction)
            
            self.known_hashes.discard(primary_hash)
            self.known_hashes.discard(signature_hash)
            
            # Persist to S3
            return self.persistence.save_duplicate_hashes(self.known_hashes)
            
        except Exception as e:
            logger.error(f"Failed to remove transaction hash: {e}")
            return False
    
    def rebuild_hash_database(self) -> bool:
        """Rebuild the hash database from existing transactions."""
        try:
            logger.info(f"Rebuilding hash database for fund {self.fund_id}")
            
            # Load all existing transactions
            transactions_df = self.persistence.load_transactions()
            
            new_hashes = set()
            
            for _, row in transactions_df.iterrows():
                try:
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
                    
                    primary_hash = self._generate_primary_hash(transaction)
                    signature_hash = self._generate_signature_hash(transaction)
                    
                    new_hashes.add(primary_hash)
                    new_hashes.add(signature_hash)
                    
                except Exception as e:
                    logger.error(f"Error processing transaction {row.get('tx_hash', 'unknown')}: {e}")
                    continue
            
            self.known_hashes = new_hashes
            
            # Save to S3
            success = self.persistence.save_duplicate_hashes(self.known_hashes)
            
            if success:
                logger.info(f"Successfully rebuilt hash database with {len(self.known_hashes)} hashes")
            else:
                logger.error("Failed to save rebuilt hash database")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to rebuild hash database: {e}")
            return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get duplicate detection statistics."""
        return {
            'fund_id': self.fund_id,
            'known_hashes_count': len(self.known_hashes),
            'existing_transactions_count': len(self.existing_transactions),
            'time_proximity_threshold_minutes': self.time_proximity_threshold.total_seconds() / 60,
            'amount_tolerance': float(self.amount_tolerance),
            'last_updated': datetime.now(timezone.utc).isoformat()
        }