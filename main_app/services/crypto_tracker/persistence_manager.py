"""
S3 Persistence Manager for Crypto Tracker

Manages data persistence for FIFO lots, transactions, and metadata with
atomic operations, versioning, and backup capabilities.
"""

import boto3
import pandas as pd
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from decimal import Decimal
import logging
from io import BytesIO, StringIO
import gzip
import hashlib
from pathlib import Path

logger = logging.getLogger(__name__)

# S3 Configuration
BUCKET_NAME = "realworldnav-beta"
CRYPTO_TRACKER_PREFIX = "crypto_tracker"


@dataclass
class TransactionRecord:
    """Enhanced transaction record with metadata for persistence."""
    tx_hash: str
    block_number: int
    date: datetime
    fund_id: str
    wallet_id: str
    asset: str
    side: str  # 'buy', 'sell', 'transfer_in', 'transfer_out'
    token_amount: Decimal
    eth_value: Decimal
    usd_value: Decimal
    gas_fee_eth: Decimal = Decimal('0')
    gas_fee_usd: Decimal = Decimal('0')
    
    # Persistence metadata
    created_at: Optional[datetime] = None
    last_modified: Optional[datetime] = None
    source: str = 'manual_entry'  # 'blockchain_scan', 'manual_entry', 'csv_import'
    
    # Deduplication fields
    duplicate_check_hash: Optional[str] = None
    is_verified: bool = True
    
    def __post_init__(self):
        """Initialize metadata fields."""
        now = datetime.now(timezone.utc)
        if self.created_at is None:
            self.created_at = now
        if self.last_modified is None:
            self.last_modified = now
        if self.duplicate_check_hash is None:
            self.duplicate_check_hash = self._generate_duplicate_hash()
    
    def _generate_duplicate_hash(self) -> str:
        """Generate hash for duplicate detection."""
        # Use multiple fields to create unique identifier
        hash_input = f"{self.tx_hash}|{self.wallet_id}|{self.asset}|{self.token_amount}|{self.date.isoformat()}"
        return hashlib.sha256(hash_input.encode()).hexdigest()[:16]


@dataclass
class FIFOLot:
    """FIFO lot record for cost basis tracking."""
    lot_id: str
    fund_id: str
    wallet_id: str
    asset: str
    purchase_date: datetime
    original_quantity: Decimal
    remaining_quantity: Decimal
    cost_basis_eth: Decimal
    cost_basis_usd: Decimal
    source_tx_hash: str
    
    # Analytics fields
    unrealized_gain_eth: Optional[Decimal] = None
    unrealized_gain_usd: Optional[Decimal] = None
    days_held: Optional[int] = None
    is_long_term: Optional[bool] = None
    
    # Metadata
    created_at: Optional[datetime] = None
    last_modified: Optional[datetime] = None
    
    def __post_init__(self):
        """Initialize metadata fields."""
        now = datetime.now(timezone.utc)
        if self.created_at is None:
            self.created_at = now
        if self.last_modified is None:
            self.last_modified = now


class PersistenceManager:
    """
    Manages S3 persistence for crypto tracker data with atomic operations,
    versioning, and backup capabilities.
    """
    
    def __init__(self, fund_id: str):
        """Initialize persistence manager for specific fund."""
        self.fund_id = fund_id
        self.s3_client = boto3.client('s3')
        self.bucket = BUCKET_NAME
        
        # S3 key prefixes
        self.base_prefix = f"{CRYPTO_TRACKER_PREFIX}/{fund_id}"
        self.transactions_key = f"{self.base_prefix}/transactions.parquet"
        self.lots_key = f"{self.base_prefix}/fifo_lots.parquet"
        self.metadata_key = f"{self.base_prefix}/metadata.json"
        self.duplicate_hashes_key = f"{self.base_prefix}/duplicate_hashes.json"
        
        # Backup and staging
        self.backup_prefix = f"{CRYPTO_TRACKER_PREFIX}/backups/{fund_id}"
        self.staging_prefix = f"{CRYPTO_TRACKER_PREFIX}/staging/{fund_id}"
        
        logger.info(f"Initialized PersistenceManager for fund: {fund_id}")
    
    def _decimal_to_float(self, obj):
        """Convert Decimal objects to float for JSON serialization."""
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
    
    def _s3_key_exists(self, key: str) -> bool:
        """Check if S3 key exists."""
        try:
            self.s3_client.head_object(Bucket=self.bucket, Key=key)
            return True
        except self.s3_client.exceptions.NoSuchKey:
            return False
        except Exception as e:
            logger.error(f"Error checking S3 key {key}: {e}")
            return False
    
    def _backup_current_data(self) -> str:
        """Create backup of current data before modifications."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        backup_key = f"{self.backup_prefix}/{timestamp}"
        
        try:
            # Backup transactions
            if self._s3_key_exists(self.transactions_key):
                backup_transactions_key = f"{backup_key}/transactions.parquet"
                self.s3_client.copy_object(
                    Bucket=self.bucket,
                    CopySource={'Bucket': self.bucket, 'Key': self.transactions_key},
                    Key=backup_transactions_key
                )
            
            # Backup lots
            if self._s3_key_exists(self.lots_key):
                backup_lots_key = f"{backup_key}/fifo_lots.parquet"
                self.s3_client.copy_object(
                    Bucket=self.bucket,
                    CopySource={'Bucket': self.bucket, 'Key': self.lots_key},
                    Key=backup_lots_key
                )
            
            # Backup metadata
            if self._s3_key_exists(self.metadata_key):
                backup_metadata_key = f"{backup_key}/metadata.json"
                self.s3_client.copy_object(
                    Bucket=self.bucket,
                    CopySource={'Bucket': self.bucket, 'Key': self.metadata_key},
                    Key=backup_metadata_key
                )
            
            logger.info(f"Created backup at: {backup_key}")
            return backup_key
            
        except Exception as e:
            logger.error(f"Failed to create backup: {e}")
            raise
    
    def load_transactions(self) -> pd.DataFrame:
        """Load all transactions for the fund."""
        try:
            if not self._s3_key_exists(self.transactions_key):
                logger.info(f"No existing transactions file for fund {self.fund_id}")
                return pd.DataFrame()
            
            # Load parquet from S3
            obj = self.s3_client.get_object(Bucket=self.bucket, Key=self.transactions_key)
            df = pd.read_parquet(BytesIO(obj['Body'].read()))
            
            # Convert date columns back to datetime
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
            if 'created_at' in df.columns:
                df['created_at'] = pd.to_datetime(df['created_at'])
            if 'last_modified' in df.columns:
                df['last_modified'] = pd.to_datetime(df['last_modified'])
            
            logger.info(f"Loaded {len(df)} transactions for fund {self.fund_id}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to load transactions: {e}")
            return pd.DataFrame()
    
    def save_transactions(self, transactions: List[TransactionRecord], create_backup: bool = True) -> bool:
        """Save transactions with atomic operation."""
        try:
            if create_backup and self._s3_key_exists(self.transactions_key):
                self._backup_current_data()
            
            # Convert to DataFrame
            data_dicts = []
            for tx in transactions:
                tx_dict = asdict(tx)
                # Convert Decimal to float for storage
                for key, value in tx_dict.items():
                    if isinstance(value, Decimal):
                        tx_dict[key] = float(value)
                data_dicts.append(tx_dict)
            
            df = pd.DataFrame(data_dicts)
            
            if df.empty:
                logger.warning("No transactions to save")
                return True
            
            # Stage the data first
            staging_key = f"{self.staging_prefix}/transactions.parquet"
            buffer = BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)
            
            # Upload to staging
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=staging_key,
                Body=buffer.getvalue()
            )
            
            # Atomic move from staging to production
            self.s3_client.copy_object(
                Bucket=self.bucket,
                CopySource={'Bucket': self.bucket, 'Key': staging_key},
                Key=self.transactions_key
            )
            
            # Clean up staging
            self.s3_client.delete_object(Bucket=self.bucket, Key=staging_key)
            
            logger.info(f"Saved {len(transactions)} transactions for fund {self.fund_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save transactions: {e}")
            return False
    
    def load_fifo_lots(self) -> pd.DataFrame:
        """Load FIFO lots for the fund."""
        try:
            if not self._s3_key_exists(self.lots_key):
                logger.info(f"No existing FIFO lots file for fund {self.fund_id}")
                return pd.DataFrame()
            
            # Load parquet from S3
            obj = self.s3_client.get_object(Bucket=self.bucket, Key=self.lots_key)
            df = pd.read_parquet(BytesIO(obj['Body'].read()))
            
            # Convert date columns back to datetime
            if 'purchase_date' in df.columns:
                df['purchase_date'] = pd.to_datetime(df['purchase_date'])
            if 'created_at' in df.columns:
                df['created_at'] = pd.to_datetime(df['created_at'])
            if 'last_modified' in df.columns:
                df['last_modified'] = pd.to_datetime(df['last_modified'])
            
            logger.info(f"Loaded {len(df)} FIFO lots for fund {self.fund_id}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to load FIFO lots: {e}")
            return pd.DataFrame()
    
    def save_fifo_lots(self, lots: List[FIFOLot], create_backup: bool = True) -> bool:
        """Save FIFO lots with atomic operation."""
        try:
            if create_backup and self._s3_key_exists(self.lots_key):
                self._backup_current_data()
            
            # Convert to DataFrame
            data_dicts = []
            for lot in lots:
                lot_dict = asdict(lot)
                # Convert Decimal to float for storage
                for key, value in lot_dict.items():
                    if isinstance(value, Decimal):
                        lot_dict[key] = float(value)
                data_dicts.append(lot_dict)
            
            df = pd.DataFrame(data_dicts)
            
            if df.empty:
                logger.warning("No FIFO lots to save")
                return True
            
            # Stage the data first
            staging_key = f"{self.staging_prefix}/fifo_lots.parquet"
            buffer = BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)
            
            # Upload to staging
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=staging_key,
                Body=buffer.getvalue()
            )
            
            # Atomic move from staging to production
            self.s3_client.copy_object(
                Bucket=self.bucket,
                CopySource={'Bucket': self.bucket, 'Key': staging_key},
                Key=self.lots_key
            )
            
            # Clean up staging
            self.s3_client.delete_object(Bucket=self.bucket, Key=staging_key)
            
            logger.info(f"Saved {len(lots)} FIFO lots for fund {self.fund_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save FIFO lots: {e}")
            return False
    
    def load_metadata(self) -> Dict[str, Any]:
        """Load metadata for the fund."""
        try:
            if not self._s3_key_exists(self.metadata_key):
                return {}
            
            obj = self.s3_client.get_object(Bucket=self.bucket, Key=self.metadata_key)
            metadata = json.loads(obj['Body'].read().decode('utf-8'))
            
            logger.info(f"Loaded metadata for fund {self.fund_id}")
            return metadata
            
        except Exception as e:
            logger.error(f"Failed to load metadata: {e}")
            return {}
    
    def save_metadata(self, metadata: Dict[str, Any]) -> bool:
        """Save metadata with timestamp."""
        try:
            # Add timestamp
            metadata['last_updated'] = datetime.now(timezone.utc).isoformat()
            metadata['fund_id'] = self.fund_id
            
            # Convert to JSON
            json_data = json.dumps(metadata, default=self._decimal_to_float, indent=2)
            
            # Save to S3
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=self.metadata_key,
                Body=json_data,
                ContentType='application/json'
            )
            
            logger.info(f"Saved metadata for fund {self.fund_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save metadata: {e}")
            return False
    
    def load_duplicate_hashes(self) -> set:
        """Load set of duplicate hashes for deduplication."""
        try:
            if not self._s3_key_exists(self.duplicate_hashes_key):
                return set()
            
            obj = self.s3_client.get_object(Bucket=self.bucket, Key=self.duplicate_hashes_key)
            hashes_data = json.loads(obj['Body'].read().decode('utf-8'))
            
            return set(hashes_data.get('hashes', []))
            
        except Exception as e:
            logger.error(f"Failed to load duplicate hashes: {e}")
            return set()
    
    def save_duplicate_hashes(self, hashes: set) -> bool:
        """Save duplicate hashes for deduplication."""
        try:
            data = {
                'fund_id': self.fund_id,
                'last_updated': datetime.now(timezone.utc).isoformat(),
                'hashes': list(hashes)
            }
            
            json_data = json.dumps(data, indent=2)
            
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=self.duplicate_hashes_key,
                Body=json_data,
                ContentType='application/json'
            )
            
            logger.info(f"Saved {len(hashes)} duplicate hashes for fund {self.fund_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save duplicate hashes: {e}")
            return False
    
    def get_data_summary(self) -> Dict[str, Any]:
        """Get summary of stored data."""
        summary = {
            'fund_id': self.fund_id,
            'transactions_exists': self._s3_key_exists(self.transactions_key),
            'lots_exists': self._s3_key_exists(self.lots_key),
            'metadata_exists': self._s3_key_exists(self.metadata_key),
            'duplicate_hashes_exists': self._s3_key_exists(self.duplicate_hashes_key),
        }
        
        # Get transaction count
        if summary['transactions_exists']:
            try:
                df = self.load_transactions()
                summary['transaction_count'] = len(df)
                if not df.empty:
                    summary['earliest_transaction'] = df['date'].min().isoformat()
                    summary['latest_transaction'] = df['date'].max().isoformat()
            except Exception as e:
                logger.error(f"Error getting transaction summary: {e}")
                summary['transaction_count'] = 'error'
        
        # Get lots count
        if summary['lots_exists']:
            try:
                df = self.load_fifo_lots()
                summary['lots_count'] = len(df)
                summary['active_lots'] = len(df[df['remaining_quantity'] > 0]) if not df.empty else 0
            except Exception as e:
                logger.error(f"Error getting lots summary: {e}")
                summary['lots_count'] = 'error'
        
        return summary
    
    def cleanup_old_backups(self, keep_days: int = 30) -> int:
        """Clean up old backup files, keeping only recent ones."""
        try:
            cutoff_date = datetime.now(timezone.utc) - pd.Timedelta(days=keep_days)
            
            # List all backup objects
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=self.backup_prefix
            )
            
            deleted_count = 0
            if 'Contents' in response:
                for obj in response['Contents']:
                    if obj['LastModified'].replace(tzinfo=timezone.utc) < cutoff_date:
                        self.s3_client.delete_object(
                            Bucket=self.bucket,
                            Key=obj['Key']
                        )
                        deleted_count += 1
            
            logger.info(f"Cleaned up {deleted_count} old backup files for fund {self.fund_id}")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error cleaning up backups: {e}")
            return 0