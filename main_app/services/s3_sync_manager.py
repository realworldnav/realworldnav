"""
S3 synchronization manager for decoded transactions.

Provides batch sync of decoded transactions to S3 with deduplication,
and retrieval of previously synced transactions.
"""

import json
import logging
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set
import hashlib

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


# Default S3 configuration
DEFAULT_BUCKET = "realworldnav-beta"
DEFAULT_PREFIX = "drip_capital/decoded_transactions/"


class DecimalEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal types."""

    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        if hasattr(obj, "__dict__"):
            return obj.__dict__
        return super().default(obj)


@dataclass
class SyncResult:
    """Result of a sync operation."""
    synced: int                          # Number of transactions synced
    skipped: int                         # Number already existed
    errors: int = 0                      # Number of errors
    error_messages: List[str] = field(default_factory=list)
    batch_key: Optional[str] = None      # S3 key where batch was stored
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "synced": self.synced,
            "skipped": self.skipped,
            "errors": self.errors,
            "error_messages": self.error_messages,
            "batch_key": self.batch_key,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class SyncedTransaction:
    """Metadata about a synced transaction."""
    tx_hash: str
    synced_at: datetime
    batch_key: str
    platform: Optional[str] = None
    category: Optional[str] = None


class S3SyncManager:
    """
    Manages synchronization of decoded transactions to S3.

    Stores decoded transactions in batch JSON files, with an index
    for quick lookup of already-synced transactions.

    Directory structure:
        {prefix}/{fund_id}/
            batches/
                {batch_name}.json       # Batch of decoded transactions
            index/
                synced_hashes.json      # Index of synced tx hashes
            latest/
                current.json            # Most recent batch

    Usage:
        from main_app.services.s3_sync_manager import S3SyncManager

        sync = S3SyncManager(fund_id="drip_capital")

        # Sync decoded transactions
        result = await sync.sync_to_s3(decoded_transactions)
        print(f"Synced {result.synced}, skipped {result.skipped}")

        # Load previously synced
        transactions = sync.load_synced_transactions()
    """

    def __init__(
        self,
        fund_id: str = "drip_capital",
        bucket: str = DEFAULT_BUCKET,
        prefix: str = DEFAULT_PREFIX,
        s3_client: Optional[Any] = None,
    ):
        """
        Initialize sync manager.

        Args:
            fund_id: Identifier for the fund (used in S3 path)
            bucket: S3 bucket name
            prefix: S3 key prefix
            s3_client: Optional boto3 S3 client (created if not provided)
        """
        self.fund_id = fund_id
        self.bucket = bucket
        self.prefix = prefix.rstrip("/") + "/"

        self._s3 = s3_client or boto3.client("s3")
        self._synced_hashes: Optional[Set[str]] = None

    @property
    def base_path(self) -> str:
        """Get base S3 path for this fund."""
        return f"{self.prefix}{self.fund_id}/"

    async def sync_to_s3(
        self,
        transactions: List[Any],
        batch_name: Optional[str] = None,
        force: bool = False,
    ) -> SyncResult:
        """
        Sync decoded transactions to S3.

        Args:
            transactions: List of DecodedTransaction objects
            batch_name: Optional name for this batch (auto-generated if not provided)
            force: If True, sync even if already synced

        Returns:
            SyncResult with counts and batch info
        """
        if not transactions:
            return SyncResult(synced=0, skipped=0)

        # Load index of already-synced transactions
        synced_hashes = self._load_synced_index()

        # Filter out already-synced (unless force)
        new_transactions = []
        skipped_count = 0

        for tx in transactions:
            tx_hash = self._get_tx_hash(tx)
            if tx_hash and (force or tx_hash not in synced_hashes):
                new_transactions.append(tx)
            else:
                skipped_count += 1

        if not new_transactions:
            return SyncResult(synced=0, skipped=skipped_count)

        # Generate batch name if not provided
        if not batch_name:
            batch_name = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

        # Serialize transactions
        try:
            batch_data = self._serialize_batch(new_transactions, batch_name)
        except Exception as e:
            logger.error(f"Error serializing transactions: {e}")
            return SyncResult(
                synced=0,
                skipped=skipped_count,
                errors=len(new_transactions),
                error_messages=[f"Serialization error: {str(e)}"],
            )

        # Upload to S3
        batch_key = f"{self.base_path}batches/{batch_name}.json"

        try:
            self._s3.put_object(
                Bucket=self.bucket,
                Key=batch_key,
                Body=json.dumps(batch_data, cls=DecimalEncoder),
                ContentType="application/json",
            )
            logger.info(f"Synced {len(new_transactions)} transactions to {batch_key}")

        except ClientError as e:
            logger.error(f"S3 upload failed: {e}")
            return SyncResult(
                synced=0,
                skipped=skipped_count,
                errors=len(new_transactions),
                error_messages=[f"S3 upload failed: {str(e)}"],
            )

        # Update synced index
        new_hashes = {self._get_tx_hash(tx) for tx in new_transactions if self._get_tx_hash(tx)}
        synced_hashes.update(new_hashes)
        self._save_synced_index(synced_hashes)

        # Update "latest" pointer
        self._update_latest(batch_key, len(new_transactions))

        return SyncResult(
            synced=len(new_transactions),
            skipped=skipped_count,
            batch_key=batch_key,
        )

    def sync_to_s3_sync(
        self,
        transactions: List[Any],
        batch_name: Optional[str] = None,
        force: bool = False,
    ) -> SyncResult:
        """Synchronous version of sync_to_s3."""
        import asyncio
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(
                self.sync_to_s3(transactions, batch_name, force)
            )
        finally:
            loop.close()

    def load_synced_transactions(
        self,
        limit: Optional[int] = None,
        since: Optional[datetime] = None,
    ) -> List[Dict]:
        """
        Load previously synced transactions from S3.

        Args:
            limit: Maximum number of transactions to load
            since: Only load transactions synced after this time

        Returns:
            List of transaction dictionaries
        """
        transactions = []

        # List all batch files
        try:
            paginator = self._s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(
                Bucket=self.bucket,
                Prefix=f"{self.base_path}batches/",
            )

            batch_keys = []
            for page in pages:
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    last_modified = obj["LastModified"]

                    # Filter by time if specified
                    if since and last_modified < since:
                        continue

                    batch_keys.append((key, last_modified))

            # Sort by modification time (newest first)
            batch_keys.sort(key=lambda x: x[1], reverse=True)

            # Load batches
            for key, _ in batch_keys:
                batch_txs = self._load_batch(key)
                transactions.extend(batch_txs)

                if limit and len(transactions) >= limit:
                    transactions = transactions[:limit]
                    break

        except ClientError as e:
            logger.error(f"Error listing S3 batches: {e}")

        return transactions

    def is_synced(self, tx_hash: str) -> bool:
        """Check if a transaction has been synced."""
        synced_hashes = self._load_synced_index()
        return tx_hash.lower() in synced_hashes

    def get_sync_stats(self) -> Dict:
        """Get synchronization statistics."""
        synced_hashes = self._load_synced_index()

        # Count batches
        batch_count = 0
        try:
            paginator = self._s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(
                Bucket=self.bucket,
                Prefix=f"{self.base_path}batches/",
            )
            for page in pages:
                batch_count += len(page.get("Contents", []))
        except ClientError:
            pass

        return {
            "fund_id": self.fund_id,
            "bucket": self.bucket,
            "base_path": self.base_path,
            "total_synced_transactions": len(synced_hashes),
            "batch_count": batch_count,
        }

    def _get_tx_hash(self, tx: Any) -> Optional[str]:
        """Extract transaction hash from various formats."""
        if hasattr(tx, "tx_hash"):
            return tx.tx_hash.lower() if tx.tx_hash else None
        elif isinstance(tx, dict):
            tx_hash = tx.get("tx_hash") or tx.get("hash") or tx.get("transactionHash")
            return tx_hash.lower() if tx_hash else None
        return None

    def _serialize_batch(
        self,
        transactions: List[Any],
        batch_name: str,
    ) -> Dict:
        """Serialize transactions to JSON-compatible format."""
        serialized_txs = []

        for tx in transactions:
            if hasattr(tx, "to_dict"):
                tx_dict = tx.to_dict()
            elif hasattr(tx, "__dict__"):
                tx_dict = self._object_to_dict(tx)
            elif isinstance(tx, dict):
                tx_dict = tx
            else:
                tx_dict = {"data": str(tx)}

            serialized_txs.append(tx_dict)

        return {
            "batch_name": batch_name,
            "fund_id": self.fund_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "transaction_count": len(serialized_txs),
            "transactions": serialized_txs,
        }

    def _object_to_dict(self, obj: Any) -> Dict:
        """Convert object to dictionary, handling nested objects."""
        if hasattr(obj, "__dict__"):
            result = {}
            for key, value in obj.__dict__.items():
                if not key.startswith("_"):
                    if hasattr(value, "__dict__"):
                        result[key] = self._object_to_dict(value)
                    elif isinstance(value, list):
                        result[key] = [
                            self._object_to_dict(item) if hasattr(item, "__dict__") else item
                            for item in value
                        ]
                    elif isinstance(value, Decimal):
                        result[key] = str(value)
                    elif isinstance(value, datetime):
                        result[key] = value.isoformat()
                    elif hasattr(value, "value"):  # Enum
                        result[key] = value.value
                    else:
                        result[key] = value
            return result
        return obj

    def _load_batch(self, key: str) -> List[Dict]:
        """Load a batch file from S3."""
        try:
            response = self._s3.get_object(Bucket=self.bucket, Key=key)
            data = json.loads(response["Body"].read().decode("utf-8"))
            return data.get("transactions", [])
        except ClientError as e:
            logger.warning(f"Error loading batch {key}: {e}")
            return []

    def _load_synced_index(self) -> Set[str]:
        """Load the index of synced transaction hashes."""
        if self._synced_hashes is not None:
            return self._synced_hashes

        index_key = f"{self.base_path}index/synced_hashes.json"

        try:
            response = self._s3.get_object(Bucket=self.bucket, Key=index_key)
            data = json.loads(response["Body"].read().decode("utf-8"))
            self._synced_hashes = set(data.get("hashes", []))
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                self._synced_hashes = set()
            else:
                logger.warning(f"Error loading synced index: {e}")
                self._synced_hashes = set()

        return self._synced_hashes

    def _save_synced_index(self, hashes: Set[str]) -> None:
        """Save the index of synced transaction hashes."""
        index_key = f"{self.base_path}index/synced_hashes.json"

        try:
            data = {
                "hashes": list(hashes),
                "count": len(hashes),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            self._s3.put_object(
                Bucket=self.bucket,
                Key=index_key,
                Body=json.dumps(data),
                ContentType="application/json",
            )
            self._synced_hashes = hashes
        except ClientError as e:
            logger.error(f"Error saving synced index: {e}")

    def _update_latest(self, batch_key: str, count: int) -> None:
        """Update the 'latest' pointer."""
        latest_key = f"{self.base_path}latest/current.json"

        try:
            data = {
                "batch_key": batch_key,
                "transaction_count": count,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            self._s3.put_object(
                Bucket=self.bucket,
                Key=latest_key,
                Body=json.dumps(data),
                ContentType="application/json",
            )
        except ClientError as e:
            logger.warning(f"Error updating latest pointer: {e}")

    def clear_synced_index(self) -> None:
        """Clear the synced index (useful for re-syncing)."""
        self._synced_hashes = None
        index_key = f"{self.base_path}index/synced_hashes.json"

        try:
            self._s3.delete_object(Bucket=self.bucket, Key=index_key)
        except ClientError:
            pass

    def delete_all_synced(self) -> int:
        """
        Delete all synced data for this fund.

        Returns:
            Number of objects deleted
        """
        deleted = 0

        try:
            paginator = self._s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(
                Bucket=self.bucket,
                Prefix=self.base_path,
            )

            for page in pages:
                objects = page.get("Contents", [])
                if objects:
                    delete_keys = [{"Key": obj["Key"]} for obj in objects]
                    self._s3.delete_objects(
                        Bucket=self.bucket,
                        Delete={"Objects": delete_keys},
                    )
                    deleted += len(delete_keys)

            self._synced_hashes = None

        except ClientError as e:
            logger.error(f"Error deleting synced data: {e}")

        return deleted
