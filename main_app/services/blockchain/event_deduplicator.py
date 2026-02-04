"""
Event deduplication for blockchain transaction processing.

Ensures each event (identified by tx_hash + log_index) is only processed once,
even when receiving from multiple endpoints or during reconnection.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Set, Tuple
from collections import OrderedDict
import threading

logger = logging.getLogger(__name__)


@dataclass
class EventRecord:
    """Record of a seen event."""
    tx_hash: str
    log_index: int
    first_seen: datetime
    source: Optional[str] = None  # Which endpoint first reported this


class EventDeduplicator:
    """
    Deduplicates blockchain events using tx_hash + log_index as unique key.

    Maintains a bounded cache of seen events with automatic cleanup of old entries.
    Thread-safe for use in async environments.

    Usage:
        deduplicator = EventDeduplicator(max_size=10000)

        # Check and record in one call
        if not deduplicator.is_duplicate(tx_hash, log_index):
            process_event(event)

        # Or use with events that have transaction hash in the data
        if not deduplicator.is_duplicate_event(event_dict):
            process_event(event_dict)
    """

    def __init__(
        self,
        max_size: int = 10000,
        cleanup_threshold: float = 0.9,
        max_age_hours: int = 24,
    ):
        """
        Initialize deduplicator.

        Args:
            max_size: Maximum number of events to track
            cleanup_threshold: Trigger cleanup when this % of max_size reached
            max_age_hours: Remove events older than this many hours
        """
        self.max_size = max_size
        self.cleanup_threshold = cleanup_threshold
        self.max_age = timedelta(hours=max_age_hours)

        # Use OrderedDict for LRU-like behavior (oldest first)
        self._seen_events: OrderedDict[str, EventRecord] = OrderedDict()
        self._lock = threading.Lock()

        # Statistics
        self._total_checked = 0
        self._duplicates_found = 0

    def make_key(self, tx_hash: str, log_index: int) -> str:
        """
        Create unique key from transaction hash and log index.

        Args:
            tx_hash: Transaction hash (0x-prefixed)
            log_index: Index of the log within the transaction

        Returns:
            Unique identifier string
        """
        # Normalize hash to lowercase
        normalized_hash = tx_hash.lower() if tx_hash else ""
        return f"{normalized_hash}:{log_index}"

    def is_duplicate(
        self,
        tx_hash: str,
        log_index: int,
        source: Optional[str] = None,
    ) -> bool:
        """
        Check if event is a duplicate and record if new.

        Args:
            tx_hash: Transaction hash
            log_index: Log index within transaction
            source: Optional identifier of the source (endpoint name)

        Returns:
            True if this event was already seen, False if new
        """
        key = self.make_key(tx_hash, log_index)
        now = datetime.now(timezone.utc)

        with self._lock:
            self._total_checked += 1

            if key in self._seen_events:
                self._duplicates_found += 1
                # Move to end (mark as recently accessed)
                self._seen_events.move_to_end(key)
                return True

            # New event - record it
            self._seen_events[key] = EventRecord(
                tx_hash=tx_hash,
                log_index=log_index,
                first_seen=now,
                source=source,
            )

            # Trigger cleanup if needed
            if len(self._seen_events) >= self.max_size * self.cleanup_threshold:
                self._cleanup()

            return False

    def is_duplicate_event(
        self,
        event: Dict,
        source: Optional[str] = None,
    ) -> bool:
        """
        Check if event dict is duplicate.

        Extracts tx_hash and log_index from common event formats:
        - Standard: {"transactionHash": "0x...", "logIndex": 0}
        - Web3.py: {"transactionHash": HexBytes("0x..."), "logIndex": 0}

        Args:
            event: Event dictionary from Web3 or WebSocket
            source: Optional source identifier

        Returns:
            True if duplicate, False if new
        """
        # Extract transaction hash
        tx_hash = event.get("transactionHash") or event.get("tx_hash") or event.get("hash")
        if tx_hash is None:
            logger.warning("Event missing transaction hash, cannot deduplicate")
            return False

        # Handle HexBytes
        if hasattr(tx_hash, "hex"):
            tx_hash = tx_hash.hex()
        elif isinstance(tx_hash, bytes):
            tx_hash = "0x" + tx_hash.hex()

        # Extract log index (default to 0 for transaction-level events)
        log_index = event.get("logIndex", 0)
        if isinstance(log_index, str):
            log_index = int(log_index, 16) if log_index.startswith("0x") else int(log_index)

        return self.is_duplicate(tx_hash, log_index, source)

    def is_transaction_seen(self, tx_hash: str) -> bool:
        """
        Check if any event from this transaction was seen.

        Useful for quick transaction-level deduplication without log index.

        Args:
            tx_hash: Transaction hash to check

        Returns:
            True if any event from this transaction was recorded
        """
        normalized_hash = tx_hash.lower() if tx_hash else ""

        with self._lock:
            for key in self._seen_events.keys():
                if key.startswith(normalized_hash + ":"):
                    return True
            return False

    def _cleanup(self) -> int:
        """
        Remove old entries to stay within size limit.

        Called automatically when threshold is reached.

        Returns:
            Number of entries removed
        """
        now = datetime.now(timezone.utc)
        removed = 0

        # First, remove entries older than max_age
        keys_to_remove = []
        for key, record in self._seen_events.items():
            if now - record.first_seen > self.max_age:
                keys_to_remove.append(key)

        for key in keys_to_remove:
            del self._seen_events[key]
            removed += 1

        # If still too large, remove oldest entries (FIFO)
        while len(self._seen_events) >= self.max_size:
            self._seen_events.popitem(last=False)  # Remove oldest
            removed += 1

        if removed > 0:
            logger.debug(f"Deduplicator cleanup: removed {removed} old entries")

        return removed

    def clear(self) -> int:
        """
        Clear all tracked events.

        Returns:
            Number of entries cleared
        """
        with self._lock:
            count = len(self._seen_events)
            self._seen_events.clear()
            return count

    def get_stats(self) -> Dict:
        """
        Get deduplication statistics.

        Returns:
            Dictionary with stats about deduplication performance
        """
        with self._lock:
            oldest = None
            newest = None

            if self._seen_events:
                # Get first and last items
                first_key = next(iter(self._seen_events))
                last_key = next(reversed(self._seen_events))
                oldest = self._seen_events[first_key].first_seen
                newest = self._seen_events[last_key].first_seen

            duplicate_rate = 0.0
            if self._total_checked > 0:
                duplicate_rate = (self._duplicates_found / self._total_checked) * 100

            return {
                "total_events_tracked": len(self._seen_events),
                "max_size": self.max_size,
                "utilization_pct": round(len(self._seen_events) / self.max_size * 100, 2),
                "total_checked": self._total_checked,
                "duplicates_found": self._duplicates_found,
                "duplicate_rate_pct": round(duplicate_rate, 2),
                "oldest_event": oldest.isoformat() if oldest else None,
                "newest_event": newest.isoformat() if newest else None,
            }

    def get_recent_events(self, limit: int = 10) -> list:
        """
        Get most recently seen events (for debugging).

        Args:
            limit: Maximum number of events to return

        Returns:
            List of recent EventRecords as dicts
        """
        with self._lock:
            recent = list(self._seen_events.items())[-limit:]
            return [
                {
                    "key": key,
                    "tx_hash": record.tx_hash,
                    "log_index": record.log_index,
                    "first_seen": record.first_seen.isoformat(),
                    "source": record.source,
                }
                for key, record in recent
            ]


class TransactionDeduplicator:
    """
    Simplified deduplicator for transaction-level deduplication.

    Use this when you only care about transaction hashes, not individual logs.
    """

    def __init__(self, max_size: int = 5000):
        self.max_size = max_size
        self._seen: OrderedDict[str, datetime] = OrderedDict()
        self._lock = threading.Lock()

    def is_duplicate(self, tx_hash: str) -> bool:
        """Check if transaction was already seen."""
        key = tx_hash.lower() if tx_hash else ""

        with self._lock:
            if key in self._seen:
                self._seen.move_to_end(key)
                return True

            self._seen[key] = datetime.now(timezone.utc)

            # Evict oldest if at capacity
            while len(self._seen) > self.max_size:
                self._seen.popitem(last=False)

            return False

    def clear(self) -> None:
        """Clear all tracked transactions."""
        with self._lock:
            self._seen.clear()
