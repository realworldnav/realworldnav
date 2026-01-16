"""
Adapter to unify legacy decoded_tx_cache with DecoderRegistry interface.

This module provides LegacyRegistryAdapter that wraps the legacy reactive
decoded_tx_cache dict with the same interface as DecoderRegistry, enabling
consistent UI code regardless of which cache is in use.
"""

from typing import Dict, List, Any, Optional, Callable
from datetime import datetime
from decimal import Decimal
import logging

from .base import (
    DecodedTransaction,
    DecodedEvent,
    JournalEntry,
    PostingStatus,
    Platform,
    TransactionCategory,
)

logger = logging.getLogger(__name__)


class LegacyRegistryAdapter:
    """
    Wraps legacy decoded_tx_cache dict to match DecoderRegistry interface.

    This adapter allows the UI layer to work consistently whether using
    the new DecoderRegistry or the legacy blur_auto_decoder cache.

    Usage:
        adapter = LegacyRegistryAdapter(decoded_tx_cache_value.get)
        transactions = adapter.decoded_cache  # Returns Dict[str, DecodedTransaction]
        stats = adapter.stats  # Returns stats dict
    """

    def __init__(self, legacy_cache_getter: Callable[[], Dict]):
        """
        Initialize adapter with a getter function for the legacy cache.

        Args:
            legacy_cache_getter: Callable that returns the legacy cache dict
                                 (typically decoded_tx_cache_value.get)
        """
        self._get_cache = legacy_cache_getter
        self._converted_cache: Dict[str, DecodedTransaction] = {}
        self._cache_version = None

    @property
    def decoded_cache(self) -> Dict[str, DecodedTransaction]:
        """
        Convert legacy cache to DecodedTransaction objects.

        Returns:
            Dict mapping tx_hash to DecodedTransaction
        """
        raw = self._get_cache() or {}

        # Simple cache invalidation based on length change
        cache_version = len(raw)
        if cache_version != self._cache_version:
            self._converted_cache = {}
            self._cache_version = cache_version

            for tx_hash, data in raw.items():
                try:
                    if isinstance(data, DecodedTransaction):
                        self._converted_cache[tx_hash] = data
                    elif isinstance(data, dict):
                        self._converted_cache[tx_hash] = self._dict_to_decoded_tx(data)
                except Exception as e:
                    logger.debug(f"Could not convert legacy tx {tx_hash}: {e}")

        return self._converted_cache

    def _dict_to_decoded_tx(self, d: Dict) -> DecodedTransaction:
        """
        Convert legacy dict format to DecodedTransaction.

        Handles various legacy field names and formats.
        """
        # Parse timestamp
        timestamp = d.get('timestamp', datetime.now())
        if isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except ValueError:
                timestamp = datetime.now()

        # Parse platform
        platform_str = d.get('platform', d.get('decoder_type', 'unknown'))
        try:
            platform = Platform(platform_str)
        except ValueError:
            platform = Platform.UNKNOWN

        # Parse category
        category_str = d.get('category', 'UNKNOWN')
        try:
            category = TransactionCategory(category_str)
        except ValueError:
            category = TransactionCategory.UNKNOWN

        # Parse numeric values safely
        def safe_decimal(val, default=0) -> Decimal:
            try:
                return Decimal(str(val)) if val is not None else Decimal(default)
            except (ValueError, TypeError):
                return Decimal(default)

        return DecodedTransaction(
            status=d.get('status', 'success'),
            tx_hash=d.get('tx_hash', d.get('hash', '')),
            platform=platform,
            category=category,
            block=d.get('block', 0),
            timestamp=timestamp,
            eth_price=safe_decimal(d.get('eth_price', 0)),
            gas_used=d.get('gas_used', 0),
            gas_fee=safe_decimal(d.get('gas_fee', 0)),
            from_address=d.get('from_address', d.get('from', '')),
            to_address=d.get('to_address', d.get('to', '')),
            value=safe_decimal(d.get('value', 0)),
            function_name=d.get('function_name', d.get('function', 'unknown')),
            function_params=d.get('function_params', {}),
            events=[],  # Legacy format doesn't preserve decoded events
            journal_entries=[],  # Legacy format doesn't have journal entries
            wallet_roles=d.get('wallet_roles', {}),
            positions={},
            raw_data=d.get('raw_data', {}),
            error=d.get('error')
        )

    @property
    def stats(self) -> Dict[str, Any]:
        """
        Return stats matching DecoderRegistry.stats format.

        Returns:
            Dict with total_decoded, success_count, error_count,
            auto_post_ready, review_queue, platforms, decoders_loaded
        """
        cache = self.decoded_cache
        platforms: Dict[str, int] = {}

        for tx in cache.values():
            p = tx.platform.value if hasattr(tx.platform, 'value') else str(tx.platform)
            platforms[p] = platforms.get(p, 0) + 1

        success_count = sum(1 for t in cache.values() if t.status == "success")
        error_count = sum(1 for t in cache.values() if t.status == "error")

        return {
            "total_decoded": len(cache),
            "success_count": success_count,
            "error_count": error_count,
            "auto_post_ready": 0,  # Legacy doesn't track this
            "review_queue": success_count,  # All legacy txs go to review
            "platforms": platforms,
            "decoders_loaded": [],
        }

    def get_auto_post_ready(self) -> List[DecodedTransaction]:
        """
        Get transactions ready for auto-posting.

        Legacy cache has no auto-post tracking, so returns empty list.
        All legacy transactions require manual review.
        """
        return []

    def get_review_queue(self) -> List[DecodedTransaction]:
        """
        Get transactions pending review.

        All legacy transactions go to review queue.
        """
        return [
            tx for tx in self.decoded_cache.values()
            if tx.status == "success"
        ]

    def clear_cache(self):
        """
        Clear the internal converted cache.

        Note: This doesn't clear the underlying legacy cache,
        just the converted version.
        """
        self._converted_cache.clear()
        self._cache_version = None

    def decode_transaction(self, tx_hash: str) -> Optional[DecodedTransaction]:
        """
        Get a specific decoded transaction by hash.

        Args:
            tx_hash: Transaction hash to look up

        Returns:
            DecodedTransaction if found, None otherwise
        """
        return self.decoded_cache.get(tx_hash)
