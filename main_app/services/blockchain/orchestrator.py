"""
Central orchestrator for blockchain monitoring and transaction decoding.

Coordinates all blockchain infrastructure components:
- Multi-endpoint failover (EndpointManager)
- WebSocket/polling hybrid monitoring (HybridMonitor)
- Event deduplication (EventDeduplicator)
- Tiered caching (CacheManager)
- Transaction decoding (DecoderRegistry)
- S3 synchronization (S3SyncManager)

This replaces the older BlockchainService with a more resilient architecture.
"""

import asyncio
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Set

from .endpoint_manager import (
    EndpointManager,
    EndpointConfig,
    AllEndpointsFailedError,
)
from .websocket_manager import WebSocketManager, HybridMonitor
from .event_deduplicator import EventDeduplicator, TransactionDeduplicator
from .cache_manager import CacheManager, CacheTier, TTLConfig

logger = logging.getLogger(__name__)


@dataclass
class OrchestratorConfig:
    """Configuration for BlockchainOrchestrator."""
    # Monitoring
    enable_websocket: bool = True
    polling_interval: float = 12.0  # seconds
    ws_retry_interval: float = 60.0  # seconds

    # Caching
    l1_cache_size: int = 1000
    l2_cache_size: int = 10000
    enable_s3_cache: bool = True

    # Deduplication
    dedup_max_size: int = 10000
    dedup_max_age_hours: int = 24

    # Feature flags (read from environment)
    @classmethod
    def from_environment(cls) -> "OrchestratorConfig":
        return cls(
            enable_websocket=os.getenv("ENABLE_WEBSOCKET_MONITORING", "true").lower() == "true",
            enable_s3_cache=os.getenv("ENABLE_S3_CACHE", "true").lower() == "true",
        )


@dataclass
class MonitoringState:
    """Current state of blockchain monitoring."""
    is_monitoring: bool = False
    mode: str = "idle"  # idle, websocket, polling, hybrid
    last_block: Optional[int] = None
    transactions_processed: int = 0
    events_processed: int = 0
    duplicates_skipped: int = 0
    errors: int = 0
    started_at: Optional[datetime] = None


class BlockchainOrchestrator:
    """
    Central coordinator for blockchain monitoring and transaction processing.

    Provides a unified API for:
    - Connecting to blockchain with automatic failover
    - Monitoring wallet addresses for new transactions
    - Decoding transactions using platform-specific decoders
    - Caching decoded transactions
    - Syncing decoded transactions to S3

    Usage:
        # Initialize
        orchestrator = BlockchainOrchestrator(
            fund_wallets=["0x1234...", "0x5678..."],
            fund_id="drip_capital"
        )

        # Start monitoring
        await orchestrator.start_monitoring(callback=on_new_transaction)

        # Or fetch historical transactions
        transactions = orchestrator.fetch_historical_transactions(limit=100)

        # Decode a specific transaction
        decoded = orchestrator.decode_transaction(tx_hash)

        # Sync to S3
        result = await orchestrator.sync_decoded_to_s3()
    """

    def __init__(
        self,
        fund_wallets: List[str],
        fund_id: str = "drip_capital",
        config: Optional[OrchestratorConfig] = None,
        decoder_registry: Optional[Any] = None,
    ):
        """
        Initialize orchestrator.

        Args:
            fund_wallets: List of wallet addresses to monitor
            fund_id: Identifier for the fund (used in S3 paths)
            config: Optional configuration (uses defaults if not provided)
            decoder_registry: Optional existing DecoderRegistry instance
        """
        self.fund_wallets = [w.lower() for w in fund_wallets]
        self.fund_id = fund_id
        self.config = config or OrchestratorConfig.from_environment()

        # Initialize components
        self._init_endpoint_manager()
        self._init_cache_manager()
        self._init_deduplicator()
        self._init_monitors()
        self._init_s3_sync()
        self._init_decoder_registry(decoder_registry)

        # State
        self.state = MonitoringState()
        self._callback: Optional[Callable] = None
        self._monitoring_task: Optional[asyncio.Task] = None

    def _init_endpoint_manager(self) -> None:
        """Initialize multi-endpoint manager."""
        try:
            self.endpoint_manager = EndpointManager.from_environment()
            logger.info(
                f"Initialized endpoint manager with {len(self.endpoint_manager.endpoints)} endpoints"
            )
        except ValueError as e:
            logger.warning(f"No endpoints configured: {e}")
            self.endpoint_manager = None

    def _init_cache_manager(self) -> None:
        """Initialize tiered cache."""
        s3_client = None
        s3_bucket = None

        if self.config.enable_s3_cache:
            try:
                import boto3
                s3_client = boto3.client("s3")
                s3_bucket = os.getenv("S3_CACHE_BUCKET", "realworldnav-beta")
            except Exception as e:
                logger.warning(f"S3 cache disabled: {e}")

        self.cache_manager = CacheManager(
            l1_max_size=self.config.l1_cache_size,
            l2_max_size=self.config.l2_cache_size,
            s3_client=s3_client,
            s3_bucket=s3_bucket,
            s3_prefix="cache/blockchain/",
        )

    def _init_deduplicator(self) -> None:
        """Initialize event deduplicator."""
        self.event_deduplicator = EventDeduplicator(
            max_size=self.config.dedup_max_size,
            max_age_hours=self.config.dedup_max_age_hours,
        )
        self.tx_deduplicator = TransactionDeduplicator(
            max_size=self.config.dedup_max_size // 2
        )

    def _init_monitors(self) -> None:
        """Initialize WebSocket and hybrid monitors."""
        if self.endpoint_manager:
            self.ws_manager = WebSocketManager(self.endpoint_manager)
            self.hybrid_monitor = HybridMonitor(
                self.endpoint_manager,
                polling_interval=self.config.polling_interval,
                ws_retry_interval=self.config.ws_retry_interval,
            )
        else:
            self.ws_manager = None
            self.hybrid_monitor = None

    def _init_s3_sync(self) -> None:
        """Initialize S3 sync manager."""
        try:
            from ..s3_sync_manager import S3SyncManager
            self.s3_sync = S3SyncManager(fund_id=self.fund_id)
        except Exception as e:
            logger.warning(f"S3 sync disabled: {e}")
            self.s3_sync = None

    def _init_decoder_registry(self, registry: Optional[Any]) -> None:
        """Initialize or use provided decoder registry."""
        if registry:
            self.decoder_registry = registry
        else:
            try:
                from ..decoders.registry import DecoderRegistry
                self.decoder_registry = DecoderRegistry(fund_wallets=self.fund_wallets)
            except Exception as e:
                logger.error(f"Failed to initialize decoder registry: {e}")
                self.decoder_registry = None

    # ==================== Connection Methods ====================

    def is_connected(self) -> bool:
        """Check if connected to blockchain."""
        if not self.endpoint_manager:
            return False

        primary = self.endpoint_manager.get_primary_web3()
        return primary is not None and primary.is_connected()

    def get_connection_status(self) -> Dict:
        """Get detailed connection status."""
        if not self.endpoint_manager:
            return {"connected": False, "reason": "No endpoints configured"}

        return {
            "connected": self.is_connected(),
            "endpoints": self.endpoint_manager.get_endpoint_status(),
            "monitoring": {
                "active": self.state.is_monitoring,
                "mode": self.state.mode,
                "last_block": self.state.last_block,
            },
        }

    def get_web3(self) -> Optional[Any]:
        """Get the primary Web3 instance."""
        if self.endpoint_manager:
            return self.endpoint_manager.get_primary_web3()
        return None

    # ==================== Monitoring Methods ====================

    async def start_monitoring(
        self,
        callback: Optional[Callable] = None,
        use_websocket: Optional[bool] = None,
    ) -> None:
        """
        Start monitoring wallets for new transactions.

        Args:
            callback: Function called for each new event/transaction
            use_websocket: Override config to force WebSocket or polling
        """
        if self.state.is_monitoring:
            logger.warning("Already monitoring, stop first")
            return

        if not self.hybrid_monitor:
            logger.error("Cannot monitor: no endpoints configured")
            return

        self._callback = callback
        self.state.is_monitoring = True
        self.state.started_at = datetime.now(timezone.utc)

        # Determine monitoring mode
        use_ws = use_websocket if use_websocket is not None else self.config.enable_websocket

        if use_ws:
            self.state.mode = "hybrid"
            self._monitoring_task = asyncio.create_task(
                self.hybrid_monitor.start(
                    watched_addresses=self.fund_wallets,
                    callback=self._handle_event,
                )
            )
        else:
            self.state.mode = "polling"
            self._monitoring_task = asyncio.create_task(
                self._polling_loop()
            )

        logger.info(f"Started monitoring in {self.state.mode} mode")

    async def stop_monitoring(self) -> None:
        """Stop monitoring for new transactions."""
        self.state.is_monitoring = False
        self.state.mode = "idle"

        if self.hybrid_monitor:
            await self.hybrid_monitor.stop()

        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
            self._monitoring_task = None

        logger.info("Stopped monitoring")

    async def _polling_loop(self) -> None:
        """Polling-only monitoring loop."""
        last_block = None

        while self.state.is_monitoring:
            try:
                current_block = await self.endpoint_manager.execute_with_failover(
                    "eth.block_number"
                )

                if last_block is None:
                    last_block = current_block

                if current_block > last_block:
                    await self._check_blocks(last_block + 1, current_block)
                    last_block = current_block
                    self.state.last_block = current_block

            except AllEndpointsFailedError as e:
                logger.error(f"All endpoints failed: {e}")
                self.state.errors += 1
            except Exception as e:
                logger.error(f"Polling error: {e}")
                self.state.errors += 1

            await asyncio.sleep(self.config.polling_interval)

    async def _check_blocks(self, from_block: int, to_block: int) -> None:
        """Check blocks for transactions to watched wallets."""
        for address in self.fund_wallets:
            try:
                logs = await self.endpoint_manager.execute_with_failover(
                    "eth.get_logs",
                    {
                        "address": address,
                        "fromBlock": hex(from_block),
                        "toBlock": hex(to_block),
                    }
                )

                for log in logs:
                    await self._handle_event(log)

            except Exception as e:
                logger.warning(f"Error fetching logs for {address}: {e}")

    async def _handle_event(self, event: Dict) -> None:
        """Handle incoming event from WebSocket or polling."""
        # Deduplicate
        if self.event_deduplicator.is_duplicate_event(event):
            self.state.duplicates_skipped += 1
            return

        self.state.events_processed += 1

        # Extract transaction hash
        tx_hash = event.get("transactionHash")
        if hasattr(tx_hash, "hex"):
            tx_hash = tx_hash.hex()

        # Decode if we have a registry
        if tx_hash and self.decoder_registry:
            try:
                decoded = await self._decode_transaction_async(tx_hash)
                if decoded:
                    # Cache the decoded transaction
                    cache_key = f"decoded_{tx_hash}"
                    self.cache_manager.set(cache_key, decoded, tier=CacheTier.L1_MEMORY)
                    self.state.transactions_processed += 1

                    # Call callback with decoded transaction
                    if self._callback:
                        if asyncio.iscoroutinefunction(self._callback):
                            await self._callback(decoded)
                        else:
                            self._callback(decoded)
                    return

            except Exception as e:
                logger.warning(f"Error decoding transaction {tx_hash}: {e}")

        # Fallback: call callback with raw event
        if self._callback:
            if asyncio.iscoroutinefunction(self._callback):
                await self._callback(event)
            else:
                self._callback(event)

    # ==================== Transaction Methods ====================

    def fetch_historical_transactions(
        self,
        limit: int = 100,
        start_block: int = 0,
    ) -> List[Dict]:
        """
        Fetch historical transactions for watched wallets.

        Uses Etherscan API if available, falls back to RPC getLogs.

        Args:
            limit: Maximum transactions to fetch
            start_block: Starting block number

        Returns:
            List of transaction dictionaries
        """
        transactions = []

        # Try Etherscan first (faster for historical data)
        etherscan_key = os.getenv("ETHERSCAN_API_KEY")
        if etherscan_key:
            try:
                transactions = self._fetch_from_etherscan(limit, start_block)
                if transactions:
                    return transactions
            except Exception as e:
                logger.warning(f"Etherscan fetch failed: {e}")

        # Fallback to RPC
        if self.endpoint_manager:
            try:
                transactions = self._fetch_from_rpc(limit, start_block)
            except Exception as e:
                logger.error(f"RPC fetch failed: {e}")

        return transactions

    def _fetch_from_etherscan(self, limit: int, start_block: int) -> List[Dict]:
        """Fetch transactions from Etherscan API."""
        import requests

        api_key = os.getenv("ETHERSCAN_API_KEY")
        transactions = []

        for address in self.fund_wallets:
            try:
                response = requests.get(
                    "https://api.etherscan.io/v2/api",
                    params={
                        "chainid": 1,
                        "module": "account",
                        "action": "txlist",
                        "address": address,
                        "startblock": start_block,
                        "endblock": 99999999,
                        "page": 1,
                        "offset": limit,
                        "sort": "desc",
                        "apikey": api_key,
                    },
                    timeout=30,
                )
                data = response.json()

                if data.get("status") == "1":
                    for tx in data.get("result", []):
                        transactions.append({
                            "hash": tx.get("hash"),
                            "block": int(tx.get("blockNumber", 0)),
                            "from": tx.get("from"),
                            "to": tx.get("to"),
                            "value": int(tx.get("value", 0)) / 1e18,
                            "gas_fee": int(tx.get("gasUsed", 0)) * int(tx.get("gasPrice", 0)) / 1e18,
                            "timestamp": datetime.fromtimestamp(int(tx.get("timeStamp", 0)), tz=timezone.utc),
                            "status": "Confirmed" if tx.get("txreceipt_status") == "1" else "Failed",
                            "type": "IN" if tx.get("to", "").lower() == address.lower() else "OUT",
                        })

            except Exception as e:
                logger.warning(f"Etherscan fetch error for {address}: {e}")

        return transactions[:limit]

    def _fetch_from_rpc(self, limit: int, start_block: int) -> List[Dict]:
        """Fetch transactions from RPC endpoint."""
        # This would require iterating through blocks which is slow
        # For now, return empty and rely on Etherscan
        return []

    def decode_transaction(self, tx_hash: str) -> Optional[Any]:
        """
        Decode a transaction synchronously.

        Args:
            tx_hash: Transaction hash to decode

        Returns:
            DecodedTransaction or None
        """
        if not self.decoder_registry:
            logger.error("No decoder registry available")
            return None

        # Check cache first
        cache_key = f"decoded_{tx_hash.lower()}"
        cached = self.cache_manager.get(cache_key)
        if cached:
            return cached

        # Get Web3 instance
        w3 = self.get_web3()
        if not w3:
            logger.error("No Web3 connection available")
            return None

        try:
            # Fetch transaction and receipt
            tx = w3.eth.get_transaction(tx_hash)
            receipt = w3.eth.get_transaction_receipt(tx_hash)

            # Decode
            decoded = self.decoder_registry.decode_transaction(tx, receipt, w3)

            # Cache result
            if decoded:
                self.cache_manager.set(cache_key, decoded, tier=CacheTier.L1_MEMORY)

            return decoded

        except Exception as e:
            logger.error(f"Error decoding transaction {tx_hash}: {e}")
            return None

    async def _decode_transaction_async(self, tx_hash: str) -> Optional[Any]:
        """Async version of decode_transaction."""
        # Run in executor since Web3 calls are sync
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.decode_transaction, tx_hash)

    # ==================== S3 Sync Methods ====================

    async def sync_decoded_to_s3(
        self,
        batch_name: Optional[str] = None,
    ) -> Optional["SyncResult"]:
        """
        Sync all cached decoded transactions to S3.

        Args:
            batch_name: Optional name for the batch

        Returns:
            SyncResult with counts
        """
        if not self.s3_sync:
            logger.error("S3 sync not available")
            return None

        if not self.decoder_registry:
            logger.error("No decoder registry available")
            return None

        # Get all decoded transactions from registry cache
        decoded_txs = list(self.decoder_registry.decoded_cache.values())

        if not decoded_txs:
            logger.info("No decoded transactions to sync")
            from ..s3_sync_manager import SyncResult
            return SyncResult(synced=0, skipped=0)

        return await self.s3_sync.sync_to_s3(decoded_txs, batch_name)

    def sync_decoded_to_s3_sync(
        self,
        batch_name: Optional[str] = None,
    ) -> Optional["SyncResult"]:
        """Synchronous version of sync_decoded_to_s3."""
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(self.sync_decoded_to_s3(batch_name))
        finally:
            loop.close()

    # ==================== Cache Management ====================

    def clear_caches(self, include_decoded: bool = True) -> Dict[str, int]:
        """
        Clear all caches.

        Args:
            include_decoded: Also clear decoded transaction cache

        Returns:
            Dict with counts of cleared items
        """
        cleared = {}

        # Clear tiered cache
        cache_cleared = self.cache_manager.clear()
        cleared.update(cache_cleared)

        # Clear deduplicator
        cleared["deduplicator"] = self.event_deduplicator.clear()
        self.tx_deduplicator.clear()

        # Clear decoded cache
        if include_decoded and self.decoder_registry:
            count = len(self.decoder_registry.decoded_cache)
            self.decoder_registry.decoded_cache.clear()
            cleared["decoded_transactions"] = count

        return cleared

    # ==================== Status & Statistics ====================

    def get_status(self) -> Dict:
        """Get comprehensive orchestrator status."""
        return {
            "connection": self.get_connection_status(),
            "monitoring": {
                "is_active": self.state.is_monitoring,
                "mode": self.state.mode,
                "started_at": self.state.started_at.isoformat() if self.state.started_at else None,
                "last_block": self.state.last_block,
                "transactions_processed": self.state.transactions_processed,
                "events_processed": self.state.events_processed,
                "duplicates_skipped": self.state.duplicates_skipped,
                "errors": self.state.errors,
            },
            "cache": self.cache_manager.get_stats(),
            "deduplicator": self.event_deduplicator.get_stats(),
            "websocket": self.ws_manager.get_status() if self.ws_manager else None,
            "s3_sync": self.s3_sync.get_sync_stats() if self.s3_sync else None,
            "decoder": {
                "available": self.decoder_registry is not None,
                "decoded_count": len(self.decoder_registry.decoded_cache) if self.decoder_registry else 0,
            },
            "config": {
                "enable_websocket": self.config.enable_websocket,
                "polling_interval": self.config.polling_interval,
                "fund_wallets": len(self.fund_wallets),
            },
        }

    def reset_circuit_breakers(self) -> None:
        """Reset all circuit breakers to allow retry."""
        if self.endpoint_manager:
            self.endpoint_manager.reset_circuit_breaker()
