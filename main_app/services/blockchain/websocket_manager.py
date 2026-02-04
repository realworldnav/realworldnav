"""
WebSocket management for real-time blockchain event monitoring.

Provides WebSocket subscriptions with auto-reconnection and exponential backoff,
plus a hybrid monitor that falls back to polling when WebSocket is unavailable.
"""

import asyncio
import logging
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set
from collections import deque

logger = logging.getLogger(__name__)


class ConnectionState(Enum):
    """WebSocket connection states."""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    FAILED = "failed"


@dataclass
class SubscriptionInfo:
    """Information about an active subscription."""
    subscription_id: str
    subscription_type: str  # "logs", "newHeads", "newPendingTransactions"
    filter_params: Optional[Dict] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class WebSocketManager:
    """
    WebSocket manager with auto-reconnection and exponential backoff.

    Supports eth_subscribe for:
    - newHeads: New block headers
    - logs: Contract event logs (filtered by address/topics)
    - newPendingTransactions: Pending transaction hashes

    Usage:
        from .endpoint_manager import EndpointManager

        endpoint_manager = EndpointManager.from_environment()
        ws_manager = WebSocketManager(endpoint_manager)

        async def handle_event(event):
            print(f"New event: {event}")

        await ws_manager.start(
            watched_addresses=["0x1234..."],
            callback=handle_event
        )
    """

    def __init__(
        self,
        endpoint_manager: "EndpointManager",
        initial_reconnect_delay: float = 1.0,
        max_reconnect_delay: float = 60.0,
        backoff_multiplier: float = 2.0,
    ):
        self.endpoint_manager = endpoint_manager
        self.initial_reconnect_delay = initial_reconnect_delay
        self.max_reconnect_delay = max_reconnect_delay
        self.backoff_multiplier = backoff_multiplier

        self._reconnect_delay = initial_reconnect_delay
        self._running = False
        self._ws = None
        self._state = ConnectionState.DISCONNECTED
        self._subscriptions: Dict[str, SubscriptionInfo] = {}
        self._callback: Optional[Callable] = None
        self._event_queue: deque = deque(maxlen=1000)
        self._reconnect_count = 0
        self._last_event_time: Optional[datetime] = None

    @property
    def state(self) -> ConnectionState:
        """Get current connection state."""
        return self._state

    @property
    def is_connected(self) -> bool:
        """Check if WebSocket is connected."""
        return self._state == ConnectionState.CONNECTED

    async def start(
        self,
        watched_addresses: List[str],
        callback: Optional[Callable] = None,
        subscription_types: Optional[List[str]] = None,
    ) -> None:
        """
        Start WebSocket monitoring.

        Args:
            watched_addresses: Contract addresses to watch for logs
            callback: Async function called for each event
            subscription_types: Types to subscribe to (default: ["logs", "newHeads"])
        """
        self._running = True
        self._callback = callback
        subscription_types = subscription_types or ["logs", "newHeads"]

        while self._running:
            try:
                await self._connect_and_subscribe(watched_addresses, subscription_types)
            except asyncio.CancelledError:
                logger.info("WebSocket monitoring cancelled")
                break
            except Exception as e:
                if not self._running:
                    break

                self._state = ConnectionState.RECONNECTING
                self._reconnect_count += 1

                logger.warning(
                    f"WebSocket error (attempt {self._reconnect_count}): {e}. "
                    f"Reconnecting in {self._reconnect_delay:.1f}s..."
                )

                await asyncio.sleep(self._reconnect_delay)

                # Exponential backoff with cap
                self._reconnect_delay = min(
                    self._reconnect_delay * self.backoff_multiplier,
                    self.max_reconnect_delay
                )

    async def stop(self) -> None:
        """Stop WebSocket monitoring."""
        self._running = False
        self._state = ConnectionState.DISCONNECTED

        # Unsubscribe from all subscriptions
        for sub_id in list(self._subscriptions.keys()):
            try:
                await self._unsubscribe(sub_id)
            except Exception:
                pass

        self._subscriptions.clear()

        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

    async def _connect_and_subscribe(
        self,
        watched_addresses: List[str],
        subscription_types: List[str],
    ) -> None:
        """Connect to WebSocket and set up subscriptions."""
        self._state = ConnectionState.CONNECTING

        # Get WebSocket URL from endpoint manager
        ws_url = self._get_websocket_url()
        if not ws_url:
            raise ConnectionError("No WebSocket URL available")

        try:
            # Import websockets dynamically to handle optional dependency
            import websockets

            async with websockets.connect(ws_url) as ws:
                self._ws = ws
                self._state = ConnectionState.CONNECTED
                self._reconnect_delay = self.initial_reconnect_delay  # Reset on success
                self._reconnect_count = 0

                logger.info(f"WebSocket connected to {ws_url[:50]}...")

                # Set up subscriptions
                for sub_type in subscription_types:
                    await self._subscribe(sub_type, watched_addresses)

                # Listen for events
                async for message in ws:
                    if not self._running:
                        break

                    try:
                        data = json.loads(message)
                        await self._handle_message(data)
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse WebSocket message: {e}")

        except ImportError:
            logger.error("websockets package not installed. Run: pip install websockets")
            self._state = ConnectionState.FAILED
            raise
        except Exception as e:
            self._state = ConnectionState.DISCONNECTED
            raise

    def _get_websocket_url(self) -> Optional[str]:
        """Get WebSocket URL from highest priority endpoint."""
        for name, config in self.endpoint_manager.configs.items():
            health = self.endpoint_manager.health.get(name)
            if health and self.endpoint_manager.circuit_breaker.should_allow_request(health):
                if config.websocket_url:
                    return config.websocket_url
        return None

    async def _subscribe(
        self,
        subscription_type: str,
        watched_addresses: List[str],
    ) -> Optional[str]:
        """Create a subscription."""
        if not self._ws:
            return None

        request_id = len(self._subscriptions) + 1

        if subscription_type == "logs":
            # Subscribe to logs from watched addresses
            params = [{
                "address": watched_addresses,
            }]
            request = {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": "eth_subscribe",
                "params": ["logs", params[0]]
            }
        elif subscription_type == "newHeads":
            request = {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": "eth_subscribe",
                "params": ["newHeads"]
            }
        elif subscription_type == "newPendingTransactions":
            request = {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": "eth_subscribe",
                "params": ["newPendingTransactions"]
            }
        else:
            logger.warning(f"Unknown subscription type: {subscription_type}")
            return None

        await self._ws.send(json.dumps(request))
        logger.debug(f"Sent subscription request for {subscription_type}")

        # Note: Subscription ID will be received in response message
        return None

    async def _unsubscribe(self, subscription_id: str) -> None:
        """Cancel a subscription."""
        if not self._ws:
            return

        request = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_unsubscribe",
            "params": [subscription_id]
        }
        await self._ws.send(json.dumps(request))

    async def _handle_message(self, data: Dict) -> None:
        """Handle incoming WebSocket message."""
        # Handle subscription confirmation
        if "result" in data and isinstance(data.get("result"), str):
            sub_id = data["result"]
            # Store subscription (we'd need request context to know the type)
            self._subscriptions[sub_id] = SubscriptionInfo(
                subscription_id=sub_id,
                subscription_type="unknown"  # Would need request tracking
            )
            logger.info(f"Subscription confirmed: {sub_id}")
            return

        # Handle subscription event
        if data.get("method") == "eth_subscription":
            params = data.get("params", {})
            sub_id = params.get("subscription")
            result = params.get("result", {})

            self._last_event_time = datetime.now(timezone.utc)
            self._event_queue.append({
                "subscription": sub_id,
                "data": result,
                "timestamp": self._last_event_time.isoformat()
            })

            # Call callback if set
            if self._callback:
                try:
                    if asyncio.iscoroutinefunction(self._callback):
                        await self._callback(result)
                    else:
                        self._callback(result)
                except Exception as e:
                    logger.error(f"Error in WebSocket callback: {e}")

    def get_status(self) -> Dict:
        """Get WebSocket status for monitoring."""
        return {
            "state": self._state.value,
            "is_connected": self.is_connected,
            "reconnect_count": self._reconnect_count,
            "current_reconnect_delay": self._reconnect_delay,
            "subscription_count": len(self._subscriptions),
            "subscriptions": list(self._subscriptions.keys()),
            "last_event_time": self._last_event_time.isoformat() if self._last_event_time else None,
            "event_queue_size": len(self._event_queue),
        }


class HybridMonitor:
    """
    Hybrid monitoring strategy: WebSocket primary with polling fallback.

    Automatically falls back to polling when WebSocket is unavailable or
    experiencing issues, then attempts to reconnect to WebSocket.

    Usage:
        monitor = HybridMonitor(endpoint_manager)
        await monitor.start(
            watched_addresses=["0x1234..."],
            callback=handle_event
        )
    """

    def __init__(
        self,
        endpoint_manager: "EndpointManager",
        polling_interval: float = 12.0,  # ~1 Ethereum block
        ws_retry_interval: float = 60.0,
    ):
        self.endpoint_manager = endpoint_manager
        self.polling_interval = polling_interval
        self.ws_retry_interval = ws_retry_interval

        self.ws_manager = WebSocketManager(endpoint_manager)
        self._running = False
        self._using_websocket = False
        self._last_block_checked: Optional[int] = None
        self._callback: Optional[Callable] = None
        self._watched_addresses: List[str] = []

    @property
    def is_using_websocket(self) -> bool:
        """Check if currently using WebSocket (vs polling)."""
        return self._using_websocket and self.ws_manager.is_connected

    async def start(
        self,
        watched_addresses: List[str],
        callback: Callable,
    ) -> None:
        """
        Start hybrid monitoring.

        Args:
            watched_addresses: Contract addresses to monitor
            callback: Async function called for each new event/transaction
        """
        self._running = True
        self._callback = callback
        self._watched_addresses = watched_addresses

        # Try WebSocket first
        ws_url = self.ws_manager._get_websocket_url()

        if ws_url:
            # Start WebSocket monitoring in background
            asyncio.create_task(self._run_websocket())
            self._using_websocket = True
        else:
            logger.info("No WebSocket URL available, using polling only")
            self._using_websocket = False

        # Run polling loop (handles both fallback and WebSocket health check)
        await self._run_polling_loop()

    async def stop(self) -> None:
        """Stop all monitoring."""
        self._running = False
        await self.ws_manager.stop()

    async def _run_websocket(self) -> None:
        """Run WebSocket monitoring in background."""
        try:
            await self.ws_manager.start(
                watched_addresses=self._watched_addresses,
                callback=self._callback,
            )
        except Exception as e:
            logger.error(f"WebSocket monitoring failed: {e}")
            self._using_websocket = False

    async def _run_polling_loop(self) -> None:
        """Run polling loop for fallback and health checks."""
        last_ws_retry = datetime.now(timezone.utc)

        while self._running:
            try:
                # If WebSocket is connected and healthy, just do periodic health check
                if self.ws_manager.is_connected:
                    await asyncio.sleep(self.polling_interval)
                    continue

                # WebSocket not available, use polling
                if not self._using_websocket or not self.ws_manager.is_connected:
                    await self._poll_for_events()

                    # Periodically try to reconnect WebSocket
                    now = datetime.now(timezone.utc)
                    if (now - last_ws_retry).total_seconds() >= self.ws_retry_interval:
                        if self.ws_manager._get_websocket_url():
                            logger.info("Attempting to reconnect WebSocket...")
                            asyncio.create_task(self._run_websocket())
                            self._using_websocket = True
                        last_ws_retry = now

                await asyncio.sleep(self.polling_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in polling loop: {e}")
                await asyncio.sleep(self.polling_interval)

    async def _poll_for_events(self) -> None:
        """Poll for new events using HTTP endpoint."""
        try:
            # Get current block
            current_block = await self.endpoint_manager.execute_with_failover(
                "eth.block_number"
            )

            # On first run, just set the baseline
            if self._last_block_checked is None:
                self._last_block_checked = current_block
                return

            # Check for new blocks
            if current_block <= self._last_block_checked:
                return

            # Fetch logs for new blocks
            from_block = self._last_block_checked + 1
            to_block = current_block

            for address in self._watched_addresses:
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
                        if self._callback:
                            if asyncio.iscoroutinefunction(self._callback):
                                await self._callback(log)
                            else:
                                self._callback(log)

                except Exception as e:
                    logger.warning(f"Error fetching logs for {address}: {e}")

            self._last_block_checked = current_block

        except Exception as e:
            logger.error(f"Error in polling: {e}")

    def get_status(self) -> Dict:
        """Get hybrid monitor status."""
        return {
            "mode": "websocket" if self.is_using_websocket else "polling",
            "websocket": self.ws_manager.get_status(),
            "polling_interval": self.polling_interval,
            "last_block_checked": self._last_block_checked,
            "watched_addresses": len(self._watched_addresses),
        }
