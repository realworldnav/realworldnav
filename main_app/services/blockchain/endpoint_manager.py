"""
Multi-endpoint RPC management with circuit breaker pattern.

Provides failover between multiple Ethereum RPC endpoints (Infura, Alchemy, etc.)
with automatic health tracking and circuit breaker protection.
"""

import os
import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple
from functools import wraps

from web3 import Web3
from web3.providers import HTTPProvider
try:
    from web3.providers import WebSocketProvider
except ImportError:
    from web3.providers import WebsocketProvider as WebSocketProvider
from web3.exceptions import Web3Exception

logger = logging.getLogger(__name__)


class CircuitBreakerState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation, requests allowed
    OPEN = "open"          # Failures exceeded threshold, requests blocked
    HALF_OPEN = "half_open"  # Testing if endpoint recovered


class AllEndpointsFailedError(Exception):
    """Raised when all endpoints have failed."""
    pass


@dataclass
class EndpointConfig:
    """Configuration for an RPC endpoint."""
    name: str
    http_url: str
    websocket_url: Optional[str] = None
    priority: int = 1  # Lower = higher priority
    rate_limit: int = 100  # Requests per second
    timeout: int = 30  # Request timeout in seconds

    def __post_init__(self):
        if not self.http_url:
            raise ValueError(f"HTTP URL required for endpoint {self.name}")


@dataclass
class EndpointHealth:
    """Health tracking for an endpoint."""
    name: str
    success_count: int = 0
    failure_count: int = 0
    consecutive_failures: int = 0
    last_success: Optional[datetime] = None
    last_failure: Optional[datetime] = None
    last_error: Optional[str] = None
    circuit_state: CircuitBreakerState = CircuitBreakerState.CLOSED
    circuit_opened_at: Optional[datetime] = None

    def record_success(self) -> None:
        """Record a successful request."""
        self.success_count += 1
        self.consecutive_failures = 0
        self.last_success = datetime.now(timezone.utc)

        # Reset circuit if in half-open state
        if self.circuit_state == CircuitBreakerState.HALF_OPEN:
            self.circuit_state = CircuitBreakerState.CLOSED
            self.circuit_opened_at = None
            logger.info(f"Circuit breaker CLOSED for {self.name}")

    def record_failure(self, error: Optional[str] = None) -> None:
        """Record a failed request."""
        self.failure_count += 1
        self.consecutive_failures += 1
        self.last_failure = datetime.now(timezone.utc)
        self.last_error = error

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        total = self.success_count + self.failure_count
        if total == 0:
            return 100.0
        return (self.success_count / total) * 100


@dataclass
class CircuitBreaker:
    """
    Circuit breaker implementation for endpoint protection.

    States:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Too many failures, requests are blocked
    - HALF_OPEN: Testing recovery, allows one request through
    """
    failure_threshold: int = 5  # Failures before opening circuit
    recovery_timeout: int = 60  # Seconds before trying half-open
    half_open_max_calls: int = 3  # Successful calls to close circuit

    _half_open_successes: int = field(default=0, init=False)

    def should_allow_request(self, health: EndpointHealth) -> bool:
        """Check if request should be allowed based on circuit state."""
        now = datetime.now(timezone.utc)

        if health.circuit_state == CircuitBreakerState.CLOSED:
            return True

        if health.circuit_state == CircuitBreakerState.OPEN:
            # Check if recovery timeout has elapsed
            if health.circuit_opened_at:
                elapsed = (now - health.circuit_opened_at).total_seconds()
                if elapsed >= self.recovery_timeout:
                    health.circuit_state = CircuitBreakerState.HALF_OPEN
                    self._half_open_successes = 0
                    logger.info(f"Circuit breaker HALF_OPEN for {health.name}")
                    return True
            return False

        # HALF_OPEN: allow request for testing
        return True

    def on_success(self, health: EndpointHealth) -> None:
        """Handle successful request."""
        if health.circuit_state == CircuitBreakerState.HALF_OPEN:
            self._half_open_successes += 1
            if self._half_open_successes >= self.half_open_max_calls:
                health.circuit_state = CircuitBreakerState.CLOSED
                health.circuit_opened_at = None
                logger.info(f"Circuit breaker CLOSED for {health.name} after recovery")

    def on_failure(self, health: EndpointHealth) -> None:
        """Handle failed request."""
        if health.circuit_state == CircuitBreakerState.HALF_OPEN:
            # Failed during recovery, go back to open
            health.circuit_state = CircuitBreakerState.OPEN
            health.circuit_opened_at = datetime.now(timezone.utc)
            logger.warning(f"Circuit breaker re-OPENED for {health.name}")
        elif health.consecutive_failures >= self.failure_threshold:
            # Threshold exceeded, open circuit
            health.circuit_state = CircuitBreakerState.OPEN
            health.circuit_opened_at = datetime.now(timezone.utc)
            logger.warning(
                f"Circuit breaker OPENED for {health.name} "
                f"after {health.consecutive_failures} consecutive failures"
            )


class EndpointManager:
    """
    Manages multiple RPC endpoints with failover and circuit breaker.

    Usage:
        manager = EndpointManager.from_environment()
        result = await manager.execute_with_failover("eth_blockNumber")
    """

    def __init__(
        self,
        configs: List[EndpointConfig],
        circuit_breaker: Optional[CircuitBreaker] = None,
    ):
        self.configs = {c.name: c for c in configs}
        self.endpoints: Dict[str, Web3] = {}
        self.health: Dict[str, EndpointHealth] = {}
        self.circuit_breaker = circuit_breaker or CircuitBreaker()

        # Initialize endpoints and health tracking
        for config in configs:
            self._init_endpoint(config)

    def _init_endpoint(self, config: EndpointConfig) -> None:
        """Initialize a Web3 endpoint."""
        try:
            provider = HTTPProvider(
                config.http_url,
                request_kwargs={"timeout": config.timeout}
            )
            w3 = Web3(provider)

            # Test connection
            if w3.is_connected():
                self.endpoints[config.name] = w3
                self.health[config.name] = EndpointHealth(name=config.name)
                logger.info(f"Initialized endpoint: {config.name}")
            else:
                logger.warning(f"Failed to connect to endpoint: {config.name}")
                self.health[config.name] = EndpointHealth(
                    name=config.name,
                    circuit_state=CircuitBreakerState.OPEN,
                    circuit_opened_at=datetime.now(timezone.utc),
                    last_error="Initial connection failed"
                )
        except Exception as e:
            logger.error(f"Error initializing endpoint {config.name}: {e}")
            self.health[config.name] = EndpointHealth(
                name=config.name,
                circuit_state=CircuitBreakerState.OPEN,
                circuit_opened_at=datetime.now(timezone.utc),
                last_error=str(e)
            )

    @classmethod
    def from_environment(cls) -> "EndpointManager":
        """
        Create EndpointManager from environment variables.

        Supported env vars (checked in order):
        - WEB3_HTTP_URL, WEB3_WEBSOCKET_URL (common naming)
        - WEB3_INFURA_HTTP_URL, WEB3_INFURA_WS_URL
        - WEB3_ALCHEMY_HTTP_URL, WEB3_ALCHEMY_WS_URL
        - INFURA_URL (legacy)
        """
        configs = []

        # Primary: WEB3_HTTP_URL (most common naming convention)
        web3_http = os.getenv("WEB3_HTTP_URL")
        if web3_http:
            configs.append(EndpointConfig(
                name="primary",
                http_url=web3_http,
                websocket_url=os.getenv("WEB3_WEBSOCKET_URL"),
                priority=1,
            ))

        # Infura-specific (priority 2 if primary exists, else 1)
        infura_http = os.getenv("WEB3_INFURA_HTTP_URL")
        if infura_http:
            configs.append(EndpointConfig(
                name="infura",
                http_url=infura_http,
                websocket_url=os.getenv("WEB3_INFURA_WS_URL"),
                priority=2 if web3_http else 1,
            ))

        # Alchemy (priority 3/2)
        alchemy_http = os.getenv("WEB3_ALCHEMY_HTTP_URL")
        if alchemy_http:
            configs.append(EndpointConfig(
                name="alchemy",
                http_url=alchemy_http,
                websocket_url=os.getenv("WEB3_ALCHEMY_WS_URL"),
                priority=3 if web3_http else 2,
            ))

        # Fallback to legacy INFURA_URL
        legacy_infura = os.getenv("INFURA_URL")
        if legacy_infura and not web3_http and not infura_http:
            configs.append(EndpointConfig(
                name="infura_legacy",
                http_url=legacy_infura,
                priority=1,
            ))

        if not configs:
            raise ValueError(
                "No RPC endpoints configured. Set WEB3_HTTP_URL or WEB3_INFURA_HTTP_URL"
            )

        return cls(configs)

    def _get_sorted_healthy_endpoints(self) -> List[Tuple[str, Web3]]:
        """Get endpoints sorted by priority, filtered by circuit breaker."""
        healthy = []

        for name, w3 in self.endpoints.items():
            health = self.health.get(name)
            if health and self.circuit_breaker.should_allow_request(health):
                config = self.configs[name]
                healthy.append((name, w3, config.priority))

        # Sort by priority (lower = higher priority)
        healthy.sort(key=lambda x: x[2])

        return [(name, w3) for name, w3, _ in healthy]

    async def execute_with_failover(
        self,
        method: str,
        *args,
        **kwargs
    ) -> Any:
        """
        Execute an RPC method with automatic failover.

        Args:
            method: The Web3 method to call (e.g., "eth.get_block")
            *args: Positional arguments for the method
            **kwargs: Keyword arguments for the method

        Returns:
            The result from the first successful endpoint

        Raises:
            AllEndpointsFailedError: If all endpoints fail
        """
        errors = []

        for name, w3 in self._get_sorted_healthy_endpoints():
            health = self.health[name]

            try:
                # Navigate to method (e.g., "eth.get_block" -> w3.eth.get_block)
                obj = w3
                for attr in method.split("."):
                    obj = getattr(obj, attr)

                # Execute the call
                if asyncio.iscoroutinefunction(obj):
                    result = await obj(*args, **kwargs)
                else:
                    # Run sync calls in executor
                    loop = asyncio.get_event_loop()
                    result = await loop.run_in_executor(
                        None, lambda: obj(*args, **kwargs)
                    )

                # Record success
                health.record_success()
                self.circuit_breaker.on_success(health)

                return result

            except Exception as e:
                error_msg = f"{name}: {type(e).__name__}: {str(e)}"
                errors.append(error_msg)
                logger.warning(f"Endpoint {name} failed: {e}")

                health.record_failure(str(e))
                self.circuit_breaker.on_failure(health)
                continue

        raise AllEndpointsFailedError(
            f"All endpoints failed:\n" + "\n".join(errors)
        )

    def execute_sync(self, method: str, *args, **kwargs) -> Any:
        """
        Synchronous version of execute_with_failover.

        Use this when not in an async context.
        """
        errors = []

        for name, w3 in self._get_sorted_healthy_endpoints():
            health = self.health[name]

            try:
                # Navigate to method
                obj = w3
                for attr in method.split("."):
                    obj = getattr(obj, attr)

                result = obj(*args, **kwargs)

                health.record_success()
                self.circuit_breaker.on_success(health)

                return result

            except Exception as e:
                error_msg = f"{name}: {type(e).__name__}: {str(e)}"
                errors.append(error_msg)
                logger.warning(f"Endpoint {name} failed: {e}")

                health.record_failure(str(e))
                self.circuit_breaker.on_failure(health)
                continue

        raise AllEndpointsFailedError(
            f"All endpoints failed:\n" + "\n".join(errors)
        )

    def get_primary_web3(self) -> Optional[Web3]:
        """Get the highest-priority healthy Web3 instance."""
        healthy = self._get_sorted_healthy_endpoints()
        return healthy[0][1] if healthy else None

    def get_endpoint_status(self) -> Dict[str, Dict]:
        """Get status of all endpoints for monitoring."""
        status = {}
        for name, health in self.health.items():
            config = self.configs.get(name)
            status[name] = {
                "connected": name in self.endpoints,
                "circuit_state": health.circuit_state.value,
                "success_rate": round(health.success_rate, 2),
                "consecutive_failures": health.consecutive_failures,
                "last_success": health.last_success.isoformat() if health.last_success else None,
                "last_failure": health.last_failure.isoformat() if health.last_failure else None,
                "last_error": health.last_error,
                "priority": config.priority if config else None,
            }
        return status

    def reset_circuit_breaker(self, endpoint_name: Optional[str] = None) -> None:
        """Reset circuit breaker for one or all endpoints."""
        if endpoint_name:
            if endpoint_name in self.health:
                self.health[endpoint_name].circuit_state = CircuitBreakerState.CLOSED
                self.health[endpoint_name].circuit_opened_at = None
                self.health[endpoint_name].consecutive_failures = 0
        else:
            for health in self.health.values():
                health.circuit_state = CircuitBreakerState.CLOSED
                health.circuit_opened_at = None
                health.consecutive_failures = 0
