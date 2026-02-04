"""
Blockchain infrastructure package for RealWorldNAV.

This package provides:
- Multi-endpoint RPC failover with circuit breaker
- WebSocket subscriptions with auto-reconnection
- Event deduplication
- Tiered caching (L1/L2/L3)
- Central orchestration layer
"""

from .endpoint_manager import (
    EndpointConfig,
    EndpointHealth,
    CircuitBreaker,
    CircuitBreakerState,
    EndpointManager,
    AllEndpointsFailedError,
)
from .websocket_manager import (
    WebSocketManager,
    HybridMonitor,
)
from .event_deduplicator import EventDeduplicator
from .cache_manager import (
    CacheManager,
    CacheEntry,
    CacheTier,
    TTLConfig,
)
from .orchestrator import BlockchainOrchestrator

__all__ = [
    # Endpoint management
    "EndpointConfig",
    "EndpointHealth",
    "CircuitBreaker",
    "CircuitBreakerState",
    "EndpointManager",
    "AllEndpointsFailedError",
    # WebSocket
    "WebSocketManager",
    "HybridMonitor",
    # Deduplication
    "EventDeduplicator",
    # Caching
    "CacheManager",
    "CacheEntry",
    "CacheTier",
    "TTLConfig",
    # Orchestration
    "BlockchainOrchestrator",
]
