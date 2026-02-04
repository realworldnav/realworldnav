"""
Function and event signature resolution using 4byte.directory.

Provides fallback signature lookup when contract ABI is unavailable,
enabling basic decoding of function calls and events.
"""

import logging
import requests
from functools import lru_cache
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


@dataclass
class SignatureMatch:
    """A matched function or event signature."""
    hex_signature: str      # 4-byte selector or 32-byte topic
    text_signature: str     # Human-readable signature (e.g., "transfer(address,uint256)")
    name: str               # Function/event name only (e.g., "transfer")
    parameters: List[str]   # Parameter types (e.g., ["address", "uint256"])


class SignatureResolver:
    """
    Resolves function selectors and event topics to human-readable signatures.

    Uses 4byte.directory API as primary source, with local caching.

    Usage:
        resolver = SignatureResolver()

        # Resolve function selector
        match = resolver.resolve_function_selector("0xa9059cbb")
        print(match.text_signature)  # "transfer(address,uint256)"

        # Resolve event topic
        match = resolver.resolve_event_topic("0xddf252ad...")
        print(match.name)  # "Transfer"
    """

    # 4byte.directory API endpoints
    FOURYTE_API = "https://www.4byte.directory/api/v1"
    FOURYTE_SIGNATURES_URL = f"{FOURYTE_API}/signatures/"
    FOURYTE_EVENT_SIGNATURES_URL = f"{FOURYTE_API}/event-signatures/"

    # Alternative: Sourcify 4byte API
    SOURCIFY_API = "https://api.4byte.sourcify.dev"

    # Request timeout
    TIMEOUT = 5  # seconds

    def __init__(self, use_sourcify_fallback: bool = True):
        """
        Initialize resolver.

        Args:
            use_sourcify_fallback: Try Sourcify API if 4byte.directory fails
        """
        self.use_sourcify_fallback = use_sourcify_fallback
        self._session = requests.Session()

        # Local cache for failed lookups (avoid repeated API calls)
        self._not_found_cache: set = set()

    @lru_cache(maxsize=10000)
    def resolve_function_selector(self, selector: str) -> Optional[SignatureMatch]:
        """
        Resolve a 4-byte function selector to its signature.

        Args:
            selector: 4-byte hex selector (e.g., "0xa9059cbb" or "a9059cbb")

        Returns:
            SignatureMatch if found, None otherwise
        """
        # Normalize selector
        selector = self._normalize_selector(selector, 4)
        if not selector:
            return None

        # Check not-found cache
        cache_key = f"func:{selector}"
        if cache_key in self._not_found_cache:
            return None

        # Try 4byte.directory
        match = self._query_4byte_functions(selector)
        if match:
            return match

        # Try Sourcify fallback
        if self.use_sourcify_fallback:
            match = self._query_sourcify(selector, is_event=False)
            if match:
                return match

        # Cache the miss
        self._not_found_cache.add(cache_key)
        return None

    @lru_cache(maxsize=10000)
    def resolve_event_topic(self, topic: str) -> Optional[SignatureMatch]:
        """
        Resolve a 32-byte event topic to its signature.

        Args:
            topic: 32-byte hex topic (e.g., "0xddf252ad...")

        Returns:
            SignatureMatch if found, None otherwise
        """
        # Normalize topic
        topic = self._normalize_selector(topic, 32)
        if not topic:
            return None

        # Check not-found cache
        cache_key = f"event:{topic}"
        if cache_key in self._not_found_cache:
            return None

        # Try 4byte.directory
        match = self._query_4byte_events(topic)
        if match:
            return match

        # Try Sourcify fallback
        if self.use_sourcify_fallback:
            match = self._query_sourcify(topic, is_event=True)
            if match:
                return match

        # Cache the miss
        self._not_found_cache.add(cache_key)
        return None

    def resolve_function_name(self, selector: str) -> Optional[str]:
        """
        Get just the function name from a selector.

        Args:
            selector: 4-byte hex selector

        Returns:
            Function name (e.g., "transfer") or None
        """
        match = self.resolve_function_selector(selector)
        return match.name if match else None

    def resolve_event_name(self, topic: str) -> Optional[str]:
        """
        Get just the event name from a topic.

        Args:
            topic: 32-byte hex topic

        Returns:
            Event name (e.g., "Transfer") or None
        """
        match = self.resolve_event_topic(topic)
        return match.name if match else None

    def batch_resolve_functions(
        self,
        selectors: List[str],
    ) -> Dict[str, Optional[SignatureMatch]]:
        """
        Resolve multiple function selectors.

        Args:
            selectors: List of 4-byte selectors

        Returns:
            Dict mapping selector -> SignatureMatch (or None if not found)
        """
        results = {}
        for selector in selectors:
            results[selector] = self.resolve_function_selector(selector)
        return results

    def batch_resolve_events(
        self,
        topics: List[str],
    ) -> Dict[str, Optional[SignatureMatch]]:
        """
        Resolve multiple event topics.

        Args:
            topics: List of 32-byte topics

        Returns:
            Dict mapping topic -> SignatureMatch (or None if not found)
        """
        results = {}
        for topic in topics:
            results[topic] = self.resolve_event_topic(topic)
        return results

    def _normalize_selector(self, selector: str, expected_bytes: int) -> Optional[str]:
        """Normalize selector to lowercase with 0x prefix."""
        if not selector:
            return None

        # Remove 0x prefix if present
        clean = selector.lower()
        if clean.startswith("0x"):
            clean = clean[2:]

        # Check length (4 bytes = 8 hex chars, 32 bytes = 64 hex chars)
        expected_chars = expected_bytes * 2
        if len(clean) < expected_chars:
            logger.warning(f"Selector too short: {selector}")
            return None

        # Take only the expected bytes (in case of padding)
        clean = clean[:expected_chars]

        return f"0x{clean}"

    def _query_4byte_functions(self, selector: str) -> Optional[SignatureMatch]:
        """Query 4byte.directory for function signatures."""
        try:
            response = self._session.get(
                self.FOURYTE_SIGNATURES_URL,
                params={"hex_signature": selector},
                timeout=self.TIMEOUT,
            )
            response.raise_for_status()

            data = response.json()
            results = data.get("results", [])

            if not results:
                return None

            # Return first (most popular) match
            text_sig = results[0].get("text_signature", "")
            return self._parse_signature(selector, text_sig)

        except requests.RequestException as e:
            logger.debug(f"4byte.directory function lookup failed: {e}")
            return None
        except Exception as e:
            logger.warning(f"Error parsing 4byte response: {e}")
            return None

    def _query_4byte_events(self, topic: str) -> Optional[SignatureMatch]:
        """Query 4byte.directory for event signatures."""
        try:
            response = self._session.get(
                self.FOURYTE_EVENT_SIGNATURES_URL,
                params={"hex_signature": topic},
                timeout=self.TIMEOUT,
            )
            response.raise_for_status()

            data = response.json()
            results = data.get("results", [])

            if not results:
                return None

            text_sig = results[0].get("text_signature", "")
            return self._parse_signature(topic, text_sig)

        except requests.RequestException as e:
            logger.debug(f"4byte.directory event lookup failed: {e}")
            return None
        except Exception as e:
            logger.warning(f"Error parsing 4byte event response: {e}")
            return None

    def _query_sourcify(
        self,
        selector: str,
        is_event: bool,
    ) -> Optional[SignatureMatch]:
        """Query Sourcify 4byte API as fallback."""
        try:
            endpoint = "event-signatures" if is_event else "signatures"
            url = f"{self.SOURCIFY_API}/{endpoint}/{selector}"

            response = self._session.get(url, timeout=self.TIMEOUT)
            response.raise_for_status()

            data = response.json()
            results = data.get("results", [])

            if not results:
                return None

            text_sig = results[0] if isinstance(results[0], str) else results[0].get("text", "")
            return self._parse_signature(selector, text_sig)

        except requests.RequestException as e:
            logger.debug(f"Sourcify lookup failed: {e}")
            return None
        except Exception as e:
            logger.warning(f"Error parsing Sourcify response: {e}")
            return None

    def _parse_signature(self, hex_sig: str, text_sig: str) -> Optional[SignatureMatch]:
        """Parse text signature into structured format."""
        if not text_sig:
            return None

        try:
            # Extract name and parameters
            # Format: "functionName(type1,type2,...)"
            paren_idx = text_sig.find("(")
            if paren_idx == -1:
                name = text_sig
                parameters = []
            else:
                name = text_sig[:paren_idx]
                params_str = text_sig[paren_idx + 1:-1]  # Remove ( and )
                parameters = [p.strip() for p in params_str.split(",")] if params_str else []

            return SignatureMatch(
                hex_signature=hex_sig,
                text_signature=text_sig,
                name=name,
                parameters=parameters,
            )

        except Exception as e:
            logger.warning(f"Error parsing signature '{text_sig}': {e}")
            return None

    def clear_cache(self) -> None:
        """Clear all caches."""
        self.resolve_function_selector.cache_clear()
        self.resolve_event_topic.cache_clear()
        self._not_found_cache.clear()

    def get_cache_stats(self) -> Dict:
        """Get cache statistics."""
        func_info = self.resolve_function_selector.cache_info()
        event_info = self.resolve_event_topic.cache_info()

        return {
            "function_cache": {
                "hits": func_info.hits,
                "misses": func_info.misses,
                "size": func_info.currsize,
                "maxsize": func_info.maxsize,
            },
            "event_cache": {
                "hits": event_info.hits,
                "misses": event_info.misses,
                "size": event_info.currsize,
                "maxsize": event_info.maxsize,
            },
            "not_found_cache_size": len(self._not_found_cache),
        }


# Well-known signatures for common functions (avoid API calls)
KNOWN_FUNCTION_SIGNATURES = {
    "0xa9059cbb": SignatureMatch("0xa9059cbb", "transfer(address,uint256)", "transfer", ["address", "uint256"]),
    "0x23b872dd": SignatureMatch("0x23b872dd", "transferFrom(address,address,uint256)", "transferFrom", ["address", "address", "uint256"]),
    "0x095ea7b3": SignatureMatch("0x095ea7b3", "approve(address,uint256)", "approve", ["address", "uint256"]),
    "0x70a08231": SignatureMatch("0x70a08231", "balanceOf(address)", "balanceOf", ["address"]),
    "0xdd62ed3e": SignatureMatch("0xdd62ed3e", "allowance(address,address)", "allowance", ["address", "address"]),
    "0x18160ddd": SignatureMatch("0x18160ddd", "totalSupply()", "totalSupply", []),
}

KNOWN_EVENT_SIGNATURES = {
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef": SignatureMatch(
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        "Transfer(address,address,uint256)",
        "Transfer",
        ["address", "address", "uint256"]
    ),
    "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925": SignatureMatch(
        "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
        "Approval(address,address,uint256)",
        "Approval",
        ["address", "address", "uint256"]
    ),
}


class EnhancedSignatureResolver(SignatureResolver):
    """
    SignatureResolver with local known signatures for common functions.

    Checks local cache first before hitting API.
    """

    def resolve_function_selector(self, selector: str) -> Optional[SignatureMatch]:
        """Check known signatures first, then API."""
        normalized = self._normalize_selector(selector, 4)
        if normalized and normalized in KNOWN_FUNCTION_SIGNATURES:
            return KNOWN_FUNCTION_SIGNATURES[normalized]

        return super().resolve_function_selector(selector)

    def resolve_event_topic(self, topic: str) -> Optional[SignatureMatch]:
        """Check known signatures first, then API."""
        normalized = self._normalize_selector(topic, 32)
        if normalized and normalized in KNOWN_EVENT_SIGNATURES:
            return KNOWN_EVENT_SIGNATURES[normalized]

        return super().resolve_event_topic(topic)
