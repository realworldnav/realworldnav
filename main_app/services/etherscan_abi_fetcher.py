"""
Etherscan ABI Fetcher - Auto-fetch contract ABIs from Etherscan API.

This module provides automatic ABI fetching from Etherscan when ABIs are not found
in the local embedded ABIs or S3 storage. Fetched ABIs can be cached to S3 for
future use.

Usage:
    from main_app.services.etherscan_abi_fetcher import fetch_abi_from_etherscan

    abi = fetch_abi_from_etherscan("0x81b2f8fc75bab64a6b144aa6d2faa127b4fa7fd9")
    if abi:
        print(f"Fetched ABI with {len(abi)} items")
"""

import json
import logging
import os
import time
import requests
from typing import Optional, List

logger = logging.getLogger(__name__)

# Rate limiting: Etherscan allows ~5 calls/second for free tier
ETHERSCAN_RATE_LIMIT_DELAY = 0.25  # 250ms between calls


class EtherscanABIFetcher:
    """
    Fetches contract ABIs from Etherscan API V2.

    Features:
    - Rate limiting to avoid hitting API limits
    - Handles proxy contracts (uses getsourcecode to get implementation ABI)
    - Returns None for unverified contracts
    - Detailed logging for debugging
    """

    def __init__(self, api_key: str = None):
        """
        Initialize the Etherscan ABI fetcher.

        Args:
            api_key: Etherscan API key. If not provided, reads from environment
                     or blockchain_config.
        """
        if api_key:
            self.api_key = api_key
        else:
            # Try to get from config, fall back to environment
            try:
                from ..config.blockchain_config import ETHERSCAN_API_KEY
                self.api_key = ETHERSCAN_API_KEY
            except ImportError:
                self.api_key = os.getenv('ETHERSCAN_API_KEY', '')

        self.base_url = "https://api.etherscan.io/v2/api"
        self._last_call_time = 0

        if not self.api_key:
            logger.warning("EtherscanABIFetcher initialized without API key")

    def _rate_limit(self):
        """Enforce rate limiting between API calls."""
        elapsed = time.time() - self._last_call_time
        if elapsed < ETHERSCAN_RATE_LIMIT_DELAY:
            time.sleep(ETHERSCAN_RATE_LIMIT_DELAY - elapsed)
        self._last_call_time = time.time()

    def fetch_abi(self, contract_address: str, chain_id: int = 1) -> Optional[List]:
        """
        Fetch contract ABI from Etherscan.

        Args:
            contract_address: Ethereum contract address (with or without 0x prefix)
            chain_id: Chain ID (1 for mainnet, default)

        Returns:
            ABI as a list of dicts if found and verified, None otherwise
        """
        self._rate_limit()

        # Normalize address
        addr = contract_address.lower()
        if not addr.startswith('0x'):
            addr = f'0x{addr}'

        try:
            params = {
                'chainid': chain_id,
                'module': 'contract',
                'action': 'getsourcecode',
                'address': addr,
                'apikey': self.api_key,
            }

            logger.debug(f"Fetching ABI from Etherscan for {addr}")
            response = requests.get(self.base_url, params=params, timeout=20)
            data = response.json()

            # Check API response status
            if data.get('status') != '1':
                message = data.get('message', 'Unknown error')
                result = data.get('result', '')
                logger.warning(f"Etherscan API error for {addr}: {message} - {result}")
                return None

            # Extract result
            result = data.get('result', [{}])
            if not result or not isinstance(result, list):
                logger.warning(f"Unexpected Etherscan response format for {addr}")
                return None

            contract_info = result[0]
            abi_str = contract_info.get('ABI', '')

            # Check if contract is verified
            if not abi_str or abi_str == 'Contract source code not verified':
                logger.warning(f"Contract not verified on Etherscan: {addr}")
                return None

            # Parse ABI JSON
            abi = json.loads(abi_str)

            # Log success with contract name if available
            contract_name = contract_info.get('ContractName', 'Unknown')
            logger.info(f"Fetched ABI from Etherscan for {addr} "
                       f"({contract_name}, {len(abi)} items)")

            return abi

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse ABI JSON for {addr}: {e}")
            return None
        except requests.Timeout:
            logger.error(f"Timeout fetching ABI for {addr} (20s)")
            return None
        except requests.RequestException as e:
            logger.error(f"Network error fetching ABI for {addr}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching ABI for {addr}: {e}")
            return None

    def fetch_abi_for_proxy(self, proxy_address: str, chain_id: int = 1) -> Optional[List]:
        """
        Fetch ABI for a proxy contract by resolving its implementation.

        This uses Etherscan's getsourcecode which returns the implementation
        address for proxies, then fetches that implementation's ABI.

        Args:
            proxy_address: Proxy contract address
            chain_id: Chain ID

        Returns:
            Implementation ABI if found, None otherwise
        """
        self._rate_limit()

        addr = proxy_address.lower()
        if not addr.startswith('0x'):
            addr = f'0x{addr}'

        try:
            params = {
                'chainid': chain_id,
                'module': 'contract',
                'action': 'getsourcecode',
                'address': addr,
                'apikey': self.api_key,
            }

            response = requests.get(self.base_url, params=params, timeout=20)
            data = response.json()

            if data.get('status') != '1':
                return None

            result = data.get('result', [{}])[0]

            # Check if this is a proxy
            implementation = result.get('Implementation', '')
            if implementation and implementation != '':
                logger.info(f"Detected proxy {addr}, fetching implementation {implementation}")
                return self.fetch_abi(implementation, chain_id)

            # Not a proxy, return direct ABI
            abi_str = result.get('ABI', '')
            if abi_str and abi_str != 'Contract source code not verified':
                return json.loads(abi_str)

            return None

        except Exception as e:
            logger.error(f"Error fetching proxy ABI for {addr}: {e}")
            return None


# Singleton instance for convenience
_fetcher: Optional[EtherscanABIFetcher] = None


def get_etherscan_fetcher() -> EtherscanABIFetcher:
    """
    Get singleton EtherscanABIFetcher instance.

    Returns:
        Shared EtherscanABIFetcher instance
    """
    global _fetcher
    if _fetcher is None:
        _fetcher = EtherscanABIFetcher()
    return _fetcher


def fetch_abi_from_etherscan(contract_address: str, chain_id: int = 1,
                             resolve_proxy: bool = True) -> Optional[List]:
    """
    Convenience function to fetch ABI from Etherscan.

    Args:
        contract_address: Contract address to fetch ABI for
        chain_id: Chain ID (default: 1 for mainnet)
        resolve_proxy: If True, resolve proxy contracts to implementation ABI

    Returns:
        ABI list if found and verified, None otherwise
    """
    fetcher = get_etherscan_fetcher()

    if resolve_proxy:
        # Try proxy resolution first (handles both proxy and direct contracts)
        abi = fetcher.fetch_abi_for_proxy(contract_address, chain_id)
        if abi:
            return abi

    # Fallback to direct fetch
    return fetcher.fetch_abi(contract_address, chain_id)
