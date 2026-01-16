"""
Embedded ABIs for multi-platform transaction decoders.

Hybrid approach:
1. Essential ABIs are embedded here for fast startup
2. S3 fallback for rare/new contracts via load_abi_from_s3()
"""

import logging
from typing import Dict, Optional, Any

logger = logging.getLogger(__name__)

# Import embedded ABIs
from .common import ERC20_ABI, ERC721_ABI, WETH_ABI

# Contract address to embedded ABI mapping
_EMBEDDED_ABIS: Dict[str, list] = {
    # WETH
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": WETH_ABI,
}


def get_embedded_abi(contract_address: str, platform: str = None) -> Optional[list]:
    """
    Get embedded ABI for a contract address.

    Args:
        contract_address: Ethereum contract address
        platform: Optional platform hint for selection

    Returns:
        ABI list if found, None otherwise
    """
    address = contract_address.lower()

    # Check direct mapping
    if address in _EMBEDDED_ABIS:
        return _EMBEDDED_ABIS[address]

    return None


def load_abi(contract_address: str, platform: str = None) -> Optional[list]:
    """
    Load ABI with hybrid strategy: embedded first, S3 fallback.

    Args:
        contract_address: Ethereum contract address
        platform: Optional platform hint

    Returns:
        ABI list if found, None otherwise
    """
    # 1. Try embedded first
    embedded = get_embedded_abi(contract_address, platform)
    if embedded:
        logger.debug(f"Using embedded ABI for {contract_address}")
        return embedded

    # 2. Try S3 fallback
    try:
        from ....s3_utils import load_abi_from_s3
        s3_abi = load_abi_from_s3(contract_address)
        if s3_abi:
            logger.info(f"Loaded ABI from S3 for {contract_address}")
            return s3_abi
    except ImportError:
        logger.warning("S3 utils not available for ABI fallback")
    except Exception as e:
        logger.warning(f"S3 ABI lookup failed for {contract_address}: {e}")

    return None


def register_embedded_abi(contract_address: str, abi: list):
    """
    Register an ABI for caching.

    Args:
        contract_address: Ethereum contract address
        abi: ABI list to register
    """
    _EMBEDDED_ABIS[contract_address.lower()] = abi


__all__ = [
    'get_embedded_abi',
    'load_abi',
    'register_embedded_abi',
    'ERC20_ABI',
    'ERC721_ABI',
    'WETH_ABI',
]
