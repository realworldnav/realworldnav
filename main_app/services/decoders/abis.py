"""
ABI Loading Module for Transaction Decoders

Loads contract ABIs from S3 with caching.
ABIs are stored at: s3://realworldnav-beta-1/drip_capital/smart_contract_ABIs/
"""

import logging
from typing import Optional, Dict
from functools import lru_cache

logger = logging.getLogger(__name__)


@lru_cache(maxsize=128)
def load_abi(contract_address: str, platform: str = None) -> Optional[Dict]:
    """
    Load contract ABI from S3.

    Args:
        contract_address: Ethereum contract address (checksummed or lowercase)
        platform: Optional platform name for logging

    Returns:
        Contract ABI as dict, or None if not found
    """
    try:
        # Import here to avoid circular imports
        from ...s3_utils import load_abi_from_s3

        abi = load_abi_from_s3(contract_address)

        if abi:
            logger.info(f"Loaded ABI for {contract_address[:10]}... ({platform or 'unknown'})")
            return abi
        else:
            logger.warning(f"ABI not found for {contract_address} ({platform or 'unknown'})")
            return None

    except ImportError as e:
        logger.error(f"Could not import S3 utils: {e}")
        return None
    except Exception as e:
        logger.error(f"Error loading ABI for {contract_address}: {e}")
        return None


def list_available_abis() -> list:
    """List all available ABIs in S3"""
    try:
        from ...s3_utils import list_available_abis as s3_list_abis
        return s3_list_abis()
    except ImportError:
        logger.error("Could not import S3 utils")
        return []
    except Exception as e:
        logger.error(f"Error listing ABIs: {e}")
        return []


# Known ABI mappings (address -> friendly name in S3)
# These map contract addresses to their ABI file names
ABI_NAME_MAPPING = {
    # Blur
    "0x29469395eaf6f95920e59f858042f0e28d98a20b": "blur lending",
    "0x0000000000a39bb272e79075ade125fd351887ac": "blur pool",

    # Gondi
    "0xf41b389e0c1950dc0b16c9498eae77131cc08a56": None,  # Has address-based file
    "0x478f6f994c6fb3cf3e444a489b3ad9edb8ccae16": None,  # Has address-based file
    "0xf65b99ce6dc5f6c556172bcc0ff27d3665a7d9a8": None,  # Has address-based file

    # WETH
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": "weth",
}
