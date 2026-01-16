"""
Multi-platform transaction decoders for NFT lending protocols.

Supports:
- Gondi (Pool-based NFT lending) - COMPLETE
- Blur Blend (NFT lending with callable loans) - COMPLETE
- NFTfi (Multi-version NFT loans V1/V2/Coordinator) - COMPLETE
- Arcade (P2P NFT loans) - COMPLETE
- Zharta (Peer-to-pool NFT lending) - COMPLETE
- Generic (WETH, ETH, ERC20, ERC721, Seaport, Gnosis Safe) - COMPLETE

Development workflow:
1. Test/debug transactions in debug_decoder.py
2. Once working, extract logic into platform-specific decoder
3. Add decoder to this module
"""

from .base import (
    # Enums
    TransactionCategory,
    PostingStatus,
    TaxTreatment,
    Platform,
    # Dataclasses
    DecodedEvent,
    LoanPosition,
    JournalEntry,
    DecodedTransaction,
    # Base class
    BaseDecoder,
    # Helpers
    wei_to_eth,
    eth_to_wei,
    format_address,
    calculate_gas_fee,
)

from .registry import DecoderRegistry
from .adapter import LegacyRegistryAdapter

# Platform-specific decoders
from .gondi_decoder import GondiDecoder
from .blur_decoder import BlurDecoder
from .nftfi_decoder import NFTfiDecoder
from .arcade_decoder import ArcadeDecoder
from .zharta_decoder import ZhartaDecoder
from .generic_decoder import GenericDecoder

__all__ = [
    # Enums
    'TransactionCategory',
    'PostingStatus',
    'TaxTreatment',
    'Platform',
    # Dataclasses
    'DecodedEvent',
    'LoanPosition',
    'JournalEntry',
    'DecodedTransaction',
    # Base classes
    'BaseDecoder',
    'DecoderRegistry',
    'LegacyRegistryAdapter',
    # Platform decoders
    'GondiDecoder',
    'BlurDecoder',
    'NFTfiDecoder',
    'ArcadeDecoder',
    'ZhartaDecoder',
    'GenericDecoder',
    # Helpers
    'wei_to_eth',
    'eth_to_wei',
    'format_address',
    'calculate_gas_fee',
]
