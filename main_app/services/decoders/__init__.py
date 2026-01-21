"""
Multi-platform transaction decoders for NFT lending protocols.

Decoder versions (from realworld_nav_production_code_v1.ipynb):
- Gondi v1.7.1 (Cell 1285) - Multi-tranche, accrual grid, V2 continuation
- Blur v1.0.0 (Cell 1092) - Continuous compounding, retrospective accruals
- NFTfi v2.0.0 (Cell 1227) - Multi-version with refinance rollover
- Arcade v2.0.0 (Cell 1171) - Promissory note based
- Zharta v3.0.0 (Cell 1846) - All contract types (WETH Pool, USDC, P2P)
- Generic - WETH, ETH, ERC20, ERC721, Seaport, Gnosis Safe

Each decoder module exports:
- {Platform}EventDecoder: Decodes transaction receipts into events
- {Platform}JournalEntryGenerator: Generates GAAP-compliant journal entries

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
    # Interest accrual
    compute_continuous_interest,
    generate_daily_interest_accruals,
    WAD,
    SECONDS_PER_YEAR,
)

from .registry import DecoderRegistry

# Try to import adapter (may not exist)
try:
    from .adapter import LegacyRegistryAdapter
except ImportError:
    LegacyRegistryAdapter = None

# === GONDI v1.7.1 ===
try:
    from .gondi_decoder import (
        GondiEventDecoder,
        GondiJournalEntryGenerator,
        DecodedGondiEvent,
        Loan as GondiLoan,
        Tranche as GondiTranche,
        GondiEventType,
        GONDI_CONTRACTS,
        GONDI_CONTRACT_ADDRESSES,
    )
except ImportError as e:
    GondiEventDecoder = None
    GondiJournalEntryGenerator = None

# === BLUR v1.0.0 ===
try:
    from .blur_decoder import (
        BlurEventDecoder,
        BlurJournalEntryGenerator,
        DecodedBlurEvent,
        LienData as BlurLienData,
        BlurEventType,
        BlurAccounts,
        BLUR_BLEND_PROXY,
        BLUR_POOL,
    )
except ImportError as e:
    BlurEventDecoder = None
    BlurJournalEntryGenerator = None

# === NFTFI v2.0.0 ===
try:
    from .nftfi_decoder import (
        NFTfiEventDecoder,
        NFTfiJournalEntryGenerator,
        DecodedNFTfiEvent,
        NFTfiEventType,
        RefinancingContext,
        ALL_NFTFI_LOAN_CONTRACTS,
        NFTFI_REFINANCING_CONTRACT,
    )
except ImportError as e:
    NFTfiEventDecoder = None
    NFTfiJournalEntryGenerator = None

# === ARCADE v2.0.0 ===
try:
    from .arcade_decoder import (
        ArcadeEventDecoder,
        ArcadeJournalEntryGenerator,
        DecodedArcadeEvent,
        ArcadeEventType,
        LoanState as ArcadeLoanState,
        ARCADE_ORIGINATION_CONTROLLER,
        ARCADE_REPAYMENT_CONTROLLER,
    )
except ImportError as e:
    ArcadeEventDecoder = None
    ArcadeJournalEntryGenerator = None

# === ZHARTA v3.0.0 ===
try:
    from .zharta_decoder import (
        ZhartaDecoder,
        ZhartaJournalGenerator,
        ZhartaEvent,
        ZhartaEventType,
        ZHARTA_CONTRACTS,
    )
except ImportError as e:
    ZhartaDecoder = None
    ZhartaJournalGenerator = None

# === GENERIC ===
try:
    from .generic_decoder import GenericDecoder
except ImportError:
    GenericDecoder = None

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
    # Gondi v1.7.1
    'GondiEventDecoder',
    'GondiJournalEntryGenerator',
    'DecodedGondiEvent',
    'GondiLoan',
    'GondiTranche',
    'GondiEventType',
    'GONDI_CONTRACTS',
    'GONDI_CONTRACT_ADDRESSES',
    # Blur v1.0.0
    'BlurEventDecoder',
    'BlurJournalEntryGenerator',
    'DecodedBlurEvent',
    'BlurLienData',
    'BlurEventType',
    'BlurAccounts',
    'BLUR_BLEND_PROXY',
    'BLUR_POOL',
    # NFTfi v2.0.0
    'NFTfiEventDecoder',
    'NFTfiJournalEntryGenerator',
    'DecodedNFTfiEvent',
    'NFTfiEventType',
    'RefinancingContext',
    'ALL_NFTFI_LOAN_CONTRACTS',
    'NFTFI_REFINANCING_CONTRACT',
    # Arcade v2.0.0
    'ArcadeEventDecoder',
    'ArcadeJournalEntryGenerator',
    'DecodedArcadeEvent',
    'ArcadeEventType',
    'ArcadeLoanState',
    'ARCADE_ORIGINATION_CONTROLLER',
    'ARCADE_REPAYMENT_CONTROLLER',
    # Zharta v3.0.0
    'ZhartaDecoder',
    'ZhartaJournalGenerator',
    'ZhartaEvent',
    'ZhartaEventType',
    'ZHARTA_CONTRACTS',
    # Generic
    'GenericDecoder',
    # Helpers
    'wei_to_eth',
    'eth_to_wei',
    'format_address',
    'calculate_gas_fee',
    # Interest accrual
    'compute_continuous_interest',
    'generate_daily_interest_accruals',
    'WAD',
    'SECONDS_PER_YEAR',
]
