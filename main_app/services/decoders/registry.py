"""
Decoder Registry - Central routing for multi-platform transaction decoding.

Routes transactions to appropriate decoder based on:
1. Contract address mapping (with proxy resolution)
2. Function selector detection
3. Event log analysis

Proxy Detection:
- EIP-1967 implementation slot
- EIP-1967 beacon pattern
- EIP-1167 minimal proxy pattern
- Custom getImplementation() function
"""

from web3 import Web3
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Any, Tuple, TYPE_CHECKING
from functools import lru_cache
import logging

from .base import (
    BaseDecoder,
    DecodedTransaction,
    TransactionCategory,
    PostingStatus,
    Platform,
    wei_to_eth,
    calculate_gas_fee,
)

if TYPE_CHECKING:
    from ..decoder_fifo_integrator import DecoderFIFOIntegrator

logger = logging.getLogger(__name__)


def _normalize_tx_hash(tx: Dict) -> str:
    """Extract and normalize transaction hash to always include 0x prefix."""
    raw_hash = tx.get('hash', b'')
    if isinstance(raw_hash, bytes):
        hex_str = raw_hash.hex()
    else:
        hex_str = str(raw_hash)

    # Remove any existing 0x prefix, then add it back
    if hex_str.startswith('0x'):
        return hex_str
    return f"0x{hex_str}"


# ============================================================================
# PROXY DETECTION (from production notebook Cell 49)
# ============================================================================

def get_implementation_address(w3: Web3, proxy: str) -> Optional[str]:
    """
    Resolve a proxy contract to its implementation address.

    Tries multiple proxy detection patterns:
    1. EIP-1967 implementation slot
    2. EIP-1967 beacon pattern
    3. EIP-1167 minimal proxy pattern
    4. Custom getImplementation() function

    Args:
        w3: Web3 instance
        proxy: Proxy contract address

    Returns:
        Implementation address or None if not a proxy
    """
    try:
        proxy = w3.to_checksum_address(proxy)
    except Exception:
        return None

    # 1. Try EIP-1967 implementation slot
    # bytes32(uint256(keccak256('eip1967.proxy.implementation')) - 1)
    impl_slot = int("0x360894A13BA1A3210667C828492DB98DCA3E2076CC3735A920A3CA505D382BBC", 16)
    try:
        raw_impl = w3.eth.get_storage_at(proxy, impl_slot)
        impl = w3.to_checksum_address(raw_impl[-20:].hex())
        if impl != "0x0000000000000000000000000000000000000000":
            logger.debug(f"Proxy {proxy[:10]}... resolved via EIP-1967 to {impl[:10]}...")
            return impl.lower()
    except Exception as e:
        logger.debug(f"EIP-1967 implementation check failed: {e}")

    # 2. Check for EIP-1967 beacon pattern
    beacon_slot = int("0xa3f0ad74e5423aebfd80d3ef4346578335a9a72aeaee59ff6cb3582b35133d50", 16)
    try:
        raw_beacon = w3.eth.get_storage_at(proxy, beacon_slot)
        beacon = w3.to_checksum_address(raw_beacon[-20:].hex())
        if beacon != "0x0000000000000000000000000000000000000000":
            beacon_abi = [{
                "inputs": [],
                "name": "implementation",
                "outputs": [{"internalType": "address", "name": "", "type": "address"}],
                "stateMutability": "view",
                "type": "function"
            }]
            beacon_ct = w3.eth.contract(address=beacon, abi=beacon_abi)
            impl = beacon_ct.functions.implementation().call()
            logger.debug(f"Proxy {proxy[:10]}... resolved via beacon to {impl[:10]}...")
            return impl.lower()
    except Exception as e:
        logger.debug(f"EIP-1967 beacon check failed: {e}")

    # 3. Check for EIP-1167 minimal proxy pattern
    try:
        code = w3.eth.get_code(proxy).hex()
        # EIP-1167 format: 0x363d3d373d3d3d363d73<impl>5af43d82803e903d91602b57fd5bf3
        if code.startswith("0x363d3d373d3d3d363d73") and len(code) >= 86:
            impl_bytes = code[22:62]  # 20 bytes = 40 hex chars
            impl = w3.to_checksum_address("0x" + impl_bytes)
            logger.debug(f"Proxy {proxy[:10]}... resolved via EIP-1167 to {impl[:10]}...")
            return impl.lower()
    except Exception as e:
        logger.debug(f"EIP-1167 minimal proxy check failed: {e}")

    # 4. Try custom getImplementation() function
    try:
        contract = w3.eth.contract(address=proxy, abi=[{
            "inputs": [],
            "name": "getImplementation",
            "outputs": [{"internalType": "address", "name": "", "type": "address"}],
            "stateMutability": "view",
            "type": "function"
        }])
        impl = contract.functions.getImplementation().call()
        if impl != "0x0000000000000000000000000000000000000000":
            logger.debug(f"Proxy {proxy[:10]}... resolved via getImplementation() to {impl[:10]}...")
            return impl.lower()
    except Exception as e:
        logger.debug(f"Custom getImplementation() check failed: {e}")

    return None  # Not a proxy or couldn't resolve


@lru_cache(maxsize=256)
def resolve_proxy_cached(w3_id: int, address: str) -> str:
    """
    Cached proxy resolution to avoid repeated RPC calls.

    Args:
        w3_id: ID of Web3 instance (for cache key)
        address: Contract address to resolve

    Returns:
        Implementation address if proxy, otherwise original address
    """
    # Note: w3_id is passed for cache key differentiation
    # We access the actual w3 from the registry instance
    return address  # Placeholder - actual resolution done in registry


# ============================================================================
# CONTRACT ADDRESS ROUTING
# Based on production notebook Cell 128/1294
# ============================================================================

# Contract address to platform mapping (lowercase)
CONTRACT_ROUTING: Dict[str, Platform] = {
    # === BLUR ===
    "0x29469395eaf6f95920e59f858042f0e28d98a20b": Platform.BLUR,  # Blur Blend (Lending Proxy)
    "0x0000000000a39bb272e79075ade125fd351887ac": Platform.BLUR,  # Blur Pool
    "0x000000000000ad05ccc4f10045630fb830b95127": Platform.BLUR,  # Blur Marketplace

    # === GONDI (version-aware) ===
    # From production GONDI_CONTRACT_INFO
    "0xf41b389e0c1950dc0b16c9498eae77131cc08a56": Platform.GONDI,  # Gondi v1 (tranche-based)
    "0x478f6f994c6fb3cf3e444a489b3ad9edb8ccae16": Platform.GONDI,  # Gondi v2 (source-based)
    "0xf65b99ce6dc5f6c556172bcc0ff27d3665a7d9a8": Platform.GONDI,  # Gondi v3 (tranche-based)
    "0x59e0b87e3dcfb5d34c06c71c3fbf7f6b7d77a4ff": Platform.GONDI,  # MultiSourceLoan

    # === ARCADE ===
    "0x81b2f8fc75bab64a6b144aa6d2faa127b4fa7fd9": Platform.ARCADE,  # LoanCore v3 (Proxy)
    "0x6ddb57101a17854109c3b9feb80ae19662ea950f": Platform.ARCADE,  # LoanCore v3 (Implementation)
    "0x89bc08ba00f135d608bc335f6b33d7a9abcc98af": Platform.ARCADE,  # OriginationController
    "0xb39dab85fa05c381767ff992ccde4c94619993d4": Platform.ARCADE,  # RepaymentController (active)
    "0x349a026a43ffa8e2ab4c4e59fcaa93f87bd8ddee": Platform.ARCADE,  # Lender Note (aLN)
    "0x337104a4f06260ff327d6734c555a0f5d8f863aa": Platform.ARCADE,  # Borrower Note (aBN)

    # === NFTfi ===
    "0xf896527c49b44aab3cf22ae356fa3af8e331f280": Platform.NFTFI,  # DirectLoanFixedOffer V1
    "0x8252df1d8b29057d1afe3062bf5a64d503152bc8": Platform.NFTFI,  # DirectLoanFixedOffer V2
    "0xe52cec0e90115abeb3304baa36bc2655731f7934": Platform.NFTFI,  # DirectLoanCoordinator
    "0xd0a40eb7fcd530a13866b9e893e4a9e0d15d03eb": Platform.NFTFI,  # DirectLoanFixedOfferRedeploy
    "0x4bc5fa56f2931e7a37417fa55dda71e4b7c2f2a3": Platform.NFTFI,  # NFTfi Refinancing v2
    "0x1e0447b19bb6ecfdae1e4ae1694b0c3659614e4e": Platform.NFTFI,  # DirectLoanFixedCollectionOffer v2.3
    "0xd0c6e59b50c32530c627107f50acc71958c4341f": Platform.NFTFI,  # DirectLoanFixedCollectionOffer v2.3 (alternate)
    "0x9f10d706d789e4c76a1a6434cd1a9841c875c0a6": Platform.NFTFI,  # AssetOfferLoan V3
    "0xb6adec2acc851d30d5fb64f3137234bcdcbbad0d": Platform.NFTFI,  # CollectionOfferLoan V3

    # === ZHARTA (Peer-to-Pool NFT Lending) ===
    "0xb7c8c74ed765267b54f4c327f279d7e850725ef2": Platform.ZHARTA,  # Zharta Loans (main interface)
    "0x5be916cff5f07870e9aef205960e07d9e287ef27": Platform.ZHARTA,  # Zharta LoansCore (state storage)
    "0x6474ab1b56b47bc26ba8cb471d566b8cc528f308": Platform.ZHARTA,  # Zharta LendingPoolPeripheral
    "0x35b8545ae12d89cd4997d5485e2e68c857df24a8": Platform.ZHARTA,  # Zharta CollateralVaultPeripheral

    # === GENERIC ===
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": Platform.GENERIC,  # WETH
    "0x0000000000000068f116a894984e2db1123eb395": Platform.GENERIC,  # Seaport 1.6
}

# Gondi version info for specialized handling
GONDI_VERSION_INFO: Dict[str, Dict[str, str]] = {
    "0xf41b389e0c1950dc0b16c9498eae77131cc08a56": {"version": "v1", "type": "tranche"},
    "0x478f6f994c6fb3cf3e444a489b3ad9edb8ccae16": {"version": "v2", "type": "source"},
    "0xf65b99ce6dc5f6c556172bcc0ff27d3665a7d9a8": {"version": "v3", "type": "tranche"},
}

# Function selectors for routing when contract is unknown
# Based on production notebook Cell 128
FUNCTION_SELECTORS: Dict[str, Tuple[Platform, str]] = {
    # === WETH ===
    "0xd0e30db0": (Platform.GENERIC, "deposit"),      # WETH deposit()
    "0x2e1a7d4d": (Platform.GENERIC, "withdraw"),     # WETH withdraw(uint256)

    # === ERC20 ===
    "0xa9059cbb": (Platform.GENERIC, "transfer"),     # transfer(address,uint256)
    "0x23b872dd": (Platform.GENERIC, "transferFrom"), # transferFrom(address,address,uint256)
    "0x095ea7b3": (Platform.GENERIC, "approve"),      # approve(address,uint256)

    # === Gnosis Safe ===
    "0x6a761202": (Platform.GENERIC, "execTransaction"),  # execTransaction(...)

    # === NFT Marketplaces ===
    "0xfb0f3ee1": (Platform.GENERIC, "fulfillBasicOrder"),  # Seaport
    "0xe7acab24": (Platform.GENERIC, "fulfillAvailableAdvancedOrders"),  # Seaport
    "0xab834bab": (Platform.GENERIC, "atomicMatch_"),  # OpenSea Wyvern
    "0x0a0a5e48": (Platform.GENERIC, "execute"),  # NFT trade

    # === NFT Lending ===
    "0x3b1d21a2": (Platform.NFTFI, "initializeLoan"),
    "0x58e644b7": (Platform.NFTFI, "beginLoan"),
    "0x6d5f9e56": (Platform.NFTFI, "repayLoan"),
    "0x8c7a63ae": (Platform.NFTFI, "payBackLoan"),
    "0x766df841": (Platform.NFTFI, "liquidateOverdueLoan"),

    # === GONDI ===
    "0x65e03b9c": (Platform.GONDI, "refinanceFull"),  # Gondi refinanceFull (alternate selector)
    "0xc09c4e7e": (Platform.GONDI, "refinanceFull"),  # Gondi refinanceFull (production selector)

    # === ZHARTA ===
    "0x5a5cd02e": (Platform.ZHARTA, "reserveEth"),  # Create loan with ETH
    "0xc290d691": (Platform.ZHARTA, "pay"),  # Repay loan

    # === DEX ===
    "0x38ed1739": (Platform.GENERIC, "swapExactTokensForTokens"),
    "0x7ff36ab5": (Platform.GENERIC, "swapExactETHForTokens"),
    "0x18cbafe5": (Platform.GENERIC, "swapExactTokensForETH"),
    "0xc04b8d59": (Platform.GENERIC, "exactInput"),
    "0x414bf389": (Platform.GENERIC, "exactInputSingle"),
}


class DecoderRegistry:
    """
    Central registry for all platform decoders.
    Routes transactions to appropriate decoder based on contract address and function signature.
    """

    def __init__(self, w3: Web3, fund_wallets: List[str], fund_id: str = "",
                 fifo_integrator: Optional["DecoderFIFOIntegrator"] = None):
        """
        Initialize decoder registry.

        Args:
            w3: Web3 instance connected to Ethereum
            fund_wallets: List of wallet addresses to track
            fund_id: Fund identifier for GL posting
            fifo_integrator: Optional FIFO cost basis integrator for tracking acquisitions/disposals
        """
        self.w3 = w3
        self.fund_wallets = [w.lower() for w in fund_wallets]
        self.fund_id = fund_id
        self.fifo_integrator = fifo_integrator
        self.decoders: Dict[Platform, BaseDecoder] = {}
        self.decoded_cache: Dict[str, DecodedTransaction] = {}
        self._proxy_cache: Dict[str, str] = {}  # Cache for proxy -> implementation resolution
        self._initialize_decoders()

    def _initialize_decoders(self):
        """Initialize all platform decoders (lazy loading)"""
        # Import ADAPTER classes that wrap the notebook decoders
        # Adapters provide the BaseDecoder interface (w3, fund_wallets) -> can_decode(), decode()
        #
        # Notebook decoder versions (from realworld_nav_production_code_v1.ipynb):
        # - Gondi v1.7.1 (Cell 1285) - interest accrual grid, reversals, V2 continuation
        # - Blur v1.0.0 (Cell 1092) - continuous compounding, retrospective accruals
        # - Arcade v2.0.0 (Cell 1171) - promissory note based
        # - NFTfi v2.0.0 (Cell 1227) - multi-version with refinance rollover
        # - Zharta v3.0.0 (Cell 1846) - all contract types
        self._decoder_classes = {}

        # === BLUR (v1.0.0 via adapter) ===
        try:
            from .decoder_adapters import BlurDecoderAdapter
            self._decoder_classes[Platform.BLUR] = BlurDecoderAdapter
            logger.info("Loaded Blur adapter v1.0.0")
        except ImportError as e:
            logger.warning(f"BlurDecoderAdapter not available: {e}")

        # === ARCADE (v2.0.0 via adapter) ===
        try:
            from .decoder_adapters import ArcadeDecoderAdapter
            self._decoder_classes[Platform.ARCADE] = ArcadeDecoderAdapter
            logger.info("Loaded Arcade adapter v2.0.0")
        except ImportError as e:
            logger.warning(f"ArcadeDecoderAdapter not available: {e}")

        # === NFTFI (v2.0.0 via adapter) ===
        try:
            from .decoder_adapters import NFTfiDecoderAdapter
            self._decoder_classes[Platform.NFTFI] = NFTfiDecoderAdapter
            logger.info("Loaded NFTfi adapter v2.0.0")
        except ImportError as e:
            logger.warning(f"NFTfiDecoderAdapter not available: {e}")

        # === GONDI (v1.7.1 via adapter) ===
        try:
            from .decoder_adapters import GondiDecoderAdapter
            self._decoder_classes[Platform.GONDI] = GondiDecoderAdapter
            logger.info("Loaded Gondi adapter v1.7.1")
        except ImportError as e:
            logger.warning(f"GondiDecoderAdapter not available: {e}")

        # === ZHARTA (v3.0.0 via adapter) ===
        try:
            from .decoder_adapters import ZhartaDecoderAdapter
            self._decoder_classes[Platform.ZHARTA] = ZhartaDecoderAdapter
            logger.info("Loaded Zharta adapter v3.0.0")
        except ImportError as e:
            logger.warning(f"ZhartaDecoderAdapter not available: {e}")

        # === GENERIC ===
        try:
            from .generic_decoder import GenericDecoder
            self._decoder_classes[Platform.GENERIC] = GenericDecoder
        except ImportError as e:
            logger.warning(f"GenericDecoder not available: {e}")

        logger.info(f"Initialized decoder registry with {len(self._decoder_classes)} platform decoders")

    def _get_decoder(self, platform: Platform) -> Optional[BaseDecoder]:
        """Get or create decoder instance for platform (lazy initialization)"""
        if platform not in self.decoders:
            if platform in self._decoder_classes:
                try:
                    self.decoders[platform] = self._decoder_classes[platform](self.w3, self.fund_wallets)
                    logger.info(f"Initialized {platform.value} decoder")
                except Exception as e:
                    logger.error(f"Failed to initialize {platform.value} decoder: {e}")
                    return None
        return self.decoders.get(platform)

    def _resolve_address(self, address: str) -> str:
        """
        Resolve an address, checking if it's a proxy and returning the implementation.

        Uses caching to avoid repeated RPC calls.

        Args:
            address: Contract address to resolve

        Returns:
            Implementation address if proxy, otherwise original address
        """
        address = address.lower()

        # Check cache first
        if address in self._proxy_cache:
            return self._proxy_cache[address]

        # Try to resolve proxy
        impl = get_implementation_address(self.w3, address)
        if impl:
            self._proxy_cache[address] = impl
            logger.debug(f"Resolved proxy {address[:10]}... -> {impl[:10]}...")
            return impl

        # Not a proxy, cache the original
        self._proxy_cache[address] = address
        return address

    def route_transaction(self, tx: Dict, receipt: Dict) -> Platform:
        """
        Determine which decoder to use for a transaction.

        Routing logic:
        1. Check direct contract address mapping (with proxy resolution)
        2. Check function selector
        3. Check log addresses (with proxy resolution)
        4. Try each decoder's can_decode method
        5. Default to GENERIC

        Args:
            tx: Transaction data
            receipt: Transaction receipt

        Returns:
            Platform enum for the appropriate decoder
        """
        to_address = (tx.get('to') or '').lower()
        logger.debug(f"ROUTING: to_address={to_address[:20]}..." if to_address else "ROUTING: to_address=None")

        # 1. Direct contract address mapping
        if to_address in CONTRACT_ROUTING:
            logger.debug(f"  [1] DIRECT MATCH: {to_address[:16]}... -> {CONTRACT_ROUTING[to_address].value}")
            return CONTRACT_ROUTING[to_address]
        else:
            logger.debug(f"  [1] No direct contract match")

        # 1b. If not found, try resolving proxy to implementation
        if to_address:
            logger.debug(f"  [1b] Attempting proxy resolution...")
            impl_address = self._resolve_address(to_address)
            if impl_address != to_address:
                logger.debug(f"  [1b] Proxy resolved: {to_address[:16]}... -> {impl_address[:16]}...")
                if impl_address in CONTRACT_ROUTING:
                    logger.debug(f"  [1b] PROXY MATCH: -> {CONTRACT_ROUTING[impl_address].value}")
                    return CONTRACT_ROUTING[impl_address]
                else:
                    logger.debug(f"  [1b] Implementation not in CONTRACT_ROUTING")
            else:
                logger.debug(f"  [1b] Not a proxy (or resolution failed)")

        # 2. Check function selector from input data
        input_data = tx.get('input', '0x')
        if isinstance(input_data, bytes):
            input_data = input_data.hex()
        if len(input_data) >= 10:
            selector = input_data[:10].lower()
            logger.debug(f"  [2] Function selector: {selector}")
            if selector in FUNCTION_SELECTORS:
                platform, func_name = FUNCTION_SELECTORS[selector]
                logger.debug(f"  [2] SELECTOR MATCH: {selector} -> {platform.value} ({func_name})")
                return platform
            else:
                logger.debug(f"  [2] Selector not in FUNCTION_SELECTORS")
        else:
            logger.debug(f"  [2] No input data (empty or too short)")

        # 3. Check logs for known event topics
        # Collect all matching platforms, prefer specific over GENERIC
        found_platforms = set()
        logger.debug(f"  [3] Checking {len(receipt.get('logs', []))} logs...")
        for i, log in enumerate(receipt.get('logs', [])):
            log_address = log.get('address', '').lower()

            # Direct match
            if log_address in CONTRACT_ROUTING:
                found_platforms.add(CONTRACT_ROUTING[log_address])
                logger.debug(f"    Log[{i}]: {log_address[:16]}... -> {CONTRACT_ROUTING[log_address].value}")
            else:
                # Try proxy resolution for log addresses
                impl_address = self._resolve_address(log_address)
                if impl_address != log_address and impl_address in CONTRACT_ROUTING:
                    found_platforms.add(CONTRACT_ROUTING[impl_address])
                    logger.debug(f"    Log[{i}]: {log_address[:16]}... (proxy) -> {CONTRACT_ROUTING[impl_address].value}")

        if found_platforms:
            logger.debug(f"  [3] Found platforms from logs: {[p.value for p in found_platforms]}")

        # Return first specific platform found, or GENERIC if only GENERIC found
        for platform in found_platforms:
            if platform != Platform.GENERIC:
                logger.debug(f"  [3] LOG MATCH: -> {platform.value}")
                return platform
        if Platform.GENERIC in found_platforms:
            logger.debug(f"  [3] LOG MATCH: -> GENERIC")
            return Platform.GENERIC

        # 4. Try each decoder's can_decode method
        logger.debug(f"  [4] Trying decoder can_decode() methods...")
        for platform in [Platform.BLUR, Platform.ARCADE, Platform.NFTFI, Platform.GONDI, Platform.ZHARTA]:
            decoder = self._get_decoder(platform)
            if decoder and decoder.can_decode(tx, receipt):
                logger.debug(f"  [4] CAN_DECODE MATCH: {platform.value}")
                return platform

        # 5. Default to generic decoder
        logger.debug(f"  [5] DEFAULT: -> GENERIC")
        return Platform.GENERIC

    @lru_cache(maxsize=512)
    def _get_eth_price_at_block(self, block_number: int) -> Decimal:
        """Get ETH/USD price at specific block (cached)"""
        try:
            from ..blockchain_service import get_eth_usd_price
            return Decimal(str(get_eth_usd_price(block_number)))
        except ImportError:
            pass

        # Fallback: try Chainlink aggregator directly
        try:
            from ...config.blockchain_config import CHAINLINK_ETH_USD_FEED, CHAINLINK_AGGREGATOR_V3_ABI
            aggregator = self.w3.eth.contract(
                address=Web3.to_checksum_address(CHAINLINK_ETH_USD_FEED),
                abi=CHAINLINK_AGGREGATOR_V3_ABI
            )
            _, answer, *_ = aggregator.functions.latestRoundData().call(block_identifier=block_number)
            return Decimal(answer) / Decimal(1e8)
        except Exception as e:
            logger.warning(f"Failed to get ETH price at block {block_number}: {e}")
            return Decimal("3000")  # Default fallback

    def decode_transaction(self, tx_hash: str, skip_spam_check: bool = False) -> DecodedTransaction:
        """
        Main entry point for decoding a transaction.

        Args:
            tx_hash: Transaction hash to decode
            skip_spam_check: If True, skip spam detection (for known good txs)

        Returns:
            DecodedTransaction with events, journal entries, and metadata
        """
        logger.debug(f"{'='*60}")
        logger.debug(f"DECODING TX: {tx_hash}")
        logger.debug(f"{'='*60}")

        # Check cache first
        if tx_hash in self.decoded_cache:
            logger.debug(f"Cache HIT for {tx_hash[:16]}...")
            return self.decoded_cache[tx_hash]

        logger.debug("Cache MISS - fetching from chain")

        try:
            # Fetch transaction data
            tx = self.w3.eth.get_transaction(tx_hash)
            receipt = self.w3.eth.get_transaction_receipt(tx_hash)
            block = self.w3.eth.get_block(tx.blockNumber)
            eth_price = self._get_eth_price_at_block(tx.blockNumber)

            logger.debug(f"TX DETAILS:")
            logger.debug(f"  Block: {tx.blockNumber}")
            logger.debug(f"  From: {tx.get('from', 'N/A')}")
            logger.debug(f"  To: {tx.get('to', 'N/A')}")
            logger.debug(f"  Value: {wei_to_eth(tx.get('value', 0))} ETH")
            logger.debug(f"  Logs: {len(receipt.get('logs', []))}")
            logger.debug(f"  ETH Price: ${eth_price}")

            # === SPAM DETECTION ===
            # Check for phishing/spam transactions before processing
            if not skip_spam_check:
                try:
                    from .spam_filter import is_spam_transaction, SpamReason
                    is_spam, spam_result = is_spam_transaction(dict(tx), dict(receipt))

                    if is_spam:
                        logger.warning(
                            f"SPAM DETECTED: {tx_hash[:16]}... "
                            f"(confidence={spam_result.confidence:.0%}, "
                            f"reasons={[r.value for r in spam_result.reasons]}, "
                            f"events={spam_result.details.get('num_events', 0)})"
                        )
                        # Return a flagged result instead of processing
                        result = self._create_spam_result(
                            tx, receipt, block, eth_price, spam_result
                        )
                        self.decoded_cache[tx_hash] = result
                        return result
                except ImportError:
                    logger.debug("Spam filter not available, skipping check")

            # Route to appropriate decoder
            logger.debug(f"\nROUTING TRANSACTION...")
            platform = self.route_transaction(dict(tx), dict(receipt))
            logger.debug(f"ROUTED TO: {platform.value}")

            decoder = self._get_decoder(platform)

            if decoder:
                logger.debug(f"\nDECODING with {platform.value} decoder...")
                result = decoder.decode(dict(tx), dict(receipt), dict(block), eth_price)
                logger.debug(f"DECODE RESULT:")
                logger.debug(f"  Status: {result.status}")
                logger.debug(f"  Category: {result.category.value}")
                logger.debug(f"  Events: {len(result.events)}")
                logger.debug(f"  Journal Entries: {len(result.journal_entries)}")
                if result.journal_entries:
                    for i, je in enumerate(result.journal_entries):
                        logger.debug(f"    JE[{i}]: {je.description[:50]}...")
                        for entry in je.entries:
                            logger.debug(f"      {entry.get('type', '?')} {entry.get('account', '?')}: {entry.get('amount', 0)}")
            else:
                logger.debug(f"No decoder available for {platform.value}, using basic result")
                # No decoder available, create basic result
                result = self._create_basic_result(tx, receipt, block, eth_price)

            # Set fund_id on all journal entries for GL posting
            if self.fund_id:
                for entry in result.journal_entries:
                    entry.fund_id = self.fund_id

            # Process through FIFO integrator if available
            if self.fifo_integrator and result.status == "success" and result.journal_entries:
                try:
                    fifo_result = self.fifo_integrator.process_decoded_transaction(
                        result, eth_price_usd=eth_price
                    )
                    # Attach FIFO tracking data to result
                    result.raw_data['fifo_tracking'] = {
                        'acquisitions': len(fifo_result.acquisitions),
                        'disposals': len(fifo_result.disposals),
                        'total_gain_loss_usd': float(fifo_result.total_gain_loss_usd),
                        'errors': fifo_result.errors
                    }
                    # Replace journal entries with enriched versions
                    if fifo_result.journal_entries:
                        result.raw_data['fifo_enriched_entries'] = fifo_result.journal_entries
                    logger.debug(f"FIFO tracking: {len(fifo_result.acquisitions)} acquisitions, "
                                f"{len(fifo_result.disposals)} disposals, "
                                f"gain/loss: ${fifo_result.total_gain_loss_usd}")
                except Exception as e:
                    logger.warning(f"FIFO integration error for {tx_hash}: {e}")
                    result.raw_data['fifo_error'] = str(e)

            # Cache result
            self.decoded_cache[tx_hash] = result
            return result

        except Exception as e:
            logger.error(f"Error decoding transaction {tx_hash}: {e}")
            error_result = self._create_error_result(tx_hash, str(e))
            self.decoded_cache[tx_hash] = error_result
            return error_result

    def _create_basic_result(self, tx: Dict, receipt: Dict, block: Dict, eth_price: Decimal) -> DecodedTransaction:
        """Create basic decoded result when no specific decoder is available"""
        timestamp = datetime.fromtimestamp(block.get('timestamp', 0), tz=timezone.utc)
        gas_fee = calculate_gas_fee(dict(receipt), dict(tx))

        return DecodedTransaction(
            status="success",
            tx_hash=_normalize_tx_hash(tx),
            platform=Platform.GENERIC,
            category=TransactionCategory.CONTRACT_CALL,
            block=tx.get('blockNumber', 0),
            timestamp=timestamp,
            eth_price=eth_price,
            gas_used=receipt.get('gasUsed', 0),
            gas_fee=gas_fee,
            from_address=tx.get('from', ''),
            to_address=tx.get('to', '') or '',
            value=wei_to_eth(tx.get('value', 0)),
            function_name="unknown",
            journal_entries=[],
            events=[],
            wallet_roles={},
            positions={},
            raw_data={'tx': dict(tx), 'receipt': dict(receipt)}
        )

    def _create_spam_result(self, tx: Dict, receipt: Dict, block: Dict,
                             eth_price: Decimal, spam_result) -> DecodedTransaction:
        """Create decoded result for spam/phishing transaction (not posted to GL)"""
        timestamp = datetime.fromtimestamp(block.get('timestamp', 0), tz=timezone.utc)
        gas_fee = calculate_gas_fee(dict(receipt), dict(tx))
        tx_hash = _normalize_tx_hash(tx)

        # Build spam details string
        reasons_str = ", ".join([r.value for r in spam_result.reasons])
        details = spam_result.details

        return DecodedTransaction(
            status="spam",  # Special status indicating filtered
            tx_hash=tx_hash,
            platform=Platform.UNKNOWN,
            category=TransactionCategory.SPAM,
            block=tx.get('blockNumber', 0),
            timestamp=timestamp,
            eth_price=eth_price,
            gas_used=receipt.get('gasUsed', 0),
            gas_fee=gas_fee,
            from_address=tx.get('from', ''),
            to_address=tx.get('to', '') or '',
            value=wei_to_eth(tx.get('value', 0)),
            function_name="spam_filtered",
            journal_entries=[],  # No journal entries for spam
            events=[],
            wallet_roles={},
            positions={},
            _posting_status_override=PostingStatus.SPAM_FILTERED,
            raw_data={
                'spam_detection': {
                    'confidence': spam_result.confidence,
                    'reasons': reasons_str,
                    'num_events': details.get('num_events', 0),
                    'details': details
                }
            }
        )

    def _create_error_result(self, tx_hash: str, error: str) -> DecodedTransaction:
        """Create error result for failed decoding"""
        return DecodedTransaction(
            status="error",
            tx_hash=tx_hash,
            platform=Platform.UNKNOWN,
            category=TransactionCategory.UNKNOWN,
            block=0,
            timestamp=datetime.now(timezone.utc),
            eth_price=Decimal(0),
            gas_used=0,
            gas_fee=Decimal(0),
            from_address="",
            to_address="",
            value=Decimal(0),
            function_name="",
            error=error
        )

    def get_decoded_transactions(self, status_filter: Optional[PostingStatus] = None,
                                  platform_filter: Optional[Platform] = None,
                                  category_filter: Optional[TransactionCategory] = None) -> List[DecodedTransaction]:
        """
        Get decoded transactions with optional filters.

        Args:
            status_filter: Filter by posting status
            platform_filter: Filter by platform
            category_filter: Filter by category

        Returns:
            List of matching DecodedTransaction objects
        """
        results = list(self.decoded_cache.values())

        if status_filter:
            results = [r for r in results if r.posting_status == status_filter]

        if platform_filter:
            results = [r for r in results if r.platform == platform_filter]

        if category_filter:
            results = [r for r in results if r.category == category_filter]

        # Sort by timestamp descending
        results.sort(key=lambda x: x.timestamp, reverse=True)

        return results

    def get_auto_post_ready(self) -> List[DecodedTransaction]:
        """Get transactions ready for auto-posting to GL"""
        return [
            tx for tx in self.decoded_cache.values()
            if tx.status == "success"
            and tx.posting_status == PostingStatus.AUTO_POST
            and tx.entries_balanced
        ]

    def get_review_queue(self) -> List[DecodedTransaction]:
        """Get transactions pending review"""
        return [
            tx for tx in self.decoded_cache.values()
            if tx.status == "success"
            and tx.posting_status == PostingStatus.REVIEW_QUEUE
        ]

    def clear_cache(self):
        """Clear the decoded transaction cache"""
        self.decoded_cache.clear()
        self._get_eth_price_at_block.cache_clear()

    def set_fifo_integrator(self, integrator: "DecoderFIFOIntegrator"):
        """Set or replace the FIFO integrator"""
        self.fifo_integrator = integrator
        logger.info("FIFO integrator attached to registry")

    def get_fifo_positions(self) -> Optional[Dict]:
        """Get current FIFO position summary if integrator is available"""
        if self.fifo_integrator:
            return self.fifo_integrator.get_position_summary()
        return None

    @property
    def stats(self) -> Dict[str, Any]:
        """Get registry statistics"""
        transactions = list(self.decoded_cache.values())
        success_count = sum(1 for t in transactions if t.status == "success")
        error_count = sum(1 for t in transactions if t.status == "error")

        platform_counts = {}
        for t in transactions:
            platform_counts[t.platform.value] = platform_counts.get(t.platform.value, 0) + 1

        stats = {
            "total_decoded": len(transactions),
            "success_count": success_count,
            "error_count": error_count,
            "auto_post_ready": len(self.get_auto_post_ready()),
            "review_queue": len(self.get_review_queue()),
            "platforms": platform_counts,
            "decoders_loaded": list(self.decoders.keys()),
            "fifo_enabled": self.fifo_integrator is not None,
        }

        # Add FIFO position summary if available
        if self.fifo_integrator:
            fifo_summary = self.fifo_integrator.get_position_summary()
            stats["fifo_positions_count"] = len(fifo_summary.get('positions', []))
            stats["fifo_total_cost_basis"] = fifo_summary.get('total_cost_basis_usd', 0)

        return stats
