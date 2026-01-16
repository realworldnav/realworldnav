"""
Decoder Registry - Central routing for multi-platform transaction decoding.

Routes transactions to appropriate decoder based on:
1. Contract address mapping
2. Function selector detection
3. Event log analysis
"""

from web3 import Web3
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Any, Tuple
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

logger = logging.getLogger(__name__)


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

    def __init__(self, w3: Web3, fund_wallets: List[str], fund_id: str = ""):
        """
        Initialize decoder registry.

        Args:
            w3: Web3 instance connected to Ethereum
            fund_wallets: List of wallet addresses to track
            fund_id: Fund identifier for GL posting
        """
        self.w3 = w3
        self.fund_wallets = [w.lower() for w in fund_wallets]
        self.fund_id = fund_id
        self.decoders: Dict[Platform, BaseDecoder] = {}
        self.decoded_cache: Dict[str, DecodedTransaction] = {}
        self._initialize_decoders()

    def _initialize_decoders(self):
        """Initialize all platform decoders (lazy loading)"""
        # Import decoders here to avoid circular imports
        # Each decoder is initialized only when first needed
        self._decoder_classes = {}

        try:
            from .blur_decoder import BlurDecoder
            self._decoder_classes[Platform.BLUR] = BlurDecoder
        except ImportError as e:
            logger.warning(f"BlurDecoder not available: {e}")

        try:
            from .arcade_decoder import ArcadeDecoder
            self._decoder_classes[Platform.ARCADE] = ArcadeDecoder
        except ImportError as e:
            logger.warning(f"ArcadeDecoder not available: {e}")

        try:
            from .nftfi_decoder import NFTfiDecoder
            self._decoder_classes[Platform.NFTFI] = NFTfiDecoder
        except ImportError as e:
            logger.warning(f"NFTfiDecoder not available: {e}")

        try:
            from .gondi_decoder import GondiDecoder
            self._decoder_classes[Platform.GONDI] = GondiDecoder
        except ImportError as e:
            logger.warning(f"GondiDecoder not available: {e}")

        try:
            from .zharta_decoder import ZhartaDecoder
            self._decoder_classes[Platform.ZHARTA] = ZhartaDecoder
        except ImportError as e:
            logger.warning(f"ZhartaDecoder not available: {e}")

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

    def route_transaction(self, tx: Dict, receipt: Dict) -> Platform:
        """
        Determine which decoder to use for a transaction.

        Routing logic:
        1. Check direct contract address mapping
        2. Check function selector
        3. Try each decoder's can_decode method
        4. Default to GENERIC

        Args:
            tx: Transaction data
            receipt: Transaction receipt

        Returns:
            Platform enum for the appropriate decoder
        """
        to_address = (tx.get('to') or '').lower()

        # 1. Direct contract address mapping
        if to_address in CONTRACT_ROUTING:
            return CONTRACT_ROUTING[to_address]

        # 2. Check function selector from input data
        input_data = tx.get('input', '0x')
        if isinstance(input_data, bytes):
            input_data = input_data.hex()
        if len(input_data) >= 10:
            selector = input_data[:10].lower()
            if selector in FUNCTION_SELECTORS:
                return FUNCTION_SELECTORS[selector][0]

        # 3. Check logs for known event topics
        # Collect all matching platforms, prefer specific over GENERIC
        found_platforms = set()
        for log in receipt.get('logs', []):
            log_address = log.get('address', '').lower()
            if log_address in CONTRACT_ROUTING:
                found_platforms.add(CONTRACT_ROUTING[log_address])

        # Return first specific platform found, or GENERIC if only GENERIC found
        for platform in found_platforms:
            if platform != Platform.GENERIC:
                return platform
        if Platform.GENERIC in found_platforms:
            return Platform.GENERIC

        # 4. Try each decoder's can_decode method
        for platform in [Platform.BLUR, Platform.ARCADE, Platform.NFTFI, Platform.GONDI, Platform.ZHARTA]:
            decoder = self._get_decoder(platform)
            if decoder and decoder.can_decode(tx, receipt):
                return platform

        # 5. Default to generic decoder
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

    def decode_transaction(self, tx_hash: str) -> DecodedTransaction:
        """
        Main entry point for decoding a transaction.

        Args:
            tx_hash: Transaction hash to decode

        Returns:
            DecodedTransaction with events, journal entries, and metadata
        """
        # Check cache first
        if tx_hash in self.decoded_cache:
            return self.decoded_cache[tx_hash]

        try:
            # Fetch transaction data
            tx = self.w3.eth.get_transaction(tx_hash)
            receipt = self.w3.eth.get_transaction_receipt(tx_hash)
            block = self.w3.eth.get_block(tx.blockNumber)
            eth_price = self._get_eth_price_at_block(tx.blockNumber)

            # Route to appropriate decoder
            platform = self.route_transaction(dict(tx), dict(receipt))
            decoder = self._get_decoder(platform)

            if decoder:
                result = decoder.decode(dict(tx), dict(receipt), dict(block), eth_price)
            else:
                # No decoder available, create basic result
                result = self._create_basic_result(tx, receipt, block, eth_price)

            # Set fund_id on all journal entries for GL posting
            if self.fund_id:
                for entry in result.journal_entries:
                    entry.fund_id = self.fund_id

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
            tx_hash=tx.get('hash', b'').hex() if isinstance(tx.get('hash'), bytes) else str(tx.get('hash', '')),
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

    @property
    def stats(self) -> Dict[str, Any]:
        """Get registry statistics"""
        transactions = list(self.decoded_cache.values())
        success_count = sum(1 for t in transactions if t.status == "success")
        error_count = sum(1 for t in transactions if t.status == "error")

        platform_counts = {}
        for t in transactions:
            platform_counts[t.platform.value] = platform_counts.get(t.platform.value, 0) + 1

        return {
            "total_decoded": len(transactions),
            "success_count": success_count,
            "error_count": error_count,
            "auto_post_ready": len(self.get_auto_post_ready()),
            "review_queue": len(self.get_review_queue()),
            "platforms": platform_counts,
            "decoders_loaded": list(self.decoders.keys()),
        }
