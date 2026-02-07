"""
Base classes and data structures for multi-platform transaction decoders.
Provides unified interfaces for Blur, Arcade, NFTfi, Gondi, Zharta, and generic decoders.
"""

from web3 import Web3
from datetime import datetime, timezone
from decimal import Decimal, getcontext
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
from collections import defaultdict
from functools import lru_cache
from abc import ABC, abstractmethod
import math
import logging

# Set decimal precision for financial calculations
getcontext().prec = 28

logger = logging.getLogger(__name__)

# ============================================================================
# CONSTANTS FOR INTEREST CALCULATIONS
# ============================================================================

WAD = Decimal(10**18)  # Wei per ETH
SECONDS_PER_YEAR = Decimal(365 * 24 * 3600)  # 31,536,000 seconds

# Assets treated as equivalent for journal entry balance validation
# WETH and ETH are economically equivalent (1:1 wrap/unwrap)
ETH_EQUIVALENT_ASSETS = {'ETH', 'WETH'}


# ============================================================================
# ENUMS
# ============================================================================

class TransactionCategory(Enum):
    """Transaction categories for routing and GL posting"""
    # Lending events
    LOAN_ORIGINATION = "LOAN_ORIGINATION"
    LOAN_REPAYMENT = "LOAN_REPAYMENT"
    LOAN_REFINANCE = "LOAN_REFINANCE"
    LOAN_AUCTION = "LOAN_AUCTION"
    COLLATERAL_SEIZURE = "COLLATERAL_SEIZURE"
    LOAN_LIQUIDATION = "LOAN_LIQUIDATION"
    LOAN_EXTENSION = "LOAN_EXTENSION"
    INTEREST_ACCRUAL = "INTEREST_ACCRUAL"

    # Transfer events
    ETH_TRANSFER = "ETH_TRANSFER"
    ERC20_TRANSFER = "ERC20_TRANSFER"
    NFT_TRANSFER = "NFT_TRANSFER"
    WETH_WRAP = "WETH_WRAP"
    WETH_UNWRAP = "WETH_UNWRAP"

    # DeFi events
    SEAPORT_TRADE = "SEAPORT_TRADE"
    GNOSIS_SAFE = "GNOSIS_SAFE"

    # Generic
    CONTRACT_CALL = "CONTRACT_CALL"
    UNKNOWN = "UNKNOWN"

    # Spam/Phishing (filtered out, do not post)
    SPAM = "SPAM"


class PostingStatus(Enum):
    """Journal entry posting status for hybrid GL workflow"""
    AUTO_POST = "auto_post"       # Auto-post to GL (known transaction types)
    REVIEW_QUEUE = "review_queue" # Queue for manual review
    POSTED = "posted"             # Already posted to GL
    REJECTED = "rejected"         # Rejected by user
    SPAM_FILTERED = "spam_filtered"  # Filtered as spam/phishing


class TaxTreatment(Enum):
    """Tax treatment classifications"""
    NON_TAXABLE = "NON_TAXABLE"
    TAXABLE_INCOME = "TAXABLE_INCOME"
    CAPITAL_GAIN = "CAPITAL_GAIN"
    CAPITAL_LOSS = "CAPITAL_LOSS"
    DEDUCTIBLE_EXPENSE = "DEDUCTIBLE_EXPENSE"


class Platform(Enum):
    """Supported decoding platforms"""
    BLUR = "blur"
    ARCADE = "arcade"
    NFTFI = "nftfi"
    GONDI = "gondi"
    ZHARTA = "zharta"
    GENERIC = "generic"
    UNKNOWN = "unknown"


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class DecodedEvent:
    """Decoded blockchain event from transaction receipt"""
    name: str
    args: Dict[str, Any]
    log_index: int
    contract_address: str
    topic: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'args': {k: str(v) if isinstance(v, (bytes, int)) else v for k, v in self.args.items()},
            'log_index': self.log_index,
            'contract_address': self.contract_address,
            'topic': self.topic
        }


@dataclass
class LoanPosition:
    """NFT-collateralized loan position with continuous compound interest"""
    lien_id: int
    lender: str
    borrower: str
    collection: str
    token_id: int
    principal: Decimal
    rate: Decimal  # Annual rate in basis points
    start_time: datetime
    duration: int  # seconds
    auction_duration: int = 0
    status: str = "ACTIVE"
    platform: Platform = Platform.UNKNOWN

    def calculate_interest(self, as_of: datetime) -> Decimal:
        """Calculate accrued interest using continuous compounding"""
        time_elapsed_seconds = Decimal((as_of - self.start_time).total_seconds())
        time_elapsed_seconds = max(time_elapsed_seconds, Decimal(1))
        seconds_per_year = Decimal(365 * 24 * 3600)
        time_in_years = time_elapsed_seconds / seconds_per_year
        rate_decimal = self.rate / Decimal(10000)
        exponent = float(rate_decimal * time_in_years)
        compound_factor = Decimal(str(math.exp(exponent)))
        total_debt = self.principal * compound_factor
        return total_debt - self.principal

    def total_due(self, as_of: datetime) -> Decimal:
        """Calculate total amount due"""
        return self.principal + self.calculate_interest(as_of)

    def to_dict(self) -> dict:
        return {
            'lien_id': self.lien_id,
            'lender': self.lender,
            'borrower': self.borrower,
            'collection': self.collection,
            'token_id': self.token_id,
            'principal': float(self.principal),
            'rate': float(self.rate),
            'start_time': self.start_time.isoformat(),
            'duration': self.duration,
            'auction_duration': self.auction_duration,
            'status': self.status,
            'platform': self.platform.value
        }


@dataclass
class JournalEntry:
    """
    Double-entry bookkeeping journal entry.

    GL-compatible fields for save_GL_file():
    - transaction_id, date, transaction_type, wallet_id, cryptocurrency,
    - account_name, debit_crypto, credit_crypto, eth_usd_price, debit_USD, credit_USD, hash
    """
    entry_id: str
    date: datetime
    description: str
    tx_hash: str
    category: TransactionCategory
    platform: Platform
    entries: List[Dict[str, Any]] = field(default_factory=list)
    tax_implications: List[Dict[str, Any]] = field(default_factory=list)
    wallet_address: str = ""
    wallet_role: str = ""
    fund_id: str = ""  # Added for GL compatibility
    posting_status: PostingStatus = PostingStatus.AUTO_POST
    eth_usd_price: Decimal = Decimal(0)

    def add_debit(self, account: str, amount: Decimal, asset: str = "ETH"):
        self.entries.append({
            "type": "DEBIT",
            "account": account,
            "amount": float(amount),
            "asset": asset
        })

    def add_credit(self, account: str, amount: Decimal, asset: str = "ETH"):
        self.entries.append({
            "type": "CREDIT",
            "account": account,
            "amount": float(amount),
            "asset": asset
        })

    def add_tax_implication(self, treatment: TaxTreatment, amount: Decimal, description: str):
        self.tax_implications.append({
            "treatment": treatment.value,
            "amount": float(amount),
            "description": description
        })

    def validate(self) -> bool:
        """
        Ensure debits equal credits. WETH and ETH are treated as equivalent.

        Returns:
            True if entry is balanced, False otherwise
        """
        if not self.entries:
            logger.warning(f"Journal entry {self.entry_id} has no entries")
            return False

        balances = defaultdict(lambda: {"debits": Decimal(0), "credits": Decimal(0)})

        for entry in self.entries:
            # Validate entry structure
            if not all(k in entry for k in ["asset", "amount", "type"]):
                logger.error(f"Invalid entry structure in {self.entry_id}: {entry}")
                return False

            asset = entry["asset"]
            # Normalize ETH-equivalent assets for balancing (WETH <-> ETH wraps)
            balance_key = "ETH" if asset in ETH_EQUIVALENT_ASSETS else asset

            amount = Decimal(str(entry["amount"]))
            if entry["type"] == "DEBIT":
                balances[balance_key]["debits"] += amount
            else:
                balances[balance_key]["credits"] += amount

        # Check balance with small tolerance for floating point
        tolerance = Decimal("0.000001")  # ~$0.003 at $3000/ETH
        for asset, totals in balances.items():
            diff = abs(totals["debits"] - totals["credits"])
            if diff > tolerance:
                logger.error(f"Imbalanced {asset} in {self.entry_id}: "
                           f"debits={totals['debits']}, credits={totals['credits']}, diff={diff}")
                return False
        return True

    def to_gl_records(self, wallet_to_fund_map: Dict[str, str] = None, coa_map: Dict[str, tuple] = None) -> List[Dict[str, Any]]:
        """
        Convert to GL-compatible records matching parquet schema exactly.

        Args:
            wallet_to_fund_map: Dict mapping wallet_address (lowercase) to fund_id
            coa_map: Dict mapping account_name to (GL_Acct_Number, GL_Acct_Name)

        Returns:
            List of dicts matching the parquet schema columns
        """
        records = []

        # Determine fund_id from wallet mapping if available
        fund_id = self.fund_id
        if not fund_id and wallet_to_fund_map and self.wallet_address:
            fund_id = wallet_to_fund_map.get(self.wallet_address.lower(), '')

        for entry in self.entries:
            amount = Decimal(str(entry["amount"]))
            usd_amount = amount * self.eth_usd_price if entry["asset"] in ("ETH", "WETH") else amount

            debit_crypto = amount if entry["type"] == "DEBIT" else Decimal(0)
            credit_crypto = amount if entry["type"] == "CREDIT" else Decimal(0)
            debit_usd = float(usd_amount) if entry["type"] == "DEBIT" else 0.0
            credit_usd = float(usd_amount) if entry["type"] == "CREDIT" else 0.0

            # Look up GL account number and name from COA
            account_name = entry.get("account", "")
            gl_acct_number = None
            gl_acct_name = account_name

            if coa_map:
                # Try exact match first
                if account_name in coa_map:
                    gl_acct_number, gl_acct_name = coa_map[account_name]
                else:
                    # Try lowercase match
                    account_lower = account_name.lower()
                    for coa_key, (num, name) in coa_map.items():
                        if coa_key.lower() == account_lower or name.lower() == account_lower:
                            gl_acct_number, gl_acct_name = num, name
                            break

            # Build record matching parquet schema exactly (columns 1-31 + 37, excluding 32-36)
            # Generate row_key for deduplication
            row_key = f"{self.tx_hash}:{gl_acct_number or account_name}:{self.category.value if hasattr(self.category, 'value') else self.category}:{'DR' if debit_crypto > 0 else 'CR'}"

            records.append({
                # Core identifiers (1-4)
                'date': self.date,
                'fund_id': fund_id,
                'limited_partner_ID': None,
                'wallet_id': self.wallet_address,
                # Transaction info (5-7)
                'transaction_type': self.category.value if hasattr(self.category, 'value') else str(self.category),
                'cryptocurrency': entry["asset"],
                'account_name': account_name.lower().replace(' ', '_').replace('-', '_') if account_name else '',
                # GL Account info (8-9)
                'GL_Acct_Number': gl_acct_number,
                'GL_Acct_Name': gl_acct_name,
                # Amounts (10-14)
                'debit_crypto': float(debit_crypto),
                'credit_crypto': float(credit_crypto),
                'eth_usd_price': float(self.eth_usd_price),
                'debit_USD': debit_usd,
                'credit_USD': credit_usd,
                # Event info (15-17)
                'event': entry.get('event', None),
                'function': entry.get('function', None),
                'hash': self.tx_hash,
                # Loan details (18-31)
                'loan_id': entry.get('loan_id', None),
                'lender': entry.get('lender', None),
                'borrower': entry.get('borrower', None),
                'from': entry.get('from', None),
                'to': entry.get('to', None),
                'contract_address': entry.get('contract_address', None),
                'collateral_address': entry.get('collateral_address', None),
                'token_id': entry.get('token_id', None),
                'principal_crypto': entry.get('principal_crypto', None),
                'principal_USD': entry.get('principal_USD', None),
                'annual_interest_rate': entry.get('annual_interest_rate', None),
                'payoff_amount_crypto': entry.get('payoff_amount_crypto', None),
                'payoff_amount_USD': entry.get('payoff_amount_USD', None),
                'loan_due_date': entry.get('loan_due_date', None),
                # End of day price (37)
                'end_of_day_ETH_USD': float(self.eth_usd_price) if self.eth_usd_price > 0 else None,
                # Internal deduplication key
                'row_key': row_key,
            })
        return records

    def to_dict(self) -> dict:
        """Serialize to JSON-safe dictionary."""
        return {
            'entry_id': self.entry_id,
            'date': self.date.isoformat() if hasattr(self.date, 'isoformat') else str(self.date),
            'description': self.description,
            'tx_hash': self.tx_hash,
            'category': self.category.value if hasattr(self.category, 'value') else str(self.category),
            'platform': self.platform.value if hasattr(self.platform, 'value') else str(self.platform),
            'entries': self.entries,
            'tax_implications': self.tax_implications,
            'wallet_address': self.wallet_address,
            'wallet_role': self.wallet_role,
            'fund_id': self.fund_id,
            'posting_status': self.posting_status.value if hasattr(self.posting_status, 'value') else str(self.posting_status),
            'eth_usd_price': float(self.eth_usd_price),
            'is_balanced': self.validate()
        }


@dataclass
class DecodedTransaction:
    """
    Unified decoded transaction result from any platform decoder.
    Contains all decoded events, generated journal entries, and metadata.
    """
    status: str  # "success" or "error"
    tx_hash: str
    platform: Platform
    category: TransactionCategory
    block: int
    timestamp: datetime
    eth_price: Decimal
    gas_used: int
    gas_fee: Decimal
    from_address: str
    to_address: str
    value: Decimal
    function_name: str
    function_params: Dict[str, Any] = field(default_factory=dict)
    events: List[DecodedEvent] = field(default_factory=list)
    journal_entries: List[JournalEntry] = field(default_factory=list)
    wallet_roles: Dict[str, str] = field(default_factory=dict)
    positions: Dict[int, LoanPosition] = field(default_factory=dict)
    raw_data: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    _posting_status_override: Optional[PostingStatus] = None  # For spam/special cases

    @property
    def is_success(self) -> bool:
        return self.status == "success"

    @property
    def is_spam(self) -> bool:
        """Check if transaction was flagged as spam"""
        return self.status == "spam" or self.category == TransactionCategory.SPAM

    @property
    def entries_balanced(self) -> bool:
        return all(entry.validate() for entry in self.journal_entries)

    @property
    def posting_status(self) -> PostingStatus:
        """Overall posting status based on all journal entries"""
        # Check override first (for spam filtering, etc.)
        if self._posting_status_override is not None:
            return self._posting_status_override
        if not self.journal_entries:
            return PostingStatus.REVIEW_QUEUE
        statuses = {e.posting_status for e in self.journal_entries}
        if PostingStatus.POSTED in statuses and len(statuses) == 1:
            return PostingStatus.POSTED
        if PostingStatus.AUTO_POST in statuses:
            return PostingStatus.AUTO_POST
        return PostingStatus.REVIEW_QUEUE

    def to_dict(self) -> dict:
        return {
            'status': self.status,
            'tx_hash': self.tx_hash,
            'platform': self.platform.value,
            'category': self.category.value,
            'block': self.block,
            'timestamp': self.timestamp.isoformat(),
            'eth_price': float(self.eth_price),
            'gas_used': self.gas_used,
            'gas_fee': float(self.gas_fee),
            'from_address': self.from_address,
            'to_address': self.to_address,
            'value': float(self.value),
            'function_name': self.function_name,
            'function_params': self.function_params,
            'events': [e.to_dict() for e in self.events],
            'journal_entries': [e.to_dict() for e in self.journal_entries],
            'wallet_roles': self.wallet_roles,
            'positions': {k: v.to_dict() for k, v in self.positions.items()},
            'error': self.error,
            'posting_status': self.posting_status.value,  # Transaction-level posting status
            'entries_balanced': self.entries_balanced,
        }


# ============================================================================
# BASE DECODER CLASS
# ============================================================================

class BaseDecoder(ABC):
    """
    Abstract base class for platform-specific decoders.
    All platform decoders (Blur, Arcade, NFTfi, etc.) inherit from this.
    """

    PLATFORM: Platform = Platform.UNKNOWN
    CONTRACT_ADDRESSES: List[str] = []

    # Chart of accounts mapping - override in subclasses
    # Use account keys from centralized COA (see accounts.py)
    ACCOUNTS = {
        "eth_wallet": "digital_assets_eth",      # 10200
        "gas_expense": "gas_fee_expense",        # 80001
    }

    # Categories that auto-post to GL
    AUTO_POST_CATEGORIES = {
        TransactionCategory.LOAN_ORIGINATION,
        TransactionCategory.LOAN_REPAYMENT,
        TransactionCategory.LOAN_REFINANCE,
        TransactionCategory.INTEREST_ACCRUAL,
        TransactionCategory.ETH_TRANSFER,
        TransactionCategory.WETH_WRAP,
        TransactionCategory.WETH_UNWRAP,
        TransactionCategory.ERC20_TRANSFER,
    }

    # Max ETH value for auto-posting (configurable threshold)
    # Set high for loan transactions which can be large
    MAX_AUTO_POST_ETH = Decimal("1000")

    def __init__(self, w3: Web3, fund_wallets: List[str]):
        self.w3 = w3
        self.fund_wallets = [w.lower() for w in fund_wallets]
        self.positions: Dict[int, LoanPosition] = {}
        self.contracts_cache: Dict[str, Any] = {}
        self._load_abis()

    @abstractmethod
    def _load_abis(self):
        """Load contract ABIs for this platform - implement in subclass"""
        pass

    @abstractmethod
    def can_decode(self, tx: Dict, receipt: Dict) -> bool:
        """
        Check if this decoder can handle the transaction.

        Args:
            tx: Transaction data from w3.eth.get_transaction()
            receipt: Transaction receipt from w3.eth.get_transaction_receipt()

        Returns:
            True if this decoder can handle the transaction
        """
        pass

    @abstractmethod
    def decode(self, tx: Dict, receipt: Dict, block: Dict, eth_price: Decimal) -> DecodedTransaction:
        """
        Decode transaction and generate journal entries.

        Args:
            tx: Transaction data
            receipt: Transaction receipt
            block: Block data
            eth_price: ETH/USD price at block

        Returns:
            DecodedTransaction with events, journal entries, and metadata
        """
        pass

    def determine_posting_status(self, category: TransactionCategory, value: Decimal) -> PostingStatus:
        """
        Determine if transaction should auto-post or go to review queue.

        Hybrid logic:
        - AUTO_POST: known types, validated entries, reasonable amounts
        - REVIEW_QUEUE: unknown functions, large amounts, unusual patterns
        """
        # Check category
        if category not in self.AUTO_POST_CATEGORIES:
            return PostingStatus.REVIEW_QUEUE

        # Check amount threshold
        if value > self.MAX_AUTO_POST_ETH:
            return PostingStatus.REVIEW_QUEUE

        return PostingStatus.AUTO_POST

    def is_fund_wallet(self, address: str) -> bool:
        """Check if address is one of our fund wallets"""
        return address.lower() in self.fund_wallets

    def get_wallet_role(self, address: str, tx: Dict, events: List[DecodedEvent]) -> str:
        """Determine wallet role based on transaction context - override in subclass"""
        if address.lower() == tx.get('from', '').lower():
            return "sender"
        elif address.lower() == tx.get('to', '').lower():
            return "recipient"
        return "participant"

    def _create_error_result(self, tx_hash: str, error: str) -> DecodedTransaction:
        """Create error result for failed decoding"""
        return DecodedTransaction(
            status="error",
            tx_hash=tx_hash,
            platform=self.PLATFORM,
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

    def _decode_function_input(self, contract, input_data: bytes) -> Tuple[str, Dict[str, Any]]:
        """Decode function call from input data"""
        try:
            func, params = contract.decode_function_input(input_data)
            return func.fn_name, dict(params)
        except Exception as e:
            return "unknown", {}

    def _decode_events_by_names(self, contract, receipt: Dict, event_names: List[str]) -> List[DecodedEvent]:
        """
        Decode specific events from receipt using contract ABI.

        Uses process_receipt() which is the correct web3.py pattern.
        Unknown/failed events are logged at DEBUG level, not raised.

        Args:
            contract: Web3 contract instance with ABI
            receipt: Transaction receipt dict
            event_names: List of event names to decode (e.g., ['Transfer', 'Approval'])

        Returns:
            List of DecodedEvent objects, sorted by log_index
        """
        events = []
        for name in event_names:
            try:
                # Get event class from contract
                event_cls = getattr(contract.events, name, None)
                if event_cls is None:
                    logger.debug(f"Event {name} not found in contract ABI")
                    continue

                event_instance = event_cls()

                # web3.py uses either process_receipt or processReceipt depending on version
                if hasattr(event_instance, 'process_receipt'):
                    decoded_logs = event_instance.process_receipt(receipt)
                elif hasattr(event_instance, 'processReceipt'):
                    decoded_logs = event_instance.processReceipt(receipt)
                else:
                    logger.debug(f"Event {name} has no process_receipt method")
                    continue

                for evt in decoded_logs:
                    # Convert args to JSON-safe format
                    args = {}
                    for k, v in dict(evt.get('args', {})).items():
                        if isinstance(v, bytes):
                            args[k] = v.hex()
                        elif isinstance(v, int):
                            args[k] = str(v)
                        else:
                            args[k] = v

                    events.append(DecodedEvent(
                        name=evt.get('event', name),
                        args=args,
                        log_index=evt.get('logIndex', -1),
                        contract_address=evt.get('address', ''),
                        topic=evt['topics'][0].hex() if evt.get('topics') else None
                    ))
            except Exception as e:
                logger.debug(f"Could not decode event {name}: {e}")

        # Sort by log index for consistent ordering
        events.sort(key=lambda x: x.log_index)
        return events

    def _decode_logs(self, contract, receipt: Dict) -> List[DecodedEvent]:
        """
        DEPRECATED: Use _decode_events_by_names() instead.

        This method is kept for backwards compatibility but uses incorrect web3.py API.
        It attempts to index contract.events by topic hash, which doesn't work.
        """
        logger.warning("_decode_logs() is deprecated; use _decode_events_by_names() instead")
        # Return empty list rather than crash - callers should migrate to new method
        return []


# ============================================================================
# BASE DECODER ADAPTER CLASS
# ============================================================================

class BaseDecoderAdapter(BaseDecoder):
    """
    Base class for adapters that wrap notebook decoders.

    Provides:
    - Consistent initialization pattern with wallet_metadata building
    - Lazy loading of notebook decoders via _ensure_initialized()
    - Standard error handling with _create_basic_result()
    - Health check mechanism for runtime status
    - Validation method for configuration checking

    Subclasses must implement:
    - _initialize_decoder() -> bool: Initialize the notebook decoder
    - can_decode(tx, receipt) -> bool: Check if decoder handles this transaction
    - decode(tx, receipt, block, eth_price) -> DecodedTransaction: Decode the transaction
    """

    def __init__(self, w3: Web3, fund_wallets: List[str]):
        """
        Initialize the adapter with Web3 and fund wallets.

        Note: We don't call super().__init__() because BaseDecoder's __init__
        calls _load_abis() immediately, but adapters use lazy initialization.
        """
        self.w3 = w3
        self.fund_wallets = [w.lower() for w in fund_wallets]
        self.wallet_metadata = self._build_wallet_metadata(fund_wallets)
        self.positions: Dict[int, LoanPosition] = {}
        self.contracts_cache: Dict[str, Any] = {}
        self._notebook_decoder = None
        self._journal_generator = None
        self._initialized = False
        self._initialization_error: Optional[str] = None

    @staticmethod
    def _build_wallet_metadata(fund_wallets: List[str]) -> Dict[str, Dict]:
        """
        Build wallet_metadata from S3 wallet file for proper fund identification.
        Falls back to dummy metadata if S3 load fails.

        Args:
            fund_wallets: List of wallet addresses (used as fallback)

        Returns:
            Dict mapping lowercase address to metadata dict
        """
        try:
            # Import here to avoid circular imports
            from ...s3_utils import load_WALLET_file
            wallet_df = load_WALLET_file()

            if wallet_df is not None and not wallet_df.empty:
                metadata = {}
                for _, row in wallet_df.iterrows():
                    addr = str(row.get('wallet_address', '')).strip().lower()
                    if addr:
                        metadata[addr] = {
                            'fund_id': str(row.get('fund_id', '')).strip(),
                            'friendly_name': str(row.get('friendly_name', '')).strip(),
                            'category': str(row.get('category', '')).strip(),
                            'wallet_type': str(row.get('wallet_type', 'hot')).strip(),
                        }
                if metadata:
                    logger.info(f"Loaded wallet_metadata for {len(metadata)} wallets from S3")
                    return metadata
        except Exception as e:
            logger.warning(f"Could not load wallet metadata from S3: {e}")

        # Fallback to dummy metadata
        logger.warning("Using fallback dummy wallet_metadata")
        return {
            addr.lower(): {
                'fund_id': 'fund',
                'wallet_name': f'Wallet {addr[:8]}',
                'category': 'fund',
                'wallet_type': 'hot',
            }
            for addr in fund_wallets
        }

    def _load_abis(self):
        """
        No-op for adapters - use _ensure_initialized() instead.

        Adapters use lazy initialization via _initialize_decoder() rather than
        loading ABIs in the constructor.
        """
        pass

    @abstractmethod
    def _initialize_decoder(self) -> bool:
        """
        Initialize the notebook decoder. Called lazily on first decode.

        Subclasses should:
        1. Import the notebook decoder class
        2. Load required ABIs
        3. Create contract instances
        4. Instantiate _notebook_decoder and _journal_generator
        5. Set _initialization_error if something fails
        6. Return True on success, False on failure

        Returns:
            True if initialization succeeded, False otherwise
        """
        pass

    def _ensure_initialized(self) -> bool:
        """
        Ensure decoder is initialized, performing lazy init if needed.

        Returns:
            True if decoder is ready to use, False otherwise
        """
        if self._initialized:
            return True
        if self._initialization_error:
            return False

        try:
            success = self._initialize_decoder()
            self._initialized = success
            if not success and not self._initialization_error:
                self._initialization_error = "Initialization returned False"
            return success
        except Exception as e:
            self._initialization_error = str(e)
            logger.error(f"Failed to initialize {self.PLATFORM.value} decoder: {e}")
            return False

    def health_check(self) -> Dict[str, Any]:
        """
        Check decoder health status.

        Returns:
            Dict with status, initialization state, and component availability
        """
        if self._initialized:
            status = 'healthy'
        elif self._initialization_error:
            status = 'error'
        else:
            status = 'pending'

        return {
            'platform': self.PLATFORM.value,
            'status': status,
            'initialized': self._initialized,
            'error': self._initialization_error,
            'has_notebook_decoder': self._notebook_decoder is not None,
            'has_journal_generator': self._journal_generator is not None,
        }

    def validate(self) -> List[str]:
        """
        Validate decoder configuration.

        Checks that required dependencies are available and initialization succeeds.

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []

        if not self.w3:
            errors.append("Web3 instance not provided")

        if not self.fund_wallets:
            errors.append("No fund wallets configured")

        # Try initialization to check for errors
        if not self._ensure_initialized():
            errors.append(f"Initialization failed: {self._initialization_error}")

        return errors

    def _create_basic_result(self, tx: Dict, receipt: Dict, block: Dict,
                             eth_price: Decimal, error: str = None) -> DecodedTransaction:
        """
        Create basic result when decoding fails or decoder not initialized.

        Args:
            tx: Transaction data
            receipt: Transaction receipt
            block: Block data
            eth_price: ETH/USD price
            error: Optional error message

        Returns:
            DecodedTransaction with basic info and error status
        """
        tx_hash = self._normalize_tx_hash(tx)
        timestamp = datetime.fromtimestamp(block.get('timestamp', 0), tz=timezone.utc)
        gas_fee = calculate_gas_fee(receipt, tx)

        return DecodedTransaction(
            status="error" if error else "success",
            tx_hash=tx_hash,
            platform=self.PLATFORM,
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
            error=error,
        )

    @staticmethod
    def _normalize_tx_hash(tx: Dict) -> str:
        """
        Normalize transaction hash to always include 0x prefix.

        Args:
            tx: Transaction data dict

        Returns:
            Normalized transaction hash string with 0x prefix
        """
        raw_hash = tx.get('hash', b'')
        if isinstance(raw_hash, bytes):
            hex_str = raw_hash.hex()
        else:
            hex_str = str(raw_hash)

        return hex_str if hex_str.startswith('0x') else f"0x{hex_str}"


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def wei_to_eth(wei: int) -> Decimal:
    """Convert wei to ETH"""
    return Decimal(wei) / Decimal(10**18)


def eth_to_wei(eth: Decimal) -> int:
    """Convert ETH to wei"""
    return int(eth * Decimal(10**18))


def format_address(address: str, length: int = 8) -> str:
    """Format address for display"""
    if not address:
        return ""
    return f"{address[:length]}...{address[-4:]}"


def calculate_gas_fee(receipt: Dict, tx: Dict) -> Decimal:
    """Calculate gas fee in ETH"""
    gas_used = receipt.get('gasUsed', 0)
    # Try effectiveGasPrice first (EIP-1559), fallback to gasPrice
    gas_price = receipt.get('effectiveGasPrice', tx.get('gasPrice', 0))
    return wei_to_eth(gas_used * gas_price)


# ============================================================================
# INTEREST ACCRUAL GENERATION
# ============================================================================

def compute_continuous_interest(
    principal: Decimal,
    rate_bips: int,
    start_timestamp: int,
    end_timestamp: int
) -> Decimal:
    """
    Compute total interest using continuous compounding.

    Formula: interest = principal × (e^(rate × time_in_years) - 1)

    Args:
        principal: Principal amount in ETH
        rate_bips: Annual interest rate in basis points (e.g., 1500 = 15%)
        start_timestamp: Loan start Unix timestamp
        end_timestamp: Calculation end Unix timestamp

    Returns:
        Total accrued interest in ETH
    """
    if end_timestamp <= start_timestamp or principal <= 0 or rate_bips <= 0:
        return Decimal(0)

    loan_time_seconds = Decimal(end_timestamp - start_timestamp)
    time_in_years = loan_time_seconds / SECONDS_PER_YEAR
    rate_decimal = Decimal(rate_bips) / Decimal(10000)

    # Continuous compounding: e^(r * t)
    exponent = float(rate_decimal * time_in_years)
    compound_factor = Decimal(str(math.exp(exponent)))

    # Interest = Principal × (compound_factor - 1)
    interest = principal * (compound_factor - Decimal(1))
    return interest


def generate_daily_interest_accruals(
    start_timestamp: int,
    end_timestamp: int,
    principal: Decimal,
    rate_bips: int,
    is_lender: bool,
    common_metadata: Dict[str, Any],
    platform: str = "blur",  # blur, weth, usdc - determines account suffix
) -> List[Dict[str, Any]]:
    """
    Generate daily interest accrual journal entries from loan start to end.

    Uses Wei-precise allocation to avoid rounding errors.
    Accruals are backdated to each day of the loan.

    Args:
        start_timestamp: Loan start time (Unix timestamp)
        end_timestamp: Repayment/refinance time (Unix timestamp)
        principal: Principal amount in ETH (Decimal)
        rate_bips: Annual rate in basis points (e.g., 1500 = 15%)
        is_lender: True for income accruals, False for expense accruals
        common_metadata: Common fields for all entries (tx_hash, platform, fund_id, etc.)
        platform: Platform suffix for accounts (blur, weth, usdc)

    Returns:
        List of GL-compatible journal entry dicts matching parquet schema

    Example output for a 3-day loan as lender:
        Day 1: Dr Interest Receivable 0.001 / Cr Interest Income 0.001
        Day 2: Dr Interest Receivable 0.001 / Cr Interest Income 0.001
        Day 3: Dr Interest Receivable 0.001 / Cr Interest Income 0.001
    """
    from datetime import datetime, timezone, timedelta, time as dt_time
    from .accounts import get_interest_accrual_accounts

    if start_timestamp >= end_timestamp or principal <= 0 or rate_bips <= 0:
        return []

    # Get account mappings from centralized COA (see accounts.py)
    ACCOUNTS = get_interest_accrual_accounts(platform)

    entries = []

    # Calculate total interest using continuous compounding
    total_interest = compute_continuous_interest(
        principal=principal,
        rate_bips=rate_bips,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp
    )

    # Convert to Wei for precise allocation
    total_interest_wei = int(total_interest * WAD)

    if total_interest_wei <= 0:
        return []

    # Calculate total seconds
    total_secs = end_timestamp - start_timestamp

    # Generate daily slices with Wei precision
    start_dt = datetime.fromtimestamp(start_timestamp, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_timestamp, tz=timezone.utc)

    leftover = 0
    assigned_so_far = 0
    cursor = start_dt
    day_count = 0

    while cursor < end_dt:
        # Calculate next midnight
        tomorrow = cursor.date() + timedelta(days=1)
        naive_midnight = datetime.combine(tomorrow, dt_time(0, 0, 0))
        next_midnight = naive_midnight.replace(tzinfo=timezone.utc)

        # Slice ends at midnight or loan end
        segment_end = min(next_midnight, end_dt)
        slice_secs = int((segment_end - cursor).total_seconds())

        # Wei-precise interest allocation
        numer = (total_interest_wei * slice_secs) + leftover
        slice_interest_wei = numer // total_secs
        leftover = numer % total_secs
        assigned_so_far += slice_interest_wei

        # Label timestamp (end of day at 23:59:59 or loan end)
        if segment_end == next_midnight:
            accrual_label = next_midnight - timedelta(seconds=1)
        else:
            accrual_label = end_dt

        # Convert from wei to crypto
        slice_interest = Decimal(slice_interest_wei) / WAD

        if slice_interest > 0:
            day_count += 1

            # Get USD price if available in metadata
            eth_usd_price = Decimal(str(common_metadata.get('eth_usd_price', 0)))
            slice_usd = float(slice_interest * eth_usd_price) if eth_usd_price > 0 else 0.0

            # Generate unique row key including date for deduplication
            date_str = accrual_label.strftime('%Y-%m-%d') if hasattr(accrual_label, 'strftime') else str(accrual_label)[:10]
            tx_hash = common_metadata.get('tx_hash', common_metadata.get('hash', ''))

            # Base entry data matching parquet schema (columns 1-31 + 37, excluding 32-36)
            base_entry = {
                # Core identifiers (1-4)
                'date': accrual_label,
                'fund_id': common_metadata.get('fund_id', ''),
                'limited_partner_ID': None,
                'wallet_id': common_metadata.get('wallet_id', ''),
                # Transaction info (5-7)
                'transaction_type': 'income_interest_accruals' if is_lender else 'expense_interest_accruals',
                'cryptocurrency': common_metadata.get('cryptocurrency', 'ETH'),
                # Event info (15-17)
                'event': 'InterestAccrual',
                'function': None,
                'hash': tx_hash,
                # Loan details (18-31)
                'loan_id': common_metadata.get('loan_id'),
                'lender': common_metadata.get('lender'),
                'borrower': common_metadata.get('borrower'),
                'from': common_metadata.get('from'),
                'to': common_metadata.get('to'),
                'contract_address': common_metadata.get('contract_address'),
                'collateral_address': common_metadata.get('collateral_address'),
                'token_id': common_metadata.get('token_id'),
                'principal_crypto': common_metadata.get('principal_crypto', 0.0),
                'principal_USD': common_metadata.get('principal_USD', 0.0),
                'annual_interest_rate': common_metadata.get('annual_interest_rate', 0.0),
                'payoff_amount_crypto': None,
                'payoff_amount_USD': None,
                'loan_due_date': common_metadata.get('loan_due_date'),
                # End of day price (37)
                'end_of_day_ETH_USD': float(eth_usd_price) if eth_usd_price > 0 else None,
                'eth_usd_price': float(eth_usd_price),
            }

            if is_lender:
                recv_acct_num, recv_acct_name = ACCOUNTS['interest_receivable']
                income_acct_num, income_acct_name = ACCOUNTS['interest_income']

                # Dr Interest Receivable
                entries.append({
                    **base_entry,
                    'account_name': recv_acct_name.lower().replace(' ', '_').replace('-', '_'),
                    'GL_Acct_Number': recv_acct_num,
                    'GL_Acct_Name': recv_acct_name,
                    'debit_crypto': float(slice_interest),
                    'credit_crypto': 0.0,
                    'debit_USD': slice_usd,
                    'credit_USD': 0.0,
                    'row_key': f"{tx_hash}:{recv_acct_num}:income_interest_accruals:{date_str}:DR",
                })

                # Cr Interest Income
                entries.append({
                    **base_entry,
                    'account_name': income_acct_name.lower().replace(' ', '_').replace('-', '_'),
                    'GL_Acct_Number': income_acct_num,
                    'GL_Acct_Name': income_acct_name,
                    'debit_crypto': 0.0,
                    'credit_crypto': float(slice_interest),
                    'debit_USD': 0.0,
                    'credit_USD': slice_usd,
                    'row_key': f"{tx_hash}:{income_acct_num}:income_interest_accruals:{date_str}:CR",
                })
            else:
                expense_acct_num, expense_acct_name = ACCOUNTS['interest_expense']
                payable_acct_num, payable_acct_name = ACCOUNTS['interest_payable']

                # Dr Interest Expense
                entries.append({
                    **base_entry,
                    'account_name': expense_acct_name.lower().replace(' ', '_').replace('-', '_'),
                    'GL_Acct_Number': expense_acct_num,
                    'GL_Acct_Name': expense_acct_name,
                    'debit_crypto': float(slice_interest),
                    'credit_crypto': 0.0,
                    'debit_USD': slice_usd,
                    'credit_USD': 0.0,
                    'row_key': f"{tx_hash}:{expense_acct_num}:expense_interest_accruals:{date_str}:DR",
                })

                # Cr Interest Payable
                entries.append({
                    **base_entry,
                    'account_name': payable_acct_name.lower().replace(' ', '_').replace('-', '_'),
                    'GL_Acct_Number': payable_acct_num,
                    'GL_Acct_Name': payable_acct_name,
                    'debit_crypto': 0.0,
                    'credit_crypto': float(slice_interest),
                    'debit_USD': 0.0,
                    'credit_USD': slice_usd,
                    'row_key': f"{tx_hash}:{payable_acct_num}:expense_interest_accruals:{date_str}:CR",
                })

        cursor = segment_end

    # Final adjustment for any remaining wei
    if assigned_so_far < total_interest_wei and entries:
        shortfall_wei = total_interest_wei - assigned_so_far
        shortfall = Decimal(shortfall_wei) / WAD
        shortfall_usd = float(shortfall * Decimal(str(common_metadata.get('eth_usd_price', 0))))

        # Add shortfall to last entries (debit and credit pair)
        if len(entries) >= 2:
            # Last two entries are debit then credit
            last_debit = entries[-2]
            last_credit = entries[-1]

            if last_debit.get('debit_crypto', 0) > 0:
                last_debit['debit_crypto'] = float(Decimal(str(last_debit['debit_crypto'])) + shortfall)
                last_debit['debit_USD'] = float(Decimal(str(last_debit.get('debit_USD', 0))) + Decimal(str(shortfall_usd)))
            if last_credit.get('credit_crypto', 0) > 0:
                last_credit['credit_crypto'] = float(Decimal(str(last_credit['credit_crypto'])) + shortfall)
                last_credit['credit_USD'] = float(Decimal(str(last_credit.get('credit_USD', 0))) + Decimal(str(shortfall_usd)))

    logger.info(f"Generated {len(entries)} interest accrual entries for {day_count} days "
                f"(total interest: {total_interest:.8f} ETH)")

    return entries
