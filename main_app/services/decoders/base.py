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


class PostingStatus(Enum):
    """Journal entry posting status for hybrid GL workflow"""
    AUTO_POST = "auto_post"       # Auto-post to GL (known transaction types)
    REVIEW_QUEUE = "review_queue" # Queue for manual review
    POSTED = "posted"             # Already posted to GL
    REJECTED = "rejected"         # Rejected by user


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

    def to_gl_records(self) -> List[Dict[str, Any]]:
        """Convert to GL-compatible records for save_GL_file()"""
        records = []
        for entry in self.entries:
            amount = Decimal(str(entry["amount"]))
            usd_amount = amount * self.eth_usd_price if entry["asset"] == "ETH" else amount

            debit_crypto = amount if entry["type"] == "DEBIT" else Decimal(0)
            credit_crypto = amount if entry["type"] == "CREDIT" else Decimal(0)
            debit_usd = float(usd_amount) if entry["type"] == "DEBIT" else 0.0
            credit_usd = float(usd_amount) if entry["type"] == "CREDIT" else 0.0

            # Net = debit - credit
            net_crypto = debit_crypto - credit_crypto
            net_usd = debit_usd - credit_usd

            records.append({
                'transaction_id': self.entry_id,
                'date': self.date,
                'operating_date': self.date,  # Same as date for new entries
                'transaction_type': self.category.value,
                'wallet_id': self.wallet_address,
                'fund_id': self.fund_id,
                'cryptocurrency': entry["asset"],
                'account_name': entry["account"],
                'debit_crypto': float(debit_crypto),
                'credit_crypto': float(credit_crypto),
                'eth_usd_price': float(self.eth_usd_price),
                'debit_USD': debit_usd,
                'credit_USD': credit_usd,
                'net_debit_credit_crypto': float(net_crypto),
                'net_debit_credit_USD': net_usd,
                # Loan-specific fields (null for non-loan entries)
                'principal_crypto': None,
                'principal_USD': None,
                'interest_rec_crypto': None,
                'interest_rec_USD': None,
                'payoff_amount_crypto': None,
                'payoff_amount_USD': None,
                'annual_interest_rate': None,
                'hash': self.tx_hash
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

    @property
    def is_success(self) -> bool:
        return self.status == "success"

    @property
    def entries_balanced(self) -> bool:
        return all(entry.validate() for entry in self.journal_entries)

    @property
    def posting_status(self) -> PostingStatus:
        """Overall posting status based on all journal entries"""
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
    ACCOUNTS = {
        "eth_wallet": "100.30 - ETH Wallet",
        "gas_expense": "600.10 - Gas Expense",
    }

    # Categories that auto-post to GL
    AUTO_POST_CATEGORIES = {
        TransactionCategory.LOAN_ORIGINATION,
        TransactionCategory.LOAN_REPAYMENT,
        TransactionCategory.ETH_TRANSFER,
        TransactionCategory.WETH_WRAP,
        TransactionCategory.WETH_UNWRAP,
        TransactionCategory.ERC20_TRANSFER,
    }

    # Max ETH value for auto-posting (configurable threshold)
    MAX_AUTO_POST_ETH = Decimal("100")

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
