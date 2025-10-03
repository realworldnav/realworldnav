"""
Blur NFT Lending Protocol Auto-Decoder
Automatically decodes Blur transactions in background and generates journal entries
Adapted from reference implementation for production use
"""

from web3 import Web3
from datetime import datetime, timezone
from decimal import Decimal, getcontext
from typing import Dict, List, Tuple, Optional, Any, Union
from dataclasses import dataclass, field, asdict
from enum import Enum
from collections import defaultdict
from functools import lru_cache
from pathlib import Path
import json
import math
import logging
import os

# Set decimal precision for financial calculations
getcontext().prec = 28

logger = logging.getLogger(__name__)

# Import from our config
from ...config.blockchain_config import (
    BLUR_POOL, BLUR_LENDING,
    INFURA_API_KEY, INFURA_URL, ETHERSCAN_API_KEY,
    CHAINLINK_ETH_USD_FEED, CHAINLINK_AGGREGATOR_V3_ABI
)
from ...s3_utils import load_abi_from_s3, list_available_abis

# ============================================================================
# WEB3 SETUP
# ============================================================================

w3 = None
if INFURA_API_KEY:
    w3 = Web3(Web3.HTTPProvider(INFURA_URL))
    if w3.is_connected():
        logger.info("Web3 connected to Ethereum mainnet")
    else:
        logger.warning("Web3 failed to connect")

# Chainlink Price Feed
aggregator = None
if w3 and w3.is_connected():
    try:
        aggregator = w3.eth.contract(
            address=Web3.to_checksum_address(CHAINLINK_ETH_USD_FEED),
            abi=CHAINLINK_AGGREGATOR_V3_ABI
        )
    except Exception as e:
        logger.error(f"Failed to initialize Chainlink aggregator: {e}")

# ============================================================================
# ENUMS
# ============================================================================

class AccountingEventType(Enum):
    """Accounting event classifications"""
    LOAN_ORIGINATION = "LOAN_ORIGINATION"
    LOAN_REPAYMENT = "LOAN_REPAYMENT"
    LOAN_REFINANCE = "LOAN_REFINANCE"
    COLLATERAL_SEIZURE = "COLLATERAL_SEIZURE"
    INTEREST_ACCRUAL = "INTEREST_ACCRUAL"
    LIQUIDATION_AUCTION = "LIQUIDATION_AUCTION"
    FEE_PAYMENT = "FEE_PAYMENT"


class TaxTreatment(Enum):
    """Tax treatment classifications"""
    NON_TAXABLE = "NON_TAXABLE"
    TAXABLE_INCOME = "TAXABLE_INCOME"
    CAPITAL_GAIN = "CAPITAL_GAIN"
    CAPITAL_LOSS = "CAPITAL_LOSS"
    DEDUCTIBLE_EXPENSE = "DEDUCTIBLE_EXPENSE"

# ============================================================================
# DATA CLASSES
# ============================================================================

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
    auction_duration: int
    status: str = "ACTIVE"

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
        """Convert to dictionary for JSON serialization"""
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
            'status': self.status
        }


@dataclass
class JournalEntry:
    """Double-entry bookkeeping journal entry"""
    entry_id: str
    date: datetime
    description: str
    tx_hash: str
    event_type: AccountingEventType
    entries: List[Dict[str, Any]] = field(default_factory=list)
    tax_implications: List[Dict[str, Any]] = field(default_factory=list)
    wallet_address: str = ""
    wallet_role: str = ""

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
        """Ensure debits equal credits per asset"""
        balances = defaultdict(lambda: {"debits": Decimal(0), "credits": Decimal(0)})
        for entry in self.entries:
            asset = entry["asset"]
            amount = Decimal(str(entry["amount"]))
            if entry["type"] == "DEBIT":
                balances[asset]["debits"] += amount
            else:
                balances[asset]["credits"] += amount
        for totals in balances.values():
            if abs(totals["debits"] - totals["credits"]) > Decimal("0.01"):
                return False
        return True

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        return {
            'entry_id': self.entry_id,
            'date': self.date.isoformat(),
            'description': self.description,
            'tx_hash': self.tx_hash,
            'event_type': self.event_type.value,
            'entries': self.entries,
            'tax_implications': self.tax_implications,
            'wallet_address': self.wallet_address,
            'wallet_role': self.wallet_role
        }

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

@lru_cache(maxsize=512)
def get_eth_usd_at_block(block_number: int) -> Tuple[Decimal, datetime]:
    """Get ETH/USD price at specific block"""
    if not aggregator or not w3:
        return Decimal(3000), datetime.now(timezone.utc)

    try:
        _, answer, *_ = aggregator.functions.latestRoundData().call(block_identifier=block_number)
        price = Decimal(answer) / Decimal(1e8)
    except Exception as e:
        logger.warning(f"Failed to get ETH price: {e}")
        price = Decimal(3000)

    try:
        block = w3.eth.get_block(block_number)
        return price, datetime.fromtimestamp(block.timestamp, tz=timezone.utc)
    except:
        return price, datetime.now(timezone.utc)


def get_implementation_address(proxy_address: str) -> Optional[str]:
    """Get implementation address from proxy contract (EIP-1967)"""
    if not w3:
        return None

    try:
        # EIP-1967 implementation slot
        implementation_slot = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
        storage_value = w3.eth.get_storage_at(proxy_address, implementation_slot)

        if storage_value == b'\x00' * 32:
            return None

        # Extract address from last 20 bytes
        impl_address = Web3.to_checksum_address('0x' + storage_value.hex()[-40:])
        return impl_address
    except Exception as e:
        logger.warning(f"Failed to get implementation address: {e}")
        return None


def instantiate_contract(address: str) -> Optional[Any]:
    """Instantiate a Web3 contract with ABI from S3"""
    if not w3:
        return None

    try:
        checksum_address = Web3.to_checksum_address(address)

        # Try to load ABI from S3
        abi = load_abi_from_s3(checksum_address)

        if not abi:
            # Try implementation if proxy
            impl_address = get_implementation_address(checksum_address)
            if impl_address:
                logger.info(f"Detected proxy, trying implementation: {impl_address}")
                abi = load_abi_from_s3(impl_address)

        if not abi:
            logger.warning(f"No ABI found for {checksum_address}")
            return None

        contract = w3.eth.contract(address=checksum_address, abi=abi)
        logger.info(f"Contract instantiated: {checksum_address}")
        return contract

    except Exception as e:
        logger.error(f"Failed to instantiate contract {address}: {e}")
        return None


# ============================================================================
# BLUR AUTO-DECODER
# ============================================================================

class BlurAutoDecoder:
    """Automatically decode Blur lending transactions in background"""

    def __init__(self):
        self.decoded_cache: Dict[str, Dict] = {}
        self.positions: Dict[int, LoanPosition] = {}
        self.journal_entries: List[JournalEntry] = []
        self.contracts_cache = {}

        # Chart of Accounts
        self.accounts = {
            "nft_collateral": "100.10 - NFT Collateral",
            "loans_receivable": "100.20 - Loans Receivable",
            "eth_wallet": "100.30 - ETH Wallet",
            "blur_pool": "100.31 - Blur Pool Balance",
            "accrued_interest": "100.40 - Accrued Interest Receivable",
            "loan_payable": "200.10 - Loan Payable",
            "accrued_interest_payable": "200.20 - Accrued Interest Payable",
            "interest_income": "400.10 - Interest Income",
            "liquidation_gains": "400.20 - Liquidation Gains",
            "interest_expense": "500.10 - Interest Expense",
            "gas_fees": "500.20 - Gas Fees",
            "liquidation_losses": "500.30 - Liquidation Losses",
            "protocol_fees": "500.40 - Protocol Fees"
        }

        # Load Blur contracts
        self._load_contracts()

    def _load_contracts(self):
        """Load Blur contract instances with implementation ABIs"""
        try:
            # For Blur Lending - load IMPLEMENTATION ABI for proxy contract
            impl_address = get_implementation_address(BLUR_LENDING)
            if impl_address:
                logger.info(f"Blur Lending is proxy, using implementation: {impl_address}")
                impl_abi = load_abi_from_s3(impl_address)
                if impl_abi and w3:
                    # Create contract with proxy address but implementation ABI
                    self.blur_lending_contract = w3.eth.contract(
                        address=Web3.to_checksum_address(BLUR_LENDING),
                        abi=impl_abi
                    )
                    logger.info(f"Blur Lending contract loaded with implementation ABI ({len(impl_abi)} items)")
                else:
                    self.blur_lending_contract = instantiate_contract(BLUR_LENDING)
            else:
                self.blur_lending_contract = instantiate_contract(BLUR_LENDING)

            # For Blur Pool
            self.blur_pool_contract = instantiate_contract(BLUR_POOL)

            if self.blur_pool_contract:
                logger.info("Blur Pool contract loaded")
        except Exception as e:
            logger.error(f"Failed to load Blur contracts: {e}")

    def is_blur_transaction(self, tx: Dict) -> bool:
        """Check if transaction involves Blur contracts (legacy method for backwards compatibility)"""
        if not tx:
            return False

        tx_to = tx.get('to', '').lower() if tx.get('to') else ''
        tx_from = tx.get('from', '').lower() if tx.get('from') else ''

        blur_lending_lower = BLUR_LENDING.lower()
        blur_pool_lower = BLUR_POOL.lower()

        return (tx_to == blur_lending_lower or
                tx_from == blur_lending_lower or
                tx_to == blur_pool_lower or
                tx_from == blur_pool_lower)

    def should_decode_transaction(self, tx: Dict, fund_wallets: List[str]) -> bool:
        """Check if transaction should be decoded (ALL wallet transactions)"""
        if not tx:
            return False

        tx_to = tx.get('to', '').lower() if tx.get('to') else ''
        tx_from = tx.get('from', '').lower() if tx.get('from') else ''
        fund_wallets_lower = [w.lower() for w in fund_wallets]

        # Decode ALL transactions involving fund wallets
        return (tx_from in fund_wallets_lower or tx_to in fund_wallets_lower)

    def categorize_transaction(self, tx, receipt) -> str:
        """Categorize transaction type for appropriate decoding"""

        # Check for known contracts first
        tx_to = tx.get('to', '').lower() if tx.get('to') else ''

        if tx_to == BLUR_LENDING.lower() or tx_to == BLUR_POOL.lower():
            return "BLUR_LENDING"

        # Check for token transfers in logs (ERC20 Transfer event)
        has_erc20 = False
        has_nft = False

        for log in receipt.logs:
            if len(log.topics) > 0:
                # ERC20/ERC721 Transfer event signature
                if log.topics[0].hex() == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef":
                    # Check if ERC20 (has data) or NFT (no data for amount)
                    if len(log.data) > 0:
                        has_erc20 = True
                    else:
                        has_nft = True
                # ERC1155 Transfer event signatures
                elif log.topics[0].hex() in ["0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62",
                                             "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"]:
                    has_nft = True

        if has_nft:
            return "NFT_TRANSFER"
        elif has_erc20:
            return "ERC20_TRANSFER"

        # Check for simple ETH transfer
        if tx.get('value', 0) > 0 and (not tx.get('input') or tx.get('input') == '0x'):
            return "ETH_TRANSFER"

        # Generic contract interaction
        if tx.get('input') and len(tx.get('input', '')) > 2:
            return "CONTRACT_CALL"

        return "UNKNOWN"

    def get_cached_decode(self, tx_hash: str) -> Optional[Dict]:
        """Get cached decode result if available"""
        return self.decoded_cache.get(tx_hash)

    def decode_transaction(self, tx_hash: str, fund_wallets: List[str], wallet_metadata: Dict = None) -> Dict[str, Any]:
        """
        Decode a Blur transaction and generate accounting entries
        Returns a summary dict for UI display
        """
        # Check cache first
        if tx_hash in self.decoded_cache:
            return self.decoded_cache[tx_hash]

        if not w3 or not w3.is_connected():
            return {
                "status": "error",
                "error": "Web3 not connected",
                "tx_hash": tx_hash
            }

        try:
            # Get transaction and receipt
            tx = w3.eth.get_transaction(tx_hash)
            receipt = w3.eth.get_transaction_receipt(tx_hash)
            block = w3.eth.get_block(tx.blockNumber)
            block_time = datetime.fromtimestamp(block.timestamp, tz=timezone.utc)
            eth_price, _ = get_eth_usd_at_block(tx.blockNumber)

            # Categorize transaction type
            tx_type = self.categorize_transaction(tx, receipt)

            # Route to appropriate decoder based on type
            if tx_type == "BLUR_LENDING":
                result = self._decode_blur_transaction(
                    tx, receipt, block, block_time, eth_price, fund_wallets, tx_hash
                )
            elif tx_type == "ERC20_TRANSFER":
                result = self._decode_erc20_transaction(
                    tx, receipt, block, block_time, eth_price, fund_wallets, tx_hash
                )
            elif tx_type == "NFT_TRANSFER":
                result = self._decode_nft_transaction(
                    tx, receipt, block, block_time, eth_price, fund_wallets, tx_hash
                )
            elif tx_type == "ETH_TRANSFER":
                result = self._decode_eth_transaction(
                    tx, receipt, block, block_time, eth_price, fund_wallets, tx_hash
                )
            else:
                result = self._decode_generic_transaction(
                    tx, receipt, block, block_time, eth_price, fund_wallets, tx_hash
                )

            # Store in cache
            self.decoded_cache[tx_hash] = result

            logger.info(f"Decoded transaction {tx_hash[:10]}...: {result.get('tx_type', 'UNKNOWN')}")
            return result

        except Exception as e:
            logger.error(f"Error decoding transaction {tx_hash}: {e}")
            import traceback
            traceback.print_exc()
            error_result = {
                "status": "error",
                "error": str(e),
                "tx_hash": tx_hash
            }
            self.decoded_cache[tx_hash] = error_result
            return error_result

    def _decode_blur_transaction(self, tx, receipt, block, block_time, eth_price, fund_wallets, tx_hash):
        """Decode Blur lending transaction (original logic)"""
        # Decode function call
        function_name, function_params = self._decode_function(tx)

        # Decode events
        events = self._decode_events(receipt)

        # Detect pool transfers
        pool_transfers = self._decode_pool_transfers(receipt, fund_wallets)

        # Determine wallet roles
        wallet_roles = self._determine_wallet_roles(tx, receipt, fund_wallets, events)

        # Generate journal entries
        journal_entries = self._generate_journal_entries(
            tx, receipt, block_time, eth_price,
            function_name, function_params, events,
            pool_transfers, wallet_roles, fund_wallets
        )

        return {
            "status": "success",
            "tx_hash": tx_hash,
            "tx_type": "BLUR_LENDING",
            "block": tx.blockNumber,
            "timestamp": block_time.isoformat(),
            "eth_price": float(eth_price),
            "gas_used": receipt.gasUsed,
            "from": tx['from'],
            "to": tx['to'],
            "value": float(Decimal(tx.value) / Decimal(10**18)),
            "function": function_name,
            "function_params": function_params,
            "events": [self._event_to_dict(e) for e in events],
            "pool_transfers": pool_transfers,
            "journal_entries": [je.to_dict() for je in journal_entries],
            "wallet_roles": wallet_roles,
            "positions": {k: v.to_dict() for k, v in self.positions.items()},
            "summary": {
                "total_events": len(events),
                "total_journal_entries": len(journal_entries),
                "all_balanced": all(je.validate() for je in journal_entries),
                "involves_fund_wallets": len(wallet_roles) > 0
            }
        }

    def _decode_erc20_transaction(self, tx, receipt, block, block_time, eth_price, fund_wallets, tx_hash):
        """Decode ERC20 token transfer"""
        transfers = []
        fund_wallets_lower = [w.lower() for w in fund_wallets]

        # Extract Transfer events
        for log in receipt.logs:
            if len(log.topics) > 0 and log.topics[0].hex() == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef":
                try:
                    from_addr = Web3.to_checksum_address('0x' + log.topics[1].hex()[-40:])
                    to_addr = Web3.to_checksum_address('0x' + log.topics[2].hex()[-40:])
                    amount = Decimal(int.from_bytes(log.data, 'big')) / Decimal(10**18)

                    transfers.append({
                        'from': from_addr,
                        'to': to_addr,
                        'amount': float(amount),
                        'token': log.address,
                        'involves_fund': (from_addr.lower() in fund_wallets_lower or to_addr.lower() in fund_wallets_lower)
                    })
                except Exception as e:
                    logger.warning(f"Failed to decode transfer: {e}")

        # Determine wallet roles
        wallet_roles = {}
        for transfer in transfers:
            if transfer['from'].lower() in fund_wallets_lower:
                wallet_roles[transfer['from']] = "SENDER"
            if transfer['to'].lower() in fund_wallets_lower:
                wallet_roles[transfer['to']] = "RECEIVER"

        # Generate gas entry
        journal_entries = []
        gas_entry = self._journal_gas_fee(tx, receipt, block_time, eth_price, wallet_roles)
        if gas_entry:
            journal_entries.append(gas_entry)

        return {
            "status": "success",
            "tx_hash": tx_hash,
            "tx_type": "ERC20_TRANSFER",
            "block": tx.blockNumber,
            "timestamp": block_time.isoformat(),
            "eth_price": float(eth_price),
            "gas_used": receipt.gasUsed,
            "from": tx['from'],
            "to": tx['to'],
            "value": float(Decimal(tx.value) / Decimal(10**18)),
            "function": "ERC20 Transfer",
            "events": [{"name": "Transfer", "args": t} for t in transfers],
            "journal_entries": [je.to_dict() for je in journal_entries],
            "wallet_roles": wallet_roles,
            "transfers": transfers,
            "summary": {
                "total_events": len(transfers),
                "total_journal_entries": len(journal_entries),
                "all_balanced": all(je.validate() for je in journal_entries),
                "involves_fund_wallets": len(wallet_roles) > 0
            }
        }

    def _decode_nft_transaction(self, tx, receipt, block, block_time, eth_price, fund_wallets, tx_hash):
        """Decode NFT transfer (ERC721/ERC1155)"""
        transfers = []
        fund_wallets_lower = [w.lower() for w in fund_wallets]

        # Extract NFT Transfer events
        for log in receipt.logs:
            if len(log.topics) > 0:
                # ERC721 Transfer
                if log.topics[0].hex() == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" and len(log.data) == 0:
                    try:
                        from_addr = Web3.to_checksum_address('0x' + log.topics[1].hex()[-40:])
                        to_addr = Web3.to_checksum_address('0x' + log.topics[2].hex()[-40:])
                        token_id = int.from_bytes(log.topics[3], 'big')

                        transfers.append({
                            'type': 'ERC721',
                            'from': from_addr,
                            'to': to_addr,
                            'token_id': token_id,
                            'contract': log.address
                        })
                    except:
                        pass

        wallet_roles = {}
        for transfer in transfers:
            if transfer['from'].lower() in fund_wallets_lower:
                wallet_roles[transfer['from']] = "NFT_SENDER"
            if transfer['to'].lower() in fund_wallets_lower:
                wallet_roles[transfer['to']] = "NFT_RECEIVER"

        journal_entries = []
        gas_entry = self._journal_gas_fee(tx, receipt, block_time, eth_price, wallet_roles)
        if gas_entry:
            journal_entries.append(gas_entry)

        return {
            "status": "success",
            "tx_hash": tx_hash,
            "tx_type": "NFT_TRANSFER",
            "block": tx.blockNumber,
            "timestamp": block_time.isoformat(),
            "eth_price": float(eth_price),
            "gas_used": receipt.gasUsed,
            "from": tx['from'],
            "to": tx['to'],
            "value": float(Decimal(tx.value) / Decimal(10**18)),
            "function": "NFT Transfer",
            "events": [{"name": "Transfer", "args": t} for t in transfers],
            "journal_entries": [je.to_dict() for je in journal_entries],
            "wallet_roles": wallet_roles,
            "transfers": transfers,
            "summary": {
                "total_events": len(transfers),
                "total_journal_entries": len(journal_entries),
                "all_balanced": all(je.validate() for je in journal_entries),
                "involves_fund_wallets": len(wallet_roles) > 0
            }
        }

    def _decode_eth_transaction(self, tx, receipt, block, block_time, eth_price, fund_wallets, tx_hash):
        """Decode simple ETH transfer"""
        fund_wallets_lower = [w.lower() for w in fund_wallets]
        eth_amount = Decimal(tx.value) / Decimal(10**18)

        wallet_roles = {}
        if tx['from'].lower() in fund_wallets_lower:
            wallet_roles[tx['from']] = "ETH_SENDER"
        if tx.get('to') and tx['to'].lower() in fund_wallets_lower:
            wallet_roles[tx['to']] = "ETH_RECEIVER"

        journal_entries = []
        gas_entry = self._journal_gas_fee(tx, receipt, block_time, eth_price, wallet_roles)
        if gas_entry:
            journal_entries.append(gas_entry)

        return {
            "status": "success",
            "tx_hash": tx_hash,
            "tx_type": "ETH_TRANSFER",
            "block": tx.blockNumber,
            "timestamp": block_time.isoformat(),
            "eth_price": float(eth_price),
            "gas_used": receipt.gasUsed,
            "from": tx['from'],
            "to": tx['to'],
            "value": float(eth_amount),
            "function": "ETH Transfer",
            "events": [],
            "journal_entries": [je.to_dict() for je in journal_entries],
            "wallet_roles": wallet_roles,
            "summary": {
                "total_events": 0,
                "total_journal_entries": len(journal_entries),
                "all_balanced": all(je.validate() for je in journal_entries),
                "involves_fund_wallets": len(wallet_roles) > 0
            }
        }

    def _decode_generic_transaction(self, tx, receipt, block, block_time, eth_price, fund_wallets, tx_hash):
        """Decode generic contract interaction"""
        fund_wallets_lower = [w.lower() for w in fund_wallets]

        wallet_roles = {}
        if tx['from'].lower() in fund_wallets_lower:
            wallet_roles[tx['from']] = "TX_SENDER"

        journal_entries = []
        gas_entry = self._journal_gas_fee(tx, receipt, block_time, eth_price, wallet_roles)
        if gas_entry:
            journal_entries.append(gas_entry)

        # Try to extract any Transfer events
        events = []
        for log in receipt.logs:
            if len(log.topics) > 0:
                events.append({
                    'address': log.address,
                    'topic0': log.topics[0].hex() if log.topics else None
                })

        return {
            "status": "success",
            "tx_hash": tx_hash,
            "tx_type": "CONTRACT_CALL",
            "block": tx.blockNumber,
            "timestamp": block_time.isoformat(),
            "eth_price": float(eth_price),
            "gas_used": receipt.gasUsed,
            "from": tx['from'],
            "to": tx['to'],
            "value": float(Decimal(tx.value) / Decimal(10**18)),
            "function": "Contract Call",
            "events": events,
            "journal_entries": [je.to_dict() for je in journal_entries],
            "wallet_roles": wallet_roles,
            "summary": {
                "total_events": len(events),
                "total_journal_entries": len(journal_entries),
                "all_balanced": all(je.validate() for je in journal_entries),
                "involves_fund_wallets": len(wallet_roles) > 0
            }
        }

    def _decode_function(self, tx) -> Tuple[str, Dict]:
        """Decode function call from transaction input and extract loan details"""
        if not self.blur_lending_contract or not tx.input:
            return "Unknown", {}

        try:
            func_obj, func_params = self.blur_lending_contract.decode_function_input(tx.input)
            func_name = func_obj.fn_name

            logger.info(f"Decoded function: {func_name}")

            # Special handling for repay() - extract lien struct and calculate interest
            if func_name == 'repay':
                lien = func_params.get('lien')
                lien_id = func_params.get('lienId')

                if lien and lien_id:
                    # Extract loan details from lien parameter
                    if isinstance(lien, (tuple, list)):
                        loan_amount = Decimal(lien[4]) / Decimal(10**18)
                        position = LoanPosition(
                            lien_id=lien_id,
                            lender=lien[0],
                            borrower=lien[1],
                            collection=lien[2],
                            token_id=lien[3],
                            principal=loan_amount,
                            rate=Decimal(lien[6]),
                            start_time=datetime.fromtimestamp(lien[5], tz=timezone.utc),
                            duration=90 * 24 * 3600,
                            auction_duration=lien[8]
                        )
                    else:
                        loan_amount = Decimal(lien.get('amount', 0)) / Decimal(10**18)
                        position = LoanPosition(
                            lien_id=lien_id,
                            lender=lien.get('lender'),
                            borrower=lien.get('borrower'),
                            collection=lien.get('collection'),
                            token_id=lien.get('tokenId', 0),
                            principal=loan_amount,
                            rate=Decimal(lien.get('rate', 0)),
                            start_time=datetime.fromtimestamp(lien.get('startTime', 0), tz=timezone.utc),
                            duration=90 * 24 * 3600,
                            auction_duration=lien.get('auctionDuration', 0)
                        )

                    self.positions[lien_id] = position

                    # Calculate interest as of repay block time
                    block = w3.eth.get_block(tx.blockNumber)
                    repay_time = datetime.fromtimestamp(block.timestamp, tz=timezone.utc)
                    time_elapsed = (repay_time - position.start_time).total_seconds()
                    total_debt = position.total_due(repay_time)
                    interest = position.calculate_interest(repay_time)

                    logger.info(f"Decoded Loan Details for repay():")
                    logger.info(f"  Lien ID: {lien_id}")
                    logger.info(f"  Principal: {position.principal:.6f} ETH")
                    logger.info(f"  Rate: {position.rate} bps ({float(position.rate)/100:.2f}% annual)")
                    logger.info(f"  Time Elapsed: {int(time_elapsed)} seconds ({time_elapsed/3600:.2f} hours)")
                    logger.info(f"  Interest (continuous): {interest:.8f} ETH")
                    logger.info(f"  Total Debt: {total_debt:.8f} ETH")

                    position.repayment_amount = total_debt
                    position.interest_paid = interest

            # Special handling for refinanceAuction() - extract old loan details
            elif func_name == 'refinanceAuction':
                lien = func_params.get('lien')
                lien_id = func_params.get('lienId')
                new_rate = func_params.get('rate')

                if lien and lien_id:
                    if isinstance(lien, (tuple, list)):
                        # Decode the lien tuple structure
                        old_lender = lien[0]
                        borrower = lien[1]
                        collection = lien[2]
                        token_id = lien[3]
                        original_amount = Decimal(lien[4]) / Decimal(10**18)
                        start_time = datetime.fromtimestamp(lien[5], tz=timezone.utc)
                        original_rate = Decimal(lien[6])
                        auction_start = lien[7]
                        auction_duration = lien[8]

                        # Create/update position with old loan data
                        old_position = LoanPosition(
                            lien_id=lien_id,
                            lender=old_lender,
                            borrower=borrower,
                            collection=collection,
                            token_id=token_id,
                            principal=original_amount,
                            rate=original_rate,
                            start_time=start_time,
                            duration=90 * 24 * 3600,
                            auction_duration=auction_duration,
                            status="IN_AUCTION"
                        )

                        self.positions[lien_id] = old_position

                        # Calculate amounts for refinancing
                        block = w3.eth.get_block(tx.blockNumber)
                        refinance_time = datetime.fromtimestamp(block.timestamp, tz=timezone.utc)
                        interest_accrued = old_position.calculate_interest(refinance_time)
                        full_debt = old_position.principal + interest_accrued

                        # Store refinance details
                        old_position.refinance_details = {
                            'new_lender': tx['from'],
                            'new_rate': new_rate if new_rate else original_rate,
                            'refinance_time': refinance_time,
                            'old_debt': full_debt,
                            'interest_at_refinance': interest_accrued
                        }

                        logger.info(f"Decoded Refinance Auction Details:")
                        logger.info(f"  Lien ID: {lien_id}")
                        logger.info(f"  Old Lender: {old_lender[:8]}...")
                        logger.info(f"  Borrower: {borrower[:8]}...")
                        logger.info(f"  Original Principal: {original_amount:.6f} ETH")
                        logger.info(f"  Interest Accrued: {interest_accrued:.8f} ETH")
                        logger.info(f"  Total Debt: {full_debt:.8f} ETH")
                        logger.info(f"  New Lender: {tx['from'][:8]}...")

            return func_name, func_params

        except Exception as e:
            logger.warning(f"Failed to decode function: {e}")
            return "Unknown", {}

    def _decode_events(self, receipt) -> List[Any]:
        """Decode events from transaction receipt"""
        events = []

        if not self.blur_lending_contract:
            return events

        # List of known Blur Lending events to check
        blur_lending_events = [
            'LoanOfferTaken', 'Repay', 'Refinance', 'StartAuction',
            'Seize', 'BuyLocked', 'OfferCancelled', 'NonceIncremented'
        ]

        # Try to decode each known event type
        for event_name in blur_lending_events:
            try:
                if hasattr(self.blur_lending_contract.events, event_name):
                    event_instance = getattr(self.blur_lending_contract.events, event_name)
                    decoded_events = event_instance().process_receipt(receipt)
                    events.extend(decoded_events)
            except Exception as e:
                # Event not found in receipt, continue
                pass

        # Also check Blur Pool events (but skip Transfer - handled separately in pool_transfers)
        if self.blur_pool_contract:
            blur_pool_events = ['Deposit', 'Withdraw']
            for event_name in blur_pool_events:
                try:
                    if hasattr(self.blur_pool_contract.events, event_name):
                        event_instance = getattr(self.blur_pool_contract.events, event_name)
                        decoded_events = event_instance().process_receipt(receipt)
                        events.extend(decoded_events)
                except Exception as e:
                    pass

        # Decode NFT events from all logs
        for log in receipt.logs:
            try:
                # Skip if already processed by Blur contracts
                if log.address.lower() in [BLUR_LENDING.lower(), BLUR_POOL.lower()]:
                    continue

                # Decode standard ERC721 events
                if len(log.topics) > 0:
                    topic0 = log.topics[0].hex()
                    topic0_normalized = topic0 if topic0.startswith('0x') else f'0x{topic0}'

                    # Transfer event: Transfer(address indexed from, address indexed to, uint256 indexed tokenId)
                    if topic0_normalized == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef":
                        if len(log.topics) == 4:  # ERC721 Transfer has 4 topics
                            from_addr = Web3.to_checksum_address('0x' + log.topics[1].hex()[-40:])
                            to_addr = Web3.to_checksum_address('0x' + log.topics[2].hex()[-40:])
                            token_id = int(log.topics[3].hex(), 16)
                            events.append({
                                'event': 'NFT_Transfer',
                                'args': {
                                    'collection': log.address,
                                    'from': from_addr,
                                    'to': to_addr,
                                    'tokenId': token_id
                                }
                            })

                    # Approval event: Approval(address indexed owner, address indexed approved, uint256 indexed tokenId)
                    elif topic0_normalized == "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925":
                        if len(log.topics) == 4:
                            owner = Web3.to_checksum_address('0x' + log.topics[1].hex()[-40:])
                            approved = Web3.to_checksum_address('0x' + log.topics[2].hex()[-40:])
                            token_id = int(log.topics[3].hex(), 16)
                            events.append({
                                'event': 'NFT_Approval',
                                'args': {
                                    'collection': log.address,
                                    'owner': owner,
                                    'approved': approved,
                                    'tokenId': token_id
                                }
                            })
            except Exception as e:
                logger.warning(f"Failed to decode NFT event from log: {e}")

        return events

    def _decode_pool_transfers(self, receipt, fund_wallets: List[str]) -> List[Dict]:
        """Decode ETH transfers involving Blur Pool"""
        transfers = []
        fund_wallets_lower = [w.lower() for w in fund_wallets]
        pool_lower = BLUR_POOL.lower()

        # Look for Transfer events (ERC20-style for Pool shares)
        for log in receipt.logs:
            log_addr = log.address.lower()

            if log_addr == pool_lower and len(log.topics) > 0:
                topic0 = log.topics[0].hex()

                # Transfer event signature (normalize with 0x prefix)
                topic0_normalized = topic0 if topic0.startswith('0x') else f'0x{topic0}'
                if topic0_normalized == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef":
                    try:
                        from_addr = Web3.to_checksum_address('0x' + log.topics[1].hex()[-40:])
                        to_addr = Web3.to_checksum_address('0x' + log.topics[2].hex()[-40:])
                        amount = Decimal(int.from_bytes(log.data, 'big')) / Decimal(10**18)

                        if from_addr.lower() in fund_wallets_lower or to_addr.lower() in fund_wallets_lower:
                            transfers.append({
                                'from': from_addr,
                                'to': to_addr,
                                'amount': float(amount),
                                'direction': 'IN' if to_addr.lower() in fund_wallets_lower else 'OUT'
                            })
                    except Exception as e:
                        logger.warning(f"Failed to decode pool transfer: {e}")

        return transfers

    def _determine_wallet_roles(self, tx, receipt, fund_wallets: List[str], events: List) -> Dict[str, str]:
        """Determine the role of each fund wallet in the transaction (comprehensive)"""
        wallet_roles = {}
        fund_wallets_lower = [w.lower() for w in fund_wallets]

        # Decode function if not already done
        try:
            func_obj, func_params = self.blur_lending_contract.decode_function_input(tx.input)
            func_name = func_obj.fn_name if func_obj else None
        except:
            func_name = None

        # Function-based role determination
        if func_name == "deposit":
            if tx['from'].lower() in fund_wallets_lower:
                wallet_roles[tx['from'].lower()] = "DEPOSITOR"

        elif func_name == "withdraw":
            if tx['from'].lower() in fund_wallets_lower:
                wallet_roles[tx['from'].lower()] = "WITHDRAWER"

        elif func_name == "repay":
            lien_id = func_params.get('lienId') if func_params else None
            if lien_id and lien_id in self.positions:
                position = self.positions[lien_id]
                if position.lender.lower() in fund_wallets_lower:
                    wallet_roles[position.lender.lower()] = "LENDER_RECEIVING"
                if position.borrower.lower() in fund_wallets_lower:
                    wallet_roles[position.borrower.lower()] = "BORROWER_REPAYING"
            elif tx['from'].lower() in fund_wallets_lower:
                wallet_roles[tx['from'].lower()] = "BORROWER_REPAYING"

        elif func_name in ["borrow", "buyToBorrow", "buyToBorrowETH", "buyToBorrowV2"]:
            if tx['from'].lower() in fund_wallets_lower:
                wallet_roles[tx['from'].lower()] = "BORROWER"

        elif func_name in ["refinance", "refinanceAuction", "borrowerRefinance"]:
            if tx['from'].lower() in fund_wallets_lower:
                wallet_roles[tx['from'].lower()] = "NEW_LENDER"

        elif func_name == "startAuction":
            if tx['from'].lower() in fund_wallets_lower:
                wallet_roles[tx['from'].lower()] = "LENDER_CALLING_LOAN"

        elif func_name == "seize":
            if tx['from'].lower() in fund_wallets_lower:
                wallet_roles[tx['from'].lower()] = "LENDER_SEIZING"

        elif func_name in ["buyLocked", "buyLockedETH"]:
            if tx['from'].lower() in fund_wallets_lower:
                wallet_roles[tx['from'].lower()] = "LIQUIDATOR"

        # Event-based role determination (supplement/override)
        for event in events:
            event_name = event['event'] if hasattr(event, 'event') else event.get('event', '')
            args = event['args'] if hasattr(event, 'args') else event.get('args', {})

            if event_name == "LoanOfferTaken":
                lender = args.get("lender", "").lower()
                borrower = args.get("borrower", "").lower()
                if lender in fund_wallets_lower:
                    wallet_roles[lender] = "LENDER"
                if borrower in fund_wallets_lower:
                    wallet_roles[borrower] = "BORROWER"

            elif event_name == "Repay":
                lien_id = args.get("lienId")
                if lien_id in self.positions:
                    position = self.positions[lien_id]
                    if position.lender.lower() in fund_wallets_lower:
                        wallet_roles[position.lender.lower()] = "LENDER_RECEIVING"
                    if position.borrower.lower() in fund_wallets_lower:
                        wallet_roles[position.borrower.lower()] = "BORROWER_REPAYING"

            elif event_name == "Refinance":
                new_lender = args.get("newLender", "").lower()
                lien_id = args.get("lienId")

                if new_lender in fund_wallets_lower:
                    wallet_roles[new_lender] = "NEW_LENDER"

                if lien_id in self.positions:
                    old_position = self.positions[lien_id]
                    if old_position.lender.lower() in fund_wallets_lower:
                        wallet_roles[old_position.lender.lower()] = "OLD_LENDER"
                    if old_position.borrower.lower() in fund_wallets_lower:
                        wallet_roles[old_position.borrower.lower()] = "BORROWER_REFINANCING"

            elif event_name == "StartAuction":
                lien_id = args.get("lienId")
                if lien_id in self.positions:
                    position = self.positions[lien_id]
                    if position.lender.lower() in fund_wallets_lower:
                        wallet_roles[position.lender.lower()] = "LENDER_CALLING_LOAN"

            elif event_name == "Seize":
                lien_id = args.get("lienId")
                if lien_id in self.positions:
                    position = self.positions[lien_id]
                    if position.lender.lower() in fund_wallets_lower:
                        wallet_roles[position.lender.lower()] = "LENDER_SEIZING"

            elif event_name == "BuyLocked":
                buyer = args.get("buyer", "").lower()
                if buyer in fund_wallets_lower:
                    wallet_roles[buyer] = "LIQUIDATOR"

        # Check for Pool transfers to identify depositor/withdrawer roles
        for log in receipt.logs:
            if log.address.lower() == BLUR_POOL.lower() and len(log.topics) >= 3:
                from_addr = ('0x' + log.topics[1].hex()[-40:]).lower()
                to_addr = ('0x' + log.topics[2].hex()[-40:]).lower()

                if from_addr in fund_wallets_lower and not wallet_roles.get(from_addr):
                    wallet_roles[from_addr] = "POOL_DEPOSITOR"

                if to_addr in fund_wallets_lower and not wallet_roles.get(to_addr):
                    wallet_roles[to_addr] = "POOL_WITHDRAWER"

        return wallet_roles

    def _generate_journal_entries(self, tx, receipt, block_time, eth_price,
                                 function_name, function_params, events,
                                 pool_transfers, wallet_roles, fund_wallets) -> List[JournalEntry]:
        """Generate journal entries based on decoded transaction"""
        entries = []

        # Only generate entries if fund wallets are involved
        if not wallet_roles:
            return entries

        # Process based on function type
        if function_name in ["repay", "Repay"]:
            entries.extend(self._journal_loan_repayment(
                tx, block_time, eth_price, events, pool_transfers, wallet_roles
            ))
        elif function_name in ["refinance", "Refinance", "borrowerRefinance"]:
            entries.extend(self._journal_refinance(
                tx, block_time, eth_price, events, pool_transfers, wallet_roles
            ))
        elif function_name in ["refinanceAuction", "RefinanceAuction"]:
            entries.extend(self._journal_refinance_auction(
                tx, block_time, eth_price, events, pool_transfers, wallet_roles
            ))
        elif function_name in ["buyToBorrow", "BuyToBorrow"]:
            entries.extend(self._journal_loan_origination(
                tx, block_time, eth_price, events, pool_transfers, wallet_roles
            ))

        # Process pool transfers (deposits/withdrawals)
        if pool_transfers:
            entries.extend(self._journal_pool_transfers(
                tx, block_time, eth_price, pool_transfers, wallet_roles
            ))

        # Always add gas fee entry for gas payer
        gas_entry = self._journal_gas_fee(tx, receipt, block_time, eth_price, wallet_roles)
        if gas_entry:
            entries.append(gas_entry)

        return entries

    def _journal_loan_repayment(self, tx, block_time, eth_price, events, pool_transfers, wallet_roles) -> List[JournalEntry]:
        """Create journal entry for loan repayment with principal/interest split"""
        entries = []

        # Find Repay event
        for event in events:
            event_name = event['event'] if hasattr(event, 'event') else event.get('event', '')
            if event_name != "Repay":
                continue

            args = event['args'] if hasattr(event, 'args') else event.get('args', {})
            lien_id = args.get('lienId')
            position = self.positions.get(lien_id)

            if not position:
                logger.warning(f"No position found for lien_id {lien_id}")
                continue

            wallet_address = ""
            wallet_role = "UNKNOWN"

            if position.lender.lower() in [w.lower() for w in wallet_roles.keys()]:
                wallet_address = position.lender
                wallet_role = wallet_roles.get(position.lender.lower(), "LENDER_RECEIVING")
            elif position.borrower.lower() in [w.lower() for w in wallet_roles.keys()]:
                wallet_address = position.borrower
                wallet_role = wallet_roles.get(position.borrower.lower(), "BORROWER_REPAYING")

            if not wallet_address:
                continue

            entry = JournalEntry(
                entry_id=f"JE_{tx.hash.hex()[:8]}_REPAY_{lien_id}",
                date=block_time,
                description=f"NFT Loan Repayment - Lien #{lien_id} ({wallet_role})",
                tx_hash=tx.hash.hex(),
                event_type=AccountingEventType.LOAN_REPAYMENT,
                wallet_address=wallet_address,
                wallet_role=wallet_role
            )

            # Calculate principal and interest
            principal_eth = position.principal

            # Use actual repayment if available from pool transfers, otherwise calculate
            if hasattr(position, 'actual_repayment') and position.actual_repayment:
                total_eth = position.actual_repayment
                interest_eth = position.interest_paid
            else:
                interest_eth = position.calculate_interest(block_time)
                total_eth = principal_eth + interest_eth

            logger.info(f"Creating Journal Entry (Native ETH):")
            logger.info(f"  Wallet Role: {wallet_role}")
            logger.info(f"  Principal: {principal_eth:.6f} ETH")
            logger.info(f"  Interest: {interest_eth:.6f} ETH")
            logger.info(f"  Total: {total_eth:.6f} ETH")

            if wallet_role == "LENDER_RECEIVING":
                entry.add_debit(self.accounts["eth_wallet"], total_eth)
                entry.add_credit(self.accounts["loans_receivable"], principal_eth)
                entry.add_credit(self.accounts["interest_income"], interest_eth)
                entry.add_tax_implication(TaxTreatment.NON_TAXABLE, principal_eth * eth_price, "Loan principal repayment")
                entry.add_tax_implication(TaxTreatment.TAXABLE_INCOME, interest_eth * eth_price, "Interest income")

            elif wallet_role == "BORROWER_REPAYING":
                entry.add_debit(self.accounts["loan_payable"], principal_eth)
                entry.add_debit(self.accounts["interest_expense"], interest_eth)
                entry.add_credit(self.accounts["eth_wallet"], total_eth)
                entry.add_tax_implication(TaxTreatment.NON_TAXABLE, principal_eth * eth_price, "Loan principal repayment")
                entry.add_tax_implication(TaxTreatment.DEDUCTIBLE_EXPENSE, interest_eth * eth_price, "Interest expense")

            entries.append(entry)

        return entries

    def _journal_refinance(self, tx, block_time, eth_price, events, pool_transfers, wallet_roles) -> List[JournalEntry]:
        """Create journal entries for standard refinancing (3 parties: old lender, new lender, borrower)"""
        entries = []

        # Find Refinance event
        for event in events:
            event_name = event['event'] if hasattr(event, 'event') else event.get('event', '')
            if event_name != "Refinance":
                continue

            args = event['args'] if hasattr(event, 'args') else event.get('args', {})
            lien_id = args.get('lienId')
            new_lender = args.get('newLender')
            new_amount = Decimal(args.get('newAmount', 0)) / Decimal(10**18)

            old_position = self.positions.get(lien_id)

            # NEW LENDER ENTRY - Create new loan
            if new_lender and new_lender.lower() in [w.lower() for w in wallet_roles.keys()]:
                new_lender_entry = JournalEntry(
                    entry_id=f"JE_{tx.hash.hex()[:8]}_NEW_{lien_id}",
                    date=block_time,
                    description=f"Refinance New Loan - Lien #{lien_id} (NEW_LENDER)",
                    tx_hash=tx.hash.hex(),
                    event_type=AccountingEventType.LOAN_ORIGINATION,
                    wallet_address=new_lender,
                    wallet_role="NEW_LENDER"
                )
                new_lender_entry.add_debit(self.accounts["loans_receivable"], new_amount)
                new_lender_entry.add_credit(self.accounts["blur_pool"], new_amount)
                entries.append(new_lender_entry)

            # If no old position, create one from event data
            if not old_position:
                self.positions[lien_id] = LoanPosition(
                    lien_id=lien_id,
                    lender=new_lender,
                    borrower="Unknown",
                    collection=args.get('collection', ''),
                    token_id=0,
                    principal=new_amount,
                    rate=Decimal(args.get('newRate', 0)),
                    start_time=block_time,
                    duration=90 * 24 * 3600,
                    auction_duration=args.get('newAuctionDuration', 0),
                    status="ACTIVE"
                )
                logger.info(f"Created new position from refinance: Lien #{lien_id}")
                return entries

            # OLD LENDER PAYOFF ENTRY
            principal = old_position.principal
            interest = old_position.calculate_interest(block_time)
            payoff = principal + interest

            if old_position.lender.lower() in [w.lower() for w in wallet_roles.keys()]:
                old_lender_entry = JournalEntry(
                    entry_id=f"JE_{tx.hash.hex()[:8]}_OLD_{lien_id}",
                    date=block_time,
                    description=f"Refinance Payoff - Lien #{lien_id} (OLD_LENDER)",
                    tx_hash=tx.hash.hex(),
                    event_type=AccountingEventType.LOAN_REFINANCE,
                    wallet_address=old_position.lender,
                    wallet_role="OLD_LENDER"
                )
                old_lender_entry.add_debit(self.accounts["blur_pool"], payoff)
                old_lender_entry.add_credit(self.accounts["loans_receivable"], principal)
                old_lender_entry.add_credit(self.accounts["interest_income"], interest)
                old_lender_entry.add_tax_implication(TaxTreatment.TAXABLE_INCOME, interest * eth_price,
                                                    "Interest income from refinancing")
                entries.append(old_lender_entry)

            # BORROWER ENTRY - Close old loan, open new
            if old_position.borrower.lower() in [w.lower() for w in wallet_roles.keys()]:
                borrower_entry = JournalEntry(
                    entry_id=f"JE_{tx.hash.hex()[:8]}_BREFI_{lien_id}",
                    date=block_time,
                    description=f"Refinance - Lien #{lien_id} (BORROWER_REFINANCING)",
                    tx_hash=tx.hash.hex(),
                    event_type=AccountingEventType.LOAN_REFINANCE,
                    wallet_address=old_position.borrower,
                    wallet_role="BORROWER_REFINANCING"
                )
                borrower_entry.add_debit(self.accounts["loan_payable"], principal)
                borrower_entry.add_debit(self.accounts["interest_expense"], interest)
                borrower_entry.add_credit(self.accounts["blur_pool"], payoff)
                borrower_entry.add_debit(self.accounts["blur_pool"], new_amount)
                borrower_entry.add_credit(self.accounts["loan_payable"], new_amount)
                borrower_entry.add_tax_implication(TaxTreatment.DEDUCTIBLE_EXPENSE, interest * eth_price,
                                                  "Interest expense on refinancing")
                entries.append(borrower_entry)

        return entries

    def _journal_refinance_auction(self, tx, block_time, eth_price, events, pool_transfers, wallet_roles) -> List[JournalEntry]:
        """Create journal entries for auction refinancing with pool transfer analysis"""
        entries = []

        # Find Refinance event from auction
        for event in events:
            event_name = event['event'] if hasattr(event, 'event') else event.get('event', '')
            if event_name != "Refinance":
                continue

            args = event['args'] if hasattr(event, 'args') else event.get('args', {})
            lien_id = args.get('lienId')
            new_lender = args.get('newLender')
            new_amount = Decimal(args.get('newAmount', 0)) / Decimal(10**18)
            new_rate = Decimal(args.get('newRate', 0))
            collection = args.get('collection')

            old_position = self.positions.get(lien_id)

            # NEW LENDER ENTRY - Funding the new loan
            if new_lender and new_lender.lower() in [w.lower() for w in wallet_roles.keys()]:
                new_lender_entry = JournalEntry(
                    entry_id=f"JE_{tx.hash.hex()[:8]}_NEW_LENDER_{lien_id}",
                    date=block_time,
                    description=f"Refinance Auction - New Loan Funded - Lien #{lien_id}",
                    tx_hash=tx.hash.hex(),
                    event_type=AccountingEventType.LOAN_ORIGINATION,
                    wallet_address=new_lender,
                    wallet_role="NEW_LENDER"
                )

                new_lender_entry.add_debit(self.accounts["loans_receivable"], new_amount)
                new_lender_entry.add_credit(self.accounts["blur_pool"], new_amount)
                new_lender_entry.add_debit(self.accounts["nft_collateral"], Decimal(0))  # Non-monetary

                new_lender_entry.add_tax_implication(
                    TaxTreatment.NON_TAXABLE,
                    new_amount * eth_price,
                    "Loan principal disbursement in refinancing auction"
                )
                entries.append(new_lender_entry)

            # Check pool transfers for old lender payoff
            for transfer in pool_transfers:
                transfer_to = Web3.to_checksum_address(transfer['to'])
                if transfer_to.lower() in [w.lower() for w in wallet_roles.keys()]:
                    # One of our wallets received a payoff
                    old_lender_entry = JournalEntry(
                        entry_id=f"JE_{tx.hash.hex()[:8]}_OLD_LENDER_{lien_id}",
                        date=block_time,
                        description=f"Refinance Auction - Loan Payoff Received - Lien #{lien_id}",
                        tx_hash=tx.hash.hex(),
                        event_type=AccountingEventType.LOAN_REFINANCE,
                        wallet_address=transfer_to,
                        wallet_role="OLD_LENDER"
                    )

                    # Receive auction proceeds
                    actual_proceeds = Decimal(str(transfer['amount']))
                    old_lender_entry.add_debit(self.accounts["blur_pool"], actual_proceeds)
                    old_lender_entry.add_credit(self.accounts["loans_receivable"], actual_proceeds)

                    old_lender_entry.add_tax_implication(
                        TaxTreatment.NON_TAXABLE,
                        actual_proceeds * eth_price,
                        "Loan principal recovered via refinancing auction"
                    )

                    # Remove NFT collateral
                    old_lender_entry.add_credit(self.accounts["nft_collateral"], Decimal(0))  # Non-monetary

                    entries.append(old_lender_entry)

            # Update or create position
            if old_position:
                old_position.lender = new_lender
                old_position.principal = new_amount
                old_position.rate = new_rate
                old_position.start_time = block_time
                old_position.status = "ACTIVE"
            else:
                self.positions[lien_id] = LoanPosition(
                    lien_id=lien_id,
                    lender=new_lender,
                    borrower="Unknown",
                    collection=collection,
                    token_id=0,
                    principal=new_amount,
                    rate=new_rate,
                    start_time=block_time,
                    duration=90 * 24 * 3600,
                    auction_duration=args.get('newAuctionDuration', 0),
                    status="ACTIVE"
                )

            logger.info(f"Generated {len(entries)} journal entries for refinance auction")

        return entries

    def _journal_loan_origination(self, tx, block_time, eth_price, events, pool_transfers, wallet_roles) -> List[JournalEntry]:
        """Create journal entry for loan origination"""
        entries = []

        # Find LoanOfferTaken event
        for event in events:
            event_name = event['event'] if hasattr(event, 'event') else event.get('event', '')
            if event_name != "LoanOfferTaken":
                continue

            args = event['args'] if hasattr(event, 'args') else event.get('args', {})
            lien_id = args.get('lienId')
            lender = args.get('lender')
            borrower = args.get('borrower')
            loan_amount_wei = args.get('loanAmount', args.get('amount', 0))
            loan_eth = Decimal(loan_amount_wei) / Decimal(10**18)

            # Determine which fund wallet is involved and their role
            wallet_address = ""
            wallet_role = "UNKNOWN"

            if lender.lower() in [w.lower() for w in wallet_roles.keys()]:
                wallet_address = lender
                wallet_role = wallet_roles.get(lender.lower(), "LENDER")
            elif borrower.lower() in [w.lower() for w in wallet_roles.keys()]:
                wallet_address = borrower
                wallet_role = wallet_roles.get(borrower.lower(), "BORROWER")

            if not wallet_address:
                continue

            entry = JournalEntry(
                entry_id=f"JE_{tx.hash.hex()[:8]}_{lien_id}",
                date=block_time,
                description=f"NFT Loan Origination - Lien #{lien_id} ({wallet_role})",
                tx_hash=tx.hash.hex(),
                event_type=AccountingEventType.LOAN_ORIGINATION,
                wallet_address=wallet_address,
                wallet_role=wallet_role
            )

            if wallet_role == "LENDER":
                entry.add_debit(self.accounts["loans_receivable"], loan_eth)
                entry.add_credit(self.accounts["blur_pool"], loan_eth)
                entry.add_tax_implication(TaxTreatment.NON_TAXABLE, loan_eth * eth_price, "Loan principal disbursement")
            elif wallet_role == "BORROWER":
                entry.add_debit(self.accounts["blur_pool"], loan_eth)
                entry.add_credit(self.accounts["loan_payable"], loan_eth)
                entry.add_tax_implication(TaxTreatment.NON_TAXABLE, loan_eth * eth_price, "Loan proceeds received")

            entries.append(entry)

        return entries

    def _journal_pool_transfers(self, tx, block_time, eth_price, pool_transfers, wallet_roles) -> List[JournalEntry]:
        """Create journal entries for Blur Pool deposits/withdrawals"""
        entries = []

        for transfer in pool_transfers:
            from_addr = transfer['from']
            to_addr = transfer['to']
            amount = Decimal(str(transfer['amount']))
            direction = transfer.get('direction', 'UNKNOWN')

            # Determine which fund wallet is involved
            wallet_address = None
            wallet_role = None

            if to_addr.lower() in [w.lower() for w in wallet_roles.keys()]:
                wallet_address = to_addr
                wallet_role = wallet_roles.get(to_addr.lower(), "POOL_WITHDRAWER")
            elif from_addr.lower() in [w.lower() for w in wallet_roles.keys()]:
                wallet_address = from_addr
                wallet_role = wallet_roles.get(from_addr.lower(), "POOL_DEPOSITOR")

            if not wallet_address:
                continue

            if direction == 'IN' or wallet_role == "POOL_WITHDRAWER":
                # Receiving pool tokens (withdrawal from pool or receiving as proceeds)
                entry = JournalEntry(
                    entry_id=f"JE_{tx.hash.hex()[:8]}_POOL_IN",
                    date=block_time,
                    description=f"Blur Pool Tokens Received - {amount:.6f} BLUR POOL",
                    tx_hash=tx.hash.hex(),
                    event_type=AccountingEventType.LOAN_REFINANCE,  # Pool activity related to lending
                    wallet_address=wallet_address,
                    wallet_role=wallet_role
                )

                # Debit: Blur Pool asset (we received pool tokens)
                # Credit: ETH/Cash (what we gave up - typically happens in a refinance)
                # Since this is pool token receipt, we're tracking the pool share received
                entry.add_debit(self.accounts["blur_pool"], amount)
                entry.add_credit(self.accounts["loans_receivable"], amount)

                entry.add_tax_implication(
                    TaxTreatment.NON_TAXABLE,
                    amount * eth_price,
                    "Blur Pool tokens received (represents loan principal repaid)"
                )

                entries.append(entry)

            elif direction == 'OUT' or wallet_role == "POOL_DEPOSITOR":
                # Sending pool tokens (deposit to pool or using for lending)
                entry = JournalEntry(
                    entry_id=f"JE_{tx.hash.hex()[:8]}_POOL_OUT",
                    date=block_time,
                    description=f"Blur Pool Tokens Sent - {amount:.6f} BLUR POOL",
                    tx_hash=tx.hash.hex(),
                    event_type=AccountingEventType.LOAN_ORIGINATION,  # Pool activity for new lending
                    wallet_address=wallet_address,
                    wallet_role=wallet_role
                )

                # Debit: Loans Receivable (we're deploying capital)
                # Credit: Blur Pool (we're giving up pool tokens)
                entry.add_debit(self.accounts["loans_receivable"], amount)
                entry.add_credit(self.accounts["blur_pool"], amount)

                entry.add_tax_implication(
                    TaxTreatment.NON_TAXABLE,
                    amount * eth_price,
                    "Blur Pool tokens sent (loan principal disbursement)"
                )

                entries.append(entry)

        return entries

    def _journal_gas_fee(self, tx, receipt, block_time, eth_price, wallet_roles) -> Optional[JournalEntry]:
        """Journal entry for gas fees"""
        gas_used = receipt.gasUsed
        gas_price = tx.gasPrice if hasattr(tx, 'gasPrice') else tx.get('gasPrice', 0)
        gas_fee = Decimal(gas_used * gas_price) / Decimal(10**18)

        if gas_fee == 0:
            return None

        gas_payer = None
        for addr, role in wallet_roles.items():
            if role == "GAS_PAYER":
                gas_payer = addr
                break

        if not gas_payer:
            return None

        entry = JournalEntry(
            entry_id=f"{tx.hash.hex()}_gas",
            date=block_time,
            description=f"Gas fee for {tx.hash.hex()[:10]}...",
            tx_hash=tx.hash.hex(),
            event_type=AccountingEventType.FEE_PAYMENT,
            wallet_address=gas_payer,
            wallet_role="GAS_PAYER"
        )

        entry.add_debit(self.accounts["gas_fees"], gas_fee)
        entry.add_credit(self.accounts["eth_wallet"], gas_fee)
        entry.add_tax_implication(
            TaxTreatment.DEDUCTIBLE_EXPENSE,
            gas_fee * eth_price,
            f"Gas fee: {float(gas_fee):.6f} ETH (${float(gas_fee * eth_price):.2f})"
        )

        return entry

    def _event_to_dict(self, event) -> Dict:
        """Convert Web3 event to dictionary"""
        if hasattr(event, 'event'):
            return {
                'event': event.event,  # Use 'event' key consistently
                'args': dict(event.args) if hasattr(event, 'args') else {}
            }
        return event

    def format_journal_entries_for_display(self, tx_hash: str) -> List[Dict]:
        """Format journal entries for UI display"""
        decode_result = self.decoded_cache.get(tx_hash)
        if not decode_result or decode_result.get("status") != "success":
            return []

        return decode_result.get("journal_entries", [])

    def clear_cache(self, tx_hash: str = None):
        """Clear decoded cache for a specific transaction or all"""
        if tx_hash:
            self.decoded_cache.pop(tx_hash, None)
        else:
            self.decoded_cache.clear()


# Global decoder instance
blur_auto_decoder = BlurAutoDecoder()
