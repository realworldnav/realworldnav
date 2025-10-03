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
        """Decode function call from transaction input"""
        if not self.blur_lending_contract or not tx.input:
            return "Unknown", {}

        try:
            func_obj, func_params = self.blur_lending_contract.decode_function_input(tx.input)
            return func_obj.fn_name, func_params
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

        # Also check Blur Pool events
        if self.blur_pool_contract:
            blur_pool_events = ['Transfer', 'Deposit', 'Withdraw']
            for event_name in blur_pool_events:
                try:
                    if hasattr(self.blur_pool_contract.events, event_name):
                        event_instance = getattr(self.blur_pool_contract.events, event_name)
                        decoded_events = event_instance().process_receipt(receipt)
                        events.extend(decoded_events)
                except Exception as e:
                    pass

        return events

    def _decode_pool_transfers(self, receipt, fund_wallets: List[str]) -> List[Dict]:
        """Decode ETH transfers involving Blur Pool"""
        transfers = []
        fund_wallets_lower = [w.lower() for w in fund_wallets]
        pool_lower = BLUR_POOL.lower()

        # Look for Transfer events (ERC20-style for Pool shares)
        for log in receipt.logs:
            if log.address.lower() == pool_lower and len(log.topics) > 0:
                # Transfer event signature
                if log.topics[0].hex() == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef":
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
        """Determine roles of fund wallets in transaction"""
        roles = {}
        fund_wallets_lower = [w.lower() for w in fund_wallets]

        # Check transaction sender
        if tx['from'].lower() in fund_wallets_lower:
            roles[tx['from']] = "GAS_PAYER"

        # Analyze events for roles
        for event in events:
            event_name = event['event'] if hasattr(event, 'event') else event.get('event', '')

            if event_name == "LoanOfferTaken":
                args = event['args'] if hasattr(event, 'args') else event.get('args', {})
                lender = args.get('lender', '')
                borrower = args.get('borrower', '')

                if lender.lower() in fund_wallets_lower:
                    roles[lender] = "LENDER"
                if borrower.lower() in fund_wallets_lower:
                    roles[borrower] = "BORROWER"

            elif event_name in ["Repay", "Refinance"]:
                args = event['args'] if hasattr(event, 'args') else event.get('args', {})
                lender = args.get('lender', '')
                borrower = args.get('borrower', '')

                if lender.lower() in fund_wallets_lower:
                    roles[lender] = "LENDER_RECEIVING"
                if borrower.lower() in fund_wallets_lower:
                    roles[borrower] = "BORROWER_REPAYING"

        return roles

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
        elif function_name in ["refinance", "Refinance"]:
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

        # Always add gas fee entry for gas payer
        gas_entry = self._journal_gas_fee(tx, receipt, block_time, eth_price, wallet_roles)
        if gas_entry:
            entries.append(gas_entry)

        return entries

    def _journal_loan_repayment(self, tx, block_time, eth_price, events, pool_transfers, wallet_roles) -> List[JournalEntry]:
        """Journal entries for loan repayment"""
        entries = []
        # TODO: Implement based on reference logic
        return entries

    def _journal_refinance(self, tx, block_time, eth_price, events, pool_transfers, wallet_roles) -> List[JournalEntry]:
        """Journal entries for refinancing"""
        entries = []
        # TODO: Implement based on reference logic
        return entries

    def _journal_refinance_auction(self, tx, block_time, eth_price, events, pool_transfers, wallet_roles) -> List[JournalEntry]:
        """Journal entries for auction refinancing"""
        entries = []
        # TODO: Implement based on reference logic
        return entries

    def _journal_loan_origination(self, tx, block_time, eth_price, events, pool_transfers, wallet_roles) -> List[JournalEntry]:
        """Journal entries for new loan origination"""
        entries = []
        # TODO: Implement based on reference logic
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
                'name': event.event,
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
