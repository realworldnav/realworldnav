
"""#Blur Decoder

##Function/Configuration
"""

# directory where JSON ABIs are stored
ABI_DIR = Path("/content/drive/MyDrive/Drip_Capital/smart_contract_ABIs")
print(ABI_DIR)

# Load wallet metadata (your existing code)
wallet_ID_mapping_file_df = pd.read_excel('/content/drive/MyDrive/Drip_Capital/drip_capital_wallet_ID_mapping.xlsx', engine='openpyxl')
wallet_ID_mapping_file_df = wallet_ID_mapping_file_df.fillna('')

# Load wallet metadata
wallet_metadata = {}
for _, row in wallet_ID_mapping_file_df.iterrows():
    # Store with checksummed key
    checksummed_address = Web3.to_checksum_address(row['wallet_address'].lower())
    wallet_metadata[checksummed_address] = {
        "friendly_name": row["friendly_name"],
        "wallet_address": checksummed_address,  # Store checksummed version
        "platform_variable_name": row["platform_variable_name"],
        "crypto_type": row["crypto_type"],
        "category": row["category"],
        "fund_id": row["fund_id"],
        "group": row["group"],
        "subgroup": row["subgroup"],
    }

# Get fund wallets - these will be checksummed
fund_wallet_ids = [
    Web3.to_checksum_address(m["wallet_address"])
    for m in wallet_metadata.values()
    if m["category"] == "fund"
]

"""
Blur NFT Lending Protocol Decoder and Accounting System
Complete institutional-grade accounting infrastructure for NFT-collateralized lending
ASC 946 compliant with full tax treatment and multi-wallet support
"""

from web3 import Web3
from datetime import datetime, timezone
from decimal import Decimal, getcontext
from typing import Dict, List, Tuple, Optional, Any, Union
from dataclasses import dataclass, field
from enum import Enum
import json
import math
from collections import defaultdict
from functools import lru_cache
import requests
from pathlib import Path
import pandas as pd
import time

# Set decimal precision for financial calculations
getcontext().prec = 28

# ============================================================================
# CONFIGURATION
# ============================================================================

# API Keys (as provided)
INFURA_API_KEY = "02321aab179b4085b84cda11f9bffb8a"
ETHERSCAN_API_KEY = "P13CVTCP43NWU9GX5D9VBA2QMUTJDDS941"

# Web3 Setup
w3 = Web3(Web3.HTTPProvider(f"https://mainnet.infura.io/v3/{INFURA_API_KEY}"))

# Protocol Addresses
BLUR_POOL = Web3.to_checksum_address("0x0000000000A39bb272e79075ade125fd351887Ac")
BLUR_LENDING = Web3.to_checksum_address("0x29469395eAf6f95920E59F858042f0e28D98a20B")

# Chainlink Price Feed
FEED_ADDRESS = w3.to_checksum_address("0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419")
AGGREGATOR_V3_ABI = [{
    "inputs": [],
    "name": "latestRoundData",
    "outputs": [
        {"internalType": "uint80", "name": "roundId", "type": "uint80"},
        {"internalType": "int256", "name": "answer", "type": "int256"},
        {"internalType": "uint256", "name": "startedAt", "type": "uint256"},
        {"internalType": "uint256", "name": "updatedAt", "type": "uint256"},
        {"internalType": "uint80", "name": "answeredInRound", "type": "uint80"},
    ],
    "stateMutability": "view",
    "type": "function",
}]
aggregator = w3.eth.contract(address=FEED_ADDRESS, abi=AGGREGATOR_V3_ABI)

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
# HELPER FUNCTIONS
# ============================================================================

def fetch_abi_and_name(addr: str, abi_dir: Path, chainid: int = 1) -> tuple:
    """Fetch ABI - check local directory first, then Etherscan"""
    addr = addr.lower()

    # Check if ABI exists locally
    local_abi_path = abi_dir / f"{addr}.json"
    if local_abi_path.exists():
        with open(local_abi_path, "r") as f:
            abi = json.load(f)
        name = f"Contract_{addr[:8]}"
        return name, abi

    # Fetch from Etherscan if not found locally
    url = "https://api.etherscan.io/v2/api"
    resp = requests.get(
        url,
        params={
            "chainid": chainid,
            "module": "contract",
            "action": "getsourcecode",
            "address": addr,
            "apikey": ETHERSCAN_API_KEY,
        },
        timeout=20,
    ).json()

    if not resp.get("result"):
        raise RuntimeError(f"Etherscan response missing result: {resp}")

    result = resp["result"][0] if isinstance(resp["result"], list) else resp["result"]
    abi_str = result.get("ABI")
    if not abi_str or abi_str == "Contract source code not verified":
        raise RuntimeError(f"ABI not found or not verified for {addr}")

    name = result.get("ContractName", addr)
    abi = json.loads(abi_str)

    # Save ABI to file
    abi_dir.mkdir(parents=True, exist_ok=True)
    with open(local_abi_path, "w") as f:
        json.dump(abi, f, indent=2)

    return name, abi


def get_implementation_address(w3: Web3, proxy: str) -> Optional[str]:
    """Get implementation address for EIP-1967 proxy contracts"""
    proxy = w3.to_checksum_address(proxy)
    impl_slot = int("0x360894A13BA1A3210667C828492DB98DCA3E2076CC3735A920A3CA505D382BBC", 16)
    try:
        raw_impl = w3.eth.get_storage_at(proxy, impl_slot)
        impl = w3.to_checksum_address(raw_impl[-20:].hex())
        if impl != "0x0000000000000000000000000000000000000000":
            return impl
    except Exception:
        pass
    return None

#def instantiate_contract(w3: Web3, contract_address: str, abi_dir: Path, chainid: int = 1):
 #   """Use the universal decoder"""
  #  decoder = UniversalContractDecoder(w3, ETHERSCAN_API_KEY, abi_dir)
   # return decoder.get_contract(contract_address)

def instantiate_contract(w3: Web3, contract_address: str, abi_dir: Path, chainid: int = 1):
    """Instantiate contract with proper ABI discovery (handles proxies)"""
    contract_address = Web3.to_checksum_address(contract_address)
    impl = get_implementation_address(w3, contract_address)
    if impl:
        try:
            _, abi = fetch_abi_and_name(impl, abi_dir, chainid)
        except Exception:
            _, abi = fetch_abi_and_name(contract_address, abi_dir, chainid)
    else:
        _, abi = fetch_abi_and_name(contract_address, abi_dir, chainid)
    return w3.eth.contract(address=contract_address, abi=abi)


@lru_cache(maxsize=None)
def get_eth_usd_at_block(block_number: int) -> Tuple[Decimal, datetime]:
    """Get ETH/USD price at specific block"""
    try:
        _, answer, *_ = aggregator.functions.latestRoundData().call(block_identifier=block_number)
        price = Decimal(answer) / Decimal(1e8)
    except Exception:
        price = Decimal(3000)  # Fallback price

    block = w3.eth.get_block(block_number)
    return price, datetime.fromtimestamp(block.timestamp, tz=timezone.utc)


def get_eth_usd_for_tx(tx_hash: str) -> Tuple[Decimal, datetime]:
    """Get ETH/USD price for a transaction"""
    tx = w3.eth.get_transaction(tx_hash)
    return get_eth_usd_at_block(tx.blockNumber)

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
        time_elapsed_seconds = max(time_elapsed_seconds, Decimal(1))  # minimum 1 second
        seconds_per_year = Decimal(365 * 24 * 3600)
        time_in_years = time_elapsed_seconds / seconds_per_year
        rate_decimal = self.rate / Decimal(10000)  # bps -> fraction
        exponent = float(rate_decimal * time_in_years)
        compound_factor = Decimal(str(math.exp(exponent)))
        total_debt = self.principal * compound_factor
        return total_debt - self.principal

    def total_due(self, as_of: datetime) -> Decimal:
        """Calculate total amount due"""
        return self.principal + self.calculate_interest(as_of)


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
        """Ensure debits equal credits per asset (tolerance 0.01)"""
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

# ============================================================================
# BLUR LENDING DECODER
# ============================================================================

class BlurLendingDecoder:
    """Decode and account for Blur lending protocol transactions"""

    def __init__(self, fund_wallets: Union[str, List[str]], abi_dir: Path, wallet_metadata: Dict = None):
        # Support multiple fund wallets
        if isinstance(fund_wallets, str):
            self.fund_wallets = [Web3.to_checksum_address(fund_wallets)]
        else:
            self.fund_wallets = [Web3.to_checksum_address(w) for w in fund_wallets]

        self.fund_wallets_lower = [w.lower() for w in self.fund_wallets]
        if wallet_metadata:
            self.wallet_metadata = {}
            self.wallet_metadata_lower = {}
            for addr, data in wallet_metadata.items():
                checksummed = Web3.to_checksum_address(addr)
                self.wallet_metadata[checksummed] = data
                self.wallet_metadata_lower[checksummed.lower()] = data
        else:
            self.wallet_metadata = {}
            self.wallet_metadata_lower = {}
        self.fund_wallet = self.fund_wallets[0] if self.fund_wallets else None

        self.abi_dir = Path(abi_dir)
        self.positions: Dict[int, LoanPosition] = {}
        self.journal_entries: List[JournalEntry] = []
        self.all_entries: List[Dict[str, Any]] = []
        self.contract = None
        self.event_signatures: Dict[str, str] = {}
        self.wallet_metadata = wallet_metadata or {}


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

        self._initialize_contract()

    def _get_wallet_info(self, address: str) -> Dict:
        """Get wallet metadata including friendly name, fund_id, etc."""
        checksummed = Web3.to_checksum_address(address)
        return self.wallet_metadata.get(checksummed, {
            "friendly_name": address[:8] + "...",
            "fund_id": "unknown",
            "group": "unknown",
            "subgroup": "unknown"
        })

    def _initialize_contract(self):
        """Initialize Blur lending contract and event map"""
        print("\n" + "=" * 80)
        print("INITIALIZING BLUR LENDING CONTRACT")
        print("=" * 80)

        self.contract = instantiate_contract(w3, BLUR_LENDING, self.abi_dir)


        # Initialize Pool contract
        try:
            self.pool_contract = instantiate_contract(w3, BLUR_POOL, self.abi_dir)
            print("  ✓ Blur Pool contract initialized")
        except Exception as e:
            print(f"  ⚠️ Could not initialize Blur Pool: {e}")
            self.pool_contract = None

        # Build event signatures map
        for item in self.contract.abi:
            if item.get('type') == 'event':
                name = item['name']
                inputs = ','.join([i['type'] for i in item['inputs']])
                signature = f"{name}({inputs})"
                topic = Web3.keccak(text=signature).hex()
                self.event_signatures[topic] = name
                print(f"  Event: {name}")

    # ---------------------------
    # Public entrypoint
    # ---------------------------
    def decode_transaction(self, tx_hash: str) -> Dict[str, Any]:
        """Decode a Blur lending transaction and generate journal entries"""
        tx = w3.eth.get_transaction(tx_hash)
        receipt = w3.eth.get_transaction_receipt(tx_hash)
        block = w3.eth.get_block(tx.blockNumber)
        block_time = datetime.fromtimestamp(block.timestamp, tz=timezone.utc)
        eth_price, _ = get_eth_usd_at_block(tx.blockNumber)

        print("\n" + "=" * 80)
        print(f"DECODING TRANSACTION: {tx_hash}")
        print("=" * 80)
        print(f"  Block: {tx.blockNumber}")
        print(f"  Timestamp: {block_time.isoformat()}")
        print(f"  ETH Price (USD): ${eth_price:,.2f}")
        print(f"  From: {tx['from']}")
        print(f"  To: {tx['to']}")
        print(f"  Value: {Decimal(tx.value) / Decimal(10**18):.6f} ETH")
        print(f"  Gas: {receipt.gasUsed} @ {tx.gasPrice} wei")

        print(f"\n  Input data length: {len(tx.input)} bytes")
        print(f"  Function selector: {tx.input[:4].hex() if isinstance(tx.input, (bytes, bytearray)) else tx.input[:10]}")
        print(f"  Logs in receipt: {len(receipt.logs)}")

        # Decode function call
        func_obj, func_params = self._decode_function_call(tx)
        func_name = func_obj.fn_name if func_obj else 'Unknown'
        print(f"\n  Function: {func_name}")

        # Decode events
        events = self._decode_events(receipt, eth_price, block_time)

        # Determine wallet roles
        wallet_roles = self._determine_wallet_roles(tx, receipt, func_name, func_params or {}, events)

        # Check Pool transfers - now returns tuple
        pool_transfers_total, pool_transfers_list = self._decode_pool_transfers(receipt.logs, eth_price)
        if pool_transfers_total > 0:
            print(f"\n  → Total Pool Transfers: {pool_transfers_total:.6f} ETH")
            if func_name == 'repay':
                lien_id = (func_params or {}).get('lienId')
                if lien_id and lien_id in self.positions:
                    position = self.positions[lien_id]
                    position.actual_repayment = pool_transfers_total
                    position.interest_paid = pool_transfers_total - position.principal
                    print(f"\n  → Repayment Breakdown:")
                    print(f"    Principal: {position.principal:.6f} ETH")
                    print(f"    Interest Paid: {position.interest_paid:.6f} ETH")
                    print(f"    Total Repaid: {position.actual_repayment:.6f} ETH")

        # Generate journal entries
        for event in events:
            if event["name"] == "LoanOfferTaken":
                entry = self._journal_loan_origination(event, tx, eth_price, block_time, wallet_roles)
                if entry.entries:
                    self.journal_entries.append(entry)
            elif event["name"] == "Repay":
                entry = self._journal_loan_repayment(event, tx, eth_price, block_time, wallet_roles)
                if entry.entries:
                    self.journal_entries.append(entry)
            elif event["name"] == "Refinance":
                # Check if this is an auction refinance
                if func_name == 'refinanceAuction':
                    entries = self._journal_refinance_auction(
                        event, tx, eth_price, block_time, wallet_roles, pool_transfers_list
                    )
                else:
                    entries = self._journal_refinance(event, tx, eth_price, block_time, wallet_roles)

                for e in entries:
                    if e.entries:
                        self.journal_entries.append(e)

        # Add gas fee if from fund wallet
        if tx['from'].lower() in self.fund_wallets_lower:
            gas_entry = self._journal_gas_fee(tx, receipt, eth_price, block_time)
            gas_entry.wallet_role = wallet_roles.get(tx['from'].lower(), "GAS_PAYER")
            if gas_entry.entries:
                self.journal_entries.append(gas_entry)

        return {
            "tx_hash": tx_hash,
            "block": tx.blockNumber,
            "timestamp": block_time,
            "function": func_name,
            "wallet_roles": wallet_roles,
            "events": events,
            "journal_entries": len(self.journal_entries),
            "eth_price": float(eth_price),
            "gas_used": receipt.gasUsed,
            "status": "SUCCESS" if receipt.status == 1 else "FAILED"
        }

    # ---------------------------
    # Role detection
    # ---------------------------
    def _determine_wallet_roles(self, tx, receipt, func_name: str, func_params: Dict,
                                events: List[Dict]) -> Dict[str, str]:
        """Determine the role of each fund wallet in the transaction"""
        wallet_roles: Dict[str, str] = {}

        # Function-based role determination
        if func_name == "deposit":
            if tx['from'].lower() in self.fund_wallets_lower:
                wallet_roles[tx['from'].lower()] = "DEPOSITOR"

        elif func_name == "withdraw":
            if tx['from'].lower() in self.fund_wallets_lower:
                wallet_roles[tx['from'].lower()] = "WITHDRAWER"

        elif func_name == "repay":
            lien_id = func_params.get('lienId')
            if lien_id and lien_id in self.positions:
                position = self.positions[lien_id]
                if position.lender.lower() in self.fund_wallets_lower:
                    wallet_roles[position.lender.lower()] = "LENDER_RECEIVING"
                if position.borrower.lower() in self.fund_wallets_lower:
                    wallet_roles[position.borrower.lower()] = "BORROWER_REPAYING"
            elif tx['from'].lower() in self.fund_wallets_lower:
                wallet_roles[tx['from'].lower()] = "BORROWER_REPAYING"

        elif func_name in ["borrow", "buyToBorrow", "buyToBorrowETH", "buyToBorrowV2"]:
            if tx['from'].lower() in self.fund_wallets_lower:
                wallet_roles[tx['from'].lower()] = "BORROWER"

        elif func_name in ["refinance", "refinanceAuction", "borrowerRefinance"]:
            if tx['from'].lower() in self.fund_wallets_lower:
                wallet_roles[tx['from'].lower()] = "NEW_LENDER"

        elif func_name == "startAuction":
            if tx['from'].lower() in self.fund_wallets_lower:
                wallet_roles[tx['from'].lower()] = "LENDER_CALLING_LOAN"

        elif func_name == "seize":
            if tx['from'].lower() in self.fund_wallets_lower:
                wallet_roles[tx['from'].lower()] = "LENDER_SEIZING"

        elif func_name in ["buyLocked", "buyLockedETH"]:
            if tx['from'].lower() in self.fund_wallets_lower:
                wallet_roles[tx['from'].lower()] = "LIQUIDATOR"

        # Event-based role determination (supplement/override)
        for event in events:
            if event["name"] == "LoanOfferTaken":
                lender = event["args"].get("lender", "").lower()
                borrower = event["args"].get("borrower", "").lower()
                if lender in self.fund_wallets_lower:
                    wallet_roles[lender] = "LENDER"
                if borrower in self.fund_wallets_lower:
                    wallet_roles[borrower] = "BORROWER"

            elif event["name"] == "Repay":
                lien_id = event["args"].get("lienId")
                if lien_id in self.positions:
                    position = self.positions[lien_id]
                    if position.lender.lower() in self.fund_wallets_lower:
                        wallet_roles[position.lender.lower()] = "LENDER_RECEIVING"
                    if position.borrower.lower() in self.fund_wallets_lower:
                        wallet_roles[position.borrower.lower()] = "BORROWER_REPAYING"

            elif event["name"] == "Refinance":
                new_lender = event["args"].get("newLender", "").lower()
                lien_id = event["args"].get("lienId")

                if new_lender in self.fund_wallets_lower:
                    wallet_roles[new_lender] = "NEW_LENDER"

                if lien_id in self.positions:
                    old_position = self.positions[lien_id]
                    if old_position.lender.lower() in self.fund_wallets_lower:
                        wallet_roles[old_position.lender.lower()] = "OLD_LENDER"
                    if old_position.borrower.lower() in self.fund_wallets_lower:
                        wallet_roles[old_position.borrower.lower()] = "BORROWER_REFINANCING"

            elif event["name"] == "StartAuction":
                lien_id = event["args"].get("lienId")
                if lien_id in self.positions:
                    position = self.positions[lien_id]
                    if position.lender.lower() in self.fund_wallets_lower:
                        wallet_roles[position.lender.lower()] = "LENDER_CALLING_LOAN"

            elif event["name"] == "Seize":
                lien_id = event["args"].get("lienId")
                if lien_id in self.positions:
                    position = self.positions[lien_id]
                    if position.lender.lower() in self.fund_wallets_lower:
                        wallet_roles[position.lender.lower()] = "LENDER_SEIZING"

            elif event["name"] == "BuyLocked":
                buyer = event["args"].get("buyer", "").lower()
                if buyer in self.fund_wallets_lower:
                    wallet_roles[buyer] = "LIQUIDATOR"

        # Check for Pool transfers to identify depositor/withdrawer roles
        for log in receipt.logs:
            if log.address.lower() == BLUR_POOL.lower() and len(log.topics) >= 3:
                from_addr = ('0x' + log.topics[1].hex()[-40:]).lower()
                to_addr = ('0x' + log.topics[2].hex()[-40:]).lower()

                if from_addr in self.fund_wallets_lower and not wallet_roles.get(from_addr):
                    wallet_roles[from_addr] = "POOL_DEPOSITOR"

                if to_addr in self.fund_wallets_lower and not wallet_roles.get(to_addr):
                    wallet_roles[to_addr] = "POOL_WITHDRAWER"

        return wallet_roles

    # ---------------------------
    # Decoding helpers
    # ---------------------------
    def _decode_function_call(self, tx) -> Tuple[Optional[Any], Optional[Dict]]:
        """Decode the function called in transaction"""
        try:
            # Try lending contract first
            if tx['to'].lower() == BLUR_LENDING.lower():
                func_obj, func_params = self.contract.decode_function_input(tx.input)
            # Try pool contract if available
            elif tx['to'].lower() == BLUR_POOL.lower() and self.pool_contract:
                func_obj, func_params = self.pool_contract.decode_function_input(tx.input)
            else:
                return None, None

            print(f"  Function decoded: {func_obj.fn_name}")
        #try:
         #   func_obj, func_params = self.contract.decode_function_input(tx.input)
          #  print(f"  Function decoded: {func_obj.fn_name}")

            if func_obj.fn_name == 'repay':
                lien = func_params.get('lien')
                lien_id = func_params.get('lienId')

                if lien:
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

                    print(f"\n  → Decoded Loan Details:")
                    print(f"    Lien ID: {lien_id}")
                    print(f"    Principal: {position.principal:.6f} ETH")
                    print(f"    Rate: {position.rate} bps ({float(position.rate)/100:.2f}% annual)")
                    print(f"    Time Elapsed: {int(time_elapsed)} seconds ({time_elapsed/3600:.2f} hours)")
                    print(f"    Interest (continuous): {interest:.8f} ETH")
                    print(f"    Total Debt: {total_debt:.8f} ETH")

                    position.repayment_amount = total_debt
                    position.interest_paid = interest

            elif func_obj.fn_name == 'refinanceAuction':
                # Extract parameters from refinanceAuction call
                lien = func_params.get('lien')
                lien_id = func_params.get('lienId')
                new_rate = func_params.get('rate')

                # For refinanceAuction, we need to handle the lien structure
                if lien:
                    if isinstance(lien, (tuple, list)):
                        # Decode the lien tuple structure
                        # [lender, borrower, collection, tokenId, amount, startTime, rate, auctionStartTime, auctionDuration]
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

                        # Store the old position for journal entry creation
                        self.positions[lien_id] = old_position

                        # Calculate amounts for refinancing
                        block = w3.eth.get_block(tx.blockNumber)
                        refinance_time = datetime.fromtimestamp(block.timestamp, tz=timezone.utc)

                        # In an auction refinance, the new lender pays the auction price
                        # which might be different from the original debt
                        interest_accrued = old_position.calculate_interest(refinance_time)
                        full_debt = old_position.principal + interest_accrued

                        # Store refinance details
                        old_position.refinance_details = {
                            'new_lender': tx['from'],  # The caller becomes the new lender
                            'new_rate': new_rate if new_rate else original_rate,
                            'refinance_time': refinance_time,
                            'old_debt': full_debt,
                            'interest_at_refinance': interest_accrued
                        }

                        print(f"\n  → Decoded Refinance Auction Details:")
                        print(f"    Lien ID: {lien_id}")
                        print(f"    Old Lender: {old_lender[:8]}...")
                        print(f"    Borrower: {borrower[:8]}...")
                        print(f"    Collection: {collection[:8]}...")
                        print(f"    Token ID: {token_id}")
                        print(f"    Original Principal: {original_amount:.6f} ETH")
                        print(f"    Interest Accrued: {interest_accrued:.8f} ETH")
                        print(f"    Total Debt: {full_debt:.8f} ETH")
                        print(f"    New Lender: {tx['from'][:8]}...")

            return func_obj, func_params

        except Exception as e:
            print(f"  Could not decode function: {e}")
            return None, None

    def _decode_pool_transfers(self, logs, eth_price: Decimal) -> Tuple[Decimal, List[Dict]]:
        """Decode transfers from Blur Pool contract - returns total and list of transfers"""
        total_transferred = Decimal(0)
        transfers = []

        for log in logs:
            if log.address.lower() == BLUR_POOL.lower():
                if log.topics and len(log.topics) >= 3:
                    from_addr = '0x' + log.topics[1].hex()[-40:]
                    to_addr = '0x' + log.topics[2].hex()[-40:]

                    if log.data:
                        value_hex = log.data.hex() if isinstance(log.data, (bytes, bytearray)) else str(log.data)
                        value_hex = value_hex.replace('0x', '')
                        if value_hex:
                            value_wei = int(value_hex, 16)
                            value_eth = Decimal(value_wei) / Decimal(10**18)

                            print(f"\n  → Pool Transfer Detected:")
                            print(f"    From: {from_addr}")
                            print(f"    To: {to_addr}")
                            print(f"    Amount: {value_eth:.6f} ETH (${value_eth * eth_price:,.2f})")

                            total_transferred += value_eth
                            transfers.append({
                                'from': from_addr,
                                'to': to_addr,
                                'amount': value_eth,
                                'amount_usd': value_eth * eth_price
                            })

        return total_transferred, transfers

    def _decode_events(self, receipt, eth_price: Decimal, block_time: datetime) -> List[Dict]:
        """Decode all events in receipt (Blur Lending only)"""
        decoded_events: List[Dict[str, Any]] = []

        for log in receipt.logs:
            if log.address.lower() != BLUR_LENDING.lower():
                continue

            # Try to decode against ABI
            for abi_item in self.contract.abi:
                if abi_item.get("type") != "event":
                    continue
                try:
                    event = getattr(self.contract.events, abi_item["name"])()
                    decoded_log = event.process_log(log)
                    args = dict(decoded_log["args"])
                    event_name = abi_item["name"]

                    event_data = {"name": event_name, "args": args, "extras": {}}

                    # Specific handling
                    if event_name == "LoanOfferTaken":
                        loan_amount_wei = args.get("loanAmount", args.get("amount", 0))
                        loan_amount_eth = Decimal(loan_amount_wei) / Decimal(10**18)
                        event_data["extras"]["loan_amount_eth"] = float(loan_amount_eth)

                        # Track position from event baseline (if not already from function)
                        self.positions[args["lienId"]] = LoanPosition(
                            lien_id=args["lienId"],
                            lender=args["lender"],
                            borrower=args["borrower"],
                            collection=args["collection"],
                            token_id=args["tokenId"],
                            principal=loan_amount_eth,
                            rate=Decimal(args.get("rate", 0)),
                            start_time=block_time,
                            duration=90 * 24 * 3600,
                            auction_duration=args.get("auctionDuration", 0)
                        )

                    elif event_name == "Repay":
                        lien_id = args.get("lienId")
                        if lien_id in self.positions:
                            pos = self.positions[lien_id]
                            pos.status = "REPAID"

                    decoded_events.append(event_data)
                    print(f"    - {event_name}: {args}")
                    break

                except Exception:
                    continue

        print(f"\n  Events found: {len(decoded_events)}")
        return decoded_events

    # ---------------------------
    # Journaling
    # ---------------------------
    def _journal_loan_origination(self, event: Dict, tx, eth_price: Decimal,
                                  block_time: datetime, wallet_roles: Dict[str, str]) -> JournalEntry:
        """Create journal entry for loan origination"""
        lien_id = event['args']['lienId']
        loan_eth = Decimal(event['extras']['loan_amount_eth'])
        lender = event['args']['lender']
        borrower = event['args']['borrower']

        # Determine which fund wallet is involved and their role
        wallet_address = ""
        wallet_role = "UNKNOWN"
        if lender.lower() in self.fund_wallets_lower:
            wallet_address = lender
            wallet_role = wallet_roles.get(lender.lower(), "LENDER")
        elif borrower.lower() in self.fund_wallets_lower:
            wallet_address = borrower
            wallet_role = wallet_roles.get(borrower.lower(), "BORROWER")

        wallet_info = self._get_wallet_info(wallet_address)
        entry = JournalEntry(
            entry_id=f"JE_{tx.hash.hex()[:8]}_{lien_id}",
            date=block_time,
            description=f"NFT Loan Origination - Lien #{lien_id} ({wallet_role}) - {wallet_info['friendly_name']} - Fund {wallet_info['fund_id']}",
            tx_hash=tx.hash.hex(),
            event_type=AccountingEventType.LOAN_ORIGINATION,
            wallet_address=wallet_address,
            wallet_role=wallet_role
        )

        if wallet_role == "LENDER":
            entry.add_debit("loans_receivable", loan_eth)
            entry.add_credit("blur_pool", loan_eth)
            entry.add_tax_implication(TaxTreatment.NON_TAXABLE, loan_eth * eth_price, "Loan principal disbursement")
        elif wallet_role == "BORROWER":
            entry.add_debit("blur_pool", loan_eth)
            entry.add_credit("loan_payable", loan_eth)
            entry.add_tax_implication(TaxTreatment.NON_TAXABLE, loan_eth * eth_price, "Loan proceeds received")

        return entry

    def _journal_loan_repayment(self, event: Dict, tx, eth_price: Decimal,
                                block_time: datetime, wallet_roles: Dict[str, str]) -> JournalEntry:
        """Create journal entry for loan repayment"""
        lien_id = event.get('args', {}).get('lienId')
        position = self.positions.get(lien_id)

        wallet_address = ""
        wallet_role = "UNKNOWN"
        if position:
            if position.lender.lower() in self.fund_wallets_lower:
                wallet_address = position.lender
                wallet_role = wallet_roles.get(position.lender.lower(), "LENDER_RECEIVING")
            elif position.borrower.lower() in self.fund_wallets_lower:
                wallet_address = position.borrower
                wallet_role = wallet_roles.get(position.borrower.lower(), "BORROWER_REPAYING")

        wallet_info = self._get_wallet_info(wallet_address)

        entry = JournalEntry(
            entry_id=f"JE_{tx.hash.hex()[:8]}_REPAY_{lien_id}",
            date=block_time,
            description=f"NFT Loan Repayment - Lien #{lien_id} ({wallet_role}) - {wallet_info['friendly_name']} - Fund {wallet_info['fund_id']}",
            tx_hash=tx.hash.hex(),
            event_type=AccountingEventType.LOAN_REPAYMENT,
            wallet_address=wallet_address,
            wallet_role=wallet_role
        )

        if position:
            principal_eth = position.principal
            if hasattr(position, 'actual_repayment'):
                total_eth = position.actual_repayment
                interest_eth = position.interest_paid
            else:
                interest_eth = position.calculate_interest(block_time)
                total_eth = principal_eth + interest_eth

            print(f"\n  → Creating Journal Entry (Native ETH):")
            print(f"    Wallet Role: {wallet_role}")
            print(f"    Principal: {principal_eth:.6f} ETH")
            print(f"    Interest: {interest_eth:.6f} ETH")
            print(f"    Total: {total_eth:.6f} ETH")

            if wallet_role == "LENDER_RECEIVING":
                entry.add_debit("eth_wallet", total_eth)
                entry.add_credit("loans_receivable", principal_eth)
                entry.add_credit("interest_income", interest_eth)
                entry.add_tax_implication(TaxTreatment.NON_TAXABLE, principal_eth * eth_price, "Loan principal repayment")
                entry.add_tax_implication(TaxTreatment.TAXABLE_INCOME, interest_eth * eth_price, "Interest income")

            elif wallet_role == "BORROWER_REPAYING":
                entry.add_debit("loan_payable", principal_eth)
                entry.add_debit("interest_expense", interest_eth)
                entry.add_credit("eth_wallet", total_eth)
                entry.add_tax_implication(TaxTreatment.NON_TAXABLE, principal_eth * eth_price, "Loan principal repayment")
                entry.add_tax_implication(TaxTreatment.DEDUCTIBLE_EXPENSE, interest_eth * eth_price, "Interest expense")

        return entry


    def _journal_refinance(self, event: Dict, tx, eth_price: Decimal,
                          block_time: datetime, wallet_roles: Dict[str, str]) -> List[JournalEntry]:
        """Create journal entries for refinancing"""
        entries: List[JournalEntry] = []
        lien_id = event['args'].get('lienId')
        new_lender = event['args'].get('newLender')
        new_amount = Decimal(event['args'].get('newAmount', 0)) / Decimal(10**18)

        old_position = self.positions.get(lien_id)

        # Even without old position data, create entry for new lender
        if new_lender and new_lender.lower() in self.fund_wallets_lower:
            wallet_info = self._get_wallet_info(new_lender)  # FIX: Use new_lender directly
            new_lender_entry = JournalEntry(
                entry_id=f"JE_{tx.hash.hex()[:8]}_NEW_{lien_id}",
                date=block_time,
                description=f"Refinance New Loan - Lien #{lien_id} (NEW_LENDER) - {wallet_info['friendly_name']} - Fund {wallet_info['fund_id']}",
                tx_hash=tx.hash.hex(),
                event_type=AccountingEventType.LOAN_ORIGINATION,
                wallet_address=new_lender,
                wallet_role="NEW_LENDER"
            )
            new_lender_entry.add_debit("loans_receivable", new_amount)
            new_lender_entry.add_credit("blur_pool", new_amount)
            entries.append(new_lender_entry)

        if not old_position:
            # Create a new position from refinance event
            self.positions[lien_id] = LoanPosition(
                lien_id=lien_id,
                lender=new_lender,
                borrower="Unknown",  # Would need to fetch from chain
                collection=event['args'].get('collection', ''),
                token_id=0,  # Would need to fetch from chain
                principal=new_amount,
                rate=Decimal(event['args'].get('newRate', 0)),
                start_time=block_time,
                duration=90 * 24 * 3600,
                auction_duration=event['args'].get('newAuctionDuration', 0),
                status="ACTIVE"
            )
            print(f"  → Created new position from refinance: Lien #{lien_id}")
            return entries

        principal = old_position.principal
        interest = old_position.calculate_interest(block_time)
        payoff = principal + interest

        # Old lender payoff
        if old_position.lender.lower() in self.fund_wallets_lower:
            wallet_info = self._get_wallet_info(old_position.lender)  # FIX: Use old_position.lender
            old_lender_entry = JournalEntry(
                entry_id=f"JE_{tx.hash.hex()[:8]}_OLD_{lien_id}",
                date=block_time,
                description=f"Refinance Payoff - Lien #{lien_id} (OLD_LENDER) - {wallet_info['friendly_name']} - Fund {wallet_info['fund_id']}",
                tx_hash=tx.hash.hex(),
                event_type=AccountingEventType.LOAN_REFINANCE,
                wallet_address=old_position.lender,
                wallet_role="OLD_LENDER"
            )
            old_lender_entry.add_debit("blur_pool", payoff)
            old_lender_entry.add_credit("loans_receivable", principal)
            old_lender_entry.add_credit("interest_income", interest)
            old_lender_entry.add_tax_implication(TaxTreatment.TAXABLE_INCOME, interest * eth_price,
                                                "Interest income from refinancing")
            entries.append(old_lender_entry)

        # Borrower closes old loan, opens new
        if old_position.borrower.lower() in self.fund_wallets_lower:
            wallet_info = self._get_wallet_info(old_position.borrower)  # FIX: Use old_position.borrower
            borrower_entry = JournalEntry(
                entry_id=f"JE_{tx.hash.hex()[:8]}_BREFI_{lien_id}",
                date=block_time,
                description=f"Refinance - Lien #{lien_id} (BORROWER_REFINANCING) - {wallet_info['friendly_name']} - Fund {wallet_info['fund_id']}",
                tx_hash=tx.hash.hex(),
                event_type=AccountingEventType.LOAN_REFINANCE,
                wallet_address=old_position.borrower,
                wallet_role="BORROWER_REFINANCING"
            )
            borrower_entry.add_debit("loan_payable", principal)
            borrower_entry.add_debit("interest_expense", interest)
            borrower_entry.add_credit("blur_pool", payoff)
            borrower_entry.add_debit("blur_pool", new_amount)
            borrower_entry.add_credit("loan_payable", new_amount)
            borrower_entry.add_tax_implication(TaxTreatment.DEDUCTIBLE_EXPENSE, interest * eth_price,
                                              "Interest expense on refinancing")
            entries.append(borrower_entry)

        return entries
    # Add this method to your BlurLendingDecoder class (add it after the _journal_refinance method):

    def _journal_refinance_auction(self, event: Dict, tx, eth_price: Decimal,
                                  block_time: datetime, wallet_roles: Dict[str, str],
                                  pool_transfers: List[Dict]) -> List[JournalEntry]:
        """Create journal entries for auction refinancing with Transfer events"""
        entries: List[JournalEntry] = []
        lien_id = event['args'].get('lienId')
        new_lender = event['args'].get('newLender')
        new_amount = Decimal(event['args'].get('newAmount', 0)) / Decimal(10**18)
        new_rate = Decimal(event['args'].get('newRate', 0))
        collection = event['args'].get('collection')

        # Get old position if we have it
        old_position = self.positions.get(lien_id)

        # 1. NEW LENDER ENTRY - Funding the new loan
        if new_lender and new_lender.lower() in self.fund_wallets_lower:
            wallet_info = self._get_wallet_info(new_lender)
            new_lender_entry = JournalEntry(
                entry_id=f"JE_{tx.hash.hex()[:8]}_NEW_LENDER_{lien_id}",
                date=block_time,
                description=f"Refinance Auction - New Loan Funded - Lien #{lien_id} - {wallet_info['friendly_name']} - Fund {wallet_info['fund_id']}",
                tx_hash=tx.hash.hex(),
                event_type=AccountingEventType.LOAN_ORIGINATION,
                wallet_address=new_lender,
                wallet_role="NEW_LENDER"
            )

            # New lender provides funds
            new_lender_entry.add_debit("loans_receivable", new_amount)
            new_lender_entry.add_credit("blur_pool", new_amount)

            # Add NFT collateral tracking
            new_lender_entry.add_debit("nft_collateral", Decimal(0))  # Non-monetary

            new_lender_entry.add_tax_implication(
                TaxTreatment.NON_TAXABLE,
                new_amount * eth_price,
                "Loan principal disbursement in refinancing auction"
            )
            entries.append(new_lender_entry)

        # 2. Check pool transfers for old lender payoff
        for transfer in pool_transfers:
            transfer_to = Web3.to_checksum_address(transfer['to'])
            if transfer_to in self.fund_wallets:
                # One of our wallets received a payoff
                wallet_info = self._get_wallet_info(transfer_to)
                old_lender_entry = JournalEntry(
                    entry_id=f"JE_{tx.hash.hex()[:8]}_OLD_LENDER_{lien_id}",
                    date=block_time,
                    description=f"Refinance Auction - Loan Payoff Received - Lien #{lien_id} - {wallet_info['friendly_name']} - Fund {wallet_info['fund_id']}",
                    tx_hash=tx.hash.hex(),
                    event_type=AccountingEventType.LOAN_REFINANCE,
                    wallet_address=transfer_to,
                    wallet_role="OLD_LENDER"
                )

                # Receive auction proceeds
                actual_proceeds = Decimal(str(transfer['amount']))
                old_lender_entry.add_debit("blur_pool", actual_proceeds)
                old_lender_entry.add_credit("loans_receivable", actual_proceeds)

                old_lender_entry.add_tax_implication(
                    TaxTreatment.NON_TAXABLE,
                    actual_proceeds * eth_price,
                    "Loan principal recovered via refinancing auction"
                )

                # Remove NFT collateral
                old_lender_entry.add_credit("nft_collateral", Decimal(0))  # Non-monetary

                entries.append(old_lender_entry)

        # 3. Update or create position
        if old_position:
            # Update existing position
            old_position.lender = new_lender
            old_position.principal = new_amount
            old_position.rate = new_rate
            old_position.start_time = block_time
            old_position.status = "ACTIVE"
        else:
            # Create new position
            self.positions[lien_id] = LoanPosition(
                lien_id=lien_id,
                lender=new_lender,
                borrower="Unknown",  # Would need chain data
                collection=collection,
                token_id=0,  # Would need chain data
                principal=new_amount,
                rate=new_rate,
                start_time=block_time,
                duration=90 * 24 * 3600,
                auction_duration=event['args'].get('newAuctionDuration', 0),
                status="ACTIVE"
            )

        print(f"\n  → Generated {len(entries)} journal entries for refinance auction")
        return entries

    def _journal_gas_fee(self, tx, receipt, eth_price: Decimal,
                         block_time: datetime) -> JournalEntry:
        """Create journal entry for gas fees"""
        gas_used = receipt.gasUsed
        gas_price = tx.gasPrice
        gas_eth = Decimal(gas_used * gas_price) / Decimal(10**18)

        entry = JournalEntry(
            entry_id=f"JE_{tx.hash.hex()[:8]}_GAS",
            date=block_time,
            description="Transaction Gas Fee",
            tx_hash=tx.hash.hex(),
            event_type=AccountingEventType.FEE_PAYMENT,
            wallet_address=tx['from'],
            wallet_role="GAS_PAYER"
        )

        entry.add_debit("gas_fees", gas_eth)
        entry.add_credit("eth_wallet", gas_eth)
        entry.add_tax_implication(TaxTreatment.DEDUCTIBLE_EXPENSE, gas_eth * eth_price, "Gas fees")

        return entry

    # ---------------------------
    # Reporting
    # ---------------------------
    def generate_reports(self) -> Dict[str, pd.DataFrame]:
        """Generate comprehensive accounting reports"""
        reports: Dict[str, pd.DataFrame] = {}

        # Journal Entries Report
        je_data: List[Dict[str, Any]] = []
        for entry in self.journal_entries:
            for line in entry.entries:
                je_data.append({
                    "Entry_ID": entry.entry_id,
                    "Date": entry.date,
                    "Account": self.accounts.get(line["account"], line["account"]),
                    "Debit": line["amount"] if line["type"] == "DEBIT" else 0,
                    "Credit": line["amount"] if line["type"] == "CREDIT" else 0,
                    "Asset": line["asset"],
                    "Description": entry.description,
                    "Tx_Hash": entry.tx_hash[:10] + "..."
                })
        reports["journal_entries"] = pd.DataFrame(je_data)

        # Trial Balance
        trial_balance = defaultdict(lambda: defaultdict(lambda: {"debit": Decimal(0), "credit": Decimal(0)}))
        for row in je_data:
            account = row["Account"]
            asset = row["Asset"]
            trial_balance[asset][account]["debit"] += Decimal(str(row["Debit"]))
            trial_balance[asset][account]["credit"] += Decimal(str(row["Credit"]))

        tb_data: List[Dict[str, Any]] = []
        for asset, accounts in trial_balance.items():
            for account, amounts in accounts.items():
                tb_data.append({
                    "Asset": asset,
                    "Account": account,
                    "Debit": float(amounts["debit"]),
                    "Credit": float(amounts["credit"]),
                    "Balance": float(amounts["debit"] - amounts["credit"])
                })
        reports["trial_balance"] = pd.DataFrame(tb_data)

        # Tax Report
        tax_data: List[Dict[str, Any]] = []
        for entry in self.journal_entries:
            for tax_item in entry.tax_implications:
                tax_data.append({
                    "Date": entry.date,
                    "Treatment": tax_item["treatment"],
                    "Amount_USD": tax_item["amount"],
                    "Description": tax_item["description"],
                    "Tx_Hash": entry.tx_hash[:10] + "..."
                })
        reports["tax_report"] = pd.DataFrame(tax_data)

        # Loan Portfolio
        portfolio_data: List[Dict[str, Any]] = []
        for lien_id, position in self.positions.items():
            portfolio_data.append({
                "Lien_ID": lien_id,
                "Status": position.status,
                "Lender": position.lender[:8] + "...",
                "Borrower": position.borrower[:8] + "...",
                "Collection": position.collection[:8] + "...",
                "Token_ID": position.token_id,
                "Principal_ETH": float(position.principal),
                "Rate_BPS": float(position.rate),
                "Start_Date": position.start_time
            })
        reports["loan_portfolio"] = pd.DataFrame(portfolio_data)

        return reports

# ============================================================================
# MAIN FUNCTIONS
# ============================================================================

def process_blur_transaction(tx_hash: str, fund_wallets: Union[str, List[str]],
                            abi_dir: Path, wallet_metadata: Dict = None) -> Dict[str, Any]:
    """Process a single Blur lending transaction"""
    decoder = BlurLendingDecoder(fund_wallets, abi_dir, wallet_metadata)  # Pass metadata
    result = decoder.decode_transaction(tx_hash)
    reports = decoder.generate_reports()

    print("\n" + "=" * 80)
    print("ACCOUNTING SUMMARY")
    print("=" * 80)

    if not reports["journal_entries"].empty:
        print("\nJournal Entries:")
        print(reports["journal_entries"].to_string(index=False))

    if not reports["trial_balance"].empty:
        print("\nTrial Balance:")
        print(reports["trial_balance"].to_string(index=False))

    if not reports["tax_report"].empty:
        print("\nTax Implications:")
        print(reports["tax_report"].to_string(index=False))

    return {"transaction": result, "reports": reports, "positions": decoder.positions}

def batch_process_transactions(tx_hashes: List[str], fund_wallets: Union[str, List[str]],
                               abi_dir: Path, wallet_metadata: Dict = None) -> Dict[str, Any]:

    """Process multiple transactions"""
    decoder = BlurLendingDecoder(fund_wallets, abi_dir, wallet_metadata)  # Pass wallet_metadata

    all_results: List[Dict[str, Any]] = []
    for tx_hash in tx_hashes:
        print(f"\nProcessing: {tx_hash}")
        result = decoder.decode_transaction(tx_hash)
        all_results.append(result)
        time.sleep(0.5)  # gentle rate limiting

    reports = decoder.generate_reports()

    print("\n" + "=" * 80)
    print("BATCH PROCESSING COMPLETE")
    print("=" * 80)
    print(f"✓ Processed {len(tx_hashes)} transactions")
    print(f"✓ Generated {len(decoder.journal_entries)} journal entries")
    print(f"✓ Tracking {len(decoder.positions)} loan positions")

    return {"transactions": all_results, "reports": reports, "positions": decoder.positions}

# ============================================================================
# USAGE
# ============================================================================

if __name__ == "__main__":
    # Configuration
    TEST_TX = "0x5a3b8f8a9f1e068417ff349fe153700d7b3fd15645f2a791bd9a58d330bd57fb"

    fund_wallet_ids = [
        Web3.to_checksum_address(m["wallet_address"])
        for m in wallet_metadata.values()
        if m["category"] == "fund"
    ]

    ABI_DIR = Path("/content/drive/MyDrive/Drip_Capital/smart_contract_ABIs")

    print(f"Testing with transaction: {TEST_TX}")
    print(f"Fund wallets ({len(fund_wallet_ids)}): {fund_wallet_ids}")
    print(f"ABI Directory: {ABI_DIR}")

    # Process transaction with wallet metadata
    results = process_blur_transaction(TEST_TX, fund_wallet_ids, ABI_DIR, wallet_metadata)


    print("\n" + "=" * 80)
    print("PROCESSING COMPLETE")
    print("=" * 80)
    print(f"✓ Decoded {len(results['transaction']['events'])} events")
    print(f"✓ Generated {results['transaction']['journal_entries']} journal entries")
    print(f"✓ Tracking {len(results['positions'])} loan positions")

"""####Debugging"""

# DEBUGGING STEP 1: Check if the contract is reading events correctly
TEST_TX = "0x463edced06ea3e5f394eb6e84f9b130287709da535599e2ccba0cb0a0613839a"

# Get raw transaction data
tx = w3.eth.get_transaction(TEST_TX)
receipt = w3.eth.get_transaction_receipt(TEST_TX)

print("RAW TRANSACTION ANALYSIS")
print("=" * 80)
print(f"To Address: {tx['to']}")
print(f"From Address: {tx['from']}")
print(f"Number of logs: {len(receipt.logs)}")

# Check each log
for i, log in enumerate(receipt.logs):
    print(f"\nLog {i}:")
    print(f"  Address: {log.address}")
    print(f"  Topics: {len(log.topics)}")
    if log.topics:
        print(f"  Topic[0]: {log.topics[0].hex()}")
    print(f"  Data length: {len(log.data) if log.data else 0}")

# Check what contracts are currently loaded in the decoder
print("CURRENTLY INSTANTIATED CONTRACTS:")
print("=" * 80)

print(f"1. Main Lending Contract:")
print(f"   Address: {BLUR_LENDING}")
print(f"   Instance: decoder.contract")
print(f"   Contract Name: Blur: Blend")

if decoder.pool_contract:
    print(f"\n2. Pool Contract:")
    print(f"   Address: {BLUR_POOL}")
    print(f"   Instance: decoder.pool_contract")
    print(f"   Contract Name: Blur Pool")

# Check the events available on the main contract
print(f"\n3. Available Events on Lending Contract:")
for item in decoder.contract.abi:
    if item.get('type') == 'event':
        print(f"   - {item['name']}")

# Verify the contract addresses match what we're seeing in transactions
print(f"\n4. Transaction Verification:")
print(f"   Transaction went to: {tx['to']}")
print(f"   Matches BLUR_LENDING: {tx['to'].lower() == BLUR_LENDING.lower()}")
print(f"   Pool transfers from: {BLUR_POOL}")

"""##Export to CSV Files"""

import pandas as pd
from datetime import datetime
import os

def export_blur_accounting_to_csv(decoder, result, output_dir="blur_accounting_exports"):
    """
    Export Blur lending accounting data to CSV files
    """

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 1. JOURNAL ENTRIES
    je_data = []
    for entry in decoder.journal_entries:
        for line in entry.entries:
            je_data.append({
                "entry_id": entry.entry_id,
                "date": entry.date.strftime("%Y-%m-%d %H:%M:%S"),
                "tx_hash": entry.tx_hash,
                "wallet_address": entry.wallet_address,
                "wallet_role": entry.wallet_role,
                "event_type": entry.event_type.value,
                "account": line["account"],
                "account_name": decoder.accounts.get(line["account"], line["account"]),
                "debit": line["amount"] if line["type"] == "DEBIT" else 0,
                "credit": line["amount"] if line["type"] == "CREDIT" else 0,
                "asset": line["asset"],
                "description": entry.description
            })

    df_journal = pd.DataFrame(je_data)
    journal_file = f"{output_dir}/journal_entries_{timestamp}.csv"
    df_journal.to_csv(journal_file, index=False)

    # 2. TRIAL BALANCE
    trial_balance = {}
    for row in je_data:
        key = (row["account_name"], row["asset"])
        if key not in trial_balance:
            trial_balance[key] = {"debit": 0, "credit": 0}
        trial_balance[key]["debit"] += row["debit"]
        trial_balance[key]["credit"] += row["credit"]

    tb_data = []
    for (account, asset), amounts in trial_balance.items():
        tb_data.append({
            "account": account,
            "asset": asset,
            "total_debits": amounts["debit"],
            "total_credits": amounts["credit"],
            "net_balance": amounts["debit"] - amounts["credit"]
        })

    df_trial = pd.DataFrame(tb_data)
    trial_file = f"{output_dir}/trial_balance_{timestamp}.csv"
    df_trial.to_csv(trial_file, index=False)

    # 3. TAX IMPLICATIONS
    tax_data = []
    for entry in decoder.journal_entries:
        for tax_item in entry.tax_implications:
            tax_data.append({
                "date": entry.date.strftime("%Y-%m-%d"),
                "tx_hash": entry.tx_hash,
                "wallet_address": entry.wallet_address,
                "wallet_role": entry.wallet_role,
                "tax_treatment": tax_item["treatment"],
                "amount_usd": tax_item["amount"],
                "description": tax_item["description"]
            })

    if tax_data:
        df_tax = pd.DataFrame(tax_data)
        tax_file = f"{output_dir}/tax_implications_{timestamp}.csv"
        df_tax.to_csv(tax_file, index=False)

    # 4. LOAN POSITIONS
    positions_data = []
    for lien_id, position in decoder.positions.items():
        positions_data.append({
            "lien_id": lien_id,
            "status": position.status,
            "lender": position.lender,
            "borrower": position.borrower,
            "collection": position.collection,
            "token_id": position.token_id,
            "principal_eth": float(position.principal),
            "rate_bps": float(position.rate),
            "rate_annual_pct": float(position.rate) / 100,
            "start_time": position.start_time.strftime("%Y-%m-%d %H:%M:%S")
        })

    if positions_data:
        df_positions = pd.DataFrame(positions_data)
        positions_file = f"{output_dir}/loan_positions_{timestamp}.csv"
        df_positions.to_csv(positions_file, index=False)

    # 5. TRANSACTION SUMMARY
    tx_summary = [{
        "tx_hash": result["tx_hash"],
        "block": result["block"],
        "timestamp": result["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
        "function": result["function"],
        "wallet_roles": ", ".join([f"{w[:10]}...:{r}" for w, r in result.get("wallet_roles", {}).items()]),
        "events": ", ".join([e["name"] for e in result["events"]]),
        "journal_entries_count": result["journal_entries"],
        "eth_price": result["eth_price"],
        "gas_used": result["gas_used"],
        "status": result["status"]
    }]

    df_tx = pd.DataFrame(tx_summary)
    tx_file = f"{output_dir}/transaction_summary_{timestamp}.csv"
    df_tx.to_csv(tx_file, index=False)

    # 6. INTERNAL REFINANCING REPORT (Special)
    internal_data = []
    if "OLD_LENDER" in result.get("wallet_roles", {}).values():
        # Check if this is internal refinancing
        for entry in decoder.journal_entries:
            if entry.wallet_role == "OLD_LENDER":
                # Find interest income
                for line in entry.entries:
                    if line["account"] == "interest_income" and line["type"] == "CREDIT":
                        internal_data.append({
                            "date": entry.date.strftime("%Y-%m-%d"),
                            "tx_hash": entry.tx_hash,
                            "wallet": entry.wallet_address,
                            "interest_earned_eth": line["amount"],
                            "interest_earned_usd": line["amount"] * result["eth_price"],
                            "type": "Internal Refinancing" if entry.wallet_address in [e.wallet_address for e in decoder.journal_entries if e.wallet_role == "NEW_LENDER"] else "External Refinancing"
                        })

    if internal_data:
        df_internal = pd.DataFrame(internal_data)
        internal_file = f"{output_dir}/refinancing_interest_{timestamp}.csv"
        df_internal.to_csv(internal_file, index=False)

    # Print summary
    print(f"\n📁 CSV Files Exported to '{output_dir}/':")
    print(f"  ✓ journal_entries_{timestamp}.csv ({len(df_journal)} entries)")
    print(f"  ✓ trial_balance_{timestamp}.csv ({len(df_trial)} accounts)")
    if tax_data:
        print(f"  ✓ tax_implications_{timestamp}.csv ({len(tax_data)} items)")
    if positions_data:
        print(f"  ✓ loan_positions_{timestamp}.csv ({len(positions_data)} positions)")
    print(f"  ✓ transaction_summary_{timestamp}.csv")
    if internal_data:
        print(f"  ✓ refinancing_interest_{timestamp}.csv")

    # Return dataframes for further analysis
    return {
        "journal_entries": df_journal,
        "trial_balance": df_trial,
        "tax_implications": df_tax if tax_data else pd.DataFrame(),
        "loan_positions": df_positions if positions_data else pd.DataFrame(),
        "transaction_summary": df_tx,
        "refinancing_interest": df_internal if internal_data else pd.DataFrame()
    }

"""###Single Transaction"""

# ============================================================================
# ACTIVATE DECODER AND EXPORT
# ============================================================================

# Test transaction (you can change this to any Blur lending transaction)
TEST_TX = "0x5a3b8f8a9f1e068417ff349fe153700d7b3fd15645f2a791bd9a58d330bd57fb"

print(f"Testing with transaction: {TEST_TX}")
print(f"Fund wallets ({len(fund_wallet_ids)}): {[w[:10] + '...' for w in fund_wallet_ids]}")
print(f"ABI Directory: {ABI_DIR}")

# Initialize the decoder with your fund wallets and metadata
decoder = BlurLendingDecoder(fund_wallet_ids, ABI_DIR, wallet_metadata)

# Decode the transaction
result = decoder.decode_transaction(TEST_TX)

# Generate reports
reports = decoder.generate_reports()

print("\n" + "=" * 80)
print("PROCESSING COMPLETE")
print("=" * 80)
print(f"✓ Decoded {len(result['events'])} events")
print(f"✓ Generated {result['journal_entries']} journal entries")
print(f"✓ Tracking {len(decoder.positions)} loan positions")

# Now export to CSV
output_dir = "/content/drive/MyDrive/Drip_Capital/blur_accounting"
dataframes = export_blur_accounting_to_csv(decoder, result, output_dir=output_dir)

print(f"\n✅ Successfully exported accounting data to: {output_dir}")

# Run the export
#dataframes = export_blur_accounting_to_csv(decoder, result)

# You can also save to Google Drive if in Colab
#if 'google.colab' in str(get_ipython()):
 #   drive_dir = "/content/drive/MyDrive/Drip_Capital/blur_accounting"
  #  dataframes_drive = export_blur_accounting_to_csv(decoder, result, output_dir=drive_dir)
   # print(f"\n💾 Also saved to Google Drive: {drive_dir}")