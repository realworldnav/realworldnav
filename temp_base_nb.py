"""
March 2025 Transaction Decoders
================================

Handles all transaction types identified in March 2025:
1. WETH (wrap/unwrap) - 65 transactions
2. ETH transfers to EOAs - 35 transactions
3. ERC20 (USDC transfers) - 11 transactions
4. Seaport (OpenSea NFT) - 5 transactions
5. Gnosis Safe - 2 transactions
6. LiFi Bridge - 1 transaction

Author: Real World NAV
Version: 1.0.0
"""

from __future__ import annotations

import json
import pandas as pd
from decimal import Decimal, getcontext
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
from functools import lru_cache
from web3 import Web3

# Set decimal precision
getcontext().prec = 28

# ============================================================================
# CONSTANTS
# ============================================================================

# Token Addresses
WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
USDC_ADDRESS = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
USDT_ADDRESS = "0xdAC17F958D2ee523a2206206994597C13D831ec7"

# Contract Addresses
SEAPORT_1_6 = "0x0000000000000068F116a894984e2DB1123eB395"
LIFI_DIAMOND = "0x1231DEB6f5749EF6cE6943a275A1D3E7486F4EaE"

# Decimals
WAD = Decimal(10**18)  # ETH/WETH
USDC_DECIMALS = Decimal(10**6)
USDT_DECIMALS = Decimal(10**6)

# Function Selectors
WETH_SELECTORS = {
    "0xd0e30db0": "deposit",      # deposit()
    "0x2e1a7d4d": "withdraw",     # withdraw(uint256)
}

ERC20_SELECTORS = {
    "0xa9059cbb": "transfer",           # transfer(address,uint256)
    "0x095ea7b3": "approve",            # approve(address,uint256)
    "0x23b872dd": "transferFrom",       # transferFrom(address,address,uint256)
}

SEAPORT_SELECTORS = {
    "0xfb0f3ee1": "fulfillBasicOrder",
    "0x00000000": "fulfillBasicOrder_efficient_6GL6yc",
    "0xe7acab24": "fulfillAvailableAdvancedOrders",
    "0x87201b41": "fulfillAvailableOrders",
    "0xb3a34c4c": "fulfillOrder",
    "0xed98a574": "fulfillAdvancedOrder",
}

GNOSIS_SAFE_SELECTORS = {
    "0x6a761202": "execTransaction",
}


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class DecodedTransaction:
    """Decoded transaction with all relevant fields"""
    tx_hash: str
    block_number: int
    timestamp: datetime
    from_address: str
    to_address: str

    # Transaction type
    protocol: str
    operation: str
    category: str

    # Values
    value_eth: Decimal
    value_token: Optional[Decimal] = None
    token_symbol: Optional[str] = None
    token_address: Optional[str] = None

    # Gas
    gas_used: int = 0
    gas_price_gwei: Decimal = Decimal(0)
    gas_cost_eth: Decimal = Decimal(0)

    # Additional context
    counterparty: Optional[str] = None
    nft_contract: Optional[str] = None
    nft_token_id: Optional[int] = None

    # Raw data
    input_data: str = ""
    function_selector: str = ""

    def to_dict(self) -> Dict:
        return {
            'tx_hash': self.tx_hash,
            'block_number': self.block_number,
            'timestamp': self.timestamp,
            'from_address': self.from_address,
            'to_address': self.to_address,
            'protocol': self.protocol,
            'operation': self.operation,
            'category': self.category,
            'value_eth': float(self.value_eth),
            'value_token': float(self.value_token) if self.value_token else None,
            'token_symbol': self.token_symbol,
            'token_address': self.token_address,
            'gas_used': self.gas_used,
            'gas_price_gwei': float(self.gas_price_gwei),
            'gas_cost_eth': float(self.gas_cost_eth),
            'counterparty': self.counterparty,
            'function_selector': self.function_selector,
        }


@dataclass
class JournalEntry:
    """GAAP-compliant journal entry"""
    date: datetime
    account_name: str
    debit_crypto: Decimal
    credit_crypto: Decimal
    cryptocurrency: str

    # Metadata
    tx_hash: str
    fund_id: str
    wallet_id: str
    transaction_type: str
    platform: str
    counterparty: Optional[str] = None

    # USD values (to be filled later with price data)
    debit_usd: Optional[Decimal] = None
    credit_usd: Optional[Decimal] = None
    eth_usd_price: Optional[Decimal] = None

    def to_dict(self) -> Dict:
        return {
            'date': self.date,
            'account_name': self.account_name,
            'debit_crypto': float(self.debit_crypto),
            'credit_crypto': float(self.credit_crypto),
            'cryptocurrency': self.cryptocurrency,
            'tx_hash': self.tx_hash,
            'fund_id': self.fund_id,
            'wallet_id': self.wallet_id,
            'transaction_type': self.transaction_type,
            'platform': self.platform,
            'counterparty': self.counterparty,
            'debit_USD': float(self.debit_usd) if self.debit_usd else None,
            'credit_USD': float(self.credit_usd) if self.credit_usd else None,
            'eth_usd_price': float(self.eth_usd_price) if self.eth_usd_price else None,
        }


# ============================================================================
# WETH DECODER
# ============================================================================

class WETHDecoder:
    """
    Decodes WETH wrap/unwrap transactions.

    GAAP Treatment:
    - deposit(): Exchange ETH for WETH (no gain/loss, asset reclassification)
    - withdraw(): Exchange WETH for ETH (no gain/loss, asset reclassification)

    Journal Entries:
    - Deposit: Dr WETH, Cr ETH
    - Withdraw: Dr ETH, Cr WETH
    """

    PROTOCOL = "WETH"
    CATEGORY = "TOKEN_WRAP"

    def __init__(self, w3: Web3, wallet_metadata: Dict[str, Dict]):
        self.w3 = w3
        self.wallet_metadata = {k.lower(): v for k, v in wallet_metadata.items()}
        self.weth_contract = w3.to_checksum_address(WETH_ADDRESS)

    def decode(self, tx_hash: str) -> Optional[DecodedTransaction]:
        """Decode a WETH transaction"""
        try:
            tx = self.w3.eth.get_transaction(tx_hash)
            receipt = self.w3.eth.get_transaction_receipt(tx_hash)
            block = self.w3.eth.get_block(tx['blockNumber'])

            # Verify it's a WETH transaction
            to_addr = tx.get('to', '').lower() if tx.get('to') else ''
            if to_addr != WETH_ADDRESS.lower():
                return None

            # Get function selector
            input_data = tx.get('input', b'')
            if isinstance(input_data, bytes):
                input_data = '0x' + input_data.hex()
            selector = input_data[:10] if len(input_data) >= 10 else '0x'

            # Determine operation
            operation = WETH_SELECTORS.get(selector.lower(), 'unknown')

            # Calculate values
            value_eth = Decimal(tx['value']) / WAD
            gas_used = receipt['gasUsed']
            gas_price = Decimal(tx.get('gasPrice', 0))
            gas_price_gwei = gas_price / Decimal(10**9)
            gas_cost_eth = (gas_price * gas_used) / WAD

            # For withdraw, parse amount from input data
            if operation == 'withdraw' and len(input_data) >= 74:
                # withdraw(uint256 wad) - amount is in bytes 4-36
                amount_hex = input_data[10:74]
                value_eth = Decimal(int(amount_hex, 16)) / WAD

            timestamp = datetime.fromtimestamp(block['timestamp'], tz=timezone.utc)

            return DecodedTransaction(
                tx_hash=tx_hash,
                block_number=tx['blockNumber'],
                timestamp=timestamp,
                from_address=tx['from'].lower(),
                to_address=to_addr,
                protocol=self.PROTOCOL,
                operation=operation,
                category=self.CATEGORY,
                value_eth=value_eth,
                value_token=value_eth,  # 1:1 for WETH
                token_symbol='WETH',
                token_address=WETH_ADDRESS,
                gas_used=gas_used,
                gas_price_gwei=gas_price_gwei,
                gas_cost_eth=gas_cost_eth,
                input_data=input_data,
                function_selector=selector,
            )

        except Exception as e:
            print(f"⚠️ Error decoding WETH tx {tx_hash}: {e}")
            return None

    def generate_journal_entries(
        self,
        decoded: DecodedTransaction,
        fund_id: str,
        wallet_id: str,
    ) -> List[JournalEntry]:
        """Generate journal entries for WETH wrap/unwrap"""

        entries = []
        amount = decoded.value_eth

        if decoded.operation == 'deposit':
            # Wrap ETH -> WETH
            # Dr: Digital Assets - WETH
            # Cr: Digital Assets - ETH
            entries.append(JournalEntry(
                date=decoded.timestamp,
                account_name='digital_assets_weth',
                debit_crypto=amount,
                credit_crypto=Decimal(0),
                cryptocurrency='WETH',
                tx_hash=decoded.tx_hash,
                fund_id=fund_id,
                wallet_id=wallet_id,
                transaction_type='token_wrap',
                platform=self.PROTOCOL,
            ))
            entries.append(JournalEntry(
                date=decoded.timestamp,
                account_name='digital_assets_eth',
                debit_crypto=Decimal(0),
                credit_crypto=amount,
                cryptocurrency='ETH',
                tx_hash=decoded.tx_hash,
                fund_id=fund_id,
                wallet_id=wallet_id,
                transaction_type='token_wrap',
                platform=self.PROTOCOL,
            ))

        elif decoded.operation == 'withdraw':
            # Unwrap WETH -> ETH
            # Dr: Digital Assets - ETH
            # Cr: Digital Assets - WETH
            entries.append(JournalEntry(
                date=decoded.timestamp,
                account_name='digital_assets_eth',
                debit_crypto=amount,
                credit_crypto=Decimal(0),
                cryptocurrency='ETH',
                tx_hash=decoded.tx_hash,
                fund_id=fund_id,
                wallet_id=wallet_id,
                transaction_type='token_unwrap',
                platform=self.PROTOCOL,
            ))
            entries.append(JournalEntry(
                date=decoded.timestamp,
                account_name='digital_assets_weth',
                debit_crypto=Decimal(0),
                credit_crypto=amount,
                cryptocurrency='WETH',
                tx_hash=decoded.tx_hash,
                fund_id=fund_id,
                wallet_id=wallet_id,
                transaction_type='token_unwrap',
                platform=self.PROTOCOL,
            ))

        # Add gas fee entry
        if decoded.gas_cost_eth > 0:
            entries.append(JournalEntry(
                date=decoded.timestamp,
                account_name='gas_fees_expense',
                debit_crypto=decoded.gas_cost_eth,
                credit_crypto=Decimal(0),
                cryptocurrency='ETH',
                tx_hash=decoded.tx_hash,
                fund_id=fund_id,
                wallet_id=wallet_id,
                transaction_type='gas_fee',
                platform=self.PROTOCOL,
            ))
            entries.append(JournalEntry(
                date=decoded.timestamp,
                account_name='digital_assets_eth',
                debit_crypto=Decimal(0),
                credit_crypto=decoded.gas_cost_eth,
                cryptocurrency='ETH',
                tx_hash=decoded.tx_hash,
                fund_id=fund_id,
                wallet_id=wallet_id,
                transaction_type='gas_fee',
                platform=self.PROTOCOL,
            ))

        return entries


# ============================================================================
# ETH TRANSFER DECODER
# ============================================================================

class ETHTransferDecoder:
    """
    Decodes plain ETH transfers to EOA wallets.

    GAAP Treatment:
    - Outgoing: Decrease ETH balance
    - Incoming: Increase ETH balance
    - May be loan disbursement, repayment, or internal transfer

    Tax Treatment:
    - Internal transfers: No taxable event
    - To third party: May be loan disbursement (no gain/loss)
    - From third party: May be loan repayment (interest income portion taxable)
    """

    PROTOCOL = "ETH_TRANSFER"
    CATEGORY = "TRANSFER"

    def __init__(self, w3: Web3, wallet_metadata: Dict[str, Dict]):
        self.w3 = w3
        self.wallet_metadata = {k.lower(): v for k, v in wallet_metadata.items()}
        self.fund_wallets = set(self.wallet_metadata.keys())

    def decode(self, tx_hash: str) -> Optional[DecodedTransaction]:
        """Decode an ETH transfer transaction"""
        try:
            tx = self.w3.eth.get_transaction(tx_hash)
            receipt = self.w3.eth.get_transaction_receipt(tx_hash)
            block = self.w3.eth.get_block(tx['blockNumber'])

            from_addr = tx['from'].lower()
            to_addr = tx.get('to', '').lower() if tx.get('to') else ''

            # Get input data
            input_data = tx.get('input', b'')
            if isinstance(input_data, bytes):
                input_data = '0x' + input_data.hex()

            # Check if it's a simple ETH transfer (no input data or just 0x)
            if len(input_data) > 2 and input_data != '0x':
                return None  # Has call data, not a simple transfer

            # Calculate values
            value_eth = Decimal(tx['value']) / WAD
            gas_used = receipt['gasUsed']
            gas_price = Decimal(tx.get('gasPrice', 0))
            gas_price_gwei = gas_price / Decimal(10**9)
            gas_cost_eth = (gas_price * gas_used) / WAD

            # Determine if internal transfer
            is_from_fund = from_addr in self.fund_wallets
            is_to_fund = to_addr in self.fund_wallets

            if is_from_fund and is_to_fund:
                operation = 'internal_transfer'
            elif is_from_fund:
                operation = 'outgoing_transfer'
            elif is_to_fund:
                operation = 'incoming_transfer'
            else:
                operation = 'unknown_transfer'

            timestamp = datetime.fromtimestamp(block['timestamp'], tz=timezone.utc)

            return DecodedTransaction(
                tx_hash=tx_hash,
                block_number=tx['blockNumber'],
                timestamp=timestamp,
                from_address=from_addr,
                to_address=to_addr,
                protocol=self.PROTOCOL,
                operation=operation,
                category=self.CATEGORY,
                value_eth=value_eth,
                gas_used=gas_used,
                gas_price_gwei=gas_price_gwei,
                gas_cost_eth=gas_cost_eth,
                counterparty=to_addr if is_from_fund else from_addr,
                input_data=input_data,
                function_selector='0x',
            )

        except Exception as e:
            print(f"⚠️ Error decoding ETH transfer {tx_hash}: {e}")
            return None

    def generate_journal_entries(
        self,
        decoded: DecodedTransaction,
        fund_id: str,
        wallet_id: str,
    ) -> List[JournalEntry]:
        """Generate journal entries for ETH transfer"""

        entries = []
        amount = decoded.value_eth

        if decoded.operation == 'outgoing_transfer':
            # ETH leaving the fund
            # Dr: Deemed Cash / Loan Disbursement / etc.
            # Cr: Digital Assets - ETH
            entries.append(JournalEntry(
                date=decoded.timestamp,
                account_name='deemed_cash_eth',  # Or loan_receivable if it's a loan
                debit_crypto=amount,
                credit_crypto=Decimal(0),
                cryptocurrency='ETH',
                tx_hash=decoded.tx_hash,
                fund_id=fund_id,
                wallet_id=wallet_id,
                transaction_type='eth_transfer_out',
                platform=self.PROTOCOL,
                counterparty=decoded.counterparty,
            ))
            entries.append(JournalEntry(
                date=decoded.timestamp,
                account_name='digital_assets_eth',
                debit_crypto=Decimal(0),
                credit_crypto=amount,
                cryptocurrency='ETH',
                tx_hash=decoded.tx_hash,
                fund_id=fund_id,
                wallet_id=wallet_id,
                transaction_type='eth_transfer_out',
                platform=self.PROTOCOL,
                counterparty=decoded.counterparty,
            ))

        elif decoded.operation == 'incoming_transfer':
            # ETH entering the fund
            # Dr: Digital Assets - ETH
            # Cr: Deemed Cash / Loan Repayment / etc.
            entries.append(JournalEntry(
                date=decoded.timestamp,
                account_name='digital_assets_eth',
                debit_crypto=amount,
                credit_crypto=Decimal(0),
                cryptocurrency='ETH',
                tx_hash=decoded.tx_hash,
                fund_id=fund_id,
                wallet_id=wallet_id,
                transaction_type='eth_transfer_in',
                platform=self.PROTOCOL,
                counterparty=decoded.counterparty,
            ))
            entries.append(JournalEntry(
                date=decoded.timestamp,
                account_name='deemed_cash_eth',
                debit_crypto=Decimal(0),
                credit_crypto=amount,
                cryptocurrency='ETH',
                tx_hash=decoded.tx_hash,
                fund_id=fund_id,
                wallet_id=wallet_id,
                transaction_type='eth_transfer_in',
                platform=self.PROTOCOL,
                counterparty=decoded.counterparty,
            ))

        elif decoded.operation == 'internal_transfer':
            # Transfer between fund wallets - no P&L impact
            # Just track the movement for reconciliation
            entries.append(JournalEntry(
                date=decoded.timestamp,
                account_name='digital_assets_eth',  # Different wallet
                debit_crypto=amount,
                credit_crypto=Decimal(0),
                cryptocurrency='ETH',
                tx_hash=decoded.tx_hash,
                fund_id=fund_id,
                wallet_id=decoded.to_address,  # Receiving wallet
                transaction_type='internal_transfer',
                platform=self.PROTOCOL,
            ))
            entries.append(JournalEntry(
                date=decoded.timestamp,
                account_name='digital_assets_eth',
                debit_crypto=Decimal(0),
                credit_crypto=amount,
                cryptocurrency='ETH',
                tx_hash=decoded.tx_hash,
                fund_id=fund_id,
                wallet_id=decoded.from_address,  # Sending wallet
                transaction_type='internal_transfer',
                platform=self.PROTOCOL,
            ))

        # Add gas fee entry (only for outgoing/internal - sender pays gas)
        if decoded.gas_cost_eth > 0 and decoded.from_address in self.fund_wallets:
            entries.append(JournalEntry(
                date=decoded.timestamp,
                account_name='gas_fees_expense',
                debit_crypto=decoded.gas_cost_eth,
                credit_crypto=Decimal(0),
                cryptocurrency='ETH',
                tx_hash=decoded.tx_hash,
                fund_id=fund_id,
                wallet_id=decoded.from_address,
                transaction_type='gas_fee',
                platform=self.PROTOCOL,
            ))
            entries.append(JournalEntry(
                date=decoded.timestamp,
                account_name='digital_assets_eth',
                debit_crypto=Decimal(0),
                credit_crypto=decoded.gas_cost_eth,
                cryptocurrency='ETH',
                tx_hash=decoded.tx_hash,
                fund_id=fund_id,
                wallet_id=decoded.from_address,
                transaction_type='gas_fee',
                platform=self.PROTOCOL,
            ))

        return entries


# ============================================================================
# ERC20 DECODER
# ============================================================================

class ERC20Decoder:
    """
    Decodes ERC20 token transactions (USDC, USDT, etc.)

    GAAP Treatment:
    - transfer(): Movement of token assets
    - approve(): No journal entry (just permission)
    - transferFrom(): Movement of token assets
    """

    PROTOCOL = "ERC20"
    CATEGORY = "TOKEN_TRANSFER"

    # Token decimals
    TOKEN_DECIMALS = {
        USDC_ADDRESS.lower(): 6,
        USDT_ADDRESS.lower(): 6,
        WETH_ADDRESS.lower(): 18,
    }

    TOKEN_SYMBOLS = {
        USDC_ADDRESS.lower(): 'USDC',
        USDT_ADDRESS.lower(): 'USDT',
        WETH_ADDRESS.lower(): 'WETH',
    }

    def __init__(self, w3: Web3, wallet_metadata: Dict[str, Dict]):
        self.w3 = w3
        self.wallet_metadata = {k.lower(): v for k, v in wallet_metadata.items()}
        self.fund_wallets = set(self.wallet_metadata.keys())

    def decode(self, tx_hash: str) -> Optional[DecodedTransaction]:
        """Decode an ERC20 transaction"""
        try:
            tx = self.w3.eth.get_transaction(tx_hash)
            receipt = self.w3.eth.get_transaction_receipt(tx_hash)
            block = self.w3.eth.get_block(tx['blockNumber'])

            to_addr = tx.get('to', '').lower() if tx.get('to') else ''

            # Get input data
            input_data = tx.get('input', b'')
            if isinstance(input_data, bytes):
                input_data = '0x' + input_data.hex()

            selector = input_data[:10].lower() if len(input_data) >= 10 else '0x'

            # Check if it's an ERC20 function
            operation = ERC20_SELECTORS.get(selector)
            if not operation:
                return None

            # Get token info
            decimals = self.TOKEN_DECIMALS.get(to_addr, 18)
            symbol = self.TOKEN_SYMBOLS.get(to_addr, 'UNKNOWN')

            # Parse transfer amount and recipient from input data
            value_token = Decimal(0)
            counterparty = None

            if operation == 'transfer' and len(input_data) >= 138:
                # transfer(address to, uint256 amount)
                counterparty = '0x' + input_data[34:74]
                amount_hex = input_data[74:138]
                value_token = Decimal(int(amount_hex, 16)) / Decimal(10**decimals)

            elif operation == 'approve' and len(input_data) >= 138:
                # approve(address spender, uint256 amount)
                counterparty = '0x' + input_data[34:74]
                amount_hex = input_data[74:138]
                value_token = Decimal(int(amount_hex, 16)) / Decimal(10**decimals)

            elif operation == 'transferFrom' and len(input_data) >= 202:
                # transferFrom(address from, address to, uint256 amount)
                # from = input_data[34:74]
                counterparty = '0x' + input_data[98:138]
                amount_hex = input_data[138:202]
                value_token = Decimal(int(amount_hex, 16)) / Decimal(10**decimals)

            # Calculate gas
            gas_used = receipt['gasUsed']
            gas_price = Decimal(tx.get('gasPrice', 0))
            gas_price_gwei = gas_price / Decimal(10**9)
            gas_cost_eth = (gas_price * gas_used) / WAD

            timestamp = datetime.fromtimestamp(block['timestamp'], tz=timezone.utc)

            return DecodedTransaction(
                tx_hash=tx_hash,
                block_number=tx['blockNumber'],
                timestamp=timestamp,
                from_address=tx['from'].lower(),
                to_address=to_addr,
                protocol=self.PROTOCOL,
                operation=operation,
                category=self.CATEGORY,
                value_eth=Decimal(tx['value']) / WAD,
                value_token=value_token,
                token_symbol=symbol,
                token_address=to_addr,
                gas_used=gas_used,
                gas_price_gwei=gas_price_gwei,
                gas_cost_eth=gas_cost_eth,
                counterparty=counterparty,
                input_data=input_data,
                function_selector=selector,
            )

        except Exception as e:
            print(f"⚠️ Error decoding ERC20 tx {tx_hash}: {e}")
            return None

    def generate_journal_entries(
        self,
        decoded: DecodedTransaction,
        fund_id: str,
        wallet_id: str,
    ) -> List[JournalEntry]:
        """Generate journal entries for ERC20 transactions"""

        entries = []
        amount = decoded.value_token or Decimal(0)
        symbol = decoded.token_symbol or 'TOKEN'

        if decoded.operation == 'transfer':
            is_from_fund = decoded.from_address in self.fund_wallets

            if is_from_fund:
                # Outgoing token transfer
                entries.append(JournalEntry(
                    date=decoded.timestamp,
                    account_name=f'deemed_cash_{symbol.lower()}',
                    debit_crypto=amount,
                    credit_crypto=Decimal(0),
                    cryptocurrency=symbol,
                    tx_hash=decoded.tx_hash,
                    fund_id=fund_id,
                    wallet_id=wallet_id,
                    transaction_type='token_transfer_out',
                    platform=self.PROTOCOL,
                    counterparty=decoded.counterparty,
                ))
                entries.append(JournalEntry(
                    date=decoded.timestamp,
                    account_name=f'digital_assets_{symbol.lower()}',
                    debit_crypto=Decimal(0),
                    credit_crypto=amount,
                    cryptocurrency=symbol,
                    tx_hash=decoded.tx_hash,
                    fund_id=fund_id,
                    wallet_id=wallet_id,
                    transaction_type='token_transfer_out',
                    platform=self.PROTOCOL,
                    counterparty=decoded.counterparty,
                ))

        elif decoded.operation == 'approve':
            # Approval doesn't need journal entry - just permission
            # But we track it for audit trail
            pass

        # Add gas fee entry
        if decoded.gas_cost_eth > 0:
            entries.append(JournalEntry(
                date=decoded.timestamp,
                account_name='gas_fees_expense',
                debit_crypto=decoded.gas_cost_eth,
                credit_crypto=Decimal(0),
                cryptocurrency='ETH',
                tx_hash=decoded.tx_hash,
                fund_id=fund_id,
                wallet_id=wallet_id,
                transaction_type='gas_fee',
                platform=self.PROTOCOL,
            ))
            entries.append(JournalEntry(
                date=decoded.timestamp,
                account_name='digital_assets_eth',
                debit_crypto=Decimal(0),
                credit_crypto=decoded.gas_cost_eth,
                cryptocurrency='ETH',
                tx_hash=decoded.tx_hash,
                fund_id=fund_id,
                wallet_id=wallet_id,
                transaction_type='gas_fee',
                platform=self.PROTOCOL,
            ))

        return entries


# ============================================================================
# SEAPORT DECODER (OpenSea NFT Marketplace)
# ============================================================================

class SeaportDecoder:
    """
    Decodes OpenSea Seaport transactions.

    GAAP Treatment:
    - NFT Purchase: Dr NFT Asset, Cr ETH/WETH
    - NFT Sale: Dr ETH/WETH, Cr NFT Asset, recognize gain/loss
    """

    PROTOCOL = "OpenSea Seaport"
    CATEGORY = "NFT_MARKETPLACE"

    SEAPORT_ADDRESSES = [
        "0x00000000000000adc04c56bf30ac9d3c0aaf14dc",  # Seaport 1.5
        "0x00000000000001ad428e4906ae43d8f9852d0dd6",  # Seaport 1.6
        "0x0000000000000068f116a894984e2db1123eb395",  # Seaport 1.6 alternate
    ]

    def __init__(self, w3: Web3, wallet_metadata: Dict[str, Dict]):
        self.w3 = w3
        self.wallet_metadata = {k.lower(): v for k, v in wallet_metadata.items()}
        self.fund_wallets = set(self.wallet_metadata.keys())

    def decode(self, tx_hash: str) -> Optional[DecodedTransaction]:
        """Decode a Seaport transaction"""
        try:
            tx = self.w3.eth.get_transaction(tx_hash)
            receipt = self.w3.eth.get_transaction_receipt(tx_hash)
            block = self.w3.eth.get_block(tx['blockNumber'])

            to_addr = tx.get('to', '').lower() if tx.get('to') else ''

            # Verify it's a Seaport transaction
            if to_addr not in self.SEAPORT_ADDRESSES:
                return None

            # Get input data
            input_data = tx.get('input', b'')
            if isinstance(input_data, bytes):
                input_data = '0x' + input_data.hex()

            selector = input_data[:10].lower() if len(input_data) >= 10 else '0x'
            operation = SEAPORT_SELECTORS.get(selector, 'seaport_unknown')

            # Calculate values
            value_eth = Decimal(tx['value']) / WAD
            gas_used = receipt['gasUsed']
            gas_price = Decimal(tx.get('gasPrice', 0))
            gas_price_gwei = gas_price / Decimal(10**9)
            gas_cost_eth = (gas_price * gas_used) / WAD

            # Parse NFT details from logs (simplified - would need full ABI for complete parsing)
            nft_contract = None
            nft_token_id = None

            # Look for ERC721 Transfer events
            transfer_topic = self.w3.keccak(text="Transfer(address,address,uint256)")
            for log in receipt['logs']:
                if len(log['topics']) >= 4 and log['topics'][0] == transfer_topic:
                    nft_contract = log['address'].lower()
                    nft_token_id = int(log['topics'][3].hex(), 16)
                    break

            timestamp = datetime.fromtimestamp(block['timestamp'], tz=timezone.utc)

            # Determine if buy or sell
            from_addr = tx['from'].lower()
            if from_addr in self.fund_wallets and value_eth > 0:
                operation = 'nft_purchase'
            elif from_addr in self.fund_wallets and value_eth == 0:
                operation = 'nft_sale'  # Receiving ETH from logs

            return DecodedTransaction(
                tx_hash=tx_hash,
                block_number=tx['blockNumber'],
                timestamp=timestamp,
                from_address=from_addr,
                to_address=to_addr,
                protocol=self.PROTOCOL,
                operation=operation,
                category=self.CATEGORY,
                value_eth=value_eth,
                gas_used=gas_used,
                gas_price_gwei=gas_price_gwei,
                gas_cost_eth=gas_cost_eth,
                nft_contract=nft_contract,
                nft_token_id=nft_token_id,
                input_data=input_data,
                function_selector=selector,
            )

        except Exception as e:
            print(f"⚠️ Error decoding Seaport tx {tx_hash}: {e}")
            return None

    def generate_journal_entries(
        self,
        decoded: DecodedTransaction,
        fund_id: str,
        wallet_id: str,
    ) -> List[JournalEntry]:
        """Generate journal entries for Seaport transactions"""

        entries = []
        amount = decoded.value_eth

        if decoded.operation == 'nft_purchase':
            # Buying NFT
            # Dr: Investments - NFTs
            # Cr: Digital Assets - ETH
            entries.append(JournalEntry(
                date=decoded.timestamp,
                account_name='investments_nfts',
                debit_crypto=amount,
                credit_crypto=Decimal(0),
                cryptocurrency='ETH',  # Cost basis in ETH
                tx_hash=decoded.tx_hash,
                fund_id=fund_id,
                wallet_id=wallet_id,
                transaction_type='nft_purchase',
                platform=self.PROTOCOL,
            ))
            entries.append(JournalEntry(
                date=decoded.timestamp,
                account_name='digital_assets_eth',
                debit_crypto=Decimal(0),
                credit_crypto=amount,
                cryptocurrency='ETH',
                tx_hash=decoded.tx_hash,
                fund_id=fund_id,
                wallet_id=wallet_id,
                transaction_type='nft_purchase',
                platform=self.PROTOCOL,
            ))

        elif decoded.operation == 'nft_sale':
            # Selling NFT - need cost basis lookup for gain/loss
            # Simplified: Dr ETH, Cr NFT Investment
            entries.append(JournalEntry(
                date=decoded.timestamp,
                account_name='digital_assets_eth',
                debit_crypto=amount,
                credit_crypto=Decimal(0),
                cryptocurrency='ETH',
                tx_hash=decoded.tx_hash,
                fund_id=fund_id,
                wallet_id=wallet_id,
                transaction_type='nft_sale',
                platform=self.PROTOCOL,
            ))
            entries.append(JournalEntry(
                date=decoded.timestamp,
                account_name='investments_nfts',
                debit_crypto=Decimal(0),
                credit_crypto=amount,  # Would need actual cost basis
                cryptocurrency='ETH',
                tx_hash=decoded.tx_hash,
                fund_id=fund_id,
                wallet_id=wallet_id,
                transaction_type='nft_sale',
                platform=self.PROTOCOL,
            ))

        # Add gas fee entry
        if decoded.gas_cost_eth > 0:
            entries.append(JournalEntry(
                date=decoded.timestamp,
                account_name='gas_fees_expense',
                debit_crypto=decoded.gas_cost_eth,
                credit_crypto=Decimal(0),
                cryptocurrency='ETH',
                tx_hash=decoded.tx_hash,
                fund_id=fund_id,
                wallet_id=wallet_id,
                transaction_type='gas_fee',
                platform=self.PROTOCOL,
            ))
            entries.append(JournalEntry(
                date=decoded.timestamp,
                account_name='digital_assets_eth',
                debit_crypto=Decimal(0),
                credit_crypto=decoded.gas_cost_eth,
                cryptocurrency='ETH',
                tx_hash=decoded.tx_hash,
                fund_id=fund_id,
                wallet_id=wallet_id,
                transaction_type='gas_fee',
                platform=self.PROTOCOL,
            ))

        return entries


# ============================================================================
# GNOSIS SAFE DECODER
# ============================================================================

class GnosisSafeDecoder:
    """
    Decodes Gnosis Safe multisig transactions.

    The actual operation is embedded in the execTransaction call.
    We need to decode the inner transaction.
    """

    PROTOCOL = "Gnosis Safe"
    CATEGORY = "MULTISIG"

    def __init__(self, w3: Web3, wallet_metadata: Dict[str, Dict]):
        self.w3 = w3
        self.wallet_metadata = {k.lower(): v for k, v in wallet_metadata.items()}

    def decode(self, tx_hash: str) -> Optional[DecodedTransaction]:
        """Decode a Gnosis Safe transaction"""
        try:
            tx = self.w3.eth.get_transaction(tx_hash)
            receipt = self.w3.eth.get_transaction_receipt(tx_hash)
            block = self.w3.eth.get_block(tx['blockNumber'])

            # Get input data
            input_data = tx.get('input', b'')
            if isinstance(input_data, bytes):
                input_data = '0x' + input_data.hex()

            selector = input_data[:10].lower() if len(input_data) >= 10 else '0x'

            if selector != '0x6a761202':  # execTransaction
                return None

            # Calculate values
            value_eth = Decimal(tx['value']) / WAD
            gas_used = receipt['gasUsed']
            gas_price = Decimal(tx.get('gasPrice', 0))
            gas_price_gwei = gas_price / Decimal(10**9)
            gas_cost_eth = (gas_price * gas_used) / WAD

            timestamp = datetime.fromtimestamp(block['timestamp'], tz=timezone.utc)

            return DecodedTransaction(
                tx_hash=tx_hash,
                block_number=tx['blockNumber'],
                timestamp=timestamp,
                from_address=tx['from'].lower(),
                to_address=tx.get('to', '').lower() if tx.get('to') else '',
                protocol=self.PROTOCOL,
                operation='execTransaction',
                category=self.CATEGORY,
                value_eth=value_eth,
                gas_used=gas_used,
                gas_price_gwei=gas_price_gwei,
                gas_cost_eth=gas_cost_eth,
                input_data=input_data,
                function_selector=selector,
            )

        except Exception as e:
            print(f"⚠️ Error decoding Gnosis Safe tx {tx_hash}: {e}")
            return None

    def generate_journal_entries(
        self,
        decoded: DecodedTransaction,
        fund_id: str,
        wallet_id: str,
    ) -> List[JournalEntry]:
        """Generate journal entries for Gnosis Safe transactions"""
        # The inner transaction determines the actual entries
        # For now, just record gas fees
        entries = []

        if decoded.gas_cost_eth > 0:
            entries.append(JournalEntry(
                date=decoded.timestamp,
                account_name='gas_fees_expense',
                debit_crypto=decoded.gas_cost_eth,
                credit_crypto=Decimal(0),
                cryptocurrency='ETH',
                tx_hash=decoded.tx_hash,
                fund_id=fund_id,
                wallet_id=wallet_id,
                transaction_type='gas_fee',
                platform=self.PROTOCOL,
            ))
            entries.append(JournalEntry(
                date=decoded.timestamp,
                account_name='digital_assets_eth',
                debit_crypto=Decimal(0),
                credit_crypto=decoded.gas_cost_eth,
                cryptocurrency='ETH',
                tx_hash=decoded.tx_hash,
                fund_id=fund_id,
                wallet_id=wallet_id,
                transaction_type='gas_fee',
                platform=self.PROTOCOL,
            ))

        return entries


# ============================================================================
# MASTER DECODER
# ============================================================================

class March2025Decoder:
    """
    Master decoder that routes transactions to appropriate decoders.
    """

    def __init__(self, w3: Web3, wallet_metadata: Dict[str, Dict]):
        self.w3 = w3
        self.wallet_metadata = {k.lower(): v for k, v in wallet_metadata.items()}

        # Initialize all decoders
        self.weth_decoder = WETHDecoder(w3, wallet_metadata)
        self.eth_transfer_decoder = ETHTransferDecoder(w3, wallet_metadata)
        self.erc20_decoder = ERC20Decoder(w3, wallet_metadata)
        self.seaport_decoder = SeaportDecoder(w3, wallet_metadata)
        self.gnosis_safe_decoder = GnosisSafeDecoder(w3, wallet_metadata)

        # Contract address to decoder mapping
        self.contract_decoders = {
            WETH_ADDRESS.lower(): self.weth_decoder,
            USDC_ADDRESS.lower(): self.erc20_decoder,
            USDT_ADDRESS.lower(): self.erc20_decoder,
        }

        # Add Seaport addresses
        for addr in self.seaport_decoder.SEAPORT_ADDRESSES:
            self.contract_decoders[addr] = self.seaport_decoder

    def decode_transaction(self, tx_hash: str) -> Optional[DecodedTransaction]:
        """Decode any transaction by routing to appropriate decoder"""
        try:
            tx = self.w3.eth.get_transaction(tx_hash)
            to_addr = tx.get('to', '').lower() if tx.get('to') else ''

            # Check input data
            input_data = tx.get('input', b'')
            if isinstance(input_data, bytes):
                input_data = '0x' + input_data.hex()

            # Route to appropriate decoder
            if to_addr in self.contract_decoders:
                decoder = self.contract_decoders[to_addr]
                return decoder.decode(tx_hash)

            # Check for Gnosis Safe execTransaction
            selector = input_data[:10].lower() if len(input_data) >= 10 else '0x'
            if selector == '0x6a761202':
                return self.gnosis_safe_decoder.decode(tx_hash)

            # Check if simple ETH transfer (no input data)
            if len(input_data) <= 2 or input_data == '0x':
                return self.eth_transfer_decoder.decode(tx_hash)

            # Unknown transaction type
            return None

        except Exception as e:
            print(f"⚠️ Error routing tx {tx_hash}: {e}")
            return None

    def decode_batch(self, tx_hashes: List[str]) -> List[DecodedTransaction]:
        """Decode a batch of transactions"""
        decoded = []

        for tx_hash in tx_hashes:
            result = self.decode_transaction(tx_hash)
            if result:
                decoded.append(result)

        return decoded

    def generate_all_journal_entries(
        self,
        decoded_txs: List[DecodedTransaction],
        fund_id: str,
    ) -> pd.DataFrame:
        """Generate journal entries for all decoded transactions"""
        all_entries = []

        for tx in decoded_txs:
            wallet_id = tx.from_address

            # Route to appropriate journal entry generator
            if tx.protocol == 'WETH':
                entries = self.weth_decoder.generate_journal_entries(tx, fund_id, wallet_id)
            elif tx.protocol == 'ETH_TRANSFER':
                entries = self.eth_transfer_decoder.generate_journal_entries(tx, fund_id, wallet_id)
            elif tx.protocol == 'ERC20':
                entries = self.erc20_decoder.generate_journal_entries(tx, fund_id, wallet_id)
            elif tx.protocol == 'OpenSea Seaport':
                entries = self.seaport_decoder.generate_journal_entries(tx, fund_id, wallet_id)
            elif tx.protocol == 'Gnosis Safe':
                entries = self.gnosis_safe_decoder.generate_journal_entries(tx, fund_id, wallet_id)
            else:
                entries = []

            all_entries.extend(entries)

        # Convert to DataFrame
        if all_entries:
            return pd.DataFrame([e.to_dict() for e in all_entries])

        return pd.DataFrame()


# ============================================================================
# USAGE EXAMPLE
# ============================================================================

if __name__ == "__main__":
    print("="*70)
    print("MARCH 2025 TRANSACTION DECODERS")
    print("="*70)
    print("""
Decoders included:
1. WETHDecoder - Wrap/unwrap ETH (65 txs)
2. ETHTransferDecoder - Plain ETH transfers (35 txs)
3. ERC20Decoder - USDC/USDT transfers (11 txs)
4. SeaportDecoder - OpenSea NFT trades (5 txs)
5. GnosisSafeDecoder - Multisig transactions (2 txs)

Usage:
    from march_2025_decoders import March2025Decoder

    # Initialize
    decoder = March2025Decoder(w3, wallet_metadata)

    # Decode transactions
    decoded = decoder.decode_batch(tx_hashes)

    # Generate journal entries
    journal_df = decoder.generate_all_journal_entries(decoded, fund_id='FUND_A')
    """)