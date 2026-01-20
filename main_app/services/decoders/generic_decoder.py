"""
Generic Transaction Decoder

Handles common transaction types:
- WETH wrap/unwrap
- ETH transfers
- ERC20 transfers
- Seaport NFT trades
- Gnosis Safe executions
"""

from web3 import Web3
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Any, Tuple
import logging

from .base import (
    BaseDecoder,
    DecodedTransaction,
    DecodedEvent,
    JournalEntry,
    LoanPosition,
    TransactionCategory,
    PostingStatus,
    TaxTreatment,
    Platform,
    wei_to_eth,
    calculate_gas_fee,
)
from .abis import load_abi, WETH_ABI, ERC20_ABI

logger = logging.getLogger(__name__)


# Contract addresses
WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".lower()
USDC_ADDRESS = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".lower()
USDT_ADDRESS = "0xdAC17F958D2ee523a2206206994597C13D831ec7".lower()

# Function selectors
WETH_SELECTORS = {
    "0xd0e30db0": "deposit",
    "0x2e1a7d4d": "withdraw",
}

ERC20_SELECTORS = {
    "0xa9059cbb": "transfer",
    "0x23b872dd": "transferFrom",
    "0x095ea7b3": "approve",
}

# Token decimals
TOKEN_DECIMALS = {
    "WETH": 18,
    "ETH": 18,
    "USDC": 6,
    "USDT": 6,
}


class GenericDecoder(BaseDecoder):
    """
    Generic decoder for common transaction types.

    Handles:
    - WETH deposit/withdraw
    - ETH transfers (value transfers)
    - ERC20 token transfers
    - Seaport NFT trades
    - Gnosis Safe multi-sig executions
    """

    PLATFORM = Platform.GENERIC
    CONTRACT_ADDRESSES = [WETH_ADDRESS]

    ACCOUNTS = {
        "eth_wallet": "100.30 - ETH Wallet",
        "weth_wallet": "100.31 - WETH Wallet",
        "usdc_wallet": "100.32 - USDC Wallet",
        "gas_expense": "600.10 - Gas Expense",
    }

    def __init__(self, w3: Web3, fund_wallets: List[str]):
        super().__init__(w3, fund_wallets)
        self.weth_contract = None

    def _load_abis(self):
        """Load ABIs for generic contracts"""
        try:
            self.weth_contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(WETH_ADDRESS),
                abi=WETH_ABI
            )
        except Exception as e:
            logger.warning(f"Failed to load WETH contract: {e}")

    def can_decode(self, tx: Dict, receipt: Dict) -> bool:
        """Check if this is a generic transaction we can decode"""
        to_address = (tx.get('to') or '').lower()

        # WETH operations
        if to_address == WETH_ADDRESS:
            return True

        # Simple ETH transfer (no input data)
        input_data = tx.get('input', '0x')
        if isinstance(input_data, bytes):
            input_data = input_data.hex()
        if len(input_data) <= 2 and tx.get('value', 0) > 0:
            return True

        # Check function selector for ERC20
        if len(input_data) >= 10:
            selector = input_data[:10].lower()
            if selector in ERC20_SELECTORS:
                return True

        # Check for Transfer events in logs (handles Safe execTransaction and other wrappers)
        # Transfer event topic: ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
        TRANSFER_TOPIC = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        # WETH Deposit: keccak256("Deposit(address,uint256)")
        DEPOSIT_TOPIC = "e1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c"
        # WETH Withdrawal: keccak256("Withdrawal(address,uint256)")
        WITHDRAWAL_TOPIC = "7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65"

        for log in receipt.get('logs', []):
            if log.get('topics') and len(log['topics']) > 0:
                topic0 = log['topics'][0]
                if isinstance(topic0, bytes):
                    topic0 = topic0.hex()
                topic0_clean = topic0[2:] if topic0.startswith('0x') else topic0

                # Check for WETH Deposit/Withdrawal at WETH address
                log_addr = log.get('address', '').lower()
                if log_addr == WETH_ADDRESS:
                    if topic0_clean.lower() in [DEPOSIT_TOPIC, WITHDRAWAL_TOPIC]:
                        # Found WETH wrap/unwrap - check if fund wallet involved
                        try:
                            if len(log['topics']) > 1:
                                if isinstance(log['topics'][1], bytes):
                                    wallet_addr = "0x" + log['topics'][1].hex()[-40:]
                                else:
                                    wallet_addr = "0x" + log['topics'][1][-40:]
                                if self.is_fund_wallet(wallet_addr):
                                    return True
                        except Exception:
                            pass

                # Check for Transfer event
                if topic0_clean.lower() == TRANSFER_TOPIC:
                    # Found a Transfer event - check if fund wallet is involved
                    try:
                        from_addr = "0x" + log['topics'][1].hex()[-40:] if len(log['topics']) > 1 else ""
                        to_addr = "0x" + log['topics'][2].hex()[-40:] if len(log['topics']) > 2 else ""
                        if isinstance(log['topics'][1], bytes):
                            from_addr = "0x" + log['topics'][1].hex()[-40:]
                        else:
                            from_addr = "0x" + log['topics'][1][-40:]
                        if isinstance(log['topics'][2], bytes):
                            to_addr = "0x" + log['topics'][2].hex()[-40:]
                        else:
                            to_addr = "0x" + log['topics'][2][-40:]

                        if self.is_fund_wallet(from_addr) or self.is_fund_wallet(to_addr):
                            return True
                    except Exception:
                        pass

        return False

    def decode(self, tx: Dict, receipt: Dict, block: Dict, eth_price: Decimal) -> DecodedTransaction:
        """Decode generic transaction"""
        try:
            timestamp = datetime.fromtimestamp(block.get('timestamp', 0), tz=timezone.utc)
            gas_fee = calculate_gas_fee(receipt, tx)
            tx_hash = tx.get('hash', b'')
            if isinstance(tx_hash, bytes):
                tx_hash = tx_hash.hex()
            tx_hash = tx_hash if tx_hash.startswith('0x') else f'0x{tx_hash}'
            to_address = (tx.get('to') or '').lower()

            # Get input data
            input_data = tx.get('input', '0x')
            if isinstance(input_data, bytes):
                input_data = input_data.hex()

            # Determine transaction type and decode
            if to_address == WETH_ADDRESS:
                return self._decode_weth(tx, receipt, block, eth_price, timestamp, gas_fee, tx_hash, input_data)
            elif len(input_data) <= 2 and tx.get('value', 0) > 0:
                return self._decode_eth_transfer(tx, receipt, block, eth_price, timestamp, gas_fee, tx_hash)
            elif len(input_data) >= 10:
                selector = input_data[:10].lower()
                if selector in ERC20_SELECTORS:
                    return self._decode_erc20(tx, receipt, block, eth_price, timestamp, gas_fee, tx_hash, input_data)

            # Check for events in logs (handles Safe, proxy contracts, etc.)
            TRANSFER_TOPIC = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
            DEPOSIT_TOPIC = "e1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c"
            WITHDRAWAL_TOPIC = "7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65"

            has_fund_transfer = False
            has_weth_wrap = False

            for log in receipt.get('logs', []):
                if log.get('topics') and len(log['topics']) >= 1:
                    topic0 = log['topics'][0]
                    if isinstance(topic0, bytes):
                        topic0 = topic0.hex()
                    topic0_clean = topic0[2:] if topic0.startswith('0x') else topic0
                    log_addr = log.get('address', '').lower()

                    # Check for WETH Deposit/Withdrawal
                    if log_addr == WETH_ADDRESS and topic0_clean.lower() in [DEPOSIT_TOPIC, WITHDRAWAL_TOPIC]:
                        try:
                            if len(log['topics']) > 1:
                                if isinstance(log['topics'][1], bytes):
                                    wallet_addr = "0x" + log['topics'][1].hex()[-40:]
                                else:
                                    wallet_addr = "0x" + log['topics'][1][-40:]
                                if self.is_fund_wallet(wallet_addr):
                                    has_weth_wrap = True
                        except Exception:
                            pass

                    # Check for Transfer event
                    if len(log['topics']) >= 3 and topic0_clean.lower() == TRANSFER_TOPIC:
                        try:
                            if isinstance(log['topics'][1], bytes):
                                from_addr = "0x" + log['topics'][1].hex()[-40:]
                            else:
                                from_addr = "0x" + log['topics'][1][-40:]
                            if isinstance(log['topics'][2], bytes):
                                to_addr = "0x" + log['topics'][2].hex()[-40:]
                            else:
                                to_addr = "0x" + log['topics'][2][-40:]
                            if self.is_fund_wallet(from_addr) or self.is_fund_wallet(to_addr):
                                has_fund_transfer = True
                        except Exception:
                            pass

            # Route to appropriate decoder based on detected events
            if has_weth_wrap:
                return self._decode_weth_from_events(tx, receipt, block, eth_price, timestamp, gas_fee, tx_hash)
            if has_fund_transfer:
                return self._decode_erc20(tx, receipt, block, eth_price, timestamp, gas_fee, tx_hash, input_data)

            # Fallback to basic result
            return self._create_basic_result(tx, receipt, block, eth_price, timestamp, gas_fee, tx_hash)

        except Exception as e:
            logger.error(f"Error in generic decode: {e}")
            error_hash = tx.get('hash', b'')
            if isinstance(error_hash, bytes):
                error_hash = error_hash.hex()
            error_hash = error_hash if error_hash.startswith('0x') else f'0x{error_hash}'
            return self._create_error_result(error_hash, str(e))

    def _decode_weth(self, tx: Dict, receipt: Dict, block: Dict, eth_price: Decimal,
                     timestamp: datetime, gas_fee: Decimal, tx_hash: str, input_data: str) -> DecodedTransaction:
        """Decode WETH wrap/unwrap operation"""
        selector = input_data[:10].lower() if len(input_data) >= 10 else ""
        operation = WETH_SELECTORS.get(selector, "unknown")

        events = []
        journal_entries = []
        value = wei_to_eth(tx.get('value', 0))

        # Decode based on operation
        if operation == "deposit":
            # Wrapping ETH to WETH
            category = TransactionCategory.WETH_WRAP
            function_name = "deposit"

            # Decode Deposit event
            for log in receipt.get('logs', []):
                if log.get('address', '').lower() == WETH_ADDRESS:
                    try:
                        decoded = self.weth_contract.events.Deposit().process_log(log)
                        events.append(DecodedEvent(
                            name="Deposit",
                            args=dict(decoded['args']),
                            log_index=log['logIndex'],
                            contract_address=log['address']
                        ))
                        value = wei_to_eth(decoded['args'].get('wad', 0))
                    except Exception:
                        pass

            # Create journal entry for WETH wrap
            if value > 0 and self.is_fund_wallet(tx.get('from', '')):
                entry = JournalEntry(
                    entry_id=f"weth_wrap_{tx_hash[:8]}",
                    date=timestamp,
                    description=f"Wrap {value:.6f} ETH to WETH",
                    tx_hash=tx_hash,
                    category=category,
                    platform=Platform.GENERIC,
                    wallet_address=tx.get('from', ''),
                    wallet_role="wrapper",
                    eth_usd_price=eth_price,
                    posting_status=PostingStatus.AUTO_POST
                )
                entry.add_debit(self.ACCOUNTS["weth_wallet"], value, "WETH")
                entry.add_credit(self.ACCOUNTS["eth_wallet"], value, "ETH")
                journal_entries.append(entry)

        elif operation == "withdraw":
            # Unwrapping WETH to ETH
            category = TransactionCategory.WETH_UNWRAP
            function_name = "withdraw"

            # Decode Withdrawal event
            for log in receipt.get('logs', []):
                if log.get('address', '').lower() == WETH_ADDRESS:
                    try:
                        decoded = self.weth_contract.events.Withdrawal().process_log(log)
                        events.append(DecodedEvent(
                            name="Withdrawal",
                            args=dict(decoded['args']),
                            log_index=log['logIndex'],
                            contract_address=log['address']
                        ))
                        value = wei_to_eth(decoded['args'].get('wad', 0))
                    except Exception:
                        pass

            # Create journal entry for WETH unwrap
            if value > 0 and self.is_fund_wallet(tx.get('from', '')):
                entry = JournalEntry(
                    entry_id=f"weth_unwrap_{tx_hash[:8]}",
                    date=timestamp,
                    description=f"Unwrap {value:.6f} WETH to ETH",
                    tx_hash=tx_hash,
                    category=category,
                    platform=Platform.GENERIC,
                    wallet_address=tx.get('from', ''),
                    wallet_role="unwrapper",
                    eth_usd_price=eth_price,
                    posting_status=PostingStatus.AUTO_POST
                )
                entry.add_debit(self.ACCOUNTS["eth_wallet"], value, "ETH")
                entry.add_credit(self.ACCOUNTS["weth_wallet"], value, "WETH")
                journal_entries.append(entry)
        else:
            category = TransactionCategory.CONTRACT_CALL
            function_name = operation

        # Add gas entry
        if gas_fee > 0 and self.is_fund_wallet(tx.get('from', '')):
            gas_entry = JournalEntry(
                entry_id=f"gas_{tx_hash[:8]}",
                date=timestamp,
                description=f"Gas fee for {function_name}",
                tx_hash=tx_hash,
                category=TransactionCategory.ETH_TRANSFER,
                platform=Platform.GENERIC,
                wallet_address=tx.get('from', ''),
                wallet_role="payer",
                eth_usd_price=eth_price,
                posting_status=PostingStatus.AUTO_POST
            )
            gas_entry.add_debit(self.ACCOUNTS["gas_expense"], gas_fee, "ETH")
            gas_entry.add_credit(self.ACCOUNTS["eth_wallet"], gas_fee, "ETH")
            journal_entries.append(gas_entry)

        return DecodedTransaction(
            status="success",
            tx_hash=tx_hash,
            platform=Platform.GENERIC,
            category=category,
            block=tx.get('blockNumber', 0),
            timestamp=timestamp,
            eth_price=eth_price,
            gas_used=receipt.get('gasUsed', 0),
            gas_fee=gas_fee,
            from_address=tx.get('from', ''),
            to_address=tx.get('to', '') or '',
            value=value,
            function_name=function_name,
            events=events,
            journal_entries=journal_entries,
            wallet_roles={tx.get('from', ''): "sender"},
        )

    def _decode_weth_from_events(self, tx: Dict, receipt: Dict, block: Dict, eth_price: Decimal,
                                   timestamp: datetime, gas_fee: Decimal, tx_hash: str) -> DecodedTransaction:
        """Decode WETH wrap/unwrap from events (e.g., inside Safe transactions)"""
        DEPOSIT_TOPIC = "e1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c"
        WITHDRAWAL_TOPIC = "7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65"

        events = []
        journal_entries = []
        value = Decimal(0)
        category = TransactionCategory.CONTRACT_CALL
        function_name = "weth_operation"
        wallet_roles = {}

        # Decode WETH events from logs
        for log in receipt.get('logs', []):
            if log.get('address', '').lower() != WETH_ADDRESS:
                continue
            if not log.get('topics') or len(log['topics']) < 2:
                continue

            topic0 = log['topics'][0]
            if isinstance(topic0, bytes):
                topic0 = topic0.hex()
            topic0_clean = topic0[2:] if topic0.startswith('0x') else topic0

            try:
                # Get wallet address from indexed topic
                if isinstance(log['topics'][1], bytes):
                    wallet_addr = "0x" + log['topics'][1].hex()[-40:]
                else:
                    wallet_addr = "0x" + log['topics'][1][-40:]

                # Get amount from data
                data = log.get('data', b'')
                if isinstance(data, bytes):
                    data = data.hex()
                if data.startswith('0x'):
                    data = data[2:]
                amount = int(data, 16) if data else 0
                value = wei_to_eth(amount)

                if topic0_clean.lower() == DEPOSIT_TOPIC:
                    # WETH Deposit (wrapping ETH)
                    category = TransactionCategory.WETH_WRAP
                    function_name = "deposit"
                    events.append(DecodedEvent(
                        name="Deposit",
                        args={"dst": wallet_addr, "wad": str(amount)},
                        log_index=log.get('logIndex', 0),
                        contract_address=log['address']
                    ))

                    if self.is_fund_wallet(wallet_addr) and value > 0:
                        wallet_roles[wallet_addr] = "wrapper"
                        entry = JournalEntry(
                            entry_id=f"weth_wrap_{tx_hash[:8]}",
                            date=timestamp,
                            description=f"Wrap {value:.6f} ETH to WETH",
                            tx_hash=tx_hash,
                            category=category,
                            platform=Platform.GENERIC,
                            wallet_address=wallet_addr,
                            wallet_role="wrapper",
                            eth_usd_price=eth_price,
                            posting_status=PostingStatus.AUTO_POST
                        )
                        entry.add_debit(self.ACCOUNTS["weth_wallet"], value, "WETH")
                        entry.add_credit(self.ACCOUNTS["eth_wallet"], value, "ETH")
                        journal_entries.append(entry)

                elif topic0_clean.lower() == WITHDRAWAL_TOPIC:
                    # WETH Withdrawal (unwrapping WETH)
                    category = TransactionCategory.WETH_UNWRAP
                    function_name = "withdraw"
                    events.append(DecodedEvent(
                        name="Withdrawal",
                        args={"src": wallet_addr, "wad": str(amount)},
                        log_index=log.get('logIndex', 0),
                        contract_address=log['address']
                    ))

                    if self.is_fund_wallet(wallet_addr) and value > 0:
                        wallet_roles[wallet_addr] = "unwrapper"
                        entry = JournalEntry(
                            entry_id=f"weth_unwrap_{tx_hash[:8]}",
                            date=timestamp,
                            description=f"Unwrap {value:.6f} WETH to ETH",
                            tx_hash=tx_hash,
                            category=category,
                            platform=Platform.GENERIC,
                            wallet_address=wallet_addr,
                            wallet_role="unwrapper",
                            eth_usd_price=eth_price,
                            posting_status=PostingStatus.AUTO_POST
                        )
                        entry.add_debit(self.ACCOUNTS["eth_wallet"], value, "ETH")
                        entry.add_credit(self.ACCOUNTS["weth_wallet"], value, "WETH")
                        journal_entries.append(entry)

            except Exception as e:
                logger.warning(f"Failed to decode WETH event: {e}")

        # Gas entry
        from_address = tx.get('from', '')
        if gas_fee > 0 and self.is_fund_wallet(from_address):
            wallet_roles[from_address] = wallet_roles.get(from_address, "payer")
            gas_entry = JournalEntry(
                entry_id=f"gas_{tx_hash[:8]}",
                date=timestamp,
                description=f"Gas fee for {function_name}",
                tx_hash=tx_hash,
                category=TransactionCategory.ETH_TRANSFER,
                platform=Platform.GENERIC,
                wallet_address=from_address,
                wallet_role="payer",
                eth_usd_price=eth_price,
                posting_status=PostingStatus.AUTO_POST
            )
            gas_entry.add_debit(self.ACCOUNTS["gas_expense"], gas_fee, "ETH")
            gas_entry.add_credit(self.ACCOUNTS["eth_wallet"], gas_fee, "ETH")
            journal_entries.append(gas_entry)

        return DecodedTransaction(
            status="success",
            tx_hash=tx_hash,
            platform=Platform.GENERIC,
            category=category,
            block=tx.get('blockNumber', 0),
            timestamp=timestamp,
            eth_price=eth_price,
            gas_used=receipt.get('gasUsed', 0),
            gas_fee=gas_fee,
            from_address=from_address,
            to_address=tx.get('to', '') or '',
            value=value,
            function_name=function_name,
            events=events,
            journal_entries=journal_entries,
            wallet_roles=wallet_roles,
        )

    def _decode_eth_transfer(self, tx: Dict, receipt: Dict, block: Dict, eth_price: Decimal,
                              timestamp: datetime, gas_fee: Decimal, tx_hash: str) -> DecodedTransaction:
        """Decode simple ETH transfer"""
        value = wei_to_eth(tx.get('value', 0))
        from_address = tx.get('from', '')
        to_address = tx.get('to', '') or ''

        journal_entries = []
        wallet_roles = {}

        is_from_fund = self.is_fund_wallet(from_address)
        is_to_fund = self.is_fund_wallet(to_address)

        if is_from_fund:
            wallet_roles[from_address] = "sender"
        if is_to_fund:
            wallet_roles[to_address] = "recipient"

        # Create journal entry for the transfer
        if value > 0 and (is_from_fund or is_to_fund):
            entry = JournalEntry(
                entry_id=f"eth_transfer_{tx_hash[:8]}",
                date=timestamp,
                description=f"ETH transfer: {value:.6f} ETH",
                tx_hash=tx_hash,
                category=TransactionCategory.ETH_TRANSFER,
                platform=Platform.GENERIC,
                wallet_address=from_address if is_from_fund else to_address,
                wallet_role="sender" if is_from_fund else "recipient",
                eth_usd_price=eth_price,
                posting_status=PostingStatus.AUTO_POST
            )

            if is_from_fund:
                # Outgoing transfer
                entry.add_debit("200.10 - Accounts Receivable", value, "ETH")
                entry.add_credit(self.ACCOUNTS["eth_wallet"], value, "ETH")
            else:
                # Incoming transfer
                entry.add_debit(self.ACCOUNTS["eth_wallet"], value, "ETH")
                entry.add_credit("300.10 - Revenue", value, "ETH")

            journal_entries.append(entry)

        # Gas entry
        if gas_fee > 0 and is_from_fund:
            gas_entry = JournalEntry(
                entry_id=f"gas_{tx_hash[:8]}",
                date=timestamp,
                description=f"Gas fee for ETH transfer",
                tx_hash=tx_hash,
                category=TransactionCategory.ETH_TRANSFER,
                platform=Platform.GENERIC,
                wallet_address=from_address,
                wallet_role="payer",
                eth_usd_price=eth_price,
                posting_status=PostingStatus.AUTO_POST
            )
            gas_entry.add_debit(self.ACCOUNTS["gas_expense"], gas_fee, "ETH")
            gas_entry.add_credit(self.ACCOUNTS["eth_wallet"], gas_fee, "ETH")
            journal_entries.append(gas_entry)

        return DecodedTransaction(
            status="success",
            tx_hash=tx_hash,
            platform=Platform.GENERIC,
            category=TransactionCategory.ETH_TRANSFER,
            block=tx.get('blockNumber', 0),
            timestamp=timestamp,
            eth_price=eth_price,
            gas_used=receipt.get('gasUsed', 0),
            gas_fee=gas_fee,
            from_address=from_address,
            to_address=to_address,
            value=value,
            function_name="transfer",
            events=[],
            journal_entries=journal_entries,
            wallet_roles=wallet_roles,
        )

    def _decode_erc20(self, tx: Dict, receipt: Dict, block: Dict, eth_price: Decimal,
                       timestamp: datetime, gas_fee: Decimal, tx_hash: str, input_data: str) -> DecodedTransaction:
        """Decode ERC20 token transfer"""
        selector = input_data[:10].lower()
        function_name = ERC20_SELECTORS.get(selector, "unknown")

        events = []
        journal_entries = []
        from_address = tx.get('from', '')
        to_address = tx.get('to', '') or ''
        value = Decimal(0)
        token_symbol = "UNKNOWN"

        # Transfer event signature (without 0x prefix for comparison)
        TRANSFER_TOPIC = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

        # Decode Transfer events from logs
        for log in receipt.get('logs', []):
            # Transfer event topic
            if log.get('topics') and len(log['topics']) > 0:
                topic0 = log['topics'][0].hex() if isinstance(log['topics'][0], bytes) else log['topics'][0]
                # Normalize topic (remove 0x if present for comparison)
                topic0_clean = topic0[2:] if topic0.startswith('0x') else topic0
                if topic0_clean.lower() == TRANSFER_TOPIC:
                    # This is a Transfer event
                    try:
                        # Decode indexed parameters
                        from_addr = "0x" + log['topics'][1].hex()[-40:] if len(log['topics']) > 1 else ""
                        to_addr = "0x" + log['topics'][2].hex()[-40:] if len(log['topics']) > 2 else ""
                        amount = int(log['data'].hex(), 16) if log.get('data') else 0

                        token_address = log.get('address', '').lower()
                        if token_address == USDC_ADDRESS:
                            token_symbol = "USDC"
                            value = Decimal(amount) / Decimal(10**6)
                        elif token_address == USDT_ADDRESS:
                            token_symbol = "USDT"
                            value = Decimal(amount) / Decimal(10**6)
                        else:
                            token_symbol = "TOKEN"
                            value = Decimal(amount) / Decimal(10**18)

                        events.append(DecodedEvent(
                            name="Transfer",
                            args={"from": from_addr, "to": to_addr, "value": str(amount)},
                            log_index=log['logIndex'],
                            contract_address=log['address'],
                            topic=topic0
                        ))
                    except Exception as e:
                        logger.warning(f"Failed to decode Transfer event: {e}")

        wallet_roles = {}

        # Check tx-level addresses
        is_from_fund = self.is_fund_wallet(from_address)
        is_to_fund = self.is_fund_wallet(to_address)

        if is_from_fund:
            wallet_roles[from_address] = "sender"
        if is_to_fund:
            wallet_roles[to_address] = "recipient"

        # Track ALL fund wallet transfers per token (handle multiple transfers in one tx)
        # Key: (token_address, token_symbol), Value: {"in": amount, "out": amount, "wallet": address}
        fund_flows = {}

        for evt in events:
            if evt.name == "Transfer":
                evt_from = evt.args.get('from', '').lower()
                evt_to = evt.args.get('to', '').lower()
                evt_value = Decimal(evt.args.get('value', '0'))
                evt_token_addr = evt.contract_address.lower() if evt.contract_address else ''

                # Determine token info
                if evt_token_addr == USDC_ADDRESS:
                    evt_symbol = "USDC"
                    evt_decimals = 6
                elif evt_token_addr == USDT_ADDRESS:
                    evt_symbol = "USDT"
                    evt_decimals = 6
                elif evt_token_addr == WETH_ADDRESS:
                    evt_symbol = "WETH"
                    evt_decimals = 18
                else:
                    evt_symbol = "TOKEN"
                    evt_decimals = 18

                evt_amount = evt_value / Decimal(10**evt_decimals)
                flow_key = (evt_token_addr, evt_symbol)

                if flow_key not in fund_flows:
                    fund_flows[flow_key] = {"in": Decimal(0), "out": Decimal(0), "wallet": None}

                # Fund wallet received tokens
                if self.is_fund_wallet(evt_to):
                    fund_flows[flow_key]["in"] += evt_amount
                    fund_flows[flow_key]["wallet"] = evt_to
                    wallet_roles[evt_to] = "recipient"

                # Fund wallet sent tokens
                if self.is_fund_wallet(evt_from):
                    fund_flows[flow_key]["out"] += evt_amount
                    fund_flows[flow_key]["wallet"] = evt_from
                    wallet_roles[evt_from] = "sender"

        # Create journal entries for net flows
        for (token_addr, token_sym), flow in fund_flows.items():
            net_in = flow["in"]
            net_out = flow["out"]
            fund_wallet = flow["wallet"]

            if fund_wallet is None:
                continue

            token_account = self.ACCOUNTS.get(f"{token_sym.lower()}_wallet", f"100.40 - {token_sym} Wallet")

            # Net inflow (received more than sent)
            if net_in > net_out:
                net_amount = net_in - net_out
                entry = JournalEntry(
                    entry_id=f"erc20_in_{tx_hash[:8]}_{token_sym}",
                    date=timestamp,
                    description=f"{token_sym} received: {net_amount:.6f}",
                    tx_hash=tx_hash,
                    category=TransactionCategory.ERC20_TRANSFER,
                    platform=Platform.GENERIC,
                    wallet_address=fund_wallet,
                    wallet_role="recipient",
                    eth_usd_price=eth_price,
                    posting_status=PostingStatus.AUTO_POST
                )
                entry.add_debit(token_account, net_amount, token_sym)
                entry.add_credit("400.10 - Other Income", net_amount, token_sym)
                journal_entries.append(entry)
                value = net_amount
                token_symbol = token_sym

            # Net outflow (sent more than received)
            elif net_out > net_in:
                net_amount = net_out - net_in
                entry = JournalEntry(
                    entry_id=f"erc20_out_{tx_hash[:8]}_{token_sym}",
                    date=timestamp,
                    description=f"{token_sym} sent: {net_amount:.6f}",
                    tx_hash=tx_hash,
                    category=TransactionCategory.ERC20_TRANSFER,
                    platform=Platform.GENERIC,
                    wallet_address=fund_wallet,
                    wallet_role="sender",
                    eth_usd_price=eth_price,
                    posting_status=PostingStatus.AUTO_POST
                )
                entry.add_debit("600.30 - Other Expense", net_amount, token_sym)
                entry.add_credit(token_account, net_amount, token_sym)
                journal_entries.append(entry)
                value = net_amount
                token_symbol = token_sym

        # Gas entry
        if gas_fee > 0 and is_from_fund:
            gas_entry = JournalEntry(
                entry_id=f"gas_{tx_hash[:8]}",
                date=timestamp,
                description=f"Gas fee for {token_symbol} transfer",
                tx_hash=tx_hash,
                category=TransactionCategory.ETH_TRANSFER,
                platform=Platform.GENERIC,
                wallet_address=from_address,
                wallet_role="payer",
                eth_usd_price=eth_price,
                posting_status=PostingStatus.AUTO_POST
            )
            gas_entry.add_debit(self.ACCOUNTS["gas_expense"], gas_fee, "ETH")
            gas_entry.add_credit(self.ACCOUNTS["eth_wallet"], gas_fee, "ETH")
            journal_entries.append(gas_entry)

        return DecodedTransaction(
            status="success",
            tx_hash=tx_hash,
            platform=Platform.GENERIC,
            category=TransactionCategory.ERC20_TRANSFER,
            block=tx.get('blockNumber', 0),
            timestamp=timestamp,
            eth_price=eth_price,
            gas_used=receipt.get('gasUsed', 0),
            gas_fee=gas_fee,
            from_address=from_address,
            to_address=to_address,
            value=value,
            function_name=function_name,
            function_params={"token": token_symbol},
            events=events,
            journal_entries=journal_entries,
            wallet_roles=wallet_roles,
        )

    def _create_basic_result(self, tx: Dict, receipt: Dict, block: Dict, eth_price: Decimal,
                              timestamp: datetime, gas_fee: Decimal, tx_hash: str) -> DecodedTransaction:
        """Create basic result for unknown transaction"""
        return DecodedTransaction(
            status="success",
            tx_hash=tx_hash,
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
            events=[],
            journal_entries=[],
            wallet_roles={},
        )
