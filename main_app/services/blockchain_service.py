"""
Blockchain Service Module

Provides Web3 integration for fetching and processing blockchain transactions
with efficient LRU caching to minimize API calls.
"""

from functools import lru_cache
from web3 import Web3
from decimal import Decimal, getcontext
from datetime import datetime, timezone
import pandas as pd
from typing import List, Dict, Optional, Tuple, Union
from eth_utils import to_checksum_address
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set up logging
logger = logging.getLogger(__name__)

# Set decimal precision
getcontext().prec = 50

# Import configuration
from ..config.blockchain_config import (
    INFURA_URL, TOPIC0_HASH_MAP, VERIFIED_TOKENS, TOKEN_DECIMALS,
    CHAINLINK_ETH_USD_FEED, CHAINLINK_AGGREGATOR_V3_ABI,
    BLOCK_CHUNK_SIZE, MAX_RETRIES, RETRY_DELAY, CACHE_SIZES,
    STABLECOINS, ETH_ONE_FOR_ONE, WSTETH_ASSETS, WSTETH_RATE_DEFAULT,
    TOKEN_STATUS
)

# Import S3 utilities
from ..s3_utils import load_WALLET_file

# Import token classifier
from .token_classifier import TokenClassifier
import requests


class BlockchainService:
    """
    Service for interacting with Ethereum blockchain via Web3.
    Implements efficient caching strategies to minimize API calls.
    """
    
    def __init__(self, infura_url: str = INFURA_URL):
        """Initialize blockchain service with Web3 connection."""
        print(f"      >> Connecting to Web3 provider: {infura_url[:50]}...")
        self.w3 = Web3(Web3.HTTPProvider(infura_url))
        
        # Verify connection
        print("      >> Testing Web3 connection...")
        if not self.w3.is_connected():
            print("      [ERROR] Web3 connection failed!")
            raise ConnectionError("Failed to connect to Ethereum node")
        
        print("      [OK] Web3 connection successful!")
        
        # Load wallet mapping
        self.wallet_mapping = None
        self.wallet_by_fund = {}
        self._load_wallet_mapping()
        
        # Initialize Chainlink price feed contract
        self.price_feed_contract = self.w3.eth.contract(
            address=to_checksum_address(CHAINLINK_ETH_USD_FEED),
            abi=CHAINLINK_AGGREGATOR_V3_ABI
        )
        
        # Initialize token classifier for security filtering
        self.token_classifier = TokenClassifier(self.w3)
        
        logger.info(f"BlockchainService initialized. Connected to chain ID: {self.w3.eth.chain_id}")
    
    def _load_wallet_mapping(self):
        """Load wallet mapping from S3 and organize by fund."""
        try:
            self.wallet_mapping = load_WALLET_file()
            self.wallet_by_fund = self._organize_wallets_by_fund()
            logger.info(f"Loaded {len(self.wallet_mapping)} wallet mappings")
        except Exception as e:
            logger.error(f"Failed to load wallet mapping: {e}")
            self.wallet_mapping = pd.DataFrame()
            self.wallet_by_fund = {}
    
    def _organize_wallets_by_fund(self) -> Dict[str, List[str]]:
        """Organize wallets by fund_id for efficient filtering."""
        fund_wallets = {}
        
        for _, row in self.wallet_mapping.iterrows():
            fund_id = row.get('fund_id', '')
            wallet = row.get('wallet_address', '')
            
            if fund_id and wallet:
                wallet_checksum = to_checksum_address(wallet)
                
                if fund_id not in fund_wallets:
                    fund_wallets[fund_id] = []
                fund_wallets[fund_id].append(wallet_checksum)
        
        return fund_wallets
    
    @lru_cache(maxsize=CACHE_SIZES["block_by_timestamp"])
    def get_block_by_timestamp(self, timestamp: int) -> int:
        """
        Binary search to find block number at timestamp.
        Results are cached to avoid redundant queries.
        """
        low = 0
        high = self.w3.eth.block_number
        
        while low <= high:
            mid = (low + high) // 2
            block = self.w3.eth.get_block(mid)
            
            if block.timestamp <= timestamp:
                low = mid + 1
            else:
                high = mid - 1
        
        return high
    
    @lru_cache(maxsize=CACHE_SIZES["eth_price_at_block"])
    def get_eth_price_at_block(self, block_number: int) -> Tuple[Decimal, datetime]:
        """
        Get ETH/USD price at specific block using Chainlink oracle.
        Results are cached to avoid redundant queries.
        """
        try:
            # Get price from Chainlink
            round_data = self.price_feed_contract.functions.latestRoundData().call(
                block_identifier=block_number
            )
            
            # Extract price (8 decimal places)
            price = Decimal(round_data[1]) / Decimal(10**8)
            
            # Get block timestamp
            block = self.w3.eth.get_block(block_number)
            block_time = datetime.fromtimestamp(block.timestamp, tz=timezone.utc)
            
            return price, block_time
            
        except Exception as e:
            logger.error(f"Error fetching ETH price at block {block_number}: {e}")
            # Return default price if oracle fails
            return Decimal("3200"), datetime.now(timezone.utc)
    
    def fetch_transactions_for_period(
        self,
        start_date: datetime,
        end_date: datetime,
        fund_id: Optional[str] = None,
        wallet_addresses: Optional[List[str]] = None,
        progress_callback: Optional[callable] = None
    ) -> pd.DataFrame:
        """
        Fetch all relevant transactions for date range and wallets.
        
        Args:
            start_date: Start of period (inclusive)
            end_date: End of period (inclusive)
            fund_id: Optional fund ID to filter wallets
            wallet_addresses: Optional list of specific wallet addresses
            progress_callback: Optional callback for progress updates
            
        Returns:
            DataFrame with decoded transactions
        """
        # Get wallets to query
        if wallet_addresses:
            wallets = [to_checksum_address(w) for w in wallet_addresses]
        elif fund_id and fund_id in self.wallet_by_fund:
            wallets = self.wallet_by_fund[fund_id]
        else:
            # Get all wallets from mapping
            wallets = []
            for addr in self.wallet_mapping['wallet_address'].dropna():
                try:
                    wallets.append(to_checksum_address(addr))
                except:
                    continue
        
        if not wallets:
            logger.warning("No wallets to query")
            return pd.DataFrame()
        
        # Get block range (cached) - convert date to datetime if needed
        from datetime import date
        if isinstance(start_date, date) and not isinstance(start_date, datetime):
            start_date = datetime.combine(start_date, datetime.min.time())
        if isinstance(end_date, date) and not isinstance(end_date, datetime):
            end_date = datetime.combine(end_date, datetime.max.time())
            
        start_block = self.get_block_by_timestamp(int(start_date.timestamp()))
        end_block = self.get_block_by_timestamp(int(end_date.timestamp()))
        
        logger.info(f"Fetching transactions for {len(wallets)} wallets from block {start_block} to {end_block}")
        
        # Fetch transactions for all wallets
        all_transactions = []
        total_wallets = len(wallets)
        
        # Use thread pool for parallel fetching
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_wallet = {
                executor.submit(
                    self._fetch_wallet_transactions,
                    wallet,
                    start_block,
                    end_block
                ): wallet
                for wallet in wallets
            }
            
            for i, future in enumerate(as_completed(future_to_wallet)):
                wallet = future_to_wallet[future]
                try:
                    txs = future.result()
                    all_transactions.extend(txs)
                    
                    if progress_callback:
                        progress_callback(i + 1, total_wallets, len(txs))
                    
                    logger.info(f"Fetched {len(txs)} transactions for wallet {wallet}")
                    
                except Exception as e:
                    logger.error(f"Error fetching transactions for wallet {wallet}: {e}")
        
        # Convert to DataFrame
        if all_transactions:
            df = pd.DataFrame(all_transactions)
            # Remove duplicates based on tx_hash and log_index
            df = df.drop_duplicates(subset=['tx_hash', 'log_index'])
            return df
        
        return pd.DataFrame()
    
    def _fetch_wallet_transactions(
        self,
        wallet: str,
        start_block: int,
        end_block: int
    ) -> List[Dict]:
        """Fetch and decode transactions for a single wallet."""
        transactions = []
        
        # Define filter for relevant events
        event_signatures = list(TOPIC0_HASH_MAP.keys())
        
        # Fetch logs in chunks to avoid timeouts
        for block_start in range(start_block, end_block + 1, BLOCK_CHUNK_SIZE):
            block_end = min(block_start + BLOCK_CHUNK_SIZE - 1, end_block)
            
            # Retry logic for API calls
            for retry in range(MAX_RETRIES):
                try:
                    # Create filter for Transfer, Deposit, Withdraw events
                    # We need to check both from and to addresses
                    
                    # Pad wallet address for topic comparison
                    wallet_topic = '0x' + wallet[2:].lower().zfill(64)
                    
                    # Query logs for each event signature separately to avoid tuple issues
                    all_logs = []
                    for event_sig in event_signatures:
                        try:
                            # Query logs where wallet is sender (topic1)
                            filter_from = {
                                'fromBlock': block_start,
                                'toBlock': block_end,
                                'topics': [
                                    event_sig,        # topic0: single event signature
                                    wallet_topic,     # topic1: from address
                                    None,            # topic2: to address (any)
                                ]
                            }
                            
                            # Query logs where wallet is receiver (topic2)
                            filter_to = {
                                'fromBlock': block_start,
                                'toBlock': block_end,
                                'topics': [
                                    event_sig,        # topic0: single event signature
                                    None,            # topic1: from address (any)
                                    wallet_topic,    # topic2: to address
                                ]
                            }
                            
                            # Fetch logs for both filters
                            logs_from = self.w3.eth.get_logs(filter_from)
                            logs_to = self.w3.eth.get_logs(filter_to)
                            all_logs.extend(logs_from + logs_to)
                            
                        except Exception as e:
                            logger.warning(f"Failed to fetch logs for event {event_sig}: {e}")
                            continue
                    
                    # Deduplicate logs
                    seen = set()
                    unique_logs = []
                    
                    for log in all_logs:
                        log_id = (log['transactionHash'].hex(), log['logIndex'])
                        if log_id not in seen:
                            seen.add(log_id)
                            unique_logs.append(log)
                    
                    # Decode logs
                    for log in unique_logs:
                        tx = self._decode_log(log, wallet)
                        if tx:
                            transactions.append(tx)
                    
                    break  # Success, exit retry loop
                    
                except Exception as e:
                    if retry < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY * (2 ** retry))  # Exponential backoff
                    else:
                        logger.error(f"Failed to fetch logs after {MAX_RETRIES} retries: {e}")
        
        return transactions
    
    def _decode_log(self, log, target_wallet: str) -> Optional[Dict]:
        """Decode a single log entry."""
        try:
            # Parse topics and data
            topic0 = log['topics'][0].hex() if log['topics'] else None
            event_type = TOPIC0_HASH_MAP.get(topic0, "Unknown")
            
            if event_type == "Unknown":
                return None
            
            # Initialize transaction data
            from_addr = None
            to_addr = None
            value = 0
            token_address = log['address']
            
            # Decode addresses from topics
            if len(log['topics']) > 1 and log['topics'][1]:
                # topic1 is from address (padded to 32 bytes)
                from_addr = '0x' + log['topics'][1].hex()[-40:]
                from_addr = to_checksum_address(from_addr)
            
            if len(log['topics']) > 2 and log['topics'][2]:
                # topic2 is to address (padded to 32 bytes)
                to_addr = '0x' + log['topics'][2].hex()[-40:]
                to_addr = to_checksum_address(to_addr)
            
            # Decode value from data field
            if log['data'] and log['data'] != '0x':
                # Remove '0x' prefix and convert to int
                value_hex = log['data'].hex() if isinstance(log['data'], bytes) else log['data']
                value = int(value_hex, 16)
            
            # Get block details (cached)
            block = self.w3.eth.get_block(log['blockNumber'])
            
            # Get transaction details
            tx = self.w3.eth.get_transaction(log['transactionHash'])
            
            # Get token symbol and name
            token_symbol = self._get_token_symbol_from_address(token_address)
            token_name = self._get_token_name_from_address(token_address)
            
            # Classify token for security filtering
            token_classification = self.token_classifier.classify_token(
                token_address, token_symbol
            )
            
            # Determine transaction direction
            wallet_checksum = to_checksum_address(target_wallet)
            
            if event_type == "Transfer":
                if from_addr == wallet_checksum:
                    direction = "out"
                elif to_addr == wallet_checksum:
                    direction = "in"
                else:
                    return None  # Wallet not involved
            elif event_type == "Deposit":
                direction = "in" if to_addr == wallet_checksum else None
            elif event_type == "Withdraw":
                direction = "out" if from_addr == wallet_checksum else None
            else:
                return None
            
            if not direction:
                return None
            
            # Get ETH price at block (cached)
            eth_price, block_time = self.get_eth_price_at_block(log['blockNumber'])
            
            # Calculate enhanced transaction values
            gas_used = tx.get('gas', 0)
            gas_price = tx.get('gasPrice', 0)
            gas_fee_wei = gas_used * gas_price
            gas_fee_eth = Decimal(gas_fee_wei) / Decimal(10**18)
            gas_fee_usd = gas_fee_eth * eth_price
            
            # Calculate token amounts with proper decimals
            token_decimals = self.get_token_decimals(token_symbol)
            token_amount = Decimal(value) / Decimal(10**token_decimals)
            
            # Calculate ETH equivalent value
            unit_price_usd = self.compute_unit_price_usd(token_symbol, eth_price)
            token_value_usd = token_amount * unit_price_usd
            token_value_eth = token_value_usd / eth_price if eth_price > 0 else Decimal("0")
            
            # Format direction for display
            direction_display = "IN" if direction == "in" else "OUT"
            
            # New structure with required fields: Date, Hash, Direction, Token Name, Amount (of token), Value (ETH), Value (USD), From, To
            return {
                # Required fields in order
                'date': block_time,
                'tx_hash': log['transactionHash'].hex(),
                'direction': direction_display,
                'token_name': token_name,
                'token_amount': float(token_amount),
                'token_value_eth': float(token_value_eth),
                'token_value_usd': float(token_value_usd),
                'from_address': from_addr,
                'to_address': to_addr,
                
                # Additional fields for compatibility and analysis
                'block_number': log['blockNumber'],
                'log_index': log['logIndex'],
                'event_type': event_type,
                'token_symbol': token_symbol,
                'token_address': token_address,
                'wallet_address': wallet_checksum,
                'eth_price_usd': float(eth_price),
                'gas_fee_eth': float(gas_fee_eth),
                'gas_fee_usd': float(gas_fee_usd),
                'value_raw': value,
                'fund_id': self._get_fund_id_for_wallet(wallet_checksum),
                # Token security classification
                'token_status': token_classification['status'],
                'token_risk_level': token_classification['risk_level'],
                'token_risk_factors': token_classification['risk_factors'],
                'requires_approval': token_classification['requires_approval']
            }
            
        except Exception as e:
            logger.error(f"Error decoding log: {e}")
            return None
    
    @lru_cache(maxsize=CACHE_SIZES["token_info"])
    def _get_token_symbol_from_address(self, address: str) -> str:
        """Get token symbol from contract address."""
        address_checksum = to_checksum_address(address)
        
        # Check known tokens
        for symbol, addr in VERIFIED_TOKENS.items():
            if to_checksum_address(addr) == address_checksum:
                return symbol
        
        # Try to get symbol from contract
        try:
            # Minimal ERC20 ABI for symbol()
            erc20_abi = [{
                "constant": True,
                "inputs": [],
                "name": "symbol",
                "outputs": [{"name": "", "type": "string"}],
                "type": "function"
            }]
            
            contract = self.w3.eth.contract(address=address_checksum, abi=erc20_abi)
            symbol = contract.functions.symbol().call()
            return symbol
            
        except Exception:
            return "UNKNOWN"
    
    @lru_cache(maxsize=CACHE_SIZES["wallet_fund_mapping"])
    def _get_fund_id_for_wallet(self, wallet: str) -> Optional[str]:
        """Get fund_id for wallet address - CACHED."""
        if self.wallet_mapping.empty:
            return None
        
        wallet_lower = wallet.lower()
        mask = self.wallet_mapping['wallet_address'].str.lower() == wallet_lower
        matching_rows = self.wallet_mapping[mask]
        
        if not matching_rows.empty:
            return matching_rows.iloc[0].get('fund_id')
        
        return None
    
    def get_token_decimals(self, token_symbol: str) -> int:
        """Get decimal places for token."""
        return TOKEN_DECIMALS.get(token_symbol, 18)
    
    def compute_unit_price_usd(
        self,
        token_symbol: str,
        eth_price: Decimal,
        wsteth_rate: Optional[Decimal] = None
    ) -> Decimal:
        """Calculate USD price per unit for different asset types."""
        token_upper = token_symbol.upper()
        
        if token_upper in STABLECOINS:
            return Decimal("1")
        
        if token_upper in WSTETH_ASSETS:
            rate = wsteth_rate if wsteth_rate else WSTETH_RATE_DEFAULT
            return rate * eth_price
        
        if token_upper in ETH_ONE_FOR_ONE:
            return eth_price
        
        # Default to ETH price for unknown tokens
        return eth_price
    
    @lru_cache(maxsize=CACHE_SIZES["token_info"])
    def _get_token_name_from_address(self, token_address: str) -> str:
        """Get token name from contract address using CoinGecko API with fallback."""
        try:
            # Clean the address
            address = token_address.lower().strip()
            
            # Check verified tokens first
            for symbol, addr in VERIFIED_TOKENS.items():
                if addr.lower() == address:
                    return symbol
            
            # Try CoinGecko API
            url = f"https://api.coingecko.com/api/v3/coins/ethereum/contract/{address}"
            
            import requests
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                token_name = data.get("name", "Unknown Token")
                return token_name
            else:
                logger.warning(f"CoinGecko API returned status {response.status_code} for {address}")
                
        except Exception as e:
            logger.error(f"Error fetching token name for {token_address}: {e}")
        
        # Fallback to truncated address
        return f"Token {token_address[:6]}...{token_address[-4:]}"