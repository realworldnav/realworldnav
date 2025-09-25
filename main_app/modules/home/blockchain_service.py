"""
Blockchain service for fetching and monitoring wallet transactions
Supports both Etherscan API and Web3 WebSocket connections
"""

import asyncio
import aiohttp
import requests
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import pandas as pd
from web3 import Web3
try:
    # Web3 v6+ uses WebSocketProvider (capital S)
    from web3.providers import WebSocketProvider
except ImportError:
    # Fallback for older versions
    from web3.providers import WebsocketProvider as WebSocketProvider
import json
import os
from collections import deque
import logging

# Setup logging
logging.basicConfig(level=logging.DEBUG)  # Changed to DEBUG to see all messages
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class EtherscanClient:
    """Client for interacting with Etherscan API v2"""

    def __init__(self, api_key: Optional[str] = None):
        # Try to load from environment
        import os
        from dotenv import load_dotenv
        load_dotenv()  # Explicitly load .env file

        self.api_key = api_key or os.getenv('ETHERSCAN_API_KEY', '')

        # Also try without quotes in case there's an issue
        if not self.api_key:
            self.api_key = os.environ.get('ETHERSCAN_API_KEY', '')

        self.base_url = "https://api.etherscan.io/v2/api"
        self.base_url_v1 = "https://api.etherscan.io/api"  # Fallback to v1 for some endpoints
        self.rate_limit_delay = 0.2  # 5 requests per second max
        self.last_request_time = 0

        # Log API key status (mask the actual key for security)
        if self.api_key:
            masked_key = f"{self.api_key[:4]}...{self.api_key[-4:]}" if len(self.api_key) > 8 else "***"
            logger.info(f"EtherscanClient initialized with API key: {masked_key}")
        else:
            logger.warning("EtherscanClient initialized without API key! Set ETHERSCAN_API_KEY in .env")
            logger.debug(f"Current working directory: {os.getcwd()}")
            logger.debug(f".env file exists: {os.path.exists('.env')}")
            # Try to show what's in the environment
            env_keys = [k for k in os.environ.keys() if 'ETHERSCAN' in k or 'API' in k]
            logger.debug(f"Related env vars: {env_keys}")

    def _rate_limit(self):
        """Enforce rate limiting"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - time_since_last)
        self.last_request_time = time.time()

    def get_transactions(self, address: str, chainid: int = 1, limit: int = 100) -> pd.DataFrame:
        """Fetch transactions for a wallet address"""
        self._rate_limit()

        # Use v1 API directly (more stable and reliable)
        params = {
            'module': 'account',
            'action': 'txlist',
            'address': address,
            'startblock': 0,
            'endblock': 99999999,
            'page': 1,
            'offset': limit,
            'sort': 'desc',
            'apikey': self.api_key
        }

        url = self.base_url_v1
        logger.info(f"Fetching transactions from Etherscan API")
        logger.debug(f"URL: {url}")
        logger.debug(f"Params: {params}")

        try:
            response = requests.get(url, params=params)
            logger.info(f"API Response Status: {response.status_code}")

            if response.status_code == 200:
                data = response.json()
                logger.debug(f"API Response: {json.dumps(data, indent=2)[:500]}...")  # Log first 500 chars

                # Check API status
                if data.get('status') == '1':
                    result = data.get('result', [])
                    logger.info(f"Successfully fetched {len(result)} transactions")
                    if result:
                        return self._process_transactions(result, address)
                    else:
                        logger.warning("API returned success but empty result")
                else:
                    # API returned an error
                    error_msg = data.get('message', 'Unknown error')
                    result_msg = data.get('result', '')
                    logger.error(f"Etherscan API error: {error_msg} - {result_msg}")

                    if 'NOTOK' in str(data.get('message', '')):
                        if 'Invalid API Key' in str(result_msg):
                            logger.error("Invalid API key! Please check your ETHERSCAN_API_KEY in .env")
                        elif 'Max rate limit' in str(result_msg):
                            logger.error("Rate limit exceeded! Please wait and try again")
            else:
                logger.error(f"HTTP Error: {response.status_code} - {response.text[:200]}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Network error fetching transactions: {e}")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse API response as JSON: {e}")
        except Exception as e:
            logger.error(f"Unexpected error fetching transactions: {e}")
            import traceback
            traceback.print_exc()

        return pd.DataFrame()

    def get_token_transfers(self, address: str, chainid: int = 1, limit: int = 100) -> pd.DataFrame:
        """Fetch ERC-20 token transfers for a wallet"""
        self._rate_limit()

        params = {
            'module': 'account',
            'action': 'tokentx',
            'address': address,
            'startblock': 0,
            'endblock': 99999999,
            'page': 1,
            'offset': limit,
            'sort': 'desc',
            'apikey': self.api_key
        }

        url = self.base_url_v1
        logger.info(f"Fetching token transfers from Etherscan API")

        try:
            response = requests.get(url, params=params)
            logger.info(f"Token API Response Status: {response.status_code}")

            if response.status_code == 200:
                data = response.json()

                if data.get('status') == '1':
                    result = data.get('result', [])
                    logger.info(f"Successfully fetched {len(result)} token transfers")
                    if result:
                        return self._process_token_transfers(result, address)
                else:
                    error_msg = data.get('message', 'Unknown error')
                    result_msg = data.get('result', '')
                    logger.error(f"Token API error: {error_msg} - {result_msg}")
            else:
                logger.error(f"Token HTTP Error: {response.status_code}")

        except Exception as e:
            logger.error(f"Failed to fetch token transfers: {e}")

        return pd.DataFrame()

    def _process_transactions(self, transactions: List[Dict], wallet_address: str) -> pd.DataFrame:
        """Process raw transaction data into DataFrame"""
        processed = []
        wallet_lower = wallet_address.lower()

        for tx in transactions:
            try:
                # Determine transaction type (IN/OUT)
                is_incoming = tx.get('to', '').lower() == wallet_lower

                # Calculate ETH amount
                value_wei = int(tx.get('value', 0))
                value_eth = value_wei / 10**18

                # Calculate gas fee
                gas_used = int(tx.get('gasUsed', 0))
                gas_price = int(tx.get('gasPrice', 0))
                gas_fee = (gas_used * gas_price) / 10**18

                # Format transaction
                processed_tx = {
                    'hash': tx.get('hash', ''),
                    'block': int(tx.get('blockNumber', 0)),
                    'from': tx.get('from', ''),
                    'to': tx.get('to', ''),
                    'amount': value_eth,
                    'token': 'ETH',
                    'gas_fee': gas_fee,
                    'timestamp': datetime.fromtimestamp(int(tx.get('timeStamp', 0))),
                    'status': 'Confirmed' if tx.get('isError') == '0' else 'Failed',
                    'type': 'IN' if is_incoming else 'OUT',
                    'nonce': int(tx.get('nonce', 0)),
                    'confirmations': int(tx.get('confirmations', 0))
                }
                processed.append(processed_tx)
            except Exception as e:
                logger.warning(f"Error processing transaction: {e}")
                continue

        return pd.DataFrame(processed)

    def _process_token_transfers(self, transfers: List[Dict], wallet_address: str) -> pd.DataFrame:
        """Process token transfer data into DataFrame"""
        processed = []
        wallet_lower = wallet_address.lower()

        for transfer in transfers:
            try:
                # Determine transfer type
                is_incoming = transfer.get('to', '').lower() == wallet_lower

                # Calculate token amount (handle different decimal places)
                value = int(transfer.get('value', 0))
                decimals = int(transfer.get('tokenDecimal', 18))
                amount = value / (10 ** decimals) if decimals > 0 else value

                processed_tx = {
                    'hash': transfer.get('hash', ''),
                    'block': int(transfer.get('blockNumber', 0)),
                    'from': transfer.get('from', ''),
                    'to': transfer.get('to', ''),
                    'amount': amount,
                    'token': transfer.get('tokenSymbol', 'UNKNOWN'),
                    'gas_fee': 0,  # Gas fee paid in ETH, not in token transfer data
                    'timestamp': datetime.fromtimestamp(int(transfer.get('timeStamp', 0))),
                    'status': 'Confirmed',
                    'type': 'IN' if is_incoming else 'OUT',
                    'nonce': int(transfer.get('nonce', 0)),
                    'confirmations': int(transfer.get('confirmations', 0)),
                    'token_name': transfer.get('tokenName', ''),
                    'contract_address': transfer.get('contractAddress', '')
                }
                processed.append(processed_tx)
            except Exception as e:
                logger.warning(f"Error processing token transfer: {e}")
                continue

        return pd.DataFrame(processed)


class Web3Monitor:
    """Web3 WebSocket monitor for real-time transaction monitoring"""

    def __init__(self, websocket_url: Optional[str] = None):
        self.websocket_url = websocket_url or os.getenv('WEB3_WEBSOCKET_URL', '')
        self.w3 = None
        self.is_connected = False
        self.watched_address = None
        self.transaction_queue = deque(maxlen=1000)  # Store last 1000 transactions

    async def connect(self):
        """Establish WebSocket connection"""
        if not self.websocket_url:
            logger.warning("No WebSocket URL configured")
            return False

        try:
            # Support both Infura and Alchemy formats
            if 'infura' in self.websocket_url or 'alchemy' in self.websocket_url:
                self.w3 = Web3(WebSocketProvider(self.websocket_url))
            else:
                self.w3 = Web3(WebSocketProvider(self.websocket_url))

            if self.w3.is_connected():
                self.is_connected = True
                logger.info(f"Connected to Web3 WebSocket: {self.websocket_url[:30]}...")
                return True
        except Exception as e:
            logger.error(f"Failed to connect to WebSocket: {e}")
            self.is_connected = False

        return False

    async def monitor_address(self, address: str):
        """Monitor an address for transactions"""
        self.watched_address = Web3.to_checksum_address(address)

        if not self.is_connected:
            if not await self.connect():
                return

        try:
            # Subscribe to new blocks
            block_filter = await self.w3.eth.filter('latest')

            while self.is_connected:
                try:
                    # Check for new blocks
                    new_blocks = await block_filter.get_new_entries()

                    for block_hash in new_blocks:
                        await self._process_block(block_hash)

                    await asyncio.sleep(1)  # Poll every second

                except Exception as e:
                    logger.error(f"Error in monitor loop: {e}")
                    await asyncio.sleep(5)  # Wait before retry

        except Exception as e:
            logger.error(f"Failed to set up monitoring: {e}")

    async def _process_block(self, block_hash):
        """Process a new block for relevant transactions"""
        try:
            block = await self.w3.eth.get_block(block_hash, full_transactions=True)

            for tx in block['transactions']:
                # Check if transaction involves watched address
                if (tx['from'].lower() == self.watched_address.lower() or
                    (tx['to'] and tx['to'].lower() == self.watched_address.lower())):

                    # Add to queue
                    self.transaction_queue.append(self._format_transaction(tx))
                    logger.info(f"New transaction detected: {tx['hash'].hex()}")

        except Exception as e:
            logger.error(f"Error processing block: {e}")

    def _format_transaction(self, tx) -> Dict:
        """Format Web3 transaction to standard format"""
        return {
            'hash': tx['hash'].hex(),
            'block': tx.get('blockNumber', 0),
            'from': tx['from'],
            'to': tx.get('to', ''),
            'amount': Web3.from_wei(tx['value'], 'ether'),
            'token': 'ETH',
            'gas_fee': Web3.from_wei(tx.get('gas', 0) * tx.get('gasPrice', 0), 'ether'),
            'timestamp': datetime.now(),  # Will be updated when block is confirmed
            'status': 'Pending',
            'type': 'OUT' if tx['from'].lower() == self.watched_address.lower() else 'IN',
            'nonce': tx.get('nonce', 0)
        }

    def get_recent_transactions(self) -> List[Dict]:
        """Get recent transactions from queue"""
        return list(self.transaction_queue)


class BlockchainService:
    """Main service combining Etherscan and Web3 monitoring"""

    def __init__(self):
        self.etherscan = EtherscanClient()
        self.web3_monitor = Web3Monitor()
        self.transaction_cache = {}
        self.last_update = datetime.now()
        self.wallet_address = None
        self.wallet_mapping = None
        self._load_wallet_mapping()

    async def initialize(self, wallet_address: str):
        """Initialize the service with a wallet address"""
        self.wallet_address = wallet_address

        # Start Web3 monitoring in background
        try:
            asyncio.create_task(self.web3_monitor.monitor_address(wallet_address))
        except Exception as e:
            logger.warning(f"Could not start Web3 monitoring: {e}")

        # Fetch initial transactions from Etherscan
        return self.fetch_historical_transactions()

    def fetch_historical_transactions(self, limit: int = 100) -> pd.DataFrame:
        """Fetch historical transactions from Etherscan"""
        if not self.wallet_address:
            logger.warning("No wallet address set")
            return pd.DataFrame()

        logger.info(f"Fetching transactions for wallet: {self.wallet_address}")

        # Get ETH transactions
        eth_txs = self.etherscan.get_transactions(self.wallet_address, limit=limit)
        logger.info(f"Found {len(eth_txs)} ETH transactions")

        # Get token transfers
        token_txs = self.etherscan.get_token_transfers(self.wallet_address, limit=limit)
        logger.info(f"Found {len(token_txs)} token transfers")

        # Combine and sort
        if not eth_txs.empty and not token_txs.empty:
            all_txs = pd.concat([eth_txs, token_txs], ignore_index=True)
            all_txs = all_txs.sort_values('timestamp', ascending=False)
        elif not eth_txs.empty:
            all_txs = eth_txs
        elif not token_txs.empty:
            all_txs = token_txs
        else:
            all_txs = pd.DataFrame()

        # Add friendly names for display
        if not all_txs.empty:
            all_txs['from_display'] = all_txs['from'].apply(self.get_friendly_name)
            all_txs['to_display'] = all_txs['to'].apply(self.get_friendly_name)

        # Cache transactions
        for _, tx in all_txs.iterrows():
            self.transaction_cache[tx['hash']] = tx.to_dict()

        self.last_update = datetime.now()
        return all_txs

    def _load_wallet_mapping(self):
        """Load wallet mapping from S3"""
        try:
            from ...s3_utils import load_WALLET_file
            self.wallet_mapping = load_WALLET_file()

            # Create a dictionary for fast lookup (both original and lowercase)
            self.wallet_names = {}
            if not self.wallet_mapping.empty:
                for _, row in self.wallet_mapping.iterrows():
                    wallet_addr = str(row.get('wallet_address', '')).strip()
                    friendly_name = str(row.get('friendly_name', '')).strip()

                    if wallet_addr and friendly_name:
                        # Store both original and lowercase for flexible matching
                        self.wallet_names[wallet_addr] = friendly_name
                        self.wallet_names[wallet_addr.lower()] = friendly_name

                logger.info(f"Loaded {len(self.wallet_names)} wallet mappings")
        except Exception as e:
            logger.warning(f"Could not load wallet mappings: {e}")
            self.wallet_mapping = pd.DataFrame()
            self.wallet_names = {}

    def get_friendly_name(self, wallet_address: str) -> str:
        """Get friendly name for a wallet address, or shortened address if not found"""
        if not wallet_address:
            return "Unknown"

        # Try to find friendly name
        friendly = self.wallet_names.get(wallet_address.lower(), None)
        if friendly:
            return friendly

        # If not found, return shortened address
        if len(wallet_address) > 12:
            return f"{wallet_address[:6]}...{wallet_address[-4:]}"
        return wallet_address

    def get_all_transactions(self) -> pd.DataFrame:
        """Get all transactions (cached + real-time)"""
        # Get real-time transactions from Web3
        realtime_txs = self.web3_monitor.get_recent_transactions()

        # Merge with cache (avoid duplicates)
        all_txs = list(self.transaction_cache.values())

        for tx in realtime_txs:
            if tx['hash'] not in self.transaction_cache:
                all_txs.append(tx)
                self.transaction_cache[tx['hash']] = tx

        # Convert to DataFrame and sort
        df = pd.DataFrame(all_txs)
        if not df.empty:
            # Add friendly names
            df['from_display'] = df['from'].apply(self.get_friendly_name)
            df['to_display'] = df['to'].apply(self.get_friendly_name)

            df = df.sort_values('timestamp', ascending=False)

        return df

    def is_connected(self) -> bool:
        """Check if Web3 monitor is connected"""
        return self.web3_monitor.is_connected


# Global service instance
blockchain_service = BlockchainService()