# -*- coding: utf-8 -*-
"""
Price Service Module

Multi-source cryptocurrency price fetching service with caching and fallback support.
Primary integration with CoinGecko API, with fallback to existing Chainlink oracles.
"""

import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union
from functools import lru_cache
from decimal import Decimal
import logging
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set up logging
logger = logging.getLogger(__name__)

# Configuration
COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
REQUEST_TIMEOUT = 10  # seconds
RATE_LIMIT_DELAY = 2  # seconds between requests for free tier
MAX_RETRIES = 3
CACHE_TTL_PRICES = 300  # 5 minutes for prices
CACHE_TTL_METADATA = 3600  # 1 hour for token metadata


class PriceCache:
    """Simple in-memory cache with TTL support"""
    
    def __init__(self):
        self._cache = {}
        self._timestamps = {}
    
    def get(self, key: str, ttl: int) -> Optional[Any]:
        """Get cached value if not expired"""
        if key in self._cache:
            if time.time() - self._timestamps[key] < ttl:
                return self._cache[key]
            else:
                # Clean expired entry
                self._cache.pop(key, None)
                self._timestamps.pop(key, None)
        return None
    
    def set(self, key: str, value: Any) -> None:
        """Store value in cache with current timestamp"""
        self._cache[key] = value
        self._timestamps[key] = time.time()
    
    def clear(self) -> None:
        """Clear all cached data"""
        self._cache.clear()
        self._timestamps.clear()
    
    def size(self) -> int:
        """Get current cache size"""
        return len(self._cache)


class CoinGeckoAPI:
    """CoinGecko API client with rate limiting and error handling"""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        self.session = requests.Session()
        self.last_request_time = 0
        
        # Set headers
        headers = {
            'User-Agent': 'RealWorldNAV-CryptoTracker/1.0',
            'Accept': 'application/json'
        }
        if api_key:
            headers['x-cg-pro-api-key'] = api_key
        
        self.session.headers.update(headers)
        logger.info(f"CoinGecko API initialized {'with API key' if api_key else 'using free tier'}")
    
    def _rate_limit(self):
        """Implement rate limiting for free tier"""
        if not self.api_key:  # Only rate limit for free tier
            elapsed = time.time() - self.last_request_time
            if elapsed < RATE_LIMIT_DELAY:
                sleep_time = RATE_LIMIT_DELAY - elapsed
                logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
        self.last_request_time = time.time()
    
    def _make_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """Make API request with retries and error handling"""
        url = f"{COINGECKO_BASE_URL}{endpoint}"
        
        for attempt in range(MAX_RETRIES):
            try:
                self._rate_limit()
                
                response = self.session.get(url, params=params or {}, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                
                return response.json()
                
            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:  # Rate limited
                    wait_time = 60  # Wait 1 minute for rate limit
                    logger.warning(f"Rate limited. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"HTTP error {response.status_code}: {e}")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error on attempt {attempt + 1}: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
        
        logger.error(f"Failed to fetch data from {endpoint} after {MAX_RETRIES} attempts")
        return None
    
    def get_simple_prices(self, token_ids: List[str], vs_currencies: List[str] = None) -> Dict:
        """Get current prices for multiple tokens"""
        if not token_ids:
            return {}
        
        vs_currencies = vs_currencies or ['usd', 'eth']
        
        params = {
            'ids': ','.join(token_ids),
            'vs_currencies': ','.join(vs_currencies),
            'include_24hr_change': 'true',
            'include_24hr_vol': 'true',
            'include_last_updated_at': 'true'
        }
        
        data = self._make_request('/simple/price', params)
        return data or {}
    
    def get_token_info(self, token_id: str) -> Dict:
        """Get detailed token information"""
        data = self._make_request(f'/coins/{token_id}')
        if not data:
            return {}
        
        return {
            'id': data.get('id'),
            'symbol': data.get('symbol', '').upper(),
            'name': data.get('name'),
            'market_cap_rank': data.get('market_cap_rank'),
            'market_data': {
                'current_price_usd': data.get('market_data', {}).get('current_price', {}).get('usd'),
                'current_price_eth': data.get('market_data', {}).get('current_price', {}).get('eth'),
                'market_cap_usd': data.get('market_data', {}).get('market_cap', {}).get('usd'),
                'total_volume_usd': data.get('market_data', {}).get('total_volume', {}).get('usd'),
                'circulating_supply': data.get('market_data', {}).get('circulating_supply'),
                'total_supply': data.get('market_data', {}).get('total_supply'),
                'price_change_24h_pct': data.get('market_data', {}).get('price_change_percentage_24h'),
                'price_change_7d_pct': data.get('market_data', {}).get('price_change_percentage_7d'),
                'price_change_30d_pct': data.get('market_data', {}).get('price_change_percentage_30d')
            },
            'last_updated': data.get('last_updated')
        }
    
    def search_tokens(self, query: str) -> List[Dict]:
        """Search for tokens by name or symbol"""
        params = {'query': query}
        data = self._make_request('/search', params)
        
        if not data or 'coins' not in data:
            return []
        
        return [
            {
                'id': coin['id'],
                'name': coin['name'],
                'symbol': coin['symbol'].upper(),
                'market_cap_rank': coin.get('market_cap_rank'),
                'thumb': coin.get('thumb')
            }
            for coin in data['coins'][:10]  # Limit to top 10 results
        ]
    
    def get_historical_prices(self, token_id: str, days: int = 30) -> List[Dict]:
        """Get historical price data"""
        params = {
            'vs_currency': 'usd',
            'days': str(days),
            'interval': 'daily' if days > 7 else 'hourly'
        }
        
        data = self._make_request(f'/coins/{token_id}/market_chart', params)
        
        if not data or 'prices' not in data:
            return []
        
        return [
            {
                'timestamp': int(price[0]),
                'datetime': datetime.fromtimestamp(price[0] / 1000),
                'price_usd': price[1]
            }
            for price in data['prices']
        ]


class PriceService:
    """Main price service with caching and multiple source support"""
    
    def __init__(self, coingecko_api_key: Optional[str] = None):
        self.cache = PriceCache()
        self.coingecko = CoinGeckoAPI(api_key=coingecko_api_key)
        
        # Token ID mapping (CoinGecko ID -> common symbols)
        self.token_id_map = {
            'ethereum': ['ETH', 'WETH'],
            'usd-coin': ['USDC'],
            'tether': ['USDT'],
            'dai': ['DAI'],
            'chainlink': ['LINK'],
            'uniswap': ['UNI'],
            'aave': ['AAVE'],
            'compound-governance-token': ['COMP'],
            'maker': ['MKR'],
            'wrapped-bitcoin': ['WBTC']
        }
        
        # Reverse mapping (symbol -> CoinGecko ID)
        self.symbol_to_id = {}
        for cg_id, symbols in self.token_id_map.items():
            for symbol in symbols:
                self.symbol_to_id[symbol.upper()] = cg_id
        
        logger.info(f"PriceService initialized with {len(self.symbol_to_id)} token mappings")
    
    def _get_coingecko_id(self, symbol: str) -> Optional[str]:
        """Get CoinGecko ID for a token symbol"""
        symbol_upper = symbol.upper()
        
        # Check direct mapping first
        if symbol_upper in self.symbol_to_id:
            return self.symbol_to_id[symbol_upper]
        
        # Try to search for the token
        search_results = self.coingecko.search_tokens(symbol)
        if search_results:
            best_match = search_results[0]  # Take the first result
            cg_id = best_match['id']
            
            # Cache the mapping for future use
            self.symbol_to_id[symbol_upper] = cg_id
            return cg_id
        
        return None
    
    def get_current_price(self, symbol: str, vs_currency: str = 'usd') -> Optional[Decimal]:
        """Get current price for a single token"""
        prices = self.get_current_prices([symbol], [vs_currency])
        return prices.get(symbol.upper(), {}).get(vs_currency.lower())
    
    def get_current_prices(self, symbols: List[str], vs_currencies: List[str] = None) -> Dict[str, Dict[str, Decimal]]:
        """
        Get current prices for multiple tokens
        
        Returns:
            Dict structure: {
                'ETH': {
                    'usd': Decimal('3200.50'),
                    'eth': Decimal('1.0'),
                    'change_24h': Decimal('-2.5'),
                    'volume_24h': Decimal('1000000'),
                    'last_updated': datetime
                }
            }
        """
        vs_currencies = vs_currencies or ['usd', 'eth']
        cache_key = f"prices_{'-'.join(sorted(symbols))}_{'-'.join(sorted(vs_currencies))}"
        
        # Check cache first
        cached_result = self.cache.get(cache_key, CACHE_TTL_PRICES)
        if cached_result:
            logger.debug(f"Using cached prices for {len(symbols)} tokens")
            return cached_result
        
        # Map symbols to CoinGecko IDs
        token_ids = []
        symbol_to_id_mapping = {}
        
        for symbol in symbols:
            cg_id = self._get_coingecko_id(symbol)
            if cg_id:
                token_ids.append(cg_id)
                symbol_to_id_mapping[cg_id] = symbol.upper()
            else:
                logger.warning(f"Could not find CoinGecko ID for symbol: {symbol}")
        
        if not token_ids:
            logger.warning("No valid token IDs found")
            return {}
        
        # Fetch prices from CoinGecko
        logger.info(f"Fetching prices for {len(token_ids)} tokens from CoinGecko")
        price_data = self.coingecko.get_simple_prices(token_ids, vs_currencies)
        
        # Process and format results
        result = {}
        for cg_id, data in price_data.items():
            symbol = symbol_to_id_mapping.get(cg_id)
            if not symbol:
                continue
            
            symbol_data = {}
            for currency in vs_currencies:
                if currency in data:
                    symbol_data[currency] = Decimal(str(data[currency]))
            
            # Add additional data
            if f'{vs_currencies[0]}_24h_change' in data:
                symbol_data['change_24h'] = Decimal(str(data[f'{vs_currencies[0]}_24h_change']))
            if f'{vs_currencies[0]}_24h_vol' in data:
                symbol_data['volume_24h'] = Decimal(str(data[f'{vs_currencies[0]}_24h_vol']))
            if 'last_updated_at' in data:
                symbol_data['last_updated'] = datetime.fromtimestamp(data['last_updated_at'])
            
            result[symbol] = symbol_data
        
        # Cache the result
        self.cache.set(cache_key, result)
        logger.info(f"Fetched and cached prices for {len(result)} tokens")
        
        return result
    
    def get_token_metadata(self, symbol: str) -> Dict[str, Any]:
        """Get detailed token metadata"""
        cache_key = f"metadata_{symbol.upper()}"
        
        # Check cache first
        cached_result = self.cache.get(cache_key, CACHE_TTL_METADATA)
        if cached_result:
            return cached_result
        
        cg_id = self._get_coingecko_id(symbol)
        if not cg_id:
            return {}
        
        logger.info(f"Fetching metadata for {symbol} from CoinGecko")
        metadata = self.coingecko.get_token_info(cg_id)
        
        # Cache the result
        self.cache.set(cache_key, metadata)
        
        return metadata
    
    def get_historical_prices(self, symbol: str, days: int = 30) -> pd.DataFrame:
        """Get historical price data as DataFrame"""
        cg_id = self._get_coingecko_id(symbol)
        if not cg_id:
            return pd.DataFrame()
        
        logger.info(f"Fetching {days} days of historical data for {symbol}")
        price_history = self.coingecko.get_historical_prices(cg_id, days)
        
        if not price_history:
            return pd.DataFrame()
        
        df = pd.DataFrame(price_history)
        df['symbol'] = symbol.upper()
        df.set_index('datetime', inplace=True)
        
        return df
    
    def get_portfolio_prices(self, portfolio_symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        """Get comprehensive price data for a portfolio of tokens"""
        if not portfolio_symbols:
            return {}
        
        # Get current prices
        current_prices = self.get_current_prices(portfolio_symbols, ['usd', 'eth'])
        
        # Enhance with metadata (in parallel to avoid sequential API calls)
        with ThreadPoolExecutor(max_workers=5) as executor:
            metadata_futures = {
                executor.submit(self.get_token_metadata, symbol): symbol 
                for symbol in portfolio_symbols[:5]  # Limit to avoid rate limits
            }
            
            for future in as_completed(metadata_futures):
                symbol = metadata_futures[future]
                try:
                    metadata = future.result()
                    if symbol in current_prices and metadata:
                        # Merge current prices with metadata
                        current_prices[symbol].update({
                            'name': metadata.get('name'),
                            'market_cap_rank': metadata.get('market_cap_rank'),
                            'market_cap_usd': metadata.get('market_data', {}).get('market_cap_usd'),
                            'circulating_supply': metadata.get('market_data', {}).get('circulating_supply')
                        })
                except Exception as e:
                    logger.error(f"Error fetching metadata for {symbol}: {e}")
        
        return current_prices
    
    def clear_cache(self) -> None:
        """Clear all cached data"""
        self.cache.clear()
        logger.info("Price cache cleared")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            'cache_size': self.cache.size(),
            'supported_tokens': len(self.symbol_to_id),
            'api_configured': self.coingecko.api_key is not None
        }


# Global price service instance
_price_service = None


def get_price_service() -> PriceService:
    """Get or create global price service instance"""
    global _price_service
    if _price_service is None:
        # Try to get API key from environment or config
        api_key = None  # Can be set from environment later
        _price_service = PriceService(coingecko_api_key=api_key)
    return _price_service


def get_current_prices(symbols: List[str], vs_currencies: List[str] = None) -> Dict[str, Dict[str, Decimal]]:
    """Convenience function to get current prices"""
    service = get_price_service()
    return service.get_current_prices(symbols, vs_currencies)


def get_current_price(symbol: str, vs_currency: str = 'usd') -> Optional[Decimal]:
    """Convenience function to get single price"""
    service = get_price_service()
    return service.get_current_price(symbol, vs_currency)