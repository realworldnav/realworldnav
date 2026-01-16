"""
Etherscan Balance Verification Service

Fetches current token balances from Etherscan API and compares them with FIFO positions
for balance verification and auditing purposes.
"""

import requests
import pandas as pd
import logging
from typing import Dict, List, Optional, Tuple
from decimal import Decimal, getcontext
import time

from main_app.config.blockchain_config import ETHERSCAN_API_KEY, ETHERSCAN_BASE_URL

logger = logging.getLogger(__name__)

# Set decimal precision for financial calculations
getcontext().prec = 28


class EtherscanBalanceChecker:
    """
    Service for fetching and comparing token balances from Etherscan.
    """
    
    def __init__(self, api_key: str = ETHERSCAN_API_KEY):
        """Initialize with Etherscan API key."""
        self.api_key = api_key
        self.session = requests.Session()
        self.rate_limit_delay = 0.2  # 200ms between requests to respect rate limits
        
    def get_eth_balance(self, wallet_address: str) -> Decimal:
        """
        Get ETH balance for a wallet address.
        
        Args:
            wallet_address: Ethereum wallet address
            
        Returns:
            ETH balance as Decimal
        """
        try:
            params = {
                'module': 'account',
                'action': 'balance',
                'address': wallet_address,
                'tag': 'latest',
                'apikey': self.api_key
            }
            
            response = self.session.get(ETHERSCAN_BASE_URL, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('status') == '1':
                # Convert from wei to ETH
                balance_wei = int(data.get('result', '0'))
                balance_eth = Decimal(balance_wei) / Decimal('1000000000000000000')
                return balance_eth
            else:
                logger.error(f"Etherscan API error for ETH balance: {data.get('message', 'Unknown error')}")
                return Decimal('0')
                
        except Exception as e:
            logger.error(f"Error fetching ETH balance for {wallet_address}: {e}")
            return Decimal('0')
        finally:
            time.sleep(self.rate_limit_delay)
    
    def get_token_balance(self, wallet_address: str, contract_address: str, decimals: int = 18) -> Decimal:
        """
        Get ERC-20 token balance for a wallet address.
        
        Args:
            wallet_address: Ethereum wallet address
            contract_address: ERC-20 token contract address
            decimals: Token decimal places (default 18)
            
        Returns:
            Token balance as Decimal
        """
        try:
            params = {
                'module': 'account',
                'action': 'tokenbalance',
                'contractaddress': contract_address,
                'address': wallet_address,
                'tag': 'latest',
                'apikey': self.api_key
            }
            
            response = self.session.get(ETHERSCAN_BASE_URL, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('status') == '1':
                # Convert from smallest unit to token amount
                balance_raw = int(data.get('result', '0'))
                balance_tokens = Decimal(balance_raw) / Decimal(10 ** decimals)
                return balance_tokens
            else:
                logger.error(f"Etherscan API error for token balance: {data.get('message', 'Unknown error')}")
                return Decimal('0')
                
        except Exception as e:
            logger.error(f"Error fetching token balance for {wallet_address}: {e}")
            return Decimal('0')
        finally:
            time.sleep(self.rate_limit_delay)
    
    def verify_wallet_balances(self, positions_df: pd.DataFrame, token_contracts: Dict[str, Dict] = None) -> pd.DataFrame:
        """
        Verify FIFO positions against Etherscan balances.
        
        Args:
            positions_df: DataFrame with FIFO positions
            token_contracts: Dict mapping token symbols to contract info
                           Format: {'TOKEN': {'address': '0x...', 'decimals': 18}}
                           
        Returns:
            DataFrame with comparison results
        """
        if token_contracts is None:
            # Common token contracts (can be expanded)
            token_contracts = {
                'USDC': {'address': '0xA0b86a33E6441644663FB5CDDFEF68e36E6c6C46', 'decimals': 6},
                'USDT': {'address': '0xdAC17F958D2ee523a2206206994597C13D831ec7', 'decimals': 6},
                'DAI': {'address': '0x6B175474E89094C44Da98b954EedeAC495271d0F', 'decimals': 18},
                'WETH': {'address': '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2', 'decimals': 18},
            }
        
        verification_results = []
        
        # Group positions by wallet and asset
        grouped = positions_df.groupby(['wallet_address', 'asset']) if 'wallet_address' in positions_df.columns else positions_df.groupby(['asset'])
        
        for (wallet_address, asset), group in grouped:
            try:
                logger.info(f"Verifying balance for {asset} in wallet {wallet_address}")
                
                # Get FIFO calculated position
                fifo_balance = group['token_amount'].sum()
                fifo_eth_value = group['eth_value'].sum()
                fifo_cost_basis = group['cost_basis_eth'].sum()
                
                # Get Etherscan balance
                if asset.upper() == 'ETH':
                    etherscan_balance = self.get_eth_balance(wallet_address)
                else:
                    token_info = token_contracts.get(asset.upper())
                    if token_info:
                        etherscan_balance = self.get_token_balance(
                            wallet_address, 
                            token_info['address'], 
                            token_info['decimals']
                        )
                    else:
                        logger.warning(f"No contract info for token {asset}, skipping Etherscan check")
                        etherscan_balance = Decimal('0')
                
                # Calculate difference
                difference = etherscan_balance - Decimal(str(fifo_balance))
                difference_percent = (float(difference) / float(fifo_balance) * 100) if fifo_balance > 0 else 0
                
                # Determine status
                if abs(difference) < Decimal('0.000001'):  # Very small difference threshold
                    status = "Match"
                elif abs(difference_percent) < 1.0:  # Less than 1% difference
                    status = "Minor Diff"
                else:
                    status = "Mismatch"
                
                verification_results.append({
                    'wallet_address': wallet_address,
                    'asset': asset,
                    'fifo_balance': float(fifo_balance),
                    'etherscan_balance': float(etherscan_balance),
                    'difference': float(difference),
                    'difference_percent': difference_percent,
                    'fifo_eth_value': float(fifo_eth_value),
                    'fifo_cost_basis': float(fifo_cost_basis),
                    'status': status,
                    'last_checked': pd.Timestamp.now()
                })
                
            except Exception as e:
                logger.error(f"Error verifying balance for {asset} in {wallet_address}: {e}")
                verification_results.append({
                    'wallet_address': wallet_address,
                    'asset': asset,
                    'fifo_balance': float(fifo_balance) if 'fifo_balance' in locals() else 0,
                    'etherscan_balance': 0,
                    'difference': 0,
                    'difference_percent': 0,
                    'fifo_eth_value': float(fifo_eth_value) if 'fifo_eth_value' in locals() else 0,
                    'fifo_cost_basis': float(fifo_cost_basis) if 'fifo_cost_basis' in locals() else 0,
                    'status': "Error",
                    'last_checked': pd.Timestamp.now()
                })
        
        return pd.DataFrame(verification_results)
    
    def get_multiple_balances(self, wallet_addresses: List[str], assets: List[str], token_contracts: Dict[str, Dict] = None) -> pd.DataFrame:
        """
        Get balances for multiple wallets and assets.
        
        Args:
            wallet_addresses: List of wallet addresses
            assets: List of asset symbols
            token_contracts: Token contract information
            
        Returns:
            DataFrame with current balances
        """
        balances = []
        
        for wallet in wallet_addresses:
            for asset in assets:
                try:
                    if asset.upper() == 'ETH':
                        balance = self.get_eth_balance(wallet)
                    else:
                        token_info = token_contracts.get(asset.upper(), {}) if token_contracts else {}
                        if token_info:
                            balance = self.get_token_balance(
                                wallet, 
                                token_info['address'], 
                                token_info['decimals']
                            )
                        else:
                            balance = Decimal('0')
                    
                    balances.append({
                        'wallet_address': wallet,
                        'asset': asset,
                        'balance': float(balance),
                        'timestamp': pd.Timestamp.now()
                    })
                    
                except Exception as e:
                    logger.error(f"Error getting balance for {asset} in {wallet}: {e}")
                    balances.append({
                        'wallet_address': wallet,
                        'asset': asset,
                        'balance': 0.0,
                        'timestamp': pd.Timestamp.now()
                    })
        
        return pd.DataFrame(balances)


# Global instance for use across modules
_etherscan_checker = None

def get_etherscan_checker() -> EtherscanBalanceChecker:
    """Get global Etherscan balance checker instance."""
    global _etherscan_checker
    if _etherscan_checker is None:
        _etherscan_checker = EtherscanBalanceChecker()
    return _etherscan_checker