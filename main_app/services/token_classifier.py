"""
Token Classification Service

Provides comprehensive token security analysis and classification
to filter out phishing tokens and scams from legitimate transactions.
"""

import re
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timezone
from decimal import Decimal
from eth_utils import to_checksum_address
import logging

from ..config.blockchain_config import (
    VERIFIED_TOKENS, BLACKLISTED_TOKENS, SUSPICIOUS_PATTERNS,
    TOKEN_RISK_THRESHOLDS, TOKEN_STATUS
)
from ..s3_utils import (
    load_approved_tokens_file, save_approved_tokens_file,
    load_rejected_tokens_file, save_rejected_tokens_file
)

logger = logging.getLogger(__name__)


class TokenClassifier:
    """
    Analyzes token contracts to determine legitimacy and risk level.
    Protects against phishing tokens and scams.
    """
    
    def __init__(self, w3_instance):
        """Initialize with Web3 instance for blockchain queries."""
        self.w3 = w3_instance
        
        # Load persistent token decisions from S3
        try:
            self.user_approved_tokens = load_approved_tokens_file()
            self.user_rejected_tokens = load_rejected_tokens_file()
            logger.info(f"Loaded {len(self.user_approved_tokens)} approved and {len(self.user_rejected_tokens)} rejected tokens from S3")
        except Exception as e:
            logger.warning(f"Failed to load token decisions from S3, starting with empty sets: {e}")
            self.user_approved_tokens = set()  # Runtime approved tokens
            self.user_rejected_tokens = set()  # Runtime rejected tokens
        
    def classify_token(
        self,
        token_address: str,
        token_symbol: str
    ) -> Dict[str, any]:
        """
        Comprehensive token classification and risk assessment.
        
        Args:
            token_address: Contract address
            token_symbol: Token symbol from contract
            
        Returns:
            Dict with classification results and risk assessment
        """
        address_checksum = to_checksum_address(token_address)
        
        classification = {
            'address': address_checksum,
            'symbol': token_symbol,
            'status': TOKEN_STATUS['PENDING'],
            'risk_level': 'unknown',
            'risk_factors': [],
            'is_verified': False,
            'requires_approval': True,
            'analysis': {}
        }
        
        # Step 1: Check verified tokens (immediate approval)
        if self._is_verified_token(address_checksum):
            classification.update({
                'status': TOKEN_STATUS['VERIFIED'],
                'risk_level': 'low',
                'is_verified': True,
                'requires_approval': False
            })
            return classification
        
        # Step 2: Check blacklisted tokens (immediate rejection)
        if self._is_blacklisted_token(address_checksum):
            classification.update({
                'status': TOKEN_STATUS['BLACKLISTED'],
                'risk_level': 'critical',
                'requires_approval': False,
                'risk_factors': ['Blacklisted token']
            })
            return classification
        
        # Step 3: Check user decisions
        if address_checksum in self.user_approved_tokens:
            classification.update({
                'status': TOKEN_STATUS['APPROVED'],
                'risk_level': 'user_approved',
                'requires_approval': False
            })
            return classification
            
        if address_checksum in self.user_rejected_tokens:
            classification.update({
                'status': TOKEN_STATUS['REJECTED'],
                'requires_approval': False
            })
            return classification
        
        # Step 4: Perform risk analysis
        risk_analysis = self._analyze_token_risk(address_checksum, token_symbol)
        classification.update({
            'risk_level': risk_analysis['level'],
            'risk_factors': risk_analysis['factors'],
            'analysis': risk_analysis['details']
        })
        
        return classification
    
    def _is_verified_token(self, address: str) -> bool:
        """Check if token is in verified list."""
        address_lower = address.lower()
        for verified_addr in VERIFIED_TOKENS.values():
            if verified_addr.lower() == address_lower:
                return True
        return False
    
    def _is_blacklisted_token(self, address: str) -> bool:
        """Check if token is blacklisted."""
        address_lower = address.lower()
        for blacklisted_addr in BLACKLISTED_TOKENS.values():
            if blacklisted_addr.lower() == address_lower:
                return True
        return False
    
    def _analyze_token_risk(
        self,
        address: str,
        symbol: str
    ) -> Dict[str, any]:
        """
        Perform comprehensive risk analysis on token.
        
        Returns:
            Dict with risk level, factors, and detailed analysis
        """
        risk_factors = []
        analysis = {}
        
        # Symbol-based risk assessment
        symbol_risks = self._analyze_symbol(symbol)
        if symbol_risks:
            risk_factors.extend(symbol_risks)
            analysis['symbol_analysis'] = symbol_risks
        
        # Contract age analysis
        try:
            contract_age = self._get_contract_age(address)
            analysis['contract_age_days'] = contract_age
            
            if contract_age < TOKEN_RISK_THRESHOLDS['high_risk']['contract_age_days']:
                risk_factors.append(f"Very new contract ({contract_age} days old)")
            elif contract_age < TOKEN_RISK_THRESHOLDS['medium_risk']['contract_age_days']:
                risk_factors.append(f"Recent contract ({contract_age} days old)")
                
        except Exception as e:
            risk_factors.append("Cannot determine contract age")
            logger.warning(f"Failed to get contract age for {address}: {e}")
        
        # Token metadata analysis
        try:
            metadata = self._get_token_metadata(address)
            analysis['metadata'] = metadata
            
            # Check for suspicious metadata patterns
            if metadata.get('name', '').strip() == '':
                risk_factors.append("Empty token name")
            if metadata.get('symbol', '').strip() == '':
                risk_factors.append("Empty token symbol")
                
        except Exception as e:
            risk_factors.append("Cannot retrieve token metadata")
            logger.warning(f"Failed to get metadata for {address}: {e}")
        
        # Determine overall risk level
        risk_level = self._calculate_risk_level(risk_factors)
        
        return {
            'level': risk_level,
            'factors': risk_factors,
            'details': analysis
        }
    
    def _analyze_symbol(self, symbol: str) -> List[str]:
        """Analyze token symbol for suspicious patterns."""
        risks = []
        
        if not symbol or symbol == "UNKNOWN":
            risks.append("Unknown or missing symbol")
            return risks
        
        # Check for emoji patterns
        for emoji in SUSPICIOUS_PATTERNS['emoji_symbols']:
            if emoji in symbol:
                risks.append(f"Contains suspicious emoji: {emoji}")
        
        # Check for scam keywords
        symbol_upper = symbol.upper()
        for keyword in SUSPICIOUS_PATTERNS['scam_keywords']:
            if keyword in symbol_upper:
                risks.append(f"Contains suspicious keyword: {keyword}")
        
        # Check for impersonation attempts
        for impersonation in SUSPICIOUS_PATTERNS['impersonation_attempts']:
            if impersonation in symbol_upper:
                risks.append(f"Possible impersonation attempt: {impersonation}")
        
        # Check for unusual characters
        if re.search(r'[^\w\s]', symbol) and not re.search(r'^[A-Z0-9]+$', symbol):
            risks.append("Contains unusual characters")
        
        # Check for excessive length
        if len(symbol) > 20:
            risks.append("Unusually long symbol")
        
        return risks
    
    def _get_contract_age(self, address: str) -> int:
        """Get contract age in days."""
        try:
            # Get contract creation transaction
            # This is a simplified approach - in production you'd use
            # block explorer APIs or indexed data for efficiency
            current_block = self.w3.eth.block_number
            
            # Binary search to find contract creation (simplified)
            # In reality, you'd use Etherscan API or similar service
            creation_block = self._find_contract_creation_block(address, current_block)
            
            if creation_block:
                creation_time = self.w3.eth.get_block(creation_block).timestamp
                current_time = datetime.now(timezone.utc).timestamp()
                age_seconds = current_time - creation_time
                age_days = int(age_seconds / (24 * 60 * 60))
                return age_days
            
        except Exception as e:
            logger.error(f"Error getting contract age for {address}: {e}")
        
        return 999999  # Return very high age if cannot determine
    
    def _find_contract_creation_block(self, address: str, max_block: int) -> Optional[int]:
        """
        Simplified contract creation block finder.
        In production, use Etherscan API or indexed blockchain data.
        """
        try:
            # Check if address has code (is a contract)
            code = self.w3.eth.get_code(address)
            if code == b'':
                return None  # Not a contract
            
            # For demo purposes, estimate based on current block
            # Real implementation would use proper block explorer APIs
            estimated_age_days = 365  # Default to 1 year old
            blocks_per_day = 7200  # Approximate blocks per day on Ethereum
            estimated_creation_block = max_block - (estimated_age_days * blocks_per_day)
            
            return max(0, estimated_creation_block)
            
        except Exception:
            return None
    
    def _get_token_metadata(self, address: str) -> Dict[str, str]:
        """Get basic token metadata."""
        try:
            # ERC20 ABI for name, symbol, decimals
            erc20_abi = [
                {
                    "constant": True,
                    "inputs": [],
                    "name": "name",
                    "outputs": [{"name": "", "type": "string"}],
                    "type": "function"
                },
                {
                    "constant": True,
                    "inputs": [],
                    "name": "symbol", 
                    "outputs": [{"name": "", "type": "string"}],
                    "type": "function"
                },
                {
                    "constant": True,
                    "inputs": [],
                    "name": "decimals",
                    "outputs": [{"name": "", "type": "uint8"}],
                    "type": "function"
                }
            ]
            
            contract = self.w3.eth.contract(address=address, abi=erc20_abi)
            
            return {
                'name': contract.functions.name().call(),
                'symbol': contract.functions.symbol().call(),
                'decimals': contract.functions.decimals().call()
            }
            
        except Exception as e:
            logger.warning(f"Failed to get metadata for {address}: {e}")
            return {}
    
    def _calculate_risk_level(self, risk_factors: List[str]) -> str:
        """Calculate overall risk level based on factors."""
        critical_keywords = ['blacklisted', 'scam', 'phishing']
        high_risk_keywords = ['suspicious', 'impersonation', 'new contract', 'emoji']
        
        for factor in risk_factors:
            factor_lower = factor.lower()
            for keyword in critical_keywords:
                if keyword in factor_lower:
                    return 'critical'
        
        high_risk_count = 0
        for factor in risk_factors:
            factor_lower = factor.lower()
            for keyword in high_risk_keywords:
                if keyword in factor_lower:
                    high_risk_count += 1
                    break
        
        if high_risk_count >= 3:
            return 'high'
        elif high_risk_count >= 1:
            return 'medium'
        elif risk_factors:
            return 'low'
        else:
            return 'minimal'
    
    def approve_token(self, address: str) -> None:
        """Mark token as user-approved and save to S3."""
        address_checksum = to_checksum_address(address)
        self.user_approved_tokens.add(address_checksum)
        self.user_rejected_tokens.discard(address_checksum)
        
        # Save to S3 for persistence
        try:
            save_approved_tokens_file(self.user_approved_tokens)
            save_rejected_tokens_file(self.user_rejected_tokens)
            logger.info(f"Token approved and saved to S3: {address_checksum}")
        except Exception as e:
            logger.error(f"Failed to save approved token to S3: {e}")
            # Continue anyway - the token is still approved in memory
    
    def reject_token(self, address: str) -> None:
        """Mark token as user-rejected and save to S3."""
        address_checksum = to_checksum_address(address)
        self.user_rejected_tokens.add(address_checksum)
        self.user_approved_tokens.discard(address_checksum)
        
        # Save to S3 for persistence
        try:
            save_approved_tokens_file(self.user_approved_tokens)
            save_rejected_tokens_file(self.user_rejected_tokens)
            logger.info(f"Token rejected and saved to S3: {address_checksum}")
        except Exception as e:
            logger.error(f"Failed to save rejected token to S3: {e}")
            # Continue anyway - the token is still rejected in memory
    
    def get_user_approved_tokens(self) -> set:
        """Get list of user-approved tokens."""
        return self.user_approved_tokens.copy()
    
    def get_user_rejected_tokens(self) -> set:
        """Get list of user-rejected tokens."""
        return self.user_rejected_tokens.copy()
    
    def bulk_approve_tokens(self, addresses: List[str]) -> Dict[str, str]:
        """Bulk approve multiple tokens and save to S3."""
        results = {}
        changed = False
        
        for address in addresses:
            try:
                address_checksum = to_checksum_address(address)
                if address_checksum not in self.user_approved_tokens:
                    self.user_approved_tokens.add(address_checksum)
                    self.user_rejected_tokens.discard(address_checksum)
                    changed = True
                results[address] = "approved"
            except Exception as e:
                results[address] = f"error: {e}"
        
        # Save to S3 once at the end for efficiency
        if changed:
            try:
                save_approved_tokens_file(self.user_approved_tokens)
                save_rejected_tokens_file(self.user_rejected_tokens)
                logger.info(f"Bulk approved {len([r for r in results.values() if r == 'approved'])} tokens and saved to S3")
            except Exception as e:
                logger.error(f"Failed to save bulk approved tokens to S3: {e}")
        
        return results
    
    def bulk_reject_tokens(self, addresses: List[str]) -> Dict[str, str]:
        """Bulk reject multiple tokens and save to S3."""
        results = {}
        changed = False
        
        for address in addresses:
            try:
                address_checksum = to_checksum_address(address)
                if address_checksum not in self.user_rejected_tokens:
                    self.user_rejected_tokens.add(address_checksum)
                    self.user_approved_tokens.discard(address_checksum)
                    changed = True
                results[address] = "rejected"
            except Exception as e:
                results[address] = f"error: {e}"
        
        # Save to S3 once at the end for efficiency
        if changed:
            try:
                save_approved_tokens_file(self.user_approved_tokens)
                save_rejected_tokens_file(self.user_rejected_tokens)
                logger.info(f"Bulk rejected {len([r for r in results.values() if r == 'rejected'])} tokens and saved to S3")
            except Exception as e:
                logger.error(f"Failed to save bulk rejected tokens to S3: {e}")
        
        return results
    
    def get_unverified_tokens(self) -> List[Dict[str, any]]:
        """
        Get list of tokens that require user approval/verification.
        
        Returns:
            List of token info dicts for unverified tokens
        """
        # For now, return empty list since we don't track pending tokens
        # In a full implementation, this would return tokens that need review
        return []