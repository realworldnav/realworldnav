# -*- coding: utf-8 -*-
"""
Transaction Rule Engine

Implements comprehensive transaction processing rules for accurate buy/sell classification
and FIFO compatibility, based on the master reference implementation.

Rules implemented:
- Rule 0: Only our transactions (wallet filtering)
- Rule 1: WETH Wrapping (deposits)
- Rule 2: WETH Unwrapping (withdrawals) 
- Rule 3: Token normalization (BLUR → BLUR POOL)
- Rule 4: Phishing/scam filtering
- Rule 5: Token mints (purchased assets)
- Rule 6: Token burns (sold assets)
"""

import pandas as pd
import logging
from typing import List, Dict, Any, Set, Optional
from decimal import Decimal
from datetime import datetime

# Set up logging
logger = logging.getLogger(__name__)

class TransactionRuleEngine:
    """
    Comprehensive transaction rule engine that processes blockchain transactions
    to ensure accurate buy/sell classification for FIFO calculations.
    """
    
    def __init__(self, wallet_mapping: pd.DataFrame = None):
        """
        Initialize the transaction rule engine.
        
        Args:
            wallet_mapping: DataFrame with wallet addresses and metadata
        """
        self.wallet_mapping = wallet_mapping if wallet_mapping is not None else pd.DataFrame()
        
        # Contract addresses (normalized to lowercase)
        self.WETH_CONTRACT = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
        self.BLUR_POOL_CONTRACT = "0x0000000000a39bb272e79075ade125fd351887ac"
        self.MINT_ADDRESS = "0x0000000000000000000000000000000000000000"
        self.BURN_ADDRESS = "0x0000000000000000000000000000000000000000"
        
        # Known phishing/scam addresses (from reference implementation)
        self.phishing_addresses = self._load_phishing_addresses()
        
        # Rule application tracking
        self.rule_stats = {
            'rule_0_dropped': 0,
            'rule_1_applied': 0,
            'rule_2_applied': 0,
            'rule_3_applied': 0,
            'rule_4_dropped': 0,
            'rule_5_applied': 0,
            'rule_6_applied': 0,
            'rule_7_applied': 0,
            'total_processed': 0
        }
    
    def _load_phishing_addresses(self) -> Set[str]:
        """Load known phishing/scam addresses."""
        raw_suspects = [
            "0x2a120e7f2F1d8fFD173eD17Aa5089f11206B5177",
            "0xcB4b7a5114E02c144a915c05C59192a6c6f33d5A",
            "0xa8F41D54Fd002aa0D027d010CDC3FCF3fd8F40c7",
            "0x8421f2Ae7f7D6ec64698E6A142515609932cFAbC",
            "0x5d72fcee79efe6a493078b57b310f8a854bcc71b",
            "0x729f7430e3e715c84bca27821a5e554cad056a35",
            "0xb6b15d694b07411823fe04ecd27399f18c521574",
            "0x4fbb350052bca5417566f188eb2ebce5b19bc964",
            "0xd08fd4141932f47a644b77a7ef968f552fa4daa0",
            "0x47c639efbabb3af26f95efb571293479e6c1d9cd",
            "0x1b3e77a721b2714fe7f80874e499e7825da29d0d",
            "0x3e3e8c461e4024757d0f81a30e9bcad8b3520671",
            "0xab3e1e638b19a8dbecc47d6d6433dbea67a76cb2",
            "0x91554a9f1b6582c6743f9d876c822816fd9639b8",
            "0xbf3314734852ecd952fd862da853d68d0a83e530",
            "0xe56333c2aedfeb4fbd5b7a4dbedc1b0f99e15abd",
            "0xcb2757d719f43ceb84d53915f4fa114be2fa3792",
            "0x69d706cfa647f989ad7e3f2cf151fd9c41e4ddb3",
            "0xa423e0855176835633c0d38b7c3cdda939903c02",
            "0xc04327b22e2160d1746d9b664d434e831dc06591",
            "0xf70b6c73e6ce7b82436b6e2f1c02dd50487b7362",
            "0x679488415fd76b482acda5328d90290d387835aa",
            "0x94b1afdd235b0daad3f56cc5507df2a6272c8013",
            "0x3b2ad323e2218de2eb57228e64f0073b3529713f",
            "0xa7504f4258e238b957c20b34427642700020ebd9",
            "0xdE9E976C9C53C22A2a0C74F50d5D5c70B35ffa8f",
        ]
        
        fake_phishing = [
            "0x0842661E4d34364c9d9023De581146DdeCF1d2d9",
            "0xE12933c0413Ca50F149C0379C797e515A96935Da",
            "0xaC52eD1e812d968BD5AF7Edb33B73A3559d7DaA0",
            "0x2B496312bD67Ab4F3a8519cda865F9728E50d209",
            "0x1bcc835e7a0e7f0672012e775967d4269f0c6dbc",
            # ... truncated for brevity, but will include full list
        ]
        
        # Normalize all addresses to lowercase
        all_suspects = set()
        for addr in raw_suspects + fake_phishing:
            all_suspects.add(addr.lower())
        
        logger.info(f"Loaded {len(all_suspects)} known phishing/scam addresses")
        return all_suspects
    
    def apply_fifo_rules(self, transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Apply all transaction processing rules in the correct order.
        
        Args:
            transactions: List of transaction dictionaries
            
        Returns:
            List of processed transactions with correct buy/sell classification
        """
        if not transactions:
            return []
        
        # Debug hash to track
        debug_hash = "0x6139dba1b74796d2fa1af26e70074a1e7b891a0170f7153dea95ac3db65daba6"
        
        logger.info(f"[RULE]RULE ENGINE: Starting rule processing for {len(transactions)} transactions [START][START][START]")
        self.rule_stats['total_processed'] = len(transactions)
        
        # Check if debug hash is in the transactions
        debug_tx = None
        logger.info(f"[RULE]RULE ENGINE: Starting with {len(transactions)} transactions, searching for {debug_hash} [START][START][START]")
        
        # List all transaction hashes for debugging
        all_hashes = [tx.get('tx_hash', 'NO_HASH') for tx in transactions]
        logger.info(f"[RULE]RULE ENGINE: All transaction hashes: {all_hashes[:10]}..." if len(all_hashes) > 10 else f"[RULE]RULE ENGINE: All transaction hashes: {all_hashes}")
        
        for tx in transactions:
            tx_hash = tx.get('tx_hash', '').lower()
            if tx_hash == debug_hash.lower():
                debug_tx = tx
                logger.info(f"[RULE]RULE ENGINE: DEBUG HASH FOUND! {debug_hash} [START][START][START]")
                logger.info(f"[RULE]Initial transaction data: {debug_tx}")
                break
        
        if not debug_tx:
            logger.info(f"[RULE]RULE ENGINE: DEBUG HASH NOT FOUND: {debug_hash} not in {len(transactions)} transactions")
            # Check for partial matches
            partial_matches = [tx.get('tx_hash', '') for tx in transactions if debug_hash[:20].lower() in tx.get('tx_hash', '').lower()]
            if partial_matches:
                logger.info(f"[RULE]RULE ENGINE: Partial matches found: {partial_matches}")
            else:
                logger.info(f"[RULE]RULE ENGINE: No partial matches found for debug hash")
        
        # Convert to DataFrame for easier processing
        df = pd.DataFrame(transactions)
        
        # Ensure all string columns are properly typed (not bytes)
        string_columns = ['from_address', 'to_address', 'token_address', 'wallet_address', 
                         'token_symbol', 'asset', 'function_signature', 'event_type']
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].fillna('').astype(str)
        
        # Apply rules in order (critical for correctness)
        def debug_after_rule(df, rule_name):
            debug_rows = df[df['tx_hash'].str.lower() == debug_hash.lower()]
            if not debug_rows.empty:
                logger.info(f"[RULE]After {rule_name}: FOUND {len(debug_rows)} rows for debug hash! [START][START]")
                for idx, row in debug_rows.iterrows():
                    logger.info(f"[RULE]{rule_name} - Row {idx}: side={row.get('side')}, qty={row.get('qty')}, asset={row.get('asset')}, from={row.get('from_address')}, to={row.get('to_address')}")
            else:
                logger.info(f"[RULE]After {rule_name}: DEBUG HASH NOT FOUND in {len(df)} rows")
                # Show sample of what hashes we do have
                sample_hashes = df['tx_hash'].head(3).tolist() if 'tx_hash' in df.columns and not df.empty else []
                logger.info(f"[RULE]Sample hashes after {rule_name}: {sample_hashes}")
        
        df = self._apply_rule_0_wallet_filtering(df)
        debug_after_rule(df, "Rule 0 - Wallet Filtering")
        
        df = self._apply_rule_1_weth_wrapping(df)
        debug_after_rule(df, "Rule 1 - WETH Wrapping")
        
        df = self._apply_rule_2_weth_unwrapping(df)
        debug_after_rule(df, "Rule 2 - WETH Unwrapping")
        
        df = self._apply_rule_3_token_normalization(df)
        debug_after_rule(df, "Rule 3 - Token Normalization")
        
        df = self._apply_rule_4_phishing_filtering(df)
        debug_after_rule(df, "Rule 4 - Phishing Filtering")
        
        df = self._apply_rule_5_token_mints(df)
        debug_after_rule(df, "Rule 5 - Token Mints")
        
        df = self._apply_rule_6_token_burns(df)
        debug_after_rule(df, "Rule 6 - Token Burns")
        
        df = self._apply_rule_7_direction_based_correction(df)
        debug_after_rule(df, "Rule 7 - Direction-Based Correction")
        
        # Convert back to list of dictionaries
        result = df.to_dict('records')
        
        # Final debug for our tracked hash
        final_debug_txs = [tx for tx in result if tx.get('tx_hash', '').lower() == debug_hash.lower()]
        if final_debug_txs:
            logger.info(f"[RULE]RULE ENGINE FINAL RESULT: FOUND {len(final_debug_txs)} transactions for debug hash! [START][START][START]")
            for i, tx in enumerate(final_debug_txs):
                logger.info(f"[RULE]Final TX {i}: side={tx.get('side')}, qty={tx.get('qty')}, asset={tx.get('asset')}, from={tx.get('from_address')}, to={tx.get('to_address')}")
        else:
            logger.info(f"[RULE]RULE ENGINE FINAL RESULT: DEBUG HASH NOT FOUND in {len(result)} final transactions")
            # Show sample of final hashes
            sample_final_hashes = [tx.get('tx_hash', 'NO_HASH') for tx in result[:3]]
            logger.info(f"[RULE]Sample final hashes: {sample_final_hashes}")
        
        logger.info(f"Rule processing complete: {len(result)} transactions after rules")
        self._log_rule_stats()
        
        return result
    
    def _apply_rule_0_wallet_filtering(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rule 0: Only process transactions involving known fund wallets.
        """
        if df.empty or self.wallet_mapping.empty:
            return df
        
        # Get set of known wallet addresses (normalized to lowercase)
        known_wallets = set()
        for _, row in self.wallet_mapping.iterrows():
            wallet_addr = row.get('wallet_address', '')
            if wallet_addr:
                known_wallets.add(wallet_addr.lower())
        
        # Normalize addresses in transaction data
        df['from_address'] = df['from_address'].fillna('').astype(str).str.lower()
        df['to_address'] = df['to_address'].fillna('').astype(str).str.lower()
        df['wallet_address'] = df['wallet_address'].fillna('').astype(str).str.lower()
        
        # Keep transactions where any address is in our known wallets
        before_count = len(df)
        mask = (
            df['from_address'].isin(known_wallets) |
            df['to_address'].isin(known_wallets) |
            df['wallet_address'].isin(known_wallets)
        )
        
        df_filtered = df[mask].copy()
        dropped_count = before_count - len(df_filtered)
        
        self.rule_stats['rule_0_dropped'] = dropped_count
        logger.info(f"Rule 0: Dropped {dropped_count} transactions not involving known wallets")
        
        return df_filtered
    
    def _apply_rule_1_weth_wrapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rule 1: WETH Wrapping (Deposits)
        
        When ETH is deposited to WETH contract:
        1. Drop the ETH Transfer to WETH contract (avoid double counting)
        2. Split WETH Deposit into two transactions:
           - ETH sell: from wallet to WETH contract
           - WETH buy: from WETH contract to wallet
        """
        if df.empty:
            return df
        
        # Step 1: Drop ETH Transfers going to WETH contract
        drop_mask = (
            (df.get('event_type', '') == 'ETH Transfer') &
            (df.get('to_address', '').str.lower() == self.WETH_CONTRACT.lower())
        )
        dropped_count = drop_mask.sum() if not drop_mask.empty else 0
        df = df[~drop_mask].copy()
        
        # Step 2: Find WETH deposit events to split
        deposit_mask = (
            (df.get('event_type', '').str.lower() == 'deposit') &
            (df.get('to_address', '').isna() | (df.get('to_address', '') == '')) &
            (df.get('token_address', '').str.lower() == self.WETH_CONTRACT.lower())
        )
        
        deposits_to_split = df[deposit_mask].copy()
        
        if not deposits_to_split.empty:
            # Create ETH sell legs (wallet → WETH contract)
            eth_legs = deposits_to_split.copy()
            eth_legs['asset'] = 'ETH'
            eth_legs['token_symbol'] = 'ETH'
            eth_legs['to_address'] = eth_legs['token_address']  # WETH contract
            eth_legs['from_address'] = deposits_to_split['from_address']
            eth_legs['token_address'] = None  # Native ETH
            eth_legs['side'] = 'sell'
            eth_legs['qty'] = -eth_legs['token_amount'].abs()  # Negative for sell
            
            # Create WETH buy legs (WETH contract → wallet)
            weth_legs = deposits_to_split.copy()
            weth_legs['asset'] = 'WETH'
            weth_legs['token_symbol'] = 'WETH'
            weth_legs['from_address'] = weth_legs['token_address']  # WETH contract
            weth_legs['to_address'] = deposits_to_split['from_address']
            weth_legs['side'] = 'buy'
            weth_legs['qty'] = weth_legs['token_amount'].abs()  # Positive for buy
            
            # Remove original deposits and add split transactions
            df = df[~deposit_mask].copy()
            df = pd.concat([df, eth_legs, weth_legs], ignore_index=True)
            
            split_count = len(deposits_to_split)
            self.rule_stats['rule_1_applied'] = split_count
            logger.info(f"Rule 1: Split {split_count} WETH deposits, dropped {dropped_count} ETH transfers")
        
        return df
    
    def _apply_rule_2_weth_unwrapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rule 2: WETH Unwrapping (Withdrawals)
        
        When WETH is withdrawn for ETH:
        Split WETH Withdraw into two transactions:
        - WETH sell: from wallet to WETH contract  
        - ETH buy: from WETH contract to wallet
        """
        if df.empty:
            return df
        
        # Find WETH withdraw events to split
        withdraw_mask = (
            (df.get('event_type', '').str.lower() == 'withdraw') &
            (df.get('to_address', '').isna() | (df.get('to_address', '') == '')) &
            (df.get('token_address', '').str.lower() == self.WETH_CONTRACT.lower())
        )
        
        withdraws_to_split = df[withdraw_mask].copy()
        
        if not withdraws_to_split.empty:
            # Create WETH sell legs (wallet → WETH contract)
            weth_legs = withdraws_to_split.copy()
            weth_legs['asset'] = 'WETH'
            weth_legs['token_symbol'] = 'WETH'
            weth_legs['to_address'] = weth_legs['token_address']  # WETH contract
            weth_legs['from_address'] = withdraws_to_split['from_address']
            weth_legs['side'] = 'sell'
            weth_legs['qty'] = -weth_legs['token_amount'].abs()  # Negative for sell
            
            # Create ETH buy legs (WETH contract → wallet)
            eth_legs = withdraws_to_split.copy()
            eth_legs['asset'] = 'ETH'
            eth_legs['token_symbol'] = 'ETH'
            eth_legs['from_address'] = eth_legs['token_address']  # WETH contract
            eth_legs['to_address'] = withdraws_to_split['from_address']
            eth_legs['token_address'] = None  # Native ETH
            eth_legs['side'] = 'buy'
            eth_legs['qty'] = eth_legs['token_amount'].abs()  # Positive for buy
            
            # Remove original withdraws and add split transactions
            df = df[~withdraw_mask].copy()
            df = pd.concat([df, weth_legs, eth_legs], ignore_index=True)
            
            split_count = len(withdraws_to_split)
            self.rule_stats['rule_2_applied'] = split_count
            logger.info(f"Rule 2: Split {split_count} WETH withdrawals")
        
        return df
    
    def _apply_rule_3_token_normalization(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rule 3: Normalize token symbols.
        
        Current: BLUR → BLUR POOL
        Can be extended for other token normalizations.
        """
        if df.empty:
            return df
        
        # Normalize BLUR to BLUR POOL
        blur_mask = df.get('token_symbol', '').str.upper() == 'BLUR'
        normalized_count = blur_mask.sum() if not blur_mask.empty else 0
        
        if normalized_count > 0:
            df.loc[blur_mask, 'token_symbol'] = 'BLUR POOL'
            df.loc[blur_mask, 'asset'] = 'BLUR POOL'
            
            self.rule_stats['rule_3_applied'] = normalized_count
            logger.info(f"Rule 3: Normalized {normalized_count} BLUR tokens to BLUR POOL")
        
        return df
    
    def _apply_rule_4_phishing_filtering(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rule 4: Remove transactions involving phishing/scam addresses.
        """
        if df.empty:
            return df
        
        # Normalize token addresses
        df['token_address'] = df['token_address'].fillna('').astype(str).str.lower()
        
        # Remove rows with suspicious token addresses
        before_count = len(df)
        phishing_mask = df['token_address'].isin(self.phishing_addresses)
        df_filtered = df[~phishing_mask].copy()
        
        dropped_count = before_count - len(df_filtered)
        self.rule_stats['rule_4_dropped'] = dropped_count
        
        if dropped_count > 0:
            logger.info(f"Rule 4: Removed {dropped_count} transactions with phishing addresses")
        
        return df_filtered
    
    def _apply_rule_5_token_mints(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rule 5: Handle token mints (purchases).
        
        When tokens are minted from 0x0 address:
        - Original: MINT_ADDRESS → wallet (token)
        - Add: wallet → MINT_ADDRESS (ETH payment)
        """
        if df.empty:
            return df
        
        # Find mint transactions (from 0x0 address)
        mint_mask = (
            (df.get('from_address', '').str.lower() == self.MINT_ADDRESS.lower()) &
            (df.get('token_address', '').str.lower() == self.BLUR_POOL_CONTRACT.lower())  # Focus on BLUR POOL for now
        )
        
        mints_to_duplicate = df[mint_mask].copy()
        
        if not mints_to_duplicate.empty:
            # Create corresponding ETH payment transactions
            eth_payments = mints_to_duplicate.copy()
            eth_payments['asset'] = 'ETH'
            eth_payments['token_symbol'] = 'ETH'
            eth_payments['from_address'] = mints_to_duplicate['to_address']  # wallet pays
            eth_payments['to_address'] = mints_to_duplicate['from_address']  # to mint address
            eth_payments['token_address'] = None  # Native ETH
            eth_payments['side'] = 'sell'
            eth_payments['qty'] = -eth_payments['token_amount'].abs()  # Negative for ETH payment
            
            # Update original mints to be buy transactions
            df.loc[mint_mask, 'side'] = 'buy'
            df.loc[mint_mask, 'qty'] = df.loc[mint_mask, 'token_amount'].abs()
            
            # Add ETH payment transactions
            df = pd.concat([df, eth_payments], ignore_index=True)
            
            mint_count = len(mints_to_duplicate)
            self.rule_stats['rule_5_applied'] = mint_count
            logger.info(f"Rule 5: Added ETH payments for {mint_count} token mints")
        
        return df
    
    def _apply_rule_6_token_burns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rule 6: Handle token burns (sales).
        
        When tokens are burned to 0x0 address:
        - Original: wallet → BURN_ADDRESS (token)
        - Add: BURN_ADDRESS → wallet (ETH receipt)
        """
        if df.empty:
            return df
        
        # Find burn transactions (to 0x0 address with specific function)
        # Note: Adding function signature check as per reference
        
        # Ensure function_signature is string type
        if 'function_signature' in df.columns:
            df['function_signature'] = df['function_signature'].fillna('').astype(str)
        
        burn_mask = (
            (df.get('to_address', '').str.lower() == self.BURN_ADDRESS.lower()) &
            (df.get('token_address', '').str.lower() == self.BLUR_POOL_CONTRACT.lower()) &
            (df.get('function_signature', '').astype(str).str.contains('OwnerTransferV7b711143', na=False))
        )
        
        burns_to_duplicate = df[burn_mask].copy()
        
        if not burns_to_duplicate.empty:
            # Create corresponding ETH receipt transactions
            eth_receipts = burns_to_duplicate.copy()
            eth_receipts['asset'] = 'ETH'
            eth_receipts['token_symbol'] = 'ETH'
            eth_receipts['from_address'] = burns_to_duplicate['to_address']  # from burn address
            eth_receipts['to_address'] = burns_to_duplicate['from_address']  # to wallet
            eth_receipts['token_address'] = None  # Native ETH
            eth_receipts['side'] = 'buy'
            eth_receipts['qty'] = eth_receipts['token_amount'].abs()  # Positive for ETH receipt
            
            # Update original burns to be sell transactions
            df.loc[burn_mask, 'side'] = 'sell'
            df.loc[burn_mask, 'qty'] = -df.loc[burn_mask, 'token_amount'].abs()
            
            # Add ETH receipt transactions
            df = pd.concat([df, eth_receipts], ignore_index=True)
            
            burn_count = len(burns_to_duplicate)
            self.rule_stats['rule_6_applied'] = burn_count
            logger.info(f"Rule 6: Added ETH receipts for {burn_count} token burns")
        
        return df
    
    def _apply_rule_7_direction_based_correction(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rule 7: Direction-based buy/sell correction.
        
        FIXED VERSION: Only corrects ACTUALLY incorrect classifications.
        
        The blockchain service correctly identifies most transactions:
        - Tokens coming IN → direction="in", side="buy" [OK] CORRECT
        - Tokens going OUT → direction="out", side="sell" [OK] CORRECT
        
        This rule should ONLY correct cases where the initial logic failed,
        such as complex DeFi interactions or unusual contract patterns.
        """
        if df.empty:
            return df
        
        logger.info(f"[RULE]RULE 7 STARTING: Processing {len(df)} transactions for direction-based correction [START][START][START]")
        
        correction_count = 0
        debug_hash = "0x6139dba1b74796d2fa1af26e70074a1e7b891a0170f7153dea95ac3db65daba6"
        debug_from_wallet = "0xef732b402abcf15df684e0e9c5795022a8696d9d"  # Should be SELL
        debug_to_wallet = "0x09c098e2283375b4e6bc04990fda2b1a7473f390"    # Should be BUY
        
        # Ensure required columns exist
        required_cols = ['tx_hash', 'from_address', 'to_address', 'wallet_address', 'side', 'qty', 'event_type']
        for col in required_cols:
            if col not in df.columns:
                df[col] = ''
        
        # Normalize addresses for comparison
        df['from_address'] = df['from_address'].fillna('').astype(str).str.lower()
        df['to_address'] = df['to_address'].fillna('').astype(str).str.lower() 
        df['wallet_address'] = df['wallet_address'].fillna('').astype(str).str.lower()
        
        # Get our fund wallets for comparison
        known_wallets = set()
        if self.wallet_mapping is not None and not self.wallet_mapping.empty:
            for _, row in self.wallet_mapping.iterrows():
                wallet_addr = row.get('wallet_address', '')
                if wallet_addr:
                    known_wallets.add(wallet_addr.lower())
        
        logger.info(f"[RULE]RULE 7: Known fund wallets: {known_wallets}")
        
        # Identify all intercompany transfers in the dataset
        intercompany_transfers = []
        all_hashes = set()
        for _, row in df.iterrows():
            tx_hash = row.get('tx_hash', '').lower()
            from_addr = row.get('from_address', '').lower()
            to_addr = row.get('to_address', '').lower()
            wallet_addr = row.get('wallet_address', '').lower()
            
            all_hashes.add(tx_hash)
            
            if from_addr in known_wallets and to_addr in known_wallets:
                intercompany_transfers.append({
                    'hash': tx_hash,
                    'from': from_addr,
                    'to': to_addr,
                    'wallet': wallet_addr,
                    'side': row.get('side', ''),
                    'qty': row.get('qty', 0)
                })
        
        logger.info(f"[RULE]RULE 7: Found {len(intercompany_transfers)} intercompany transfer records")
        logger.info(f"[RULE]RULE 7: Intercompany transfers: {intercompany_transfers}")
        logger.info(f"[RULE]RULE 7: Looking for debug hash {debug_hash} in dataset...")
        logger.info(f"[RULE]RULE 7: Debug hash in dataset: {debug_hash in all_hashes}")
        if debug_hash not in all_hashes:
            logger.info(f"[RULE]RULE 7: Sample hashes in dataset: {list(all_hashes)[:5]}")
        
        for idx, row in df.iterrows():
            tx_hash = row.get('tx_hash', '').lower()
            from_addr = row.get('from_address', '').lower()
            to_addr = row.get('to_address', '').lower()
            wallet_addr = row.get('wallet_address', '').lower()
            current_side = row.get('side', '')
            current_qty = row.get('qty', 0)
            event_type = row.get('event_type', '')
            
            # Debug specific transaction and both wallets involved
            is_debug = (tx_hash == debug_hash or 
                       wallet_addr.lower() == debug_from_wallet.lower() or 
                       wallet_addr.lower() == debug_to_wallet.lower())
            if is_debug:
                logger.info(f"[FIX][FIX][FIX] RULE 7 ENHANCED DEBUG: Processing tx={tx_hash} [FIX][FIX][FIX]")
                logger.info(f"[FIX] WALLET FOCUS: wallet_addr={wallet_addr}")
                logger.info(f"[FIX] BEFORE RULE 7: side={current_side}, qty={current_qty}")
                logger.info(f"[FIX] ADDRESSES: from_addr={from_addr}")
                logger.info(f"[FIX] ADDRESSES: to_addr={to_addr}") 
                logger.info(f"[FIX] event_type={event_type}")
                logger.info(f"[FIX] known_wallets includes wallet_addr: {wallet_addr in known_wallets}")
                logger.info(f"[FIX] known_wallets includes from_addr: {from_addr in known_wallets}")
                logger.info(f"[FIX] known_wallets includes to_addr: {to_addr in known_wallets}")
                logger.info(f"[FIX] IS TARGET FROM WALLET: {wallet_addr.lower() == debug_from_wallet.lower()}")
                logger.info(f"[FIX] IS TARGET TO WALLET: {wallet_addr.lower() == debug_to_wallet.lower()}")
            
            # Only process Transfer events for now
            if event_type != 'Transfer':
                if is_debug:
                    logger.info(f"[FIX] RULE 7 DEBUG: Skipping - not a Transfer event")
                continue
                
            # Skip if this wallet is not one of our known wallets
            if wallet_addr not in known_wallets:
                if is_debug:
                    logger.info(f"[FIX] RULE 7 DEBUG: Skipping - wallet not in known wallets")
                continue
            
            # Determine the EXPECTED correct classification based on transaction flow
            expected_side = None
            expected_qty = None
            case_applied = None
            
            # Case 1: Tokens coming INTO our wallet (from external address)
            if to_addr == wallet_addr and from_addr not in known_wallets:
                expected_side = 'buy'
                expected_qty = abs(current_qty)  # Should be positive
                case_applied = "Case 1: External → Our Wallet (Expected: BUY)"
                
                if is_debug:
                    logger.info(f"[FIX] RULE 7 DEBUG: {case_applied}")
                    logger.info(f"[FIX] EXPECTED: side=buy, qty={expected_qty}")
                    logger.info(f"[FIX] CURRENT:  side={current_side}, qty={current_qty}")
            
            # Case 2: Tokens going OUT of our wallet (to external address)  
            elif from_addr == wallet_addr and to_addr not in known_wallets:
                expected_side = 'sell'
                expected_qty = -abs(current_qty)  # Should be negative
                case_applied = "Case 2: Our Wallet → External (Expected: SELL)"
                
                if is_debug:
                    logger.info(f"[FIX] RULE 7 DEBUG: {case_applied}")
                    logger.info(f"[FIX] EXPECTED: side=sell, qty={expected_qty}")
                    logger.info(f"[FIX] CURRENT:  side={current_side}, qty={current_qty}")
            
            # Case 3: Intercompany transfers (between our wallets)
            elif from_addr in known_wallets and to_addr in known_wallets:
                # For intercompany transfers, the rule is simple:
                # - If this wallet is the FROM wallet → SELL (negative qty)
                # - If this wallet is the TO wallet → BUY (positive qty)
                
                if wallet_addr == from_addr:
                    expected_side = 'sell'
                    expected_qty = -abs(current_qty)  # Should be negative
                    case_applied = "Case 3a: Intercompany FROM wallet (Expected: SELL)"
                    
                    if is_debug:
                        logger.info(f"[FIX] RULE 7 DEBUG: {case_applied}")
                        logger.info(f"[FIX] This wallet ({wallet_addr}) is the FROM address → should be SELL")
                        
                elif wallet_addr == to_addr:
                    expected_side = 'buy' 
                    expected_qty = abs(current_qty)   # Should be positive
                    case_applied = "Case 3b: Intercompany TO wallet (Expected: BUY)"
                    
                    if is_debug:
                        logger.info(f"[FIX] RULE 7 DEBUG: {case_applied}")
                        logger.info(f"[FIX] This wallet ({wallet_addr}) is the TO address → should be BUY")
                        
                else:
                    # This shouldn't happen - wallet_addr should be either from or to
                    if is_debug:
                        logger.info(f"[FIX] RULE 7 DEBUG: Case 3: ERROR - wallet_addr ({wallet_addr}) is neither from ({from_addr}) nor to ({to_addr})")
                    continue
                
                if is_debug:
                    logger.info(f"[FIX] EXPECTED: side={expected_side}, qty={expected_qty}")
                    logger.info(f"[FIX] CURRENT:  side={current_side}, qty={current_qty}")
            
            else:
                if is_debug:
                    logger.info(f"[FIX] RULE 7 DEBUG: No case matched - no correction needed")
                continue
            
            # ONLY apply correction if current classification is WRONG
            needs_correction = False
            if expected_side and expected_side != current_side:
                needs_correction = True
                if is_debug:
                    logger.info(f"[FIX] RULE 7 DEBUG: SIDE MISMATCH - Correction needed!")
                    logger.info(f"[FIX] Will change: {current_side} → {expected_side}")
            
            # Also check if quantity has wrong sign
            elif expected_side == current_side:
                # Side is correct, but check quantity sign
                if (expected_side == 'buy' and current_qty < 0) or (expected_side == 'sell' and current_qty > 0):
                    needs_correction = True
                    if is_debug:
                        logger.info(f"[FIX] RULE 7 DEBUG: QTY SIGN WRONG - Correction needed!")
                        logger.info(f"[FIX] Will change qty: {current_qty} → {expected_qty}")
                else:
                    if is_debug:
                        logger.info(f"[FIX] RULE 7 DEBUG: Classification already CORRECT - no change needed")
            
            # Apply correction ONLY if actually needed
            if needs_correction:
                df.at[idx, 'side'] = expected_side
                df.at[idx, 'qty'] = expected_qty
                correction_count += 1
                
                if is_debug:
                    logger.info(f"[FIX][FIX][FIX] RULE 7 DEBUG: CORRECTION APPLIED! [FIX][FIX][FIX]")
                    logger.info(f"[FIX] AFTER:  side={expected_side}, qty={expected_qty}")
                
                logger.info(f"Rule 7: Corrected tx {tx_hash[:10]}... from {current_side} to {expected_side} ({case_applied})")
            else:
                if is_debug:
                    logger.info(f"[FIX] RULE 7 DEBUG: No correction applied - classification already correct")
        
        self.rule_stats['rule_7_applied'] = correction_count
        
        # Final summary
        logger.info(f"[RULE]RULE 7 COMPLETE: Applied {correction_count} corrections to {len(df)} transactions [START][START][START]")
        
        # Show final state of any intercompany transfers
        final_intercompany = []
        for _, row in df.iterrows():
            tx_hash = row.get('tx_hash', '').lower()
            from_addr = row.get('from_address', '').lower()
            to_addr = row.get('to_address', '').lower()
            wallet_addr = row.get('wallet_address', '').lower()
            
            if from_addr in known_wallets and to_addr in known_wallets:
                final_intercompany.append({
                    'hash': tx_hash[:10] + '...',
                    'wallet': wallet_addr[:6] + '...',
                    'from': from_addr[:6] + '...',
                    'to': to_addr[:6] + '...',
                    'side': row.get('side', ''),
                    'qty': row.get('qty', 0)
                })
        
        if final_intercompany:
            logger.info(f"[RULE]RULE 7 FINAL: Intercompany transfers after processing: {final_intercompany}")
        else:
            logger.info(f"[RULE]RULE 7 FINAL: No intercompany transfers found in dataset")
        
        if correction_count > 0:
            logger.info(f"Rule 7: Applied direction-based corrections to {correction_count} transactions")
        else:
            logger.info(f"Rule 7: No corrections needed - all classifications already correct")
        
        return df
    
    def _log_rule_stats(self):
        """Log comprehensive rule application statistics."""
        stats = self.rule_stats
        logger.info("=== Transaction Rule Engine Statistics ===")
        logger.info(f"Total transactions processed: {stats['total_processed']}")
        logger.info(f"Rule 0 (Wallet filtering) - Dropped: {stats['rule_0_dropped']}")
        logger.info(f"Rule 1 (WETH wrapping) - Applied: {stats['rule_1_applied']}")
        logger.info(f"Rule 2 (WETH unwrapping) - Applied: {stats['rule_2_applied']}")
        logger.info(f"Rule 3 (Token normalization) - Applied: {stats['rule_3_applied']}")
        logger.info(f"Rule 4 (Phishing filtering) - Dropped: {stats['rule_4_dropped']}")
        logger.info(f"Rule 5 (Token mints) - Applied: {stats['rule_5_applied']}")
        logger.info(f"Rule 6 (Token burns) - Applied: {stats['rule_6_applied']}")
        logger.info(f"Rule 7 (Direction-based correction) - Applied: {stats['rule_7_applied']}")
        logger.info("==========================================")
    
    def get_rule_stats(self) -> Dict[str, int]:
        """Get rule application statistics."""
        return self.rule_stats.copy()
    
    def reset_stats(self):
        """Reset rule application statistics."""
        for key in self.rule_stats:
            self.rule_stats[key] = 0