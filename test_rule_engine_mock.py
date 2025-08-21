#!/usr/bin/env python3
"""
Test script to verify rule engine processes our debug hash correctly using mock data
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'main_app'))

from datetime import datetime
from main_app.services.transaction_rules import TransactionRuleEngine
import pandas as pd
import logging

# Set up logging to see our debug messages
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def create_mock_transaction_data():
    """Create mock transaction data including our debug hash"""
    
    debug_hash = "0x6139dba1b74796d2fa1af26e70074a1e7b891a0170f7153dea95ac3db65daba6"
    
    # Mock wallet mapping (simplified)
    mock_wallet_mapping = pd.DataFrame([
        {
            'wallet_address': '0x1234567890123456789012345678901234567890',
            'fund_id': 'fund_i_class_B_ETH'
        },
        {
            'wallet_address': '0x0987654321098765432109876543210987654321',
            'fund_id': 'fund_i_class_B_ETH'
        }
    ])
    
    # Create mock transaction data that includes our debug hash
    mock_transactions = [
        {
            'tx_hash': debug_hash,
            'date': datetime(2024, 8, 20, 12, 0, 0),
            'wallet_id': '0x1234567890123456789012345678901234567890',
            'wallet_address': '0x1234567890123456789012345678901234567890',
            'from_address': '0x7777777777777777777777777777777777777777',  # External DeFi contract
            'to_address': '0x1234567890123456789012345678901234567890',    # Our wallet (receiving tokens)
            'token_address': '0xa0b86a33e6776d5fedd31f06f16a1db7cb6e9a46',
            'token_symbol': 'USDC',
            'asset': 'USDC',
            'event_type': 'Transfer',
            'side': 'sell',  # Initially marked as sell (which is wrong) - should be buy
            'qty': -100.0,   # Initially negative (wrong) - should be positive for buy
            'token_amount': 100.0,
            'direction': 'OUT',  # This is the issue - blockchain service marked as OUT 
            'function_signature': '0xa9059cbb',
            'intercompany': False
        },
        # Add a few more normal transactions for context
        {
            'tx_hash': '0x1111111111111111111111111111111111111111111111111111111111111111',
            'date': datetime(2024, 8, 20, 11, 0, 0),
            'wallet_id': '0x1234567890123456789012345678901234567890',
            'wallet_address': '0x1234567890123456789012345678901234567890',
            'from_address': '0x0000000000000000000000000000000000000000',
            'to_address': '0x1234567890123456789012345678901234567890',
            'token_address': '0xa0b86a33e6776d5fedd31f06f16a1db7cb6e9a46',
            'token_symbol': 'USDC',
            'asset': 'USDC',
            'event_type': 'Transfer',
            'side': 'buy',
            'qty': 50.0,
            'token_amount': 50.0,
            'direction': 'IN',
            'function_signature': '0xa9059cbb',
            'intercompany': False
        },
        {
            'tx_hash': '0x2222222222222222222222222222222222222222222222222222222222222222',
            'date': datetime(2024, 8, 20, 13, 0, 0),
            'wallet_id': '0x0987654321098765432109876543210987654321',
            'wallet_address': '0x0987654321098765432109876543210987654321',
            'from_address': '0x0987654321098765432109876543210987654321',
            'to_address': '0x3333333333333333333333333333333333333333',
            'token_address': '0xa0b86a33e6776d5fedd31f06f16a1db7cb6e9a46',
            'token_symbol': 'USDC',
            'asset': 'USDC',
            'event_type': 'Transfer',
            'side': 'sell',
            'qty': -25.0,
            'token_amount': 25.0,
            'direction': 'OUT',
            'function_signature': '0xa9059cbb',
            'intercompany': False
        }
    ]
    
    return mock_transactions, mock_wallet_mapping

def test_rule_engine_with_mock_data():
    """Test rule engine with mock data including our debug hash"""
    
    debug_hash = "0x6139dba1b74796d2fa1af26e70074a1e7b891a0170f7153dea95ac3db65daba6"
    print(f"[TEST] Testing rule engine with mock data including debug hash: {debug_hash}")
    
    try:
        # Create mock data
        mock_transactions, mock_wallet_mapping = create_mock_transaction_data()
        
        print(f"[TEST] Created {len(mock_transactions)} mock transactions")
        
        # Initialize rule engine with mock wallet mapping
        print("[TEST] Initializing transaction rule engine...")
        rule_engine = TransactionRuleEngine(mock_wallet_mapping)
        
        # Find our debug transaction in the mock data
        debug_tx_before = None
        for tx in mock_transactions:
            if tx['tx_hash'].lower() == debug_hash.lower():
                debug_tx_before = tx
                break
        
        if debug_tx_before:
            print(f"[TEST] Debug transaction BEFORE rules:")
            print(f"   - side: {debug_tx_before['side']}")
            print(f"   - qty: {debug_tx_before['qty']}")
            print(f"   - asset: {debug_tx_before['asset']}")
            print(f"   - from: {debug_tx_before['from_address']}")
            print(f"   - to: {debug_tx_before['to_address']}")
        else:
            print("[ERROR] Debug transaction not found in mock data")
            return
        
        # Apply rules (this should trigger our enhanced debug logging)
        print("[TEST] Applying transaction rules...")
        processed_transactions = rule_engine.apply_fifo_rules(mock_transactions)
        
        print(f"[TEST] Rule processing complete. Got {len(processed_transactions)} transactions")
        
        # Find our debug transaction after rule processing
        debug_tx_after = None
        for tx in processed_transactions:
            if tx.get('tx_hash', '').lower() == debug_hash.lower():
                debug_tx_after = tx
                break
        
        if debug_tx_after:
            print(f"[SUCCESS] Debug transaction AFTER rules:")
            print(f"   - side: {debug_tx_after.get('side')}")
            print(f"   - qty: {debug_tx_after.get('qty')}")
            print(f"   - asset: {debug_tx_after.get('asset')}")
            print(f"   - from: {debug_tx_after.get('from_address')}")
            print(f"   - to: {debug_tx_after.get('to_address')}")
            
            # Check if the side classification changed
            if debug_tx_before['side'] != debug_tx_after.get('side'):
                print(f"[CHANGE] Side classification changed from {debug_tx_before['side']} to {debug_tx_after.get('side')}")
            else:
                print(f"[NO CHANGE] Side classification remained: {debug_tx_after.get('side')}")
                
        else:
            print("[ERROR] Debug transaction NOT FOUND after rule processing")
            print(f"[DEBUG] Available hashes after processing: {[tx.get('tx_hash', 'NO_HASH') for tx in processed_transactions]}")
        
        # Get rule statistics
        stats = rule_engine.get_rule_stats()
        print(f"[TEST] Rule statistics: {stats}")
        
    except Exception as e:
        print(f"[ERROR] Error during test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_rule_engine_with_mock_data()