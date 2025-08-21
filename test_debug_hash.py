#!/usr/bin/env python3
"""
Test script to debug the specific transaction hash processing
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'main_app'))

from datetime import datetime, date
from main_app.services.blockchain_service import BlockchainService
import logging

# Set up logging to see our debug messages
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_debug_hash():
    """Test if our debug hash is being processed correctly"""
    
    debug_hash = "0x6139dba1b74796d2fa1af26e70074a1e7b891a0170f7153dea95ac3db65daba6"
    print(f"[TEST] Testing debug hash: {debug_hash}")
    
    try:
        # Initialize blockchain service
        print("[TEST] Initializing blockchain service...")
        blockchain_service = BlockchainService()
        
        # Test with a date range that should include this transaction
        # This hash is likely from 2024, so let's use a wide range
        start_date = date(2024, 1, 1)
        end_date = date(2024, 12, 31)
        
        print(f"[TEST] Fetching transactions from {start_date} to {end_date}")
        print("[TEST] This will trigger our enhanced debug logging...")
        
        # Fetch transactions (this should trigger our debug logging)
        df = blockchain_service.fetch_transactions_for_period(
            start_date=start_date,
            end_date=end_date,
            fund_id="fund_i_class_B_ETH"  # Use specific fund
        )
        
        print(f"[TEST] Fetched {len(df)} transactions total")
        
        # Check if our debug hash is in the results
        if not df.empty:
            debug_matches = df[df['tx_hash'].str.lower() == debug_hash.lower()]
            if not debug_matches.empty:
                print(f"[SUCCESS] Found debug hash in results:")
                for idx, row in debug_matches.iterrows():
                    print(f"   - side: {row.get('side')}")
                    print(f"   - qty: {row.get('qty')}")
                    print(f"   - asset: {row.get('asset')}")
                    print(f"   - from: {row.get('from_address')}")
                    print(f"   - to: {row.get('to_address')}")
            else:
                print(f"[FAIL] Debug hash NOT FOUND in final results")
                # Show some sample hashes for comparison
                sample_hashes = df['tx_hash'].head(5).tolist()
                print(f"[TEST] Sample transaction hashes: {sample_hashes}")
        else:
            print("[FAIL] No transactions fetched at all")
            
    except Exception as e:
        print(f"[ERROR] Error during test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_debug_hash()