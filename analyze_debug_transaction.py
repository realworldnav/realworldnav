#!/usr/bin/env python3
"""
Analyze the specific debug transaction to understand why it's misclassified
"""

import requests
import json

def lookup_transaction_on_etherscan():
    """Look up the specific transaction using Etherscan API"""
    
    debug_hash = "0x6139dba1b74796d2fa1af26e70074a1e7b891a0170f7153dea95ac3db65daba6"
    
    # You would normally use an API key, but for public lookups we can try without
    # Etherscan free tier allows some requests
    base_url = "https://api.etherscan.io/api"
    
    # Get transaction details
    tx_params = {
        'module': 'proxy',
        'action': 'eth_getTransactionByHash', 
        'txhash': debug_hash,
        'apikey': 'YourApiKeyToken'  # You'd need a real API key
    }
    
    # Get transaction receipt
    receipt_params = {
        'module': 'proxy',
        'action': 'eth_getTransactionReceipt',
        'txhash': debug_hash,
        'apikey': 'YourApiKeyToken'
    }
    
    print(f"[ANALYSIS] Analyzing transaction: {debug_hash}")
    
    try:
        # Get transaction details
        print(f"[ANALYSIS] Looking up transaction details...")
        tx_response = requests.get(base_url, params=tx_params, timeout=10)
        
        if tx_response.status_code == 200:
            tx_data = tx_response.json()
            if tx_data.get('result'):
                tx_result = tx_data['result']
                print(f"[TRANSACTION] From: {tx_result.get('from')}")
                print(f"[TRANSACTION] To: {tx_result.get('to')}")
                print(f"[TRANSACTION] Value: {tx_result.get('value')} wei")
                print(f"[TRANSACTION] Input: {tx_result.get('input', '')[:50]}...")
                print(f"[TRANSACTION] Gas: {tx_result.get('gas')}")
                print(f"[TRANSACTION] Gas Price: {tx_result.get('gasPrice')}")
                
                # Analyze the input data for function signature
                input_data = tx_result.get('input', '')
                if input_data and len(input_data) >= 10:
                    function_sig = input_data[:10]
                    print(f"[FUNCTION] Function signature: {function_sig}")
                    
                    # Common function signatures
                    function_signatures = {
                        '0xa9059cbb': 'transfer(address,uint256)',
                        '0x23b872dd': 'transferFrom(address,address,uint256)',
                        '0x095ea7b3': 'approve(address,uint256)',
                        '0x2e1a7d4d': 'withdraw(uint256)',
                        '0xd0e30db0': 'deposit()',
                        '0x40c10f19': 'mint(address,uint256)',
                        '0x42966c68': 'burn(uint256)',
                    }
                    
                    if function_sig in function_signatures:
                        print(f"[FUNCTION] Decoded: {function_signatures[function_sig]}")
                    else:
                        print(f"[FUNCTION] Unknown function signature")
            else:
                print(f"[ERROR] No transaction result: {tx_data}")
        
        # Get transaction receipt (logs)
        print(f"[ANALYSIS] Looking up transaction receipt...")
        receipt_response = requests.get(base_url, params=receipt_params, timeout=10)
        
        if receipt_response.status_code == 200:
            receipt_data = receipt_response.json()
            if receipt_data.get('result'):
                receipt = receipt_data['result']
                logs = receipt.get('logs', [])
                print(f"[RECEIPT] Status: {receipt.get('status')}")
                print(f"[RECEIPT] Gas Used: {receipt.get('gasUsed')}")
                print(f"[RECEIPT] Number of logs: {len(logs)}")
                
                for i, log in enumerate(logs):
                    print(f"[LOG {i}] Address: {log.get('address')}")
                    print(f"[LOG {i}] Topics: {log.get('topics', [])}")
                    if log.get('topics') and len(log['topics']) > 0:
                        topic0 = log['topics'][0]
                        # Common event signatures
                        event_signatures = {
                            '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef': 'Transfer(address,address,uint256)',
                            '0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925': 'Approval(address,address,uint256)',
                            '0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c': 'Deposit(address,uint256)',
                            '0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65': 'Withdraw(address,uint256)',
                        }
                        
                        if topic0 in event_signatures:
                            print(f"[LOG {i}] Event: {event_signatures[topic0]}")
                        else:
                            print(f"[LOG {i}] Unknown event: {topic0}")
            else:
                print(f"[ERROR] No receipt result: {receipt_data}")
                
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Request failed: {e}")
        print(f"[NOTE] This might be due to API rate limits or missing API key")
    except Exception as e:
        print(f"[ERROR] Analysis failed: {e}")

def analyze_buy_sell_logic():
    """Analyze the general buy/sell classification logic"""
    
    print(f"\n[LOGIC ANALYSIS] Buy/Sell Classification Rules:")
    print(f"Current rules focus on:")
    print(f"1. WETH wrapping/unwrapping (ETH <-> WETH)")
    print(f"2. Token mints (0x0 -> wallet = buy)")
    print(f"3. Token burns (wallet -> 0x0 = sell)")
    print(f"4. Token normalization (BLUR -> BLUR POOL)")
    print(f"5. Phishing filtering")
    print(f"6. Wallet filtering")
    print(f"")
    print(f"[MISSING LOGIC] Potential missing rules:")
    print(f"- Intercompany transfers (fund wallet to fund wallet)")
    print(f"- Direction-based classification (from our wallet = sell, to our wallet = buy)")
    print(f"- DeFi protocol interactions (swaps, liquidity provision)")
    print(f"- Complex multi-step transactions")
    print(f"")
    print(f"[HYPOTHESIS] The debug transaction might be:")
    print(f"- A normal transfer between fund wallets (should be split into buy/sell legs)")
    print(f"- A DeFi interaction that needs special handling")
    print(f"- A transaction where direction-based logic should apply")

if __name__ == "__main__":
    lookup_transaction_on_etherscan()
    analyze_buy_sell_logic()