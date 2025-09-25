#!/usr/bin/env python3
"""
Test script to verify Etherscan API connectivity
Run this to debug API issues
"""

import os
import sys
import requests
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_etherscan_api():
    """Test Etherscan API directly"""

    # Get API key from environment
    api_key = os.getenv('ETHERSCAN_API_KEY')
    if not api_key:
        print("❌ ERROR: ETHERSCAN_API_KEY not found in environment!")
        print("Please set it in your .env file")
        return False

    print(f"✅ API Key found: {api_key[:4]}...{api_key[-4:]}")

    # Test wallet address (has 3,458 transactions)
    wallet_address = "0x3b2A51FEC517BBc7fEaf68AcFdb068b57870713F"
    print(f"Testing wallet: {wallet_address}")

    # Build API URL
    base_url = "https://api.etherscan.io/api"
    params = {
        'module': 'account',
        'action': 'txlist',
        'address': wallet_address,
        'startblock': 0,
        'endblock': 99999999,
        'page': 1,
        'offset': 10,  # Just get 10 for testing
        'sort': 'desc',
        'apikey': api_key
    }

    print(f"\n📡 Making API request...")
    print(f"URL: {base_url}")
    print(f"Params: {json.dumps(params, indent=2)}")

    try:
        # Make the request
        response = requests.get(base_url, params=params)
        print(f"\n📊 Response Status: {response.status_code}")

        if response.status_code == 200:
            data = response.json()
            print(f"Response: {json.dumps(data, indent=2)[:1000]}...")  # First 1000 chars

            # Check if successful
            if data.get('status') == '1':
                transactions = data.get('result', [])
                print(f"\n✅ SUCCESS! Found {len(transactions)} transactions")

                if transactions:
                    # Show first transaction
                    tx = transactions[0]
                    print(f"\nMost recent transaction:")
                    print(f"  - Hash: {tx.get('hash', 'N/A')}")
                    print(f"  - From: {tx.get('from', 'N/A')}")
                    print(f"  - To: {tx.get('to', 'N/A')}")
                    print(f"  - Value: {int(tx.get('value', 0)) / 10**18:.6f} ETH")
                    print(f"  - Block: {tx.get('blockNumber', 'N/A')}")

                    import datetime
                    timestamp = int(tx.get('timeStamp', 0))
                    if timestamp:
                        dt = datetime.datetime.fromtimestamp(timestamp)
                        print(f"  - Time: {dt.strftime('%Y-%m-%d %H:%M:%S')}")

                return True
            else:
                # API error
                print(f"\n❌ API Error:")
                print(f"  Status: {data.get('status')}")
                print(f"  Message: {data.get('message', 'Unknown')}")
                print(f"  Result: {data.get('result', 'No details')}")
                return False
        else:
            print(f"❌ HTTP Error: {response.status_code}")
            print(f"Response: {response.text[:500]}")
            return False

    except Exception as e:
        print(f"\n❌ Exception: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_token_transfers():
    """Test fetching token transfers"""
    api_key = os.getenv('ETHERSCAN_API_KEY')
    wallet_address = "0x3b2A51FEC517BBc7fEaf68AcFdb068b57870713F"

    print(f"\n\n🪙 Testing Token Transfers...")

    base_url = "https://api.etherscan.io/api"
    params = {
        'module': 'account',
        'action': 'tokentx',
        'address': wallet_address,
        'startblock': 0,
        'endblock': 99999999,
        'page': 1,
        'offset': 5,  # Just get 5 for testing
        'sort': 'desc',
        'apikey': api_key
    }

    try:
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            data = response.json()
            if data.get('status') == '1':
                transfers = data.get('result', [])
                print(f"✅ Found {len(transfers)} token transfers")

                if transfers:
                    tx = transfers[0]
                    print(f"\nMost recent token transfer:")
                    print(f"  - Token: {tx.get('tokenSymbol', 'N/A')} ({tx.get('tokenName', 'N/A')})")
                    print(f"  - From: {tx.get('from', 'N/A')[:10]}...")
                    print(f"  - To: {tx.get('to', 'N/A')[:10]}...")

                    value = int(tx.get('value', 0))
                    decimals = int(tx.get('tokenDecimal', 18))
                    if decimals > 0:
                        amount = value / (10 ** decimals)
                        print(f"  - Amount: {amount:.6f} {tx.get('tokenSymbol', '')}")
            else:
                print(f"❌ Token API Error: {data.get('message', 'Unknown')}")
        else:
            print(f"❌ Token HTTP Error: {response.status_code}")

    except Exception as e:
        print(f"❌ Token Exception: {e}")


if __name__ == "__main__":
    print("=" * 60)
    print("ETHERSCAN API TEST")
    print("=" * 60)

    # Test regular transactions
    success = test_etherscan_api()

    # Test token transfers
    test_token_transfers()

    print("\n" + "=" * 60)
    if success:
        print("✅ API TEST PASSED - Etherscan API is working correctly")
        print("If you're still seeing issues in the app, check:")
        print("1. The app is loading the .env file correctly")
        print("2. No typos in the wallet address")
        print("3. Rate limiting (wait a few seconds between requests)")
    else:
        print("❌ API TEST FAILED - Please check the errors above")
        print("Common issues:")
        print("1. Invalid API key - get one at https://etherscan.io/apis")
        print("2. API key not in .env file")
        print("3. Network issues")

    print("=" * 60)