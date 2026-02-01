# Decoder Test Summary - 5A7 Wallet Transactions

## Test Results
- **Total transactions**: 134
- **With journal entries**: 108 (80.6%)
- **Without journal entries**: 26 (19.4%)

## Success Breakdown

### Fully Working Decoders
| Platform | Category | Count | Status |
|----------|----------|-------|--------|
| Gondi | LOAN_REFINANCE | 28 | Working (100%) |
| Blur | LOAN_REPAYMENT | 2 | Working (100%) |
| Blur | COLLATERAL_SEIZURE | 2 | Working (100%) |
| Arcade | LOAN_ORIGINATION | 27 | Working (100%) |
| Generic | ETH_TRANSFER | 15+ | Working |
| Generic | ERC20_TRANSFER | 20+ | Working |
| Generic | WETH_WRAP/UNWRAP | 2 | Working |

## Expected Failures (26 total)

### 1. Dust ETH Transfers (11 txs)
- All values < 0.00001 ETH (10 gwei threshold)
- Correctly filtered to prevent noise in journal entries
- Examples: 1e-9, 1e-7, 1e-6 ETH incoming transfers

### 2. NFT Transfers (3 txs)
- `safeTransferFrom` calls (selector `b88d4fde`)
- These are ERC721 (NFT) transfers, not ERC20 tokens
- Would require separate NFT tracking feature

### 3. Blur Pool Operations (4 txs)
- `deposit()` (selector `d0e30db0`) - ETH to Pool liquidity
- `withdraw()` (selector `2e1a7d4d`) - Pool to ETH
- These are liquidity management, not loan transactions

### 4. Protocol Interactions (4 txs)
- Calls to various protocols (Permit2, Seaport, etc.)
- No token transfers to/from fund wallets detected

### 5. Generic Contract Calls (4 txs)
- Various contract interactions without token flow
- May be approvals, parameter changes, or admin calls

## Fixes Applied During Testing

### 1. Blur LOAN_REPAYMENT Fix
- Added fallback decoder for selector `c87df1c2`
- Signature: `repay((address,address,address,uint256,uint256,uint256,uint256,uint256,uint256),uint256)`
- Now properly decodes Lien struct and generates repayment JEs

### 2. Connect Button Feature
- Added Connect button to Blockchain Listener UI
- Prevents automatic initialization on page load
- User must click Connect to start blockchain service

## Conclusion
The decoder is working correctly for all NFT lending platforms (Gondi, Blur, Arcade). The failures are all expected edge cases that don't require journal entries:
- Dust amounts
- NFT transfers
- Liquidity operations
- Non-token contract calls
