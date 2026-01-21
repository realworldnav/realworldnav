# Transaction Decoder Architecture - Complete Technical Reference

## Overview

The RealWorldNAV transaction decoding system converts raw Ethereum blockchain transactions into structured accounting journal entries for NFT lending protocols. This document provides a comprehensive explanation of how the system works, with two complete worked examples.

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        TRANSACTION DECODING PIPELINE                             │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  RAW TRANSACTION                                                                 │
│  (tx_hash)                                                                       │
│       │                                                                          │
│       ▼                                                                          │
│  ┌─────────────────────┐                                                         │
│  │   DecoderRegistry   │  ◄── Routes to correct decoder based on:               │
│  │     (registry.py)   │      - CONTRACT_ROUTING (address → platform)           │
│  └─────────────────────┘      - FUNCTION_SELECTORS (4-byte selector)            │
│       │                        - Log addresses                                   │
│       │                        - can_decode() fallback                           │
│       ▼                                                                          │
│  ┌─────────────────────┐      ┌─────────────────────┐                           │
│  │  Platform Adapter   │ ──── │  Notebook Decoder   │                           │
│  │ (decoder_adapters.py)│      │ (gondi_decoder.py) │                           │
│  └─────────────────────┘      │ (blur_decoder.py)  │                           │
│       │                        │ (nftfi_decoder.py) │                           │
│       │                        └─────────────────────┘                           │
│       ▼                                                                          │
│  ┌─────────────────────┐                                                         │
│  │   DecodedEvent[]    │  ◄── Structured event data extracted from logs         │
│  └─────────────────────┘                                                         │
│       │                                                                          │
│       ▼                                                                          │
│  ┌─────────────────────┐                                                         │
│  │ JournalEntryGenerator│  ◄── Converts events to double-entry bookkeeping      │
│  │  (per platform)      │      Only generates for FUND WALLETS                   │
│  └─────────────────────┘                                                         │
│       │                                                                          │
│       ▼                                                                          │
│  ┌─────────────────────┐                                                         │
│  │ DecodedTransaction  │  ◄── Final output with:                                │
│  │   + JournalEntry[]  │      - Platform, Category, Events                       │
│  └─────────────────────┘      - Balanced journal entries                         │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Key Files and Their Roles

### Core Files

| File | Purpose |
|------|---------|
| `main_app/services/decoders/registry.py` | **Central router** - routes transactions to correct decoder |
| `main_app/services/decoders/decoder_adapters.py` | **Adapters** - wrap notebook decoders with BaseDecoder interface |
| `main_app/services/decoders/base.py` | **Base classes** - DecodedTransaction, JournalEntry, enums |

### Platform Decoders (Notebook Style)

| File | Version | Protocol |
|------|---------|----------|
| `gondi_decoder.py` | v1.7.1 | Gondi multi-source lending |
| `blur_decoder.py` | v1.0.0 | Blur Blend callable loans |
| `nftfi_decoder.py` | v2.0.0 | NFTfi fixed-term lending |
| `arcade_decoder.py` | v2.0.0 | Arcade promissory note loans |
| `zharta_decoder.py` | v3.0.0 | Zharta peer-to-pool lending |
| `generic_decoder.py` | v1.0.0 | WETH, ERC20, ETH transfers |

## Proxy Detection

Many DeFi contracts use proxy patterns (upgradeable contracts). The registry automatically resolves proxies to their implementation before routing.

### Supported Proxy Patterns

| Pattern | Detection Method |
|---------|------------------|
| **EIP-1967** | Read implementation slot `0x360894...` |
| **EIP-1967 Beacon** | Read beacon slot `0xa3f0ad...`, call `implementation()` |
| **EIP-1167 Minimal** | Parse bytecode for embedded implementation address |
| **Custom** | Call `getImplementation()` function |

### How Proxy Resolution Works

```python
# In registry.py - route_transaction()

# 1. Direct lookup first
if to_address in CONTRACT_ROUTING:
    return CONTRACT_ROUTING[to_address]

# 2. If not found, try resolving proxy
impl_address = self._resolve_address(to_address)
if impl_address != to_address and impl_address in CONTRACT_ROUTING:
    # Route via implementation address
    return CONTRACT_ROUTING[impl_address]
```

### Caching

Proxy resolutions are cached in `self._proxy_cache` to avoid repeated RPC calls:
- Same address always returns same result
- Cache persists for registry lifetime
- Reduces RPC overhead for repeated transactions

---

## Contract Routing

The registry routes transactions based on `CONTRACT_ROUTING` dict:

```python
# registry.py - CONTRACT_ROUTING
CONTRACT_ROUTING = {
    # BLUR
    "0x29469395eaf6f95920e59f858042f0e28d98a20b": Platform.BLUR,  # Blend Proxy
    "0x0000000000a39bb272e79075ade125fd351887ac": Platform.BLUR,  # Pool

    # GONDI
    "0xf41b389e0c1950dc0b16c9498eae77131cc08a56": Platform.GONDI, # V1
    "0x59e0b87e3dcfb5d34c06c71c3fbf7f6b7d77a4ff": Platform.GONDI, # MultiSourceLoan

    # NFTFI
    "0x4bc5fa56f2931e7a37417fa55dda71e4b7c2f2a3": Platform.NFTFI, # Refinancing v2
    "0x1e0447b19bb6ecfdae1e4ae1694b0c3659614e4e": Platform.NFTFI, # DirectLoanFixedCollectionOffer
    "0xb6adec2acc851d30d5fb64f3137234bcdcbbad0d": Platform.NFTFI, # CollectionOfferLoan V3

    # etc...
}
```

### Routing Priority

1. **Direct contract match**: `to_address in CONTRACT_ROUTING`
2. **Function selector match**: First 4 bytes of input data
3. **Log address match**: Any log from known contract
4. **can_decode() fallback**: Each decoder's detection logic
5. **Default**: Generic decoder

## The Adapter Pattern

Each platform has an **Adapter** that wraps the notebook decoder:

```python
# decoder_adapters.py
class NFTfiDecoderAdapter(BaseDecoder):
    PLATFORM = Platform.NFTFI

    def __init__(self, w3: Web3, fund_wallets: List[str]):
        self.w3 = w3
        self.fund_wallets = fund_wallets
        self._notebook_decoder = None  # Lazy initialized
        self._journal_generator = None

    def can_decode(self, tx, receipt) -> bool:
        """Check if this decoder handles this transaction"""
        to_addr = tx.get('to', '').lower()
        if to_addr in NFTFI_CONTRACTS:
            return True
        # Also check log addresses
        for log in receipt.get('logs', []):
            if log.get('address', '').lower() in NFTFI_CONTRACTS:
                return True
        return False

    def decode(self, tx, receipt, block, eth_price) -> DecodedTransaction:
        """Main entry point"""
        self._load_abis()  # Lazy init

        # 1. Decode events using notebook decoder
        events = self._notebook_decoder.decode_transaction(tx_hash)

        # 2. Convert to DecodedEvent objects
        decoded_events = [DecodedEvent(...) for e in events]

        # 3. Generate journal entries
        journal_entries = self._generate_journal_entries(events)

        # 4. Return structured result
        return DecodedTransaction(
            platform=Platform.NFTFI,
            category=self._determine_category(events),
            events=decoded_events,
            journal_entries=journal_entries,
            ...
        )
```

## Journal Entry Generation

Journal entries are **only generated for fund wallets**. The generator checks:

```python
# From NFTfiJournalEntryGenerator.generate_loan_started_entries()
for _, row in loan_events.iterrows():
    lender = row.get('lender', '').lower()
    borrower = row.get('borrower', '').lower()

    is_lender_fund = lender in self.fund_wallet_list
    is_borrower_fund = borrower in self.fund_wallet_list

    if not is_lender_fund and not is_borrower_fund:
        continue  # Skip - not our transaction
```

### Standard Account Mappings

| Event Type | Lender Debits | Lender Credits |
|------------|---------------|----------------|
| LoanStarted (origination) | loan_receivable_cryptocurrency_weth | deemed_cash_usd |
| LoanRepaid | deemed_cash_usd | loan_receivable_cryptocurrency_weth |
| | interest_receivable_cryptocurrency_weth | interest_income_cryptocurrency_weth |
| LoanLiquidated (seizure) | investments_nfts_seized_collateral | loan_receivable_cryptocurrency_weth |

---

# Example 1: Gondi Loan Refinance

## Transaction

- **Hash**: `0x965d03351015c7ca5bdc0b79a5bb80cb36f13e37952fa03351fb2c1fb2a310b3`
- **Platform**: Gondi
- **Type**: Loan Refinance (LoanRefinancedFromNewOffers)

## Step-by-Step Flow

### 1. Registry Routing

```
Transaction To: 0xf41b389e0c1950dc0b16c9498eae77131cc08a56
                ↓
CONTRACT_ROUTING lookup:
  "0xf41b389e0c1950dc0b16c9498eae77131cc08a56": Platform.GONDI
                ↓
Route to GondiDecoderAdapter
```

### 2. Event Decoding

The Gondi decoder processes the transaction receipt logs:

```python
# GondiEventDecoder.decode_transaction(tx_hash)
receipt = self.w3.eth.get_transaction_receipt(tx_hash)

for log in receipt['logs']:
    # Try each known event type
    for event_name in ['LoanEmitted', 'LoanRepaid', 'LoanRefinancedFromNewOffers', ...]:
        try:
            decoded = contract.events[event_name]().process_log(log)
            # Success - create DecodedGondiEvent
            return DecodedGondiEvent(
                event_type='LoanRefinancedFromNewOffers',
                loan_id=7507,
                old_loan_id=3862,
                fee=6023562834224598,  # 0.006024 ETH
                ...
            )
        except:
            continue
```

### 3. Decoded Event Output

```
[Event] LoanRefinancedFromNewOffers
  loan_id: 7507
  old_loan_id: 3862
  new_loan_id: 7507
  fee: 0.006024 ETH
  transfer_outflows: {'0xf9b64dc47dbe8c75f6ffc573cbc7599404bfe5a7': 994879971590909092}
  fund_tranches: [{'loanId': 7507, 'principalAmount': 1000000000000000000}]
  contract_address: 0xf41b389e0c1950dc0b16c9498eae77131cc08a56
```

### 4. Journal Entry Generation

The fund wallet `0xf9b64dc47dbe8c75f6ffc573cbc7599404bfe5a7` is the lender.

```
Entry 1: Refinance Origination
  DR loan_receivable_cryptocurrency_weth: 1.000000 WETH  (new loan out)
  CR deemed_cash_usd:                     0.993976 WETH  (net cash sent)
  CR interest_income_cryptocurrency_weth: 0.006024 WETH  (fee received)

Entry 2: Interest Accruals (daily buckets)
  DR interest_receivable_cryptocurrency_weth: 0.000349 WETH
  CR interest_income_cryptocurrency_weth:     0.000349 WETH
  (repeated for each day of loan term)
```

### 5. Final Output

```
Status:     success
Platform:   gondi
Category:   LOAN_REFINANCE
Function:   LoanRefinancedFromNewOffers

Journal Entries: 2
  Total Debits:  1.015719 ETH
  Total Credits: 1.015719 ETH
  Balanced:      YES
```

---

# Example 2: NFTfi Refinancing via Aggregator

## Transaction

- **Hash**: `0xb99d958c27741bbcd753ceb7415398ce6b0ce1570f7ba61fec8b8c9a7f46443c`
- **Platform**: NFTfi
- **Type**: Loan Refinance (via NFTfi Refinancing contract)

## Context

This transaction uses NFTfi's **Refinancing contract** (`0x4BC5Fa56f2931E7A37417FA55Dda71E4b7c2f2a3`) to:
1. Repay an existing loan (loan 10994)
2. Originate a new loan (loan 12230) with a different lender

## Step-by-Step Flow

### 1. Registry Routing

```
Transaction To: 0x4BC5Fa56f2931E7A37417FA55Dda71E4b7c2f2a3
                ↓
CONTRACT_ROUTING lookup:
  "0x4bc5fa56f2931e7a37417fa55dda71e4b7c2f2a3": Platform.NFTFI
                ↓
Route to NFTfiDecoderAdapter
```

### 2. Event Decoding

The NFTfi decoder finds THREE events in the transaction:

```python
# Event 1: LoanRepaid (old loan being paid off)
DecodedNFTfiEvent(
    event='LoanRepaid',
    loan_id=10994,
    lender='0xe8075d7b965e8ba4938ed158de944e1e02a21d30',  # Old lender (NOT fund)
    borrower='0x4bc5fa56f2931e7a37417fa55dda71e4b7c2f2a3',  # Refinancing contract
    loanPrincipalAmount=400000000000000000,  # 0.40 ETH
    amountPaidToLender=406018166666666666,   # 0.406 ETH (includes interest)
)

# Event 2: LoanStarted (new loan being originated)
DecodedNFTfiEvent(
    event='LoanStarted',
    loan_id=12230,
    lender='0xf9b64dc47dbe8c75f6ffc573cbc7599404bfe5a7',  # FUND WALLET!
    borrower='0x4bc5fa56f2931e7a37417fa55dda71e4b7c2f2a3',
    loanPrincipalAmount=350000000000000000,  # 0.35 ETH
    maximumRepaymentAmount=354027000000000000,  # 0.354 ETH
)

# Event 3: Refinanced (links old → new)
DecodedNFTfiEvent(
    event='Refinanced',
    oldLoanContract='0xb6adec2acc851d30d5fb64f3137234bcdcbbad0d',
    oldLoanId=10994,
    newLoanId=12230,
)
```

### 3. Fund Wallet Detection

The journal generator checks each event:

| Event | Lender | Borrower | Fund Role |
|-------|--------|----------|-----------|
| LoanRepaid | 0xe8075d7... | contract | NOT FUND - skip |
| LoanStarted | 0xf9b64dc4... | contract | **FUND IS LENDER** |
| Refinanced | - | - | Linkage only |

Only `LoanStarted` generates journal entries because the fund wallet is the lender.

### 4. Journal Entry Generation

```python
# NFTfiJournalEntryGenerator.generate_loan_started_entries()

# Fund is lender - generate lender perspective entries
journal_rows.append({
    'account_name': 'loan_receivable_cryptocurrency_weth',
    'debit': Decimal('0.35'),
    'credit': Decimal('0'),
})
journal_rows.append({
    'account_name': 'deemed_cash_usd',
    'debit': Decimal('0'),
    'credit': Decimal('0.35'),
})
```

### 5. Final Output

```
Status:     success
Platform:   nftfi
Category:   LOAN_ORIGINATION
Function:   LoanRepaid  (first event)

Decoded Events: 3
  1. LoanRepaid (loan 10994) - old loan paid off
  2. LoanStarted (loan 12230) - fund originates new loan
  3. Refinanced - linkage event

Journal Entries: 1
  [Entry 1] NFTfi LoanStarted - Loan #12230
    DR loan_receivable_cryptocurrency_weth: 0.350000 WETH
    CR deemed_cash_usd:                     0.350000 WETH

  Total Debits:  0.350000 ETH
  Total Credits: 0.350000 ETH
  Balanced:      YES
```

### Before vs After Comparison

**BEFORE** (Generic Decoder - WRONG):
```
Platform: generic
Category: ERC20_TRANSFER

Journal Entry:
  DR 600.30 - Other Expense:  0.35 WETH  ← WRONG! Not an expense
  CR 100.31 - WETH Wallet:    0.35 WETH
```

**AFTER** (NFTfi Decoder - CORRECT):
```
Platform: nftfi
Category: LOAN_ORIGINATION

Journal Entry:
  DR loan_receivable_cryptocurrency_weth: 0.35 WETH  ← Correct! It's an asset
  CR deemed_cash_usd:                     0.35 WETH
```

---

## Testing Transactions

Use `explore_tx.py` to test any transaction:

```bash
python explore_tx.py 0xb99d958c27741bbcd753ceb7415398ce6b0ce1570f7ba61fec8b8c9a7f46443c
```

Output shows:
- Transaction metadata (block, gas, timestamp)
- Decoded events with all fields
- Generated journal entries
- Balance verification

## Adding New Contracts

To add support for a new contract:

1. **Identify the contract** - use Etherscan to find contract name and protocol
2. **Add to CONTRACT_ROUTING** in `registry.py`:
   ```python
   "0xnewcontractaddress": Platform.NFTFI,
   ```
3. **Add to adapter's CONTRACTS dict** in `decoder_adapters.py`:
   ```python
   NFTFI_CONTRACTS = {
       ...
       "0xnewcontractaddress": "ContractName",
   }
   ```
4. **Test with a real transaction** to verify routing and decoding

## Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Transaction routes to generic | Contract not in CONTRACT_ROUTING | Add contract address to routing |
| No journal entries generated | Fund wallet not party to transaction | Expected behavior - only fund txs recorded |
| Events decode but empty fields | ABI missing or wrong version | Check S3 ABI or add manual decoding |
| "Adapter not initialized" error | Missing w3 or wrong constructor args | Check adapter `_load_abis()` method |
