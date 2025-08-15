# FIFO Crypto Investment Tracker

A comprehensive cryptocurrency investment tracking system with FIFO (First-In-First-Out) cost basis calculations, blockchain integration, and advanced analytics.

## Core Features

### **FIFO Cost Basis Tracking**
- **First-In-First-Out** methodology for tax compliance
- **Automated lot tracking** with purchase dates and cost basis
- **Realized gains calculation** with holding period analysis
- **Short vs long-term** capital gains classification (>365 days)
- **Multi-currency support** (ETH, BTC, USDC, USDT, etc.)

### **Blockchain Integration**
- **Multiple API providers**: Etherscan, Moralis, Alchemy, Infura
- **Automatic transaction import** from wallet addresses
- **Real-time price feeds** for accurate valuation
- **Gas fee tracking** and cost basis inclusion
- **Transfer matching** between wallets

### **Advanced Analytics**
- **Current holdings** with unrealized gains/losses
- **Performance metrics** and portfolio analysis
- **Transaction history** with advanced filtering
- **Tax reporting** with period-specific calculations
- **Portfolio diversification** insights

### **Multi-Fund Architecture**
- **Fund-specific tracking**: Fund I, Fund II, Holdings
- **Wallet segregation** per fund
- **Cross-fund analytics** and consolidated reporting
- **Partner capital allocation** integration

## Technical Architecture

### Core Classes

#### `CryptoTransaction`
```python
@dataclass
class CryptoTransaction:
    tx_hash: str
    date: datetime
    fund_id: str
    wallet_id: str
    cryptocurrency: str
    transaction_type: str  # buy, sell, transfer_in, transfer_out, mining, staking
    quantity: Decimal
    price_per_unit: Decimal
    total_value_usd: Decimal
    gas_fee_usd: Decimal
    # Additional blockchain metadata
```

#### `FIFOLot`
```python
@dataclass
class FIFOLot:
    lot_id: str
    purchase_date: datetime
    cryptocurrency: str
    quantity_remaining: Decimal
    cost_basis_per_unit: Decimal
    total_cost_basis: Decimal
    fund_id: str
    wallet_id: str
    source_tx_hash: str
```

#### `RealizedGain`
```python
@dataclass
class RealizedGain:
    sale_date: datetime
    cryptocurrency: str
    quantity_sold: Decimal
    sale_price_per_unit: Decimal
    cost_basis_per_unit: Decimal
    realized_gain_loss: Decimal
    holding_period_days: int
    is_long_term: bool
    # Fund and transaction metadata
```

### FIFO Engine

#### `CryptoFIFOTracker`
The main tracking engine that:
- **Maintains lot inventory** by cryptocurrency
- **Processes transactions** in chronological order
- **Calculates realized gains** using FIFO methodology
- **Tracks unrealized positions** with current market values
- **Provides filtering** by fund, wallet, date ranges

## User Interface

### **Main Tabs**

#### 1. **Current Holdings**
- Real-time portfolio view with cost basis vs market value
- Holdings by currency pie chart
- Unrealized gains/losses analysis
- Export capabilities (CSV, PDF)

#### 2. **Transaction History**
- Comprehensive transaction log with advanced filters
- Transaction type filtering (buy, sell, transfer, mining, staking)
- Search by transaction hash
- Amount-based filtering

#### 3. **Realized Gains**
- FIFO-based realized gains and losses
- Tax period selection (YTD, 2024, 2023, custom)
- Short vs long-term classification
- Gains by currency breakdown
- Tax report generation

#### 4. **Blockchain Integration**
- Wallet address management
- API provider configuration
- Manual CSV upload with templates
- Connection testing and status

#### 5. **FIFO Settings**
- Cost basis method selection (FIFO, LIFO, Average Cost, Specific ID)
- Tax jurisdiction settings (US, UK, Canada, Australia)
- Gas fee inclusion options
- Data backup and restore

### **Control Panel**
- **Fund Selection**: Filter by specific funds or view all
- **Wallet Selection**: Focus on specific wallets
- **Currency Filter**: Analyze specific cryptocurrencies
- **Date Range**: Historical analysis and period reporting
- **Blockchain Sync**: Load latest transactions
- **FIFO Refresh**: Recalculate all positions

### **Summary Cards**
- **Total Holdings Value**: Current market value with trend
- **Unrealized Gains**: Total unrealized P&L with percentage
- **Realized Gains (YTD)**: Tax year realized gains
- **Total Transactions**: Number of processed transactions

## Implementation Plan

### Phase 1: Core FIFO Engine (Complete)
- [x] Transaction and lot data structures
- [x] FIFO calculation engine
- [x] Realized gains computation
- [x] Basic filtering and querying

### Phase 2: UI Framework (Complete)
- [x] Tab-based navigation
- [x] Summary cards and metrics
- [x] Data tables with filtering
- [x] Chart placeholders

### Phase 3: Blockchain Integration (In Progress)
- [ ] API provider abstraction layer
- [ ] Transaction fetching and parsing
- [ ] Price feed integration
- [ ] Wallet address management

### Phase 4: Advanced Analytics (In Progress)
- [ ] Interactive charts (Plotly)
- [ ] Performance metrics calculation
- [ ] Portfolio analysis tools
- [ ] Risk assessment features

### Phase 5: Tax Reporting (In Progress)
- [ ] Form 8949 generation
- [ ] Schedule D preparation
- [ ] International tax support
- [ ] Audit trail documentation

### Phase 6: Data Management (In Progress)
- [ ] S3 integration for persistence
- [ ] Backup and restore functionality
- [ ] Data validation and integrity checks
- [ ] Performance optimization

## Integration Points

### **General Ledger Integration**
- Syncs with existing GL transactions
- Maps crypto transactions to chart of accounts
- Maintains audit trail consistency
- Supports GL-based reconciliation

### **Fund Accounting Integration**
- Connects with PCAP reporting
- Supports partner capital calculations
- Integrates with NAV calculations
- Enables cross-module analytics

### **S3 Data Layer**
- Leverages existing S3 utilities
- Stores FIFO lots and calculations
- Maintains transaction history
- Supports data backup/restore

## Future Enhancements

### **Advanced Features**
- **DeFi Integration**: Support for DEX transactions, liquidity pools, yield farming
- **NFT Tracking**: Non-fungible token cost basis and sales
- **Staking Rewards**: Automated staking income calculation
- **Cross-Chain Support**: Multi-blockchain transaction tracking

### **Enterprise Features**
- **Multi-Entity Support**: Corporate structures and subsidiaries
- **Compliance Automation**: Automatic regulatory reporting
- **API Integration**: External system connectivity
- **Advanced Security**: Encryption and access controls

### **Analytics & Reporting**
- **Performance Attribution**: Source of returns analysis
- **Risk Metrics**: VaR, correlation analysis
- **Benchmark Comparison**: Index and peer comparison
- **Custom Dashboards**: User-configurable views

## Getting Started

1. **Navigate to General Ledger** → **Crypto FIFO Tracker**
2. **Configure settings** in the FIFO Settings tab
3. **Add wallet addresses** in Blockchain Integration
4. **Load transactions** via API or CSV upload
5. **Review holdings** and realized gains
6. **Generate reports** for tax filing

The system is designed to be **intuitive yet powerful**, providing both quick insights and detailed analysis for comprehensive crypto investment tracking.