# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

**Running the Application:**
```bash
python app.py
# or
shiny run app.py
```

**Running Tests:**
```bash
python -m pytest tests/
# Run specific test file
python -m pytest tests/test_calculations.py
python -m pytest tests/test_s3_storage.py
```

**Installing Dependencies:**
```bash
pip install -r requirements.txt
```

## Application Architecture

**RealWorldNAV** is a financial dashboard application built with Python Shiny for fund accounting and investment management. The application follows a modular architecture with clear separation of concerns.

### Core Structure

- **Entry Point**: `app.py` - Simple Shiny app instantiation
- **Main Application**: `main_app/` contains the core application logic
  - `ui.py` - Main UI layout with navbar navigation
  - `server.py` - Central server logic that registers module outputs
  - `state.py` - Global reactive state management
  - `s3_utils.py` - AWS S3 integration for data storage and retrieval

### Module Architecture

The application is organized into functional modules under `main_app/modules/`:

1. **Fund Accounting** (`fund_accounting/`) - NAV calculations, performance metrics
2. **Financial Reporting** (`financial_reporting/`) - Balance sheets, income statements
3. **General Ledger** (`general_ledger/`) - Journal entries, chart of accounts
4. **Investments** (`investments/`) - Loan portfolio, NFT collateral management

Each module follows a consistent pattern:
- `ui.py` - Module-specific UI components
- Main module file - Business logic and data processing
- Additional helper files as needed

### Data Layer

**S3 Integration**: All data is stored and retrieved from AWS S3
- Master trial balance files (CSV)
- General ledger data (Parquet format)
- Chart of accounts (CSV)
- Wallet mappings (Excel)

**Key S3 Files**:
- `BUCKET_NAME = "realworldnav-beta"`
- Master TB: `drip_capital/fund/holdings_class_B_ETH/master_tb/...`
- GL data: `drip_capital/all_posted_journal_entries.parquet`
- COA: `drip_capital/drip_capital_COA.csv`

### Services Layer

`main_app/services/` contains shared business logic:
- `calculations.py` - Financial calculations (currently minimal)
- `file_parser.py` - Data parsing utilities
- `s3_storage.py` - S3 operations wrapper

### UI Patterns

**Universal Header**: Fund and date selectors available across all modules
**Navigation**: Top-level navbar with module panels
**Theme**: Uses Bootstrap "simplex" theme
**Components**: Value boxes, cards, data tables, and Plotly charts

### State Management

The application uses Shiny's reactive system:
- Global state in `state.py`
- Module-specific reactive calculations
- Server-side registration pattern for outputs

### Data Flow

1. S3 data loading with LRU caching (`@lru_cache`)
2. Reactive calculations based on user selections
3. Dynamic UI updates through Shiny's render system
4. Audit logging for GL modifications

### Development Notes

- The application is designed for real-time financial data analysis
- Uses pandas extensively for data manipulation
- Plotly for interactive visualizations
- Error handling includes debug print statements throughout
- All modules register their outputs in the main server function

## GL-Based PCAP Implementation Plan

### Overview
Implement a comprehensive GL-based Partner Capital Account Projections (PCAP) system that generates detailed line-item breakdowns for each Limited Partner, showing capital flows, P&L allocations, and performance metrics calculated directly from General Ledger transactions.

### Target Output Format

For each LP, display:

```
👤 LP: LP_00001_fund_i_class_B_ETH
------------------------------------------------------------
             Line_Item                                    SCPC Category        Current_Month      Current_Quarter         Current_Year                  ITD
     Beginning Capital                       Beginning Balance  Capital  -0.0000000000000000  -0.0000000000000000  -0.0000000000000000   0.0000000000000000
 Capital Contributions                   Capital contributions  Capital 427.7683160000000271 427.7683160000000271 427.7683160000000271 427.7683160000000271
 Capital Distributions                   Capital distributions  Capital  -0.0000000000000000  -0.0000000000000000  -0.0000000000000000  -0.0000000000000000
    Operating expenses                      Operating expenses  Expense  -0.3345340000000000  -0.3345340000000000  -0.3345340000000000  -0.3345340000000000
       Interest income                         Interest income   Income   0.0210160000000000   0.0210160000000000   0.0210160000000000   0.0210160000000000
Provision for bad debt                  Provision for bad debt  Expense  -0.0031520000000000  -0.0031520000000000  -0.0031520000000000  -0.0031520000000000
       Management Fees                         Management fees     Fees  -0.1379900000000000  -0.1379900000000000  -0.1379900000000000  -0.1379900000000000
     GP Incentive Fees Incentive allocation to General Partner     Fees  -0.0000000000000000  -0.0000000000000000  -0.0000000000000000  -0.0000000000000000
        Ending Capital                          Ending Balance  Capital 427.3136559999999804 427.3136559999999804 427.3136559999999804 427.3136559999999804

Commitment summary
Total commitments 427.7683
Capital called 427.7683
Remaining commitments -

Performance metrics
Net IRR -
Gross MOIC 1.064400
NAV per unit -
```

### Phase 1: Core GL Processing Engine

#### 1.1 Create New GL-Based PCAP Module
**File**: `main_app/modules/fund_accounting/PCAP/pcap_gl_detailed.py`

**Key Functions**:
- `normalize_to_eod_utc(series)` - Convert dates to 23:59:59 UTC for consistent matching
- `process_gl_for_pcap_detailed(gl_df, coa_df, start_date, end_date, selected_fund)` - Main processing function
- `generate_lp_detailed_breakdown(gl_data, lp_id, as_of_date)` - Generate individual LP breakdown
- `calculate_period_aggregations(gl_data, lp_id, as_of_date)` - Calculate Current_Month, Current_Quarter, Current_Year, ITD
- `generate_commitment_summary(gl_data, lp_id)` - Calculate commitment metrics
- `calculate_performance_metrics(gl_data, lp_id, as_of_date)` - Calculate IRR, MOIC, NAV per unit

#### 1.2 Line Item Processing Logic

**GL Account Mapping to Line Items**:
- **Beginning Capital**: Calculate from prior period ending balances
- **Capital Contributions**: Sum all `capital_contributions_property` transactions
- **Capital Distributions**: Sum all `capital_distributions_property` transactions  
- **Operating expenses**: Sum all expense accounts (GL_Acct_Number >= 40000) excluding mgmt fees and provisions
- **Interest income**: Sum all income accounts with "interest" classification in COA
- **Provision for bad debt**: Sum accounts with "provision" or "bad debt" in account name
- **Management Fees**: Sum all `management_fee_expense` transactions
- **GP Incentive Fees**: Sum all `capital_incentive_allocation_GP_property` transactions
- **Ending Capital**: Beginning + Contributions - Distributions + Net P&L

**Period Calculations**:
- **Current_Month**: Transactions within the selected month
- **Current_Quarter**: Q1 (Jan-Mar), Q2 (Apr-Jun), Q3 (Jul-Sep), Q4 (Oct-Dec) containing as_of_date
- **Current_Year**: January 1st through December 31st of as_of_date year
- **ITD**: All transactions from fund inception through as_of_date

#### 1.3 Data Integration Points

**Required Data Sources**:
- GL transactions from S3: `drip_capital/all_posted_journal_entries.parquet`
- Chart of Accounts from S3: `drip_capital/drip_capital_COA.csv`
- GP Incentive Audit Trail: `drip_capital/20240731_fund_i_class_B_ETH_GP_incentive_audit_trail.xlsx`

**Integration with Existing Functions**:
- Use existing `load_GL_file()` from `s3_utils.py`
- Use existing `load_COA_file()` from `s3_utils.py` 
- Use existing `get_lp_net_irr_from_audit()` for Net IRR calculation

### Phase 2: Enhanced PCAP UI Components

#### 2.1 Update PCAP User Interface
**File**: Enhance existing PCAP UI module

**New UI Elements**:
- **As-of Date Picker**: Single date selection for period calculations
- **LP Multi-Select**: Choose specific LPs or "All LPs"
- **Fund Selector**: Integrate with existing fund selection
- **Generate Detailed PCAP Button**: Trigger GL-based calculations
- **View Toggle**: Switch between summary and detailed line-item view

#### 2.2 Results Display Table
**Interactive DataGrid Features**:
- **LP Grouping**: Expandable/collapsible sections per LP
- **Frozen Columns**: Line_Item and SCPC Category columns frozen for horizontal scrolling
- **High Precision Display**: Show full decimal precision as in example
- **Color Coding**: Different colors for Capital, Expense, Income, Fees categories
- **Export Buttons**: CSV and PDF export options

**Table Structure**:
```
LP Section Header: 👤 LP: [LP_ID]
------------------------------------------------------------
Line_Item | SCPC Category | Current_Month | Current_Quarter | Current_Year | ITD
[9 line items as shown in example]

Commitment summary
[3 commitment metrics]

Performance metrics  
[3 performance metrics]
```

### Phase 3: Commitment and Performance Calculations

#### 3.1 Commitment Summary Logic
- **Total commitments**: Sum of all committed capital for the LP (from commitment data or maximum contributions)
- **Capital called**: Sum of actual capital contributions to date
- **Remaining commitments**: Total commitments - Capital called

#### 3.2 Performance Metrics Calculations  
- **Net IRR**: Extract from GP incentive audit trail using existing `get_lp_net_irr_from_audit()`
- **Gross MOIC**: (Current NAV + Total Distributions) / Total Contributions
- **NAV per unit**: Current NAV / Number of units held (calculate from contribution/unit data)

#### 3.3 Data Sources for Metrics
- **Commitment Data**: Extract from GL patterns or separate commitment file
- **NAV Calculations**: Use ending capital balance as current NAV
- **Unit Calculations**: Derive from contribution amounts and unit prices

### Phase 4: Export and Reporting

#### 4.1 PDF Export Enhancement
**Professional PDF Layout**:
- **Header**: Fund name, as-of date, generation timestamp
- **LP Sections**: Each LP on separate page or clearly separated
- **Table Formatting**: Professional styling with proper decimal alignment
- **Summary Pages**: Overall fund totals and statistics

#### 4.2 CSV Export Structure
**Flat File Format**:
```
LP_ID, Line_Item, SCPC_Category, Current_Month, Current_Quarter, Current_Year, ITD
LP_00001, Beginning Capital, Beginning Balance Capital, 0.0000, 0.0000, 0.0000, 0.0000
LP_00001, Capital Contributions, Capital contributions Capital, 427.7683, 427.7683, 427.7683, 427.7683
...
```

**Additional CSV Sheets**:
- Commitment Summary data
- Performance Metrics data
- Metadata (generation date, fund, date range)

### Phase 5: Integration Architecture

#### 5.1 File Structure
**New Files**:
- `main_app/modules/fund_accounting/PCAP/pcap_gl_detailed.py` - Core GL processing
- `main_app/modules/fund_accounting/PCAP/pcap_ui_detailed.py` - Enhanced UI components

**Modified Files**:
- `main_app/modules/fund_accounting/PCAP/__init__.py` - Export new functions
- `main_app/modules/fund_accounting/PCAP/pcap.py` - Integrate GL detailed functions
- `main_app/server.py` - Register new PCAP outputs (if needed)

#### 5.2 Function Integration Plan
**New Functions to Add to `__init__.py`**:
- `process_gl_for_pcap_detailed`
- `generate_lp_detailed_breakdown` 
- `calculate_period_aggregations`
- `generate_commitment_summary`
- `calculate_performance_metrics`

#### 5.3 Error Handling Strategy
- **Missing GL Data**: Graceful degradation with clear error messages
- **Missing COA Mappings**: Default categorization with warnings
- **Missing Audit Trail**: Show "Net IRR not available" 
- **Data Inconsistencies**: Validation checks with detailed error reporting

### Phase 6: Technical Implementation Details

#### 6.1 Data Processing Optimizations
- **Efficient GL Filtering**: Pre-filter by date range and fund before processing
- **Cached COA Lookups**: Build account mapping dictionary once
- **Vectorized Calculations**: Use pandas groupby operations for aggregations
- **Memory Management**: Process LPs in batches for large datasets

#### 6.2 Decimal Precision Handling
- **High Precision**: Use Decimal type for all financial calculations
- **Display Format**: Match exact format from example (16 decimal places)
- **Rounding Strategy**: No rounding during calculations, format only for display

#### 6.3 Date Handling Strategy
- **Timezone Consistency**: All dates normalized to UTC 23:59:59 format
- **Period Boundaries**: Clear logic for month/quarter/year boundaries
- **ITD Calculations**: Handle fund inception date properly

### Phase 7: Testing and Validation

#### 7.1 Test Data Validation
- **Balance Verification**: Ensure Beginning + Activity = Ending for each LP
- **Period Consistency**: Verify ITD >= Current_Year >= Current_Quarter >= Current_Month
- **Cross-Validation**: Compare totals across different aggregation methods

#### 7.2 Performance Testing
- **Large Dataset Handling**: Test with multiple years of GL data
- **Memory Usage**: Monitor RAM usage during processing
- **Response Time**: Ensure UI remains responsive during calculations

#### 7.3 User Acceptance Testing
- **UI Usability**: Ensure table navigation is intuitive
- **Export Functionality**: Verify PDF and CSV exports work correctly
- **Error Scenarios**: Test graceful handling of missing/invalid data

### Implementation Priority Order

1. **Phase 1**: Core GL processing engine - Foundation for all calculations
2. **Phase 3**: Commitment and performance calculations - Core business logic
3. **Phase 2**: UI components - User interface for interaction
4. **Phase 4**: Export functionality - Output generation
5. **Phase 5**: Integration - System integration and testing
6. **Phase 6**: Optimization - Performance and precision improvements
7. **Phase 7**: Testing - Comprehensive validation

### Success Criteria

✅ **Functional Requirements**:
- Exact line item breakdown matching provided example format
- Accurate period aggregations (Current_Month, Current_Quarter, Current_Year, ITD)
- Proper SCPC categorization from COA mapping
- Commitment summary calculations
- Performance metrics integration
- Interactive table display with export capabilities

✅ **Technical Requirements**:
- High decimal precision (16 decimal places)
- Efficient processing of large GL datasets
- Seamless integration with existing S3 data sources
- Professional PDF and CSV export functionality
- Robust error handling and user feedback

✅ **User Experience Requirements**:
- Intuitive date and LP selection
- Clear data display with expandable LP sections
- Fast response times for typical datasets
- Clear error messages for data issues
- Consistent styling with existing application