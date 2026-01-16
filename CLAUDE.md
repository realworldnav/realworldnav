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

### Transaction Decoders

**See `DECODER_WORKFLOW.md` for full documentation.**

The `main_app/services/decoders/` module handles blockchain transaction decoding for NFT lending platforms:

```
decoders/
├── base.py              # BaseDecoder, JournalEntry, DecodedTransaction
├── gondi_decoder.py     # ✅ COMPLETE - Gondi multi-source lending
├── blur_decoder.py      # ⚠️ Needs process_log() fix
├── nftfi_decoder.py     # ❌ Placeholder
├── arcade_decoder.py    # ❌ Placeholder
└── registry.py          # Routes transactions to correct decoder
```

**Development workflow:**
1. Test in `debug_decoder.py` (run: `python debug_decoder.py`)
2. Extract working code to `main_app/services/decoders/{platform}_decoder.py`
3. Add to registry

**Critical:** Use `process_log()` not `process_receipt()` for event decoding!

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

## Shiny for Python Reference

### Core UI Components for Financial Dashboards

#### Input Controls
```python
# Date range picker for financial periods
ui.input_date_range(
    "date_range",
    "Reporting Period:",
    start="2024-01-01",
    end="2024-12-31"
)

# Fund/entity selection
ui.input_select(
    "fund_selector",
    "Select Fund:",
    {"fund_a": "Fund A", "fund_b": "Fund B", "fund_c": "Fund C"}
)

# Numeric inputs for financial parameters
ui.input_numeric(
    "amount",
    "Amount:",
    value=0,
    min=0,
    step=0.01
)
```

#### Dashboard Layout Components
```python
# Navbar layout for multi-module financial app
ui.page_navbar(
    ui.nav_panel("Fund Accounting", fund_accounting_ui()),
    ui.nav_panel("General Ledger", general_ledger_ui()),
    ui.nav_panel("Investments", investments_ui()),
    ui.nav_panel("Financial Reporting", reporting_ui()),
    title="RealWorldNAV"
)

# Column layout for KPI dashboard
ui.layout_columns(
    ui.value_box("Total NAV", "$1.2M", "Up 5.2% vs last month"),
    ui.value_box("Total Assets", "$2.1M", "Portfolio value"),
    ui.value_box("Cash Position", "$150K", "Available liquidity"),
    col_widths=[4, 4, 4]
)

# Card-based organization
ui.card(
    ui.card_header("Portfolio Performance"),
    ui.output_plot("performance_chart"),
    ui.card_footer("Data as of latest NAV calculation")
)
```

#### Financial Data Display
```python
# Interactive data tables for financial data
ui.output_data_frame("financial_table")

# Value boxes for KPIs
ui.value_box(
    title="Net Asset Value",
    value="$1,234,567.89",
    showcase="Up 12.5% YTD",
    theme="primary"
)
```

### Server-Side Patterns

#### Reactive Calculations
```python
# Reactive calculation for derived financial metrics
@reactive.calc
def portfolio_nav():
    fund = input.fund_selector()
    date_range = input.date_range()
    return calculate_nav(fund, date_range[0], date_range[1])

@reactive.calc
def filtered_transactions():
    nav_data = portfolio_nav()
    return nav_data.filter_transactions()
```

#### Render Functions
```python
# Financial data table
@render.data_frame
def financial_data():
    df = filtered_transactions()
    return render.DataGrid(
        df,
        filters=True,
        selection_mode="rows",
        width="100%",
        height="400px"
    )

# Charts for financial visualization
@render.plot
def performance_chart():
    data = portfolio_nav()
    return create_plotly_chart(data)

# Dynamic text outputs
@render.text
def current_nav():
    nav = portfolio_nav()
    return f"Current NAV: ${nav.total:,.2f}"
```

#### Event Handling
```python
# Button-triggered calculations
@reactive.event(input.calculate_btn)
@reactive.calc
def updated_calculations():
    # Expensive calculation only runs when button is clicked
    return perform_nav_calculation()

# File upload processing
@reactive.calc
def uploaded_data():
    file_info = input.file_upload()
    if file_info is None:
        return None
    return pd.read_csv(file_info[0]["datapath"])
```

### Module Patterns for Financial Applications

#### Module Structure
```python
# module_ui.py
@module.ui
def fund_performance_ui():
    return ui.card(
        ui.card_header("Fund Performance"),
        ui.input_select("metric", "Metric", ["NAV", "Returns", "Volatility"]),
        ui.output_plot("chart"),
        ui.output_data_frame("data_table")
    )

# module_server.py  
@module.server
def fund_performance_server(input, output, session, fund_data):
    @reactive.calc
    def filtered_data():
        return fund_data().filter(metric=input.metric())
    
    @render.plot
    def chart():
        return create_chart(filtered_data())
        
    @render.data_frame
    def data_table():
        return render.DataGrid(filtered_data())
```

#### Module Usage in Main App
```python
# In main server function
def server(input, output, session):
    # Load shared data
    fund_data = reactive.calc(lambda: load_fund_data())
    
    # Register modules
    fund_performance_server("module1", fund_data=fund_data)
    fund_performance_server("module2", fund_data=fund_data)
```

### Financial Dashboard Best Practices

#### Data Loading and Caching
```python
# S3 data loading with caching
@reactive.calc
def master_trial_balance():
    fund = input.selected_fund()
    date = input.as_of_date()
    return load_cached_s3_data(f"master_tb/{fund}_{date}.csv")

# File reader for real-time updates
@reactive.file_reader("path/to/live_data.csv")
def live_market_data():
    return pd.read_csv("path/to/live_data.csv")
```

#### Error Handling
```python
from shiny import req

@reactive.calc
def safe_calculation():
    data = input.data_source()
    req(data is not None, "Please upload data file")
    req(len(data) > 0, "Data file is empty")
    return perform_calculation(data)
```

#### State Management
```python
# Reactive values for application state
selected_fund = reactive.value("default_fund")
calculation_status = reactive.value("idle")

# Update state based on user actions
@reactive.event(input.fund_selector)
def update_fund():
    selected_fund.set(input.fund_selector())
    calculation_status.set("calculating")
```

### Integration with Financial Data Sources

#### Pandas DataFrame Handling
```python
@render.data_frame
def gl_transactions():
    gl_data = load_GL_file()
    coa_data = load_COA_file()
    
    # Join and process financial data
    processed = gl_data.merge(coa_data, on="account_number")
    
    return render.DataGrid(
        processed,
        filters=True,
        selection_mode="rows"
    )
```

#### High-Precision Financial Calculations
```python
from decimal import Decimal

@reactive.calc  
def precise_nav_calculation():
    # Use Decimal for financial precision
    transactions = get_transactions()
    total = Decimal('0.0')
    for tx in transactions:
        total += Decimal(str(tx.amount))
    return float(total)
```

### Layout Patterns for Financial Apps

#### Sidebar Navigation
```python
ui.page_sidebar(
    ui.sidebar(
        ui.input_select("fund", "Fund", fund_choices),
        ui.input_date_range("dates", "Period"),
        ui.input_action_button("refresh", "Refresh Data")
    ),
    # Main content area
    ui.layout_columns(
        ui.card("Portfolio Summary", portfolio_summary_ui()),
        ui.card("Recent Transactions", transactions_ui()),
        col_widths=[6, 6]
    )
)
```

#### Card-Based Dashboard
```python
ui.layout_columns(
    ui.card(
        ui.card_header("Fund Performance"),
        ui.value_box("NAV", nav_value, "Current value"),
        ui.output_plot("nav_chart")
    ),
    ui.card(
        ui.card_header("Portfolio Allocation"),
        ui.output_plot("allocation_chart")
    ),
    ui.card(
        ui.card_header("Recent Activity"),
        ui.output_data_frame("recent_transactions")
    ),
    col_widths=[4, 4, 4]
)
```
