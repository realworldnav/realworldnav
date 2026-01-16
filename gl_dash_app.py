"""
General Ledger - Dash + AG Grid

A fast, interactive General Ledger viewer using Dash and AG Grid.
Run: python gl_dash_app.py
Then open: http://127.0.0.1:8050
"""

import dash
from dash import html, dcc, callback, Input, Output, State
import dash_ag_grid as dag
import pandas as pd
from datetime import datetime, date
import os

# =============================================================================
# DATA LOADING
# =============================================================================

GL_PATH = r'G:\My Drive\Drip_Capital\accounting_records\general_ledger\holdings_class_B_ETH\holdings_class_B_ETH_general_ledger_flat.parquet'

def load_gl_data():
    """Load GL data from parquet file."""
    df = pd.read_parquet(GL_PATH)

    # Convert date columns to string for JSON serialization
    for col in ['date', 'operating_date', 'loan_due_date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d')

    # Convert numeric columns
    numeric_cols = ['debit_crypto', 'credit_crypto', 'debit_USD', 'credit_USD',
                    'eth_usd_price', 'principal_crypto', 'principal_USD',
                    'annual_interest_rate', 'payoff_amount_crypto', 'payoff_amount_USD']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    return df

# Load data at startup
print("Loading GL data...")
df = load_gl_data()
print(f"Loaded {len(df):,} rows")

# =============================================================================
# COLUMN DEFINITIONS
# =============================================================================

# Define which columns to show and how
columnDefs = [
    # Date & ID columns
    {
        "field": "date",
        "headerName": "Date",
        "filter": "agDateColumnFilter",
        "width": 110,
        "pinned": "left",
    },
    {
        "field": "GL_Acct_Number",
        "headerName": "Acct #",
        "filter": "agNumberColumnFilter",
        "width": 90,
        "pinned": "left",
    },
    {
        "field": "GL_Acct_Name",
        "headerName": "Account Name",
        "filter": "agTextColumnFilter",
        "width": 200,
        "pinned": "left",
    },

    # Transaction details
    {
        "field": "transaction_type",
        "headerName": "Type",
        "filter": "agSetColumnFilter",
        "width": 180,
    },
    {
        "field": "cryptocurrency",
        "headerName": "Crypto",
        "filter": "agSetColumnFilter",
        "width": 80,
    },

    # Amounts - Crypto
    {
        "field": "debit_crypto",
        "headerName": "Debit (Crypto)",
        "filter": "agNumberColumnFilter",
        "width": 130,
        "type": "numericColumn",
        "valueFormatter": {"function": "d3.format(',.6f')(params.value)"},
        "cellStyle": {"function": "params.value > 0 ? {'color': '#28a745', 'fontWeight': '500'} : {}"},
        "aggFunc": "sum",
    },
    {
        "field": "credit_crypto",
        "headerName": "Credit (Crypto)",
        "filter": "agNumberColumnFilter",
        "width": 130,
        "type": "numericColumn",
        "valueFormatter": {"function": "d3.format(',.6f')(params.value)"},
        "cellStyle": {"function": "params.value > 0 ? {'color': '#dc3545', 'fontWeight': '500'} : {}"},
        "aggFunc": "sum",
    },

    # Amounts - USD
    {
        "field": "debit_USD",
        "headerName": "Debit (USD)",
        "filter": "agNumberColumnFilter",
        "width": 120,
        "type": "numericColumn",
        "valueFormatter": {"function": "d3.format('$,.2f')(params.value)"},
        "cellStyle": {"function": "params.value > 0 ? {'color': '#28a745', 'fontWeight': '500'} : {}"},
        "aggFunc": "sum",
    },
    {
        "field": "credit_USD",
        "headerName": "Credit (USD)",
        "filter": "agNumberColumnFilter",
        "width": 120,
        "type": "numericColumn",
        "valueFormatter": {"function": "d3.format('$,.2f')(params.value)"},
        "cellStyle": {"function": "params.value > 0 ? {'color': '#dc3545', 'fontWeight': '500'} : {}"},
        "aggFunc": "sum",
    },

    # Price
    {
        "field": "eth_usd_price",
        "headerName": "ETH Price",
        "filter": "agNumberColumnFilter",
        "width": 100,
        "type": "numericColumn",
        "valueFormatter": {"function": "d3.format('$,.2f')(params.value)"},
    },

    # Additional context
    {
        "field": "wallet_id",
        "headerName": "Wallet",
        "filter": "agSetColumnFilter",
        "width": 120,
    },
    {
        "field": "event",
        "headerName": "Event",
        "filter": "agSetColumnFilter",
        "width": 150,
    },
    {
        "field": "hash",
        "headerName": "TX Hash",
        "filter": "agTextColumnFilter",
        "width": 150,
        "cellRenderer": "HashLink",
    },

    # Loan details
    {
        "field": "loan_id",
        "headerName": "Loan ID",
        "filter": "agTextColumnFilter",
        "width": 100,
    },
    {
        "field": "lender",
        "headerName": "Lender",
        "filter": "agTextColumnFilter",
        "width": 120,
    },
    {
        "field": "borrower",
        "headerName": "Borrower",
        "filter": "agTextColumnFilter",
        "width": 120,
    },
    {
        "field": "principal_crypto",
        "headerName": "Principal",
        "filter": "agNumberColumnFilter",
        "width": 110,
        "type": "numericColumn",
        "valueFormatter": {"function": "d3.format(',.4f')(params.value)"},
    },
    {
        "field": "annual_interest_rate",
        "headerName": "APR %",
        "filter": "agNumberColumnFilter",
        "width": 80,
        "type": "numericColumn",
        "valueFormatter": {"function": "d3.format('.2%')(params.value)"},
    },

    # Addresses
    {
        "field": "from",
        "headerName": "From",
        "filter": "agTextColumnFilter",
        "width": 120,
    },
    {
        "field": "to",
        "headerName": "To",
        "filter": "agTextColumnFilter",
        "width": 120,
    },
    {
        "field": "contract_address",
        "headerName": "Contract",
        "filter": "agTextColumnFilter",
        "width": 120,
    },

    # Meta
    {
        "field": "source_file",
        "headerName": "Source",
        "filter": "agSetColumnFilter",
        "width": 150,
    },
    {
        "field": "operating_date",
        "headerName": "Op Date",
        "filter": "agDateColumnFilter",
        "width": 100,
    },
]

# Default column properties
defaultColDef = {
    "filter": True,
    "sortable": True,
    "resizable": True,
    "floatingFilter": True,  # Shows filter inputs below headers
}

# =============================================================================
# DASH APP
# =============================================================================

app = dash.Dash(
    __name__,
    title="General Ledger",
    suppress_callback_exceptions=True,
)

# Custom CSS
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
                margin: 0;
                padding: 0;
                background-color: #f8f9fa;
            }
            .header {
                background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
                color: white;
                padding: 15px 30px;
                display: flex;
                justify-content: space-between;
                align-items: center;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            }
            .header h1 {
                margin: 0;
                font-size: 24px;
                font-weight: 600;
            }
            .stats {
                display: flex;
                gap: 30px;
            }
            .stat-box {
                text-align: center;
            }
            .stat-value {
                font-size: 20px;
                font-weight: 700;
            }
            .stat-label {
                font-size: 11px;
                opacity: 0.8;
                text-transform: uppercase;
            }
            .controls {
                padding: 15px 30px;
                background: white;
                border-bottom: 1px solid #e0e0e0;
                display: flex;
                gap: 20px;
                align-items: center;
            }
            .control-group {
                display: flex;
                flex-direction: column;
                gap: 5px;
            }
            .control-label {
                font-size: 11px;
                font-weight: 600;
                color: #666;
                text-transform: uppercase;
            }
            .grid-container {
                padding: 20px 30px;
                height: calc(100vh - 180px);
            }
            .ag-theme-alpine {
                --ag-header-background-color: #f1f3f5;
                --ag-odd-row-background-color: #fafbfc;
                --ag-row-hover-color: #e8f4ff;
                --ag-selected-row-background-color: #d4edff;
                --ag-font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                --ag-font-size: 13px;
            }
            .green { color: #28a745; }
            .red { color: #dc3545; }
            .btn {
                padding: 8px 16px;
                border: none;
                border-radius: 6px;
                cursor: pointer;
                font-weight: 500;
                transition: all 0.2s;
            }
            .btn-primary {
                background: #0066cc;
                color: white;
            }
            .btn-primary:hover {
                background: #0052a3;
            }
            .btn-secondary {
                background: #e9ecef;
                color: #495057;
            }
            .btn-secondary:hover {
                background: #dee2e6;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

# Calculate summary stats
total_debit_usd = df['debit_USD'].sum()
total_credit_usd = df['credit_USD'].sum()
total_rows = len(df)
unique_accounts = df['GL_Acct_Number'].nunique()

app.layout = html.Div([
    # Header
    html.Div([
        html.H1("General Ledger"),
        html.Div([
            html.Div([
                html.Div(f"{total_rows:,}", className="stat-value"),
                html.Div("Entries", className="stat-label"),
            ], className="stat-box"),
            html.Div([
                html.Div(f"{unique_accounts}", className="stat-value"),
                html.Div("Accounts", className="stat-label"),
            ], className="stat-box"),
            html.Div([
                html.Div(f"${total_debit_usd:,.0f}", className="stat-value green"),
                html.Div("Total Debits", className="stat-label"),
            ], className="stat-box"),
            html.Div([
                html.Div(f"${total_credit_usd:,.0f}", className="stat-value red"),
                html.Div("Total Credits", className="stat-label"),
            ], className="stat-box"),
        ], className="stats"),
    ], className="header"),

    # Controls
    html.Div([
        html.Div([
            html.Div("Quick Filter", className="control-label"),
            dcc.Input(
                id="quick-filter",
                type="text",
                placeholder="Search all columns...",
                style={"width": "250px", "padding": "8px", "borderRadius": "6px", "border": "1px solid #ccc"}
            ),
        ], className="control-group"),

        html.Div([
            html.Div("Group By", className="control-label"),
            dcc.Dropdown(
                id="group-by",
                options=[
                    {"label": "None", "value": "none"},
                    {"label": "Account", "value": "GL_Acct_Name"},
                    {"label": "Transaction Type", "value": "transaction_type"},
                    {"label": "Date", "value": "date"},
                    {"label": "Wallet", "value": "wallet_id"},
                ],
                value="none",
                clearable=False,
                style={"width": "180px"}
            ),
        ], className="control-group"),

        html.Div([
            html.Div("Actions", className="control-label"),
            html.Div([
                html.Button("Reset Filters", id="reset-filters", className="btn btn-secondary", style={"marginRight": "10px"}),
                html.Button("Export CSV", id="export-csv", className="btn btn-primary"),
            ]),
        ], className="control-group"),

        # Status
        html.Div([
            html.Div("Showing", className="control-label"),
            html.Div(id="row-count", children=f"{total_rows:,} rows", style={"fontWeight": "600"}),
        ], className="control-group", style={"marginLeft": "auto"}),

    ], className="controls"),

    # AG Grid
    html.Div([
        dag.AgGrid(
            id="gl-grid",
            rowData=df.to_dict("records"),
            columnDefs=columnDefs,
            defaultColDef=defaultColDef,
            dashGridOptions={
                "animateRows": True,
                "pagination": True,
                "paginationPageSize": 100,
                "rowSelection": "multiple",
                "suppressRowClickSelection": True,
                "enableCellTextSelection": True,
                "ensureDomOrder": True,
            },
            className="ag-theme-alpine",
            style={"height": "100%", "width": "100%"},
        ),
    ], className="grid-container"),

    # Hidden download component
    dcc.Download(id="download-csv"),
])

# =============================================================================
# CALLBACKS
# =============================================================================

@callback(
    Output("gl-grid", "dashGridOptions"),
    Input("quick-filter", "value"),
    State("gl-grid", "dashGridOptions"),
)
def update_quick_filter(filter_value, grid_options):
    """Update quick filter."""
    grid_options = grid_options or {}
    grid_options["quickFilterText"] = filter_value or ""
    return grid_options


@callback(
    Output("gl-grid", "columnDefs"),
    Input("group-by", "value"),
)
def update_grouping(group_by):
    """Update row grouping."""
    new_defs = columnDefs.copy()

    # Reset all rowGroup
    for col in new_defs:
        col["rowGroup"] = False
        col["hide"] = False

    if group_by and group_by != "none":
        for col in new_defs:
            if col["field"] == group_by:
                col["rowGroup"] = True
                col["hide"] = True
                break

    return new_defs


@callback(
    Output("download-csv", "data"),
    Input("export-csv", "n_clicks"),
    prevent_initial_call=True,
)
def export_csv(n_clicks):
    """Export grid data to CSV."""
    return dcc.send_data_frame(df.to_csv, "general_ledger_export.csv", index=False)


@callback(
    Output("quick-filter", "value"),
    Input("reset-filters", "n_clicks"),
    prevent_initial_call=True,
)
def reset_filters(n_clicks):
    """Reset all filters."""
    return ""


# =============================================================================
# RUN
# =============================================================================

if __name__ == "__main__":
    print("\n" + "="*60)
    print("  GENERAL LEDGER - Dash + AG Grid")
    print("="*60)
    print(f"  Rows: {total_rows:,}")
    print(f"  Accounts: {unique_accounts}")
    print(f"  Total Debits: ${total_debit_usd:,.2f}")
    print(f"  Total Credits: ${total_credit_usd:,.2f}")
    print("="*60)
    print("\n  Open in browser: http://127.0.0.1:8050\n")

    app.run(debug=True, port=8050)
