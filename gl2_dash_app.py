"""
General Ledger 2 - Full Accounting Interface
Dash + AG Grid with full CRUD operations

Run: python gl2_dash_app.py
Open: http://127.0.0.1:8051

Features:
- View/filter/sort all GL entries
- Edit cells inline
- Add new journal entries
- Delete entries
- Trial Balance view
- Account Ledger view
- Export to Excel
- All changes saved to S3
"""

import dash
from dash import html, dcc, callback, Input, Output, State, ctx, ALL, no_update
import dash_ag_grid as dag
import dash_bootstrap_components as dbc
import pandas as pd
import numpy as np
from datetime import datetime, date, timezone
from decimal import Decimal
import hashlib
import json
import os
import sys

# Add project root for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main_app.s3_utils import (
    load_GL2_file, save_GL2_file, clear_GL2_cache,
    load_COA_file, get_gl2_schema_columns
)

# =============================================================================
# CONFIGURATION
# =============================================================================

PORT = 8051
DEBUG = True

# =============================================================================
# DATA FUNCTIONS
# =============================================================================

def load_gl2_data():
    """Load GL2 data from S3 and prepare for display."""
    df = load_GL2_file()

    if df.empty:
        # Return empty dataframe with schema
        return pd.DataFrame(columns=get_gl2_schema_columns())

    # Fix empty account_number by extracting from account_name
    if 'account_number' in df.columns and 'account_name' in df.columns:
        import re
        def extract_acct_num(name):
            if not name:
                return ''
            match = re.match(r'^(\d+\.?\d*)', str(name))
            return match.group(1) if match else ''

        empty_mask = (df['account_number'].isna()) | (df['account_number'] == '')
        if empty_mask.any():
            df.loc[empty_mask, 'account_number'] = df.loc[empty_mask, 'account_name'].apply(extract_acct_num)

    # Convert timestamps to strings for JSON
    for col in ['timestamp', 'posted_date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')

    # Ensure numeric columns
    for col in ['debit_crypto', 'credit_crypto', 'debit_USD', 'credit_USD']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    return df


def save_gl2_data(df):
    """Save GL2 data back to S3."""
    # Convert string dates back to datetime
    for col in ['timestamp', 'posted_date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    clear_GL2_cache()
    return save_GL2_file(df)


def load_coa_options():
    """Load COA as dropdown options."""
    coa = load_COA_file()
    if coa.empty:
        return []

    options = []
    for _, row in coa.iterrows():
        try:
            acct_num = str(int(row['GL_Acct_Number']))
            acct_name = row['GL_Acct_Name']
            options.append({
                "label": f"{acct_num} - {acct_name}",
                "value": acct_num
            })
        except:
            continue

    return sorted(options, key=lambda x: x['value'])


def generate_row_key(tx_hash, account, entry_type, debit, credit):
    """Generate unique row key for deduplication."""
    return f"{tx_hash}:{account}:{entry_type}:{debit}:{credit}"


# =============================================================================
# COLUMN DEFINITIONS
# =============================================================================

columnDefs = [
    {
        "field": "timestamp",
        "headerName": "Date",
        "filter": "agDateColumnFilter",
        "width": 120,
        "pinned": "left",
        "editable": True,
        "cellEditor": "agDateStringCellEditor",
    },
    {
        "field": "account_number",
        "headerName": "Acct #",
        "filter": "agTextColumnFilter",
        "width": 90,
        "pinned": "left",
    },
    {
        "field": "account_name",
        "headerName": "Account",
        "filter": "agTextColumnFilter",
        "width": 220,
        "pinned": "left",
        "editable": True,
    },
    {
        "field": "entry_type",
        "headerName": "Type",
        "filter": "agSetColumnFilter",
        "width": 90,
        "cellStyle": {
            "function": "params.value === 'DEBIT' ? {'color': '#28a745', 'fontWeight': '600'} : {'color': '#dc3545', 'fontWeight': '600'}"
        },
    },
    {
        "field": "debit_crypto",
        "headerName": "Debit",
        "filter": "agNumberColumnFilter",
        "width": 130,
        "type": "numericColumn",
        "editable": True,
        "valueFormatter": {"function": "params.value ? d3.format(',.6f')(params.value) : ''"},
        "cellStyle": {"function": "params.value > 0 ? {'color': '#28a745', 'fontWeight': '500'} : {}"},
        "aggFunc": "sum",
    },
    {
        "field": "credit_crypto",
        "headerName": "Credit",
        "filter": "agNumberColumnFilter",
        "width": 130,
        "type": "numericColumn",
        "editable": True,
        "valueFormatter": {"function": "params.value ? d3.format(',.6f')(params.value) : ''"},
        "cellStyle": {"function": "params.value > 0 ? {'color': '#dc3545', 'fontWeight': '500'} : {}"},
        "aggFunc": "sum",
    },
    {
        "field": "debit_USD",
        "headerName": "Debit USD",
        "filter": "agNumberColumnFilter",
        "width": 110,
        "type": "numericColumn",
        "editable": True,
        "valueFormatter": {"function": "params.value ? d3.format('$,.2f')(params.value) : ''"},
        "aggFunc": "sum",
    },
    {
        "field": "credit_USD",
        "headerName": "Credit USD",
        "filter": "agNumberColumnFilter",
        "width": 110,
        "type": "numericColumn",
        "editable": True,
        "valueFormatter": {"function": "params.value ? d3.format('$,.2f')(params.value) : ''"},
        "aggFunc": "sum",
    },
    {
        "field": "asset",
        "headerName": "Asset",
        "filter": "agSetColumnFilter",
        "width": 80,
        "editable": True,
    },
    {
        "field": "description",
        "headerName": "Description",
        "filter": "agTextColumnFilter",
        "width": 250,
        "editable": True,
    },
    {
        "field": "category",
        "headerName": "Category",
        "filter": "agSetColumnFilter",
        "width": 140,
        "editable": True,
    },
    {
        "field": "platform",
        "headerName": "Platform",
        "filter": "agSetColumnFilter",
        "width": 100,
    },
    {
        "field": "tx_hash",
        "headerName": "TX Hash",
        "filter": "agTextColumnFilter",
        "width": 130,
        "valueFormatter": {"function": "params.value ? params.value.substring(0, 12) + '...' : ''"},
    },
    {
        "field": "row_key",
        "headerName": "Row Key",
        "hide": True,
    },
]

defaultColDef = {
    "filter": True,
    "sortable": True,
    "resizable": True,
    "floatingFilter": True,
}

# =============================================================================
# DASH APP
# =============================================================================

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.BOOTSTRAP],
    title="General Ledger 2",
    suppress_callback_exceptions=True,
)

# Load initial data
print("Loading GL2 data from S3...")
initial_df = load_gl2_data()
print(f"Loaded {len(initial_df):,} entries")

coa_options = load_coa_options()
print(f"Loaded {len(coa_options)} COA accounts")

# Calculate stats
def calc_stats(df):
    if df.empty:
        return 0, 0, 0, 0, "Balanced"
    total_entries = len(df)
    unique_accounts = df['account_number'].nunique() if 'account_number' in df.columns else 0
    total_debits = df['debit_crypto'].sum() if 'debit_crypto' in df.columns else 0
    total_credits = df['credit_crypto'].sum() if 'credit_crypto' in df.columns else 0
    diff = abs(total_debits - total_credits)
    balance_status = "Balanced" if diff < 0.000001 else f"Off by {diff:.6f}"
    return total_entries, unique_accounts, total_debits, total_credits, balance_status

# =============================================================================
# LAYOUT
# =============================================================================

app.layout = dbc.Container([
    # Store for data
    dcc.Store(id="gl2-data-store", data=initial_df.to_dict("records")),
    dcc.Store(id="selected-rows-store", data=[]),

    # Header
    dbc.Row([
        dbc.Col([
            html.H2([
                html.I(className="bi bi-journal-bookmark-fill me-2"),
                "General Ledger 2"
            ], className="mb-0 text-white"),
            html.Small("Interactive Accounting Interface", className="text-white-50"),
        ], width=4),
        dbc.Col([
            dbc.Row([
                dbc.Col([
                    html.Div(id="stat-entries", className="fs-4 fw-bold text-white"),
                    html.Small("Entries", className="text-white-50"),
                ], className="text-center"),
                dbc.Col([
                    html.Div(id="stat-accounts", className="fs-4 fw-bold text-white"),
                    html.Small("Accounts", className="text-white-50"),
                ], className="text-center"),
                dbc.Col([
                    html.Div(id="stat-debits", className="fs-4 fw-bold text-success"),
                    html.Small("Total Debits", className="text-white-50"),
                ], className="text-center"),
                dbc.Col([
                    html.Div(id="stat-credits", className="fs-4 fw-bold text-danger"),
                    html.Small("Total Credits", className="text-white-50"),
                ], className="text-center"),
                dbc.Col([
                    html.Div(id="stat-balance", className="fs-5 fw-bold"),
                    html.Small("Balance Check", className="text-white-50"),
                ], className="text-center"),
            ]),
        ], width=8),
    ], className="bg-dark p-3 mb-0 rounded-top"),

    # Tabs
    dbc.Tabs([
        # Tab 1: Journal Entries
        dbc.Tab([
            # Controls
            dbc.Row([
                dbc.Col([
                    dbc.InputGroup([
                        dbc.InputGroupText(html.I(className="bi bi-search")),
                        dbc.Input(id="quick-filter", placeholder="Search all columns...", type="text"),
                    ], size="sm"),
                ], width=3),
                dbc.Col([
                    dbc.Select(
                        id="group-by",
                        options=[
                            {"label": "No Grouping", "value": "none"},
                            {"label": "Group by Account", "value": "account_name"},
                            {"label": "Group by Category", "value": "category"},
                            {"label": "Group by Date", "value": "timestamp"},
                            {"label": "Group by Platform", "value": "platform"},
                        ],
                        value="none",
                        size="sm",
                    ),
                ], width=2),
                dbc.Col([
                    dbc.ButtonGroup([
                        dbc.Button([html.I(className="bi bi-plus-lg me-1"), "New Entry"], id="btn-new-entry", color="success", size="sm"),
                        dbc.Button([html.I(className="bi bi-trash me-1"), "Delete"], id="btn-delete", color="danger", size="sm", outline=True),
                        dbc.Button([html.I(className="bi bi-arrow-clockwise me-1"), "Refresh"], id="btn-refresh", color="secondary", size="sm", outline=True),
                        dbc.Button([html.I(className="bi bi-download me-1"), "Export"], id="btn-export", color="primary", size="sm", outline=True),
                    ]),
                ], width=5),
                dbc.Col([
                    html.Div(id="save-status", className="text-end"),
                ], width=2),
            ], className="py-2 px-3 bg-light border-bottom"),

            # Grid
            html.Div([
                dag.AgGrid(
                    id="gl2-grid",
                    rowData=initial_df.to_dict("records"),
                    columnDefs=columnDefs,
                    defaultColDef=defaultColDef,
                    dashGridOptions={
                        "animateRows": True,
                        "pagination": True,
                        "paginationPageSize": 50,
                        "rowSelection": "multiple",
                        "suppressRowClickSelection": False,
                        "enableCellTextSelection": True,
                        "undoRedoCellEditing": True,
                        "undoRedoCellEditingLimit": 20,
                        "stopEditingWhenCellsLoseFocus": True,
                        "getRowId": {"function": "params.data.row_key"},
                    },
                    className="ag-theme-alpine",
                    style={"height": "calc(100vh - 280px)", "width": "100%"},
                    getRowId="params.data.row_key",
                ),
            ], className="p-2"),
        ], label="Journal Entries", tab_id="tab-je"),

        # Tab 2: Trial Balance
        dbc.Tab([
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Trial Balance"),
                        dbc.CardBody([
                            dbc.Row([
                                dbc.Col([
                                    dbc.Label("As of Date"),
                                    dbc.Input(id="tb-date", type="date", value=date.today().isoformat()),
                                ], width=3),
                                dbc.Col([
                                    dbc.Label("Group By"),
                                    dbc.Select(
                                        id="tb-grouping",
                                        options=[
                                            {"label": "Individual Accounts", "value": "account"},
                                            {"label": "Account Category", "value": "category"},
                                        ],
                                        value="account",
                                    ),
                                ], width=3),
                                dbc.Col([
                                    dbc.Label(" "),
                                    dbc.Button("Generate", id="btn-gen-tb", color="primary", className="d-block"),
                                ], width=2),
                            ], className="mb-3"),
                            html.Div(id="tb-content"),
                        ]),
                    ]),
                ], width=12),
            ], className="p-3"),
        ], label="Trial Balance", tab_id="tab-tb"),

        # Tab 3: Account Ledger
        dbc.Tab([
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Account Ledger"),
                        dbc.CardBody([
                            dbc.Row([
                                dbc.Col([
                                    dbc.Label("Select Account"),
                                    dbc.Select(
                                        id="ledger-account",
                                        options=[{"label": "Select...", "value": ""}] + [
                                            {"label": opt["label"], "value": opt["value"]}
                                            for opt in coa_options
                                        ],
                                        value="",
                                    ),
                                ], width=4),
                                dbc.Col([
                                    dbc.Label("Date Range"),
                                    dbc.Input(id="ledger-start", type="date", value=(date.today().replace(month=1, day=1)).isoformat()),
                                ], width=2),
                                dbc.Col([
                                    dbc.Label(" "),
                                    dbc.Input(id="ledger-end", type="date", value=date.today().isoformat()),
                                ], width=2),
                                dbc.Col([
                                    dbc.Label(" "),
                                    dbc.Button("Load Ledger", id="btn-load-ledger", color="primary", className="d-block"),
                                ], width=2),
                            ], className="mb-3"),
                            html.Div(id="ledger-content"),
                        ]),
                    ]),
                ], width=12),
            ], className="p-3"),
        ], label="Account Ledger", tab_id="tab-ledger"),

    ], id="tabs", active_tab="tab-je", className="mb-0"),

    # New Entry Modal
    dbc.Modal([
        dbc.ModalHeader(dbc.ModalTitle("New Journal Entry")),
        dbc.ModalBody([
            dbc.Row([
                dbc.Col([
                    dbc.Label("Date"),
                    dbc.Input(id="new-entry-date", type="date", value=date.today().isoformat()),
                ], width=4),
                dbc.Col([
                    dbc.Label("Description"),
                    dbc.Input(id="new-entry-desc", type="text", placeholder="Enter description..."),
                ], width=8),
            ], className="mb-3"),

            html.Hr(),
            html.H6("Entry Lines"),
            html.Div(id="entry-lines-container"),
            dbc.Button([html.I(className="bi bi-plus me-1"), "Add Line"], id="btn-add-line", color="link", size="sm", className="mt-2"),

            html.Hr(),
            dbc.Row([
                dbc.Col([
                    html.Strong("Total Debits: "),
                    html.Span(id="new-entry-total-debits", className="text-success"),
                ], width=4),
                dbc.Col([
                    html.Strong("Total Credits: "),
                    html.Span(id="new-entry-total-credits", className="text-danger"),
                ], width=4),
                dbc.Col([
                    html.Strong("Difference: "),
                    html.Span(id="new-entry-diff"),
                ], width=4),
            ]),
            html.Div(id="new-entry-validation", className="mt-2"),
        ]),
        dbc.ModalFooter([
            dbc.Button("Cancel", id="btn-cancel-entry", color="secondary"),
            dbc.Button("Post Entry", id="btn-post-entry", color="success"),
        ]),
    ], id="modal-new-entry", size="lg", is_open=False),

    # Download component
    dcc.Download(id="download-gl2"),

    # Toast for notifications
    dbc.Toast(
        id="toast-notification",
        header="Notification",
        is_open=False,
        dismissable=True,
        duration=4000,
        style={"position": "fixed", "top": 20, "right": 20, "width": 350, "zIndex": 9999},
    ),

], fluid=True, className="px-0")

# =============================================================================
# CALLBACKS
# =============================================================================

# Update stats
@callback(
    [Output("stat-entries", "children"),
     Output("stat-accounts", "children"),
     Output("stat-debits", "children"),
     Output("stat-credits", "children"),
     Output("stat-balance", "children"),
     Output("stat-balance", "className")],
    Input("gl2-data-store", "data"),
)
def update_stats(data):
    df = pd.DataFrame(data) if data else pd.DataFrame()
    entries, accounts, debits, credits, balance = calc_stats(df)

    balance_class = "fs-5 fw-bold text-success" if balance == "Balanced" else "fs-5 fw-bold text-warning"

    return (
        f"{entries:,}",
        f"{accounts}",
        f"{debits:.4f}",
        f"{credits:.4f}",
        balance,
        balance_class,
    )


# Quick filter
@callback(
    Output("gl2-grid", "dashGridOptions"),
    Input("quick-filter", "value"),
    State("gl2-grid", "dashGridOptions"),
)
def update_quick_filter(filter_value, options):
    options = options or {}
    options["quickFilterText"] = filter_value or ""
    return options


# Group by
@callback(
    Output("gl2-grid", "columnDefs"),
    Input("group-by", "value"),
)
def update_grouping(group_by):
    new_defs = [col.copy() for col in columnDefs]
    for col in new_defs:
        col["rowGroup"] = False
        if "hide" in col and col["field"] != "row_key":
            col["hide"] = False

    if group_by and group_by != "none":
        for col in new_defs:
            if col["field"] == group_by:
                col["rowGroup"] = True
                break

    return new_defs


# Refresh data
@callback(
    [Output("gl2-grid", "rowData"),
     Output("gl2-data-store", "data"),
     Output("toast-notification", "children", allow_duplicate=True),
     Output("toast-notification", "is_open", allow_duplicate=True)],
    Input("btn-refresh", "n_clicks"),
    prevent_initial_call=True,
)
def refresh_data(n):
    clear_GL2_cache()
    df = load_gl2_data()
    return df.to_dict("records"), df.to_dict("records"), "Data refreshed from S3", True


# Export to CSV
@callback(
    Output("download-gl2", "data"),
    Input("btn-export", "n_clicks"),
    State("gl2-data-store", "data"),
    prevent_initial_call=True,
)
def export_data(n, data):
    df = pd.DataFrame(data)
    return dcc.send_data_frame(df.to_csv, f"gl2_export_{date.today().isoformat()}.csv", index=False)


# Cell edit - save to S3
@callback(
    [Output("gl2-data-store", "data", allow_duplicate=True),
     Output("save-status", "children"),
     Output("toast-notification", "children", allow_duplicate=True),
     Output("toast-notification", "is_open", allow_duplicate=True)],
    Input("gl2-grid", "cellValueChanged"),
    State("gl2-data-store", "data"),
    prevent_initial_call=True,
)
def handle_cell_edit(cell_change, data):
    if not cell_change:
        return no_update, no_update, no_update, no_update

    df = pd.DataFrame(data)

    # Update the changed cell
    row_id = cell_change.get("rowId")
    col_id = cell_change.get("colId")
    new_value = cell_change.get("value")

    if row_id and col_id:
        mask = df["row_key"] == row_id
        if mask.any():
            df.loc[mask, col_id] = new_value

            # Save to S3
            if save_gl2_data(df):
                status = html.Span([html.I(className="bi bi-check-circle text-success me-1"), "Saved"], className="text-success")
                return df.to_dict("records"), status, f"Updated {col_id}", True
            else:
                status = html.Span([html.I(className="bi bi-x-circle text-danger me-1"), "Save failed"], className="text-danger")
                return no_update, status, "Failed to save changes", True

    return no_update, no_update, no_update, no_update


# Delete selected rows
@callback(
    [Output("gl2-grid", "rowData", allow_duplicate=True),
     Output("gl2-data-store", "data", allow_duplicate=True),
     Output("toast-notification", "children", allow_duplicate=True),
     Output("toast-notification", "is_open", allow_duplicate=True)],
    Input("btn-delete", "n_clicks"),
    State("gl2-grid", "selectedRows"),
    State("gl2-data-store", "data"),
    prevent_initial_call=True,
)
def delete_rows(n, selected, data):
    if not selected:
        return no_update, no_update, "No rows selected", True

    df = pd.DataFrame(data)
    row_keys_to_delete = [row.get("row_key") for row in selected if row.get("row_key")]

    df = df[~df["row_key"].isin(row_keys_to_delete)]

    if save_gl2_data(df):
        return df.to_dict("records"), df.to_dict("records"), f"Deleted {len(row_keys_to_delete)} entries", True
    else:
        return no_update, no_update, "Failed to delete entries", True


# Open new entry modal
@callback(
    Output("modal-new-entry", "is_open"),
    [Input("btn-new-entry", "n_clicks"),
     Input("btn-cancel-entry", "n_clicks"),
     Input("btn-post-entry", "n_clicks")],
    State("modal-new-entry", "is_open"),
    prevent_initial_call=True,
)
def toggle_modal(n1, n2, n3, is_open):
    return not is_open


# Entry lines container
@callback(
    Output("entry-lines-container", "children"),
    Input("btn-add-line", "n_clicks"),
    State("entry-lines-container", "children"),
)
def manage_entry_lines(n_clicks, children):
    # Initialize with 2 lines
    if children is None:
        children = []

    num_lines = len(children) if children else 0

    if ctx.triggered_id == "btn-add-line":
        num_lines += 1
    elif num_lines == 0:
        num_lines = 2

    lines = []
    for i in range(max(num_lines, 2)):
        lines.append(
            dbc.Row([
                dbc.Col([
                    dbc.Select(
                        id={"type": "line-account", "index": i},
                        options=[{"label": "Select account...", "value": ""}] + [
                            {"label": opt["label"], "value": opt["label"]}
                            for opt in coa_options
                        ],
                        value="",
                        size="sm",
                    ),
                ], width=6),
                dbc.Col([
                    dbc.Input(id={"type": "line-debit", "index": i}, type="number", placeholder="Debit", size="sm", min=0, step=0.000001),
                ], width=3),
                dbc.Col([
                    dbc.Input(id={"type": "line-credit", "index": i}, type="number", placeholder="Credit", size="sm", min=0, step=0.000001),
                ], width=3),
            ], className="mb-2", id={"type": "entry-line", "index": i})
        )

    return lines


# Post new entry
@callback(
    [Output("gl2-grid", "rowData", allow_duplicate=True),
     Output("gl2-data-store", "data", allow_duplicate=True),
     Output("toast-notification", "children", allow_duplicate=True),
     Output("toast-notification", "is_open", allow_duplicate=True),
     Output("modal-new-entry", "is_open", allow_duplicate=True)],
    Input("btn-post-entry", "n_clicks"),
    [State("new-entry-date", "value"),
     State("new-entry-desc", "value"),
     State({"type": "line-account", "index": ALL}, "value"),
     State({"type": "line-debit", "index": ALL}, "value"),
     State({"type": "line-credit", "index": ALL}, "value"),
     State("gl2-data-store", "data")],
    prevent_initial_call=True,
)
def post_new_entry(n, entry_date, desc, accounts, debits, credits, data):
    if not n:
        return no_update, no_update, no_update, no_update, no_update

    # Collect valid lines
    lines = []
    for i, (acct, dr, cr) in enumerate(zip(accounts, debits, credits)):
        if acct and (dr or cr):
            lines.append({
                "account": acct,
                "debit": float(dr) if dr else 0,
                "credit": float(cr) if cr else 0,
            })

    if len(lines) < 2:
        return no_update, no_update, "Need at least 2 lines with amounts", True, no_update

    total_dr = sum(l["debit"] for l in lines)
    total_cr = sum(l["credit"] for l in lines)

    if abs(total_dr - total_cr) > 0.000001:
        return no_update, no_update, "Entry not balanced! Debits must equal credits.", True, no_update

    # Generate records
    tx_hash = f"manual_{hashlib.md5(f'{entry_date}{desc}{datetime.now()}'.encode()).hexdigest()[:16]}"
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    records = []
    for line in lines:
        # Extract account number from account name
        import re
        acct_match = re.match(r'^(\d+)', line["account"])
        acct_num = acct_match.group(1) if acct_match else ""

        row_key = generate_row_key(tx_hash, line["account"], "DEBIT" if line["debit"] > 0 else "CREDIT", line["debit"], line["credit"])

        records.append({
            "tx_hash": tx_hash,
            "entry_type": "DEBIT" if line["debit"] > 0 else "CREDIT",
            "account_number": acct_num,
            "account_name": line["account"],
            "debit_crypto": line["debit"],
            "credit_crypto": line["credit"],
            "debit_USD": 0,
            "credit_USD": 0,
            "asset": "ETH",
            "description": desc or "Manual Journal Entry",
            "category": "manual",
            "platform": "manual",
            "timestamp": entry_date,
            "posted_date": now,
            "row_key": row_key,
        })

    # Add to data
    df = pd.DataFrame(data)
    new_df = pd.DataFrame(records)
    combined = pd.concat([df, new_df], ignore_index=True)

    if save_gl2_data(combined):
        return combined.to_dict("records"), combined.to_dict("records"), f"Posted {len(records)} entries", True, False
    else:
        return no_update, no_update, "Failed to save entry", True, no_update


# Trial Balance generation
@callback(
    Output("tb-content", "children"),
    Input("btn-gen-tb", "n_clicks"),
    [State("tb-date", "value"),
     State("tb-grouping", "value"),
     State("gl2-data-store", "data")],
    prevent_initial_call=True,
)
def generate_trial_balance(n, as_of, grouping, data):
    df = pd.DataFrame(data)
    if df.empty:
        return dbc.Alert("No data available", color="warning")

    # Filter by date
    if as_of:
        df = df[pd.to_datetime(df["timestamp"]) <= pd.to_datetime(as_of)]

    if df.empty:
        return dbc.Alert("No data for selected date", color="warning")

    # Group by account
    grouped = df.groupby(["account_number", "account_name"]).agg({
        "debit_crypto": "sum",
        "credit_crypto": "sum",
    }).reset_index()

    grouped["net"] = grouped["debit_crypto"] - grouped["credit_crypto"]
    grouped["debit_balance"] = grouped["net"].apply(lambda x: x if x > 0 else 0)
    grouped["credit_balance"] = grouped["net"].apply(lambda x: abs(x) if x < 0 else 0)

    total_dr = grouped["debit_balance"].sum()
    total_cr = grouped["credit_balance"].sum()

    # Build table
    table = dbc.Table([
        html.Thead([
            html.Tr([
                html.Th("Account #"),
                html.Th("Account Name"),
                html.Th("Debit Balance", className="text-end"),
                html.Th("Credit Balance", className="text-end"),
            ])
        ]),
        html.Tbody([
            html.Tr([
                html.Td(row["account_number"]),
                html.Td(row["account_name"]),
                html.Td(f"{row['debit_balance']:.6f}" if row['debit_balance'] > 0 else "", className="text-end text-success"),
                html.Td(f"{row['credit_balance']:.6f}" if row['credit_balance'] > 0 else "", className="text-end text-danger"),
            ]) for _, row in grouped.iterrows()
        ] + [
            html.Tr([
                html.Td(""),
                html.Td(html.Strong("TOTALS")),
                html.Td(html.Strong(f"{total_dr:.6f}"), className="text-end text-success"),
                html.Td(html.Strong(f"{total_cr:.6f}"), className="text-end text-danger"),
            ], className="table-dark")
        ]),
    ], bordered=True, hover=True, striped=True, size="sm")

    return table


# Account Ledger
@callback(
    Output("ledger-content", "children"),
    Input("btn-load-ledger", "n_clicks"),
    [State("ledger-account", "value"),
     State("ledger-start", "value"),
     State("ledger-end", "value"),
     State("gl2-data-store", "data")],
    prevent_initial_call=True,
)
def generate_account_ledger(n, account, start_date, end_date, data):
    if not account:
        return dbc.Alert("Please select an account", color="warning")

    df = pd.DataFrame(data)
    if df.empty:
        return dbc.Alert("No data available", color="warning")

    # Filter by account
    df = df[df["account_number"] == account].copy()

    if df.empty:
        return dbc.Alert(f"No entries for account {account}", color="info")

    # Filter by date
    df = df[(pd.to_datetime(df["timestamp"]) >= pd.to_datetime(start_date)) &
            (pd.to_datetime(df["timestamp"]) <= pd.to_datetime(end_date))]

    df = df.sort_values("timestamp")

    # Calculate running balance
    running = 0
    balances = []
    for _, row in df.iterrows():
        running += float(row["debit_crypto"]) - float(row["credit_crypto"])
        balances.append(running)
    df["balance"] = balances

    # Summary
    total_dr = df["debit_crypto"].sum()
    total_cr = df["credit_crypto"].sum()
    ending_bal = balances[-1] if balances else 0

    summary = dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H6("Total Debits", className="text-muted"),
                html.H4(f"{total_dr:.6f}", className="text-success"),
            ])
        ]), width=4),
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H6("Total Credits", className="text-muted"),
                html.H4(f"{total_cr:.6f}", className="text-danger"),
            ])
        ]), width=4),
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H6("Ending Balance", className="text-muted"),
                html.H4(f"{ending_bal:.6f}", className="text-primary"),
            ])
        ]), width=4),
    ], className="mb-3")

    # Table
    table = dbc.Table([
        html.Thead([
            html.Tr([
                html.Th("Date"),
                html.Th("Description"),
                html.Th("Debit", className="text-end"),
                html.Th("Credit", className="text-end"),
                html.Th("Balance", className="text-end"),
            ])
        ]),
        html.Tbody([
            html.Tr([
                html.Td(row["timestamp"][:10] if row["timestamp"] else ""),
                html.Td(row["description"][:50] if row["description"] else ""),
                html.Td(f"{row['debit_crypto']:.6f}" if row['debit_crypto'] > 0 else "", className="text-end text-success"),
                html.Td(f"{row['credit_crypto']:.6f}" if row['credit_crypto'] > 0 else "", className="text-end text-danger"),
                html.Td(f"{row['balance']:.6f}", className="text-end fw-bold"),
            ]) for _, row in df.iterrows()
        ]),
    ], bordered=True, hover=True, striped=True, size="sm")

    return html.Div([summary, table])


# =============================================================================
# RUN
# =============================================================================

if __name__ == "__main__":
    print("\n" + "="*60)
    print("  GENERAL LEDGER 2 - Dash + AG Grid")
    print("  Full Accounting Interface")
    print("="*60)
    print(f"  Entries: {len(initial_df):,}")
    print(f"  COA Accounts: {len(coa_options)}")
    print("="*60)
    print(f"\n  Open in browser: http://127.0.0.1:{PORT}\n")

    app.run(debug=DEBUG, port=PORT)
