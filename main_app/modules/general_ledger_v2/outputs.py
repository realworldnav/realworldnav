"""
General Ledger 2 Outputs Module

Professional accounting interface with full functionality:
- Journal entries viewing, filtering, editing, deleting
- Account ledger with running balances
- Trial balance generation
- Manual journal entry creation
- Integration with decoded transactions posting
"""

from shiny import ui, reactive, render
import pandas as pd
from decimal import Decimal
from datetime import datetime, timezone
import hashlib
import re
import logging

logger = logging.getLogger(__name__)


def extract_account_number(account_name_str):
    """Extract account number from account name like '100.30 - ETH Wallet'"""
    if not account_name_str:
        return ''
    match = re.match(r'^(\d+\.?\d*)', str(account_name_str))
    if match:
        return match.group(1)
    return ''


def normalize_gl2_columns(df):
    """
    Normalize column names and derive GL_Acct_Number/GL_Acct_Name from account_name.
    GL2 parquet uses the 37-column CSV format with account_name like '100.30 - ETH Wallet'.
    This function derives GL_Acct_Number and GL_Acct_Name for internal filtering/grouping.
    """
    if df.empty:
        return df

    # Make a copy to avoid modifying the original
    df = df.copy()

    # Map old column names to new names for compatibility
    column_mapping = {
        'timestamp': 'date',
        'tx_hash': 'hash',
        'entry_type': 'transaction_type',
        'category': 'transaction_type',
        'asset': 'cryptocurrency',
    }

    # Rename columns if old names exist
    for old_name, new_name in column_mapping.items():
        if old_name in df.columns and new_name not in df.columns:
            df = df.rename(columns={old_name: new_name})

    # Handle account_number -> GL_Acct_Number (may contain "600.10 - Gas Expense" format)
    if 'account_number' in df.columns and 'GL_Acct_Number' not in df.columns:
        df = df.rename(columns={'account_number': 'GL_Acct_Number'})

    # If GL_Acct_Name doesn't exist, try to extract from account_name or GL_Acct_Number
    if 'GL_Acct_Name' not in df.columns:
        if 'account_name' in df.columns:
            # account_name might be "600.10 - Gas Expense" format
            def extract_name(val):
                if pd.isna(val):
                    return ''
                val = str(val)
                if ' - ' in val:
                    return val.split(' - ', 1)[1]
                return val
            df['GL_Acct_Name'] = df['account_name'].apply(extract_name)
        elif 'GL_Acct_Number' in df.columns:
            # GL_Acct_Number might contain "600.10 - Gas Expense" format
            def extract_name_from_num(val):
                if pd.isna(val):
                    return ''
                val = str(val)
                if ' - ' in val:
                    return val.split(' - ', 1)[1]
                return ''
            df['GL_Acct_Name'] = df['GL_Acct_Number'].apply(extract_name_from_num)
        else:
            df['GL_Acct_Name'] = ''

    # Clean GL_Acct_Number to be just the number if it contains "600.10 - Gas Expense" format
    # Also remove decimal points (100.30 -> 10030)
    if 'GL_Acct_Number' in df.columns:
        def extract_number(val):
            if pd.isna(val):
                return ''
            val = str(val)
            if ' - ' in val:
                val = val.split(' - ', 1)[0].strip()
            # Remove decimal point from account number (100.30 -> 10030)
            val = val.replace('.', '')
            return val
        df['GL_Acct_Number'] = df['GL_Acct_Number'].apply(extract_number)

    # Ensure GL_Acct_Name column exists even if empty
    if 'GL_Acct_Name' not in df.columns:
        df['GL_Acct_Name'] = ''

    # Extract GL_Acct_Number from account_name if it doesn't exist
    # Remove decimal to match COA format (100.30 -> 10030)
    if 'GL_Acct_Number' not in df.columns and 'account_name' in df.columns:
        df['GL_Acct_Number'] = df['account_name'].apply(
            lambda x: extract_account_number(x).replace('.', '')
        )

    # Ensure GL_Acct_Number column exists
    if 'GL_Acct_Number' not in df.columns:
        df['GL_Acct_Number'] = ''

    return df


def register_gl2_outputs(input, output, session):
    """Register all GL2 server outputs."""

    # Import S3 utilities
    from ...s3_utils import (
        load_GL2_file, save_GL2_file, clear_GL2_cache,
        load_COA_file, get_gl2_schema_columns
    )

    logger.info("[GL2] Registering GL2 outputs")

    # =========================================================================
    # REACTIVE VALUES
    # =========================================================================

    # Entry lines for new manual entry
    entry_lines = reactive.value([
        {"account": "", "debit": 0.0, "credit": 0.0},
        {"account": "", "debit": 0.0, "credit": 0.0}
    ])

    # GL2 data refresh trigger
    gl2_data_version = reactive.value(0)

    # Selected rows from data grid
    selected_row_keys = reactive.value([])

    # Edit mode tracking
    edit_mode_active = reactive.value(False)
    editing_row_key = reactive.value(None)

    # =========================================================================
    # DATA LOADING
    # =========================================================================

    @reactive.calc
    def gl2_data():
        """Load GL2 data from S3."""
        _ = gl2_data_version()
        try:
            df = load_GL2_file()
            logger.info(f"[GL2] Loaded {len(df)} rows from GL2")

            if not df.empty:
                # Normalize column names for compatibility with both old and new schema
                df = normalize_gl2_columns(df)

                # Fix empty GL_Acct_Number by extracting from account_name
                if 'GL_Acct_Number' in df.columns and 'account_name' in df.columns:
                    empty_mask = (df['GL_Acct_Number'].isna()) | (df['GL_Acct_Number'] == '')
                    if empty_mask.any():
                        df.loc[empty_mask, 'GL_Acct_Number'] = df.loc[empty_mask, 'account_name'].apply(
                            lambda x: extract_account_number(x).replace('.', '')
                        )

            return df
        except Exception as e:
            logger.error(f"[GL2] Error loading GL2 data: {e}")
            return pd.DataFrame(columns=get_gl2_schema_columns())

    @reactive.calc
    def coa_data():
        """Load Chart of Accounts."""
        try:
            return load_COA_file()
        except Exception as e:
            logger.error(f"[GL2] Error loading COA: {e}")
            return pd.DataFrame()

    @reactive.calc
    def account_choices():
        """Generate account choices from GL2 and COA."""
        choices = {"": "All Accounts"}

        # From GL2 data
        gl2_df = gl2_data()
        if not gl2_df.empty and 'GL_Acct_Number' in gl2_df.columns:
            # Get unique account numbers and their names
            try:
                if 'GL_Acct_Name' in gl2_df.columns:
                    accounts = gl2_df.groupby('GL_Acct_Number').agg({'GL_Acct_Name': 'first'}).reset_index()
                    for _, row in accounts.iterrows():
                        acct_num = str(row['GL_Acct_Number'])
                        acct_name = str(row.get('GL_Acct_Name', ''))
                        if acct_num and acct_num != 'None' and acct_num != 'nan':
                            choices[acct_num] = f"{acct_num} - {acct_name}" if acct_name and acct_name != 'nan' else acct_num
                else:
                    # Fall back to just account numbers
                    for acct_num in gl2_df['GL_Acct_Number'].dropna().unique():
                        acct_num = str(acct_num)
                        if acct_num and acct_num != 'None' and acct_num != 'nan':
                            choices[acct_num] = acct_num
            except Exception as e:
                logger.warning(f"[GL2] Error building account choices from GL2: {e}")

        # From COA
        coa_df = coa_data()
        if not coa_df.empty:
            for _, row in coa_df.iterrows():
                try:
                    acct_num = str(int(row['GL_Acct_Number']))
                    if acct_num not in choices:
                        choices[acct_num] = f"{acct_num} - {row['GL_Acct_Name']}"
                except:
                    continue

        return choices

    @reactive.calc
    def category_choices():
        """Generate category/transaction_type choices from GL2 data."""
        choices = {"": "All Types"}
        gl2_df = gl2_data()
        if not gl2_df.empty and 'transaction_type' in gl2_df.columns:
            for cat in sorted(gl2_df['transaction_type'].dropna().unique()):
                if cat and str(cat).strip():
                    choices[str(cat)] = str(cat)
        return choices

    # =========================================================================
    # HEADER STATS (Always visible)
    # =========================================================================

    @output
    @render.text
    def gl2_header_entries():
        df = gl2_data()
        return f"{len(df):,}"

    @output
    @render.text
    def gl2_header_accounts():
        df = gl2_data()
        if df.empty:
            return "0"
        if 'GL_Acct_Number' in df.columns:
            return str(df['GL_Acct_Number'].nunique())
        if 'account_name' in df.columns:
            return str(df['account_name'].nunique())
        return "0"

    @output
    @render.text
    def gl2_header_debits():
        df = gl2_data()
        if df.empty or 'debit_crypto' not in df.columns:
            return "0.000000"
        total = sum(float(x) if pd.notna(x) else 0 for x in df['debit_crypto'])
        return f"{total:,.6f}"

    @output
    @render.text
    def gl2_header_credits():
        df = gl2_data()
        if df.empty or 'credit_crypto' not in df.columns:
            return "0.000000"
        total = sum(float(x) if pd.notna(x) else 0 for x in df['credit_crypto'])
        return f"{total:,.6f}"

    @output
    @render.text
    def gl2_header_balance():
        df = gl2_data()
        if df.empty:
            return "Balanced"
        debits = sum(float(x) if pd.notna(x) else 0 for x in df.get('debit_crypto', []))
        credits = sum(float(x) if pd.notna(x) else 0 for x in df.get('credit_crypto', []))
        diff = abs(debits - credits)
        return "Balanced" if diff < 0.000001 else f"Off: {diff:,.6f}"

    # =========================================================================
    # JOURNAL ENTRIES TAB
    # =========================================================================

    @reactive.calc
    def filtered_journal_entries():
        """Filter journal entries based on user selections."""
        df = gl2_data()
        if df.empty:
            return df

        # Quick search
        try:
            search = input.gl2_quick_search()
            if search:
                search_lower = search.lower()
                mask = pd.Series([False] * len(df))
                if 'hash' in df.columns:
                    mask |= df['hash'].astype(str).str.lower().str.contains(search_lower, na=False)
                if 'GL_Acct_Name' in df.columns:
                    mask |= df['GL_Acct_Name'].astype(str).str.lower().str.contains(search_lower, na=False)
                if 'GL_Acct_Number' in df.columns:
                    mask |= df['GL_Acct_Number'].astype(str).str.contains(search_lower, na=False)
                if 'account_name' in df.columns:
                    mask |= df['account_name'].astype(str).str.lower().str.contains(search_lower, na=False)
                df = df[mask]
        except:
            pass

        # Date filter
        try:
            date_range = input.gl2_date_range()
            if date_range and len(date_range) == 2:
                start_date = pd.Timestamp(date_range[0], tz='UTC')
                end_date = pd.Timestamp(date_range[1], tz='UTC') + pd.Timedelta(days=1)
                if 'date' in df.columns:
                    df = df[(df['date'] >= start_date) & (df['date'] < end_date)]
        except:
            pass

        # Account filter
        try:
            account = input.gl2_account_filter()
            if account:
                df = df[df['GL_Acct_Number'].astype(str) == str(account)]
        except:
            pass

        # Category/Type filter
        try:
            category = input.gl2_category_filter()
            if category:
                df = df[df['transaction_type'] == category]
        except:
            pass

        return df

    @output
    @render.text
    def gl2_total_entries():
        return f"{len(filtered_journal_entries()):,}"

    @output
    @render.text
    def gl2_total_debits():
        df = filtered_journal_entries()
        if df.empty or 'debit_crypto' not in df.columns:
            return "0.000000"
        total = sum(float(x) if pd.notna(x) else 0 for x in df['debit_crypto'])
        return f"{total:,.6f}"

    @output
    @render.text
    def gl2_total_credits():
        df = filtered_journal_entries()
        if df.empty or 'credit_crypto' not in df.columns:
            return "0.000000"
        total = sum(float(x) if pd.notna(x) else 0 for x in df['credit_crypto'])
        return f"{total:,.6f}"

    @output
    @render.text
    def gl2_balance_check():
        df = filtered_journal_entries()
        if df.empty:
            return "Balanced"
        debits = sum(float(x) if pd.notna(x) else 0 for x in df.get('debit_crypto', []))
        credits = sum(float(x) if pd.notna(x) else 0 for x in df.get('credit_crypto', []))
        diff = abs(debits - credits)
        return "Balanced" if diff < 0.000001 else f"Off: {diff:,.6f}"

    @output
    @render.data_frame
    def gl2_journal_entries_table():
        df = filtered_journal_entries()
        if df.empty:
            return render.DataGrid(
                pd.DataFrame({"Message": ["No journal entries found. Post entries from Decoded Transactions or create manual entries."]}),
                width="100%"
            )

        # Get rows per page setting
        try:
            rows_per_page = int(input.gl2_rows_per_page())
        except:
            rows_per_page = 100

        # Select display columns matching 37-column CSV format
        display_cols = [
            'date', 'hash', 'event', 'account_name',
            'debit_crypto', 'credit_crypto', 'debit_USD', 'credit_USD',
            'cryptocurrency', 'fund_id', 'fund_role',
            'loan_id', 'lender', 'borrower',
            'collateral_address', 'token_id',
            'principal_crypto', 'annual_interest_rate', 'loan_due_date',
            'contract_address', 'platform', 'transaction_type',
            'row_key',
        ]

        display_df = df[[c for c in display_cols if c in df.columns]].copy()

        # Sort by date descending
        if 'date' in display_df.columns:
            display_df = display_df.sort_values('date', ascending=False)
            display_df['date'] = pd.to_datetime(display_df['date'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M')

        # Format numeric columns
        for col in ['debit_crypto', 'credit_crypto', 'debit_USD', 'credit_USD', 'principal_crypto']:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(
                    lambda x: f"{float(x):,.6f}" if pd.notna(x) and float(x) != 0 else ""
                )

        if 'annual_interest_rate' in display_df.columns:
            display_df['annual_interest_rate'] = display_df['annual_interest_rate'].apply(
                lambda x: f"{float(x):.2f}%" if pd.notna(x) and float(x) != 0 else ""
            )

        # Shorten hash and addresses
        for col in ['hash', 'lender', 'borrower', 'collateral_address', 'contract_address']:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(
                    lambda x: f"{x[:12]}..." if pd.notna(x) and len(str(x)) > 12 else x
                )

        # Format loan_due_date
        if 'loan_due_date' in display_df.columns:
            display_df['loan_due_date'] = display_df['loan_due_date'].apply(
                lambda x: str(x)[:10] if pd.notna(x) and str(x) not in ('', 'nan') else ""
            )

        # Rename columns for display
        col_rename = {
            'date': 'Date',
            'hash': 'TX Hash',
            'event': 'Event',
            'account_name': 'Account',
            'debit_crypto': 'Debit',
            'credit_crypto': 'Credit',
            'debit_USD': 'Debit USD',
            'credit_USD': 'Credit USD',
            'cryptocurrency': 'Asset',
            'fund_id': 'Fund',
            'fund_role': 'Role',
            'loan_id': 'Loan ID',
            'lender': 'Lender',
            'borrower': 'Borrower',
            'collateral_address': 'Collateral',
            'token_id': 'Token ID',
            'principal_crypto': 'Principal',
            'annual_interest_rate': 'APR',
            'loan_due_date': 'Due Date',
            'contract_address': 'Contract',
            'platform': 'Platform',
            'transaction_type': 'Type',
            'row_key': 'Row Key',
        }
        display_df = display_df.rename(columns={k: v for k, v in col_rename.items() if k in display_df.columns})

        # Calculate height based on rows_per_page (approximately 40px per row + 60px for header)
        table_height = f"{min(rows_per_page * 40 + 60, 2000)}px"

        # Use DataGrid with dynamic height
        return render.DataGrid(
            display_df,
            filters=True,
            selection_mode="rows",
            width="100%",
            height=table_height,
            row_selection_mode="single"
        )

    @output
    @render.ui
    def gl2_entry_detail_panel():
        """Show details for selected entry with edit functionality."""
        # Get selected rows from the data grid
        selected = input.gl2_journal_entries_table_selected_rows()
        if not selected:
            return ui.div()

        df = filtered_journal_entries()
        if df.empty or len(selected) == 0:
            return ui.div()

        # Get first selected row
        idx = selected[0]
        if idx >= len(df):
            return ui.div()

        row = df.iloc[idx]
        row_key = str(row.get('row_key', ''))

        # Check if we're in edit mode for this row
        is_editing = edit_mode_active() and editing_row_key() == row_key

        if is_editing:
            # EDIT MODE - Show editable form
            coa = coa_data()
            acct_choices = {}
            if not coa.empty:
                for _, coa_row in coa.iterrows():
                    try:
                        acct_num = str(int(coa_row['GL_Acct_Number']))
                        acct_choices[acct_num] = f"{acct_num} - {coa_row['GL_Acct_Name']}"
                    except:
                        continue

            return ui.card(
                ui.card_header(
                    ui.div(
                        ui.HTML('<i class="bi bi-pencil-square"></i> '),
                        ui.strong("Edit Entry"),
                        ui.div(
                            ui.input_action_button(
                                "gl2_save_edit",
                                ui.HTML('<i class="bi bi-check-lg"></i> Save'),
                                class_="btn-success btn-sm me-2"
                            ),
                            ui.input_action_button(
                                "gl2_cancel_edit",
                                ui.HTML('<i class="bi bi-x-lg"></i> Cancel'),
                                class_="btn-outline-secondary btn-sm"
                            ),
                            class_="d-flex"
                        ),
                        class_="d-flex justify-content-between align-items-center"
                    )
                ),
                ui.card_body(
                    # Hidden field for row_key
                    ui.tags.input(type="hidden", id="gl2_edit_row_key", value=row_key),

                    ui.layout_columns(
                        ui.div(
                            ui.tags.label("Date", class_="gl2-field-label"),
                            ui.input_date("gl2_edit_date", None, value=str(row.get('date', ''))[:10]),
                        ),
                        ui.div(
                            ui.tags.label("Transaction Type", class_="gl2-field-label"),
                            ui.input_text("gl2_edit_transaction_type", None, value=str(row.get('transaction_type', ''))),
                        ),
                        ui.div(
                            ui.tags.label("Fund ID", class_="gl2-field-label"),
                            ui.input_text("gl2_edit_fund_id", None, value=str(row.get('fund_id', ''))),
                        ),
                        col_widths=[4, 4, 4]
                    ),

                    ui.layout_columns(
                        ui.div(
                            ui.tags.label("GL Account Number", class_="gl2-field-label"),
                            ui.input_selectize("gl2_edit_gl_acct_number", None,
                                choices=acct_choices,
                                selected=str(row.get('GL_Acct_Number', '')),
                                width="100%"
                            ),
                        ),
                        ui.div(
                            ui.tags.label("Cryptocurrency", class_="gl2-field-label"),
                            ui.input_select("gl2_edit_cryptocurrency", None,
                                choices={"ETH": "ETH", "WETH": "WETH", "USDC": "USDC", "USDT": "USDT"},
                                selected=str(row.get('cryptocurrency', 'ETH'))
                            ),
                        ),
                        col_widths=[8, 4]
                    ),

                    ui.hr(),

                    ui.layout_columns(
                        ui.div(
                            ui.tags.label("Debit (Crypto)", class_="gl2-field-label"),
                            ui.input_numeric("gl2_edit_debit_crypto", None,
                                value=float(row.get('debit_crypto', 0) or 0),
                                min=0, step=0.000001
                            ),
                        ),
                        ui.div(
                            ui.tags.label("Credit (Crypto)", class_="gl2-field-label"),
                            ui.input_numeric("gl2_edit_credit_crypto", None,
                                value=float(row.get('credit_crypto', 0) or 0),
                                min=0, step=0.000001
                            ),
                        ),
                        ui.div(
                            ui.tags.label("ETH/USD Price", class_="gl2-field-label"),
                            ui.input_numeric("gl2_edit_eth_usd_price", None,
                                value=float(row.get('eth_usd_price', 0) or 0),
                                min=0, step=0.01
                            ),
                        ),
                        col_widths=[4, 4, 4]
                    ),

                    ui.layout_columns(
                        ui.div(
                            ui.tags.label("Debit (USD)", class_="gl2-field-label"),
                            ui.input_numeric("gl2_edit_debit_usd", None,
                                value=float(row.get('debit_USD', 0) or 0),
                                min=0, step=0.01
                            ),
                        ),
                        ui.div(
                            ui.tags.label("Credit (USD)", class_="gl2-field-label"),
                            ui.input_numeric("gl2_edit_credit_usd", None,
                                value=float(row.get('credit_USD', 0) or 0),
                                min=0, step=0.01
                            ),
                        ),
                        col_widths=[6, 6]
                    ),

                    ui.hr(),

                    ui.layout_columns(
                        ui.div(
                            ui.tags.label("Loan ID", class_="gl2-field-label"),
                            ui.input_text("gl2_edit_loan_id", None, value=str(row.get('loan_id', '') or '')),
                        ),
                        ui.div(
                            ui.tags.label("Contract Address", class_="gl2-field-label"),
                            ui.input_text("gl2_edit_contract_address", None, value=str(row.get('contract_address', '') or '')),
                        ),
                        col_widths=[6, 6]
                    ),

                    ui.layout_columns(
                        ui.div(
                            ui.tags.label("Lender", class_="gl2-field-label"),
                            ui.input_text("gl2_edit_lender", None, value=str(row.get('lender', '') or '')),
                        ),
                        ui.div(
                            ui.tags.label("Borrower", class_="gl2-field-label"),
                            ui.input_text("gl2_edit_borrower", None, value=str(row.get('borrower', '') or '')),
                        ),
                        col_widths=[6, 6]
                    ),

                    ui.layout_columns(
                        ui.div(
                            ui.tags.label("Principal (Crypto)", class_="gl2-field-label"),
                            ui.input_numeric("gl2_edit_principal_crypto", None,
                                value=float(row.get('principal_crypto', 0) or 0),
                                min=0, step=0.000001
                            ),
                        ),
                        ui.div(
                            ui.tags.label("Annual Interest Rate", class_="gl2-field-label"),
                            ui.input_numeric("gl2_edit_annual_interest_rate", None,
                                value=float(row.get('annual_interest_rate', 0) or 0),
                                min=0, step=0.0001
                            ),
                        ),
                        col_widths=[6, 6]
                    ),

                    ui.tags.small(
                        f"Row Key: {row_key}",
                        class_="text-muted d-block mt-3"
                    ),
                ),
                class_="mt-3 gl2-edit-form"
            )
        else:
            # VIEW MODE - Show entry details with Edit button and Etherscan link
            tx_hash = str(row.get('hash', ''))
            # Build Etherscan URL if we have a valid transaction hash
            etherscan_url = None
            if tx_hash and tx_hash.startswith('0x') and len(tx_hash) >= 66:
                etherscan_url = f"https://etherscan.io/tx/{tx_hash}"

            return ui.card(
                ui.card_header(
                    ui.div(
                        ui.strong("Entry Details"),
                        ui.div(
                            ui.tags.a(
                                ui.HTML('<i class="bi bi-box-arrow-up-right"></i> Etherscan'),
                                href=etherscan_url if etherscan_url else "#",
                                target="_blank",
                                class_=f"btn btn-outline-info btn-sm me-2 {'disabled' if not etherscan_url else ''}"
                            ) if etherscan_url else ui.span(),
                            ui.input_action_button(
                                "gl2_start_edit",
                                ui.HTML('<i class="bi bi-pencil"></i> Edit Entry'),
                                class_="btn-primary btn-sm"
                            ),
                            class_="d-flex"
                        ),
                        class_="d-flex justify-content-between align-items-center"
                    )
                ),
                ui.card_body(
                    ui.layout_columns(
                        ui.div(
                            ui.strong("TX Hash: "), str(row.get('hash', 'N/A')),
                        ),
                        ui.div(
                            ui.strong("Date: "), str(row.get('date', 'N/A'))[:19],
                        ),
                        ui.div(
                            ui.strong("Fund: "), str(row.get('fund_id', 'N/A')),
                        ),
                        col_widths=[6, 3, 3]
                    ),
                    ui.hr(),
                    ui.layout_columns(
                        ui.div(
                            ui.strong("Account: "), str(row.get('account_name', 'N/A')),
                        ),
                        ui.div(
                            ui.strong("Event: "), str(row.get('event', 'N/A')),
                        ),
                        ui.div(
                            ui.strong("Role: "), str(row.get('fund_role', 'N/A')),
                        ),
                        col_widths=[4, 4, 4]
                    ),
                    ui.hr(),
                    ui.layout_columns(
                        ui.div(
                            ui.strong("Debit: "), f"{float(row.get('debit_crypto', 0) or 0):,.6f}",
                            class_="text-success"
                        ),
                        ui.div(
                            ui.strong("Credit: "), f"{float(row.get('credit_crypto', 0) or 0):,.6f}",
                            style="color: #4f46e5;"
                        ),
                        ui.div(
                            ui.strong("Asset: "), str(row.get('cryptocurrency', 'N/A')),
                        ),
                        col_widths=[4, 4, 4]
                    ),
                    ui.layout_columns(
                        ui.div(
                            ui.strong("Debit USD: "), f"${float(row.get('debit_USD', 0) or 0):,.2f}",
                        ),
                        ui.div(
                            ui.strong("Credit USD: "), f"${float(row.get('credit_USD', 0) or 0):,.2f}",
                        ),
                        ui.div(
                            ui.strong("ETH/USD: "), f"${float(row.get('eth_usd_price', 0) or 0):,.2f}",
                        ),
                        col_widths=[4, 4, 4]
                    ),
                    ui.hr(),
                    ui.layout_columns(
                        ui.div(
                            ui.strong("Loan ID: "), str(row.get('loan_id', 'N/A') or 'N/A'),
                        ),
                        ui.div(
                            ui.strong("Principal: "), f"{float(row.get('principal_crypto', 0) or 0):,.6f}" if row.get('principal_crypto') else 'N/A',
                        ),
                        ui.div(
                            ui.strong("APR: "), f"{float(row.get('annual_interest_rate', 0) or 0):.2f}%" if row.get('annual_interest_rate') else 'N/A',
                        ),
                        col_widths=[4, 4, 4]
                    ),
                    ui.layout_columns(
                        ui.div(
                            ui.strong("Lender: "), str(row.get('lender', 'N/A') or 'N/A'),
                        ),
                        ui.div(
                            ui.strong("Borrower: "), str(row.get('borrower', 'N/A') or 'N/A'),
                        ),
                        ui.div(
                            ui.strong("Due Date: "), str(row.get('loan_due_date', 'N/A') or 'N/A')[:10],
                        ),
                        col_widths=[4, 4, 4]
                    ),
                    ui.layout_columns(
                        ui.div(
                            ui.strong("Collateral: "), str(row.get('collateral_address', 'N/A') or 'N/A'),
                        ),
                        ui.div(
                            ui.strong("Token ID: "), str(row.get('token_id', 'N/A') or 'N/A'),
                        ),
                        ui.div(
                            ui.strong("Contract: "), str(row.get('contract_address', 'N/A') or 'N/A'),
                        ),
                        col_widths=[4, 4, 4]
                    ),
                    ui.tags.small(
                        f"Row Key: {row_key}",
                        class_="text-muted d-block mt-3"
                    ),
                ),
                class_="mt-3"
            )

    # Update dropdowns using reactive effects (not render.ui)
    @reactive.effect
    def _update_account_dropdown():
        """Update account filter dropdown when data changes."""
        choices = account_choices()
        ui.update_selectize("gl2_account_filter", choices=choices, selected="")

    @reactive.effect
    def _update_category_dropdown():
        """Update category filter dropdown when data changes."""
        choices = category_choices()
        ui.update_selectize("gl2_category_filter", choices=choices, selected="")

    # Refresh button
    @reactive.effect
    @reactive.event(input.gl2_refresh)
    def _refresh_gl2_data():
        clear_GL2_cache()
        gl2_data_version.set(gl2_data_version() + 1)
        ui.notification_show("GL2 data refreshed", type="message")

    # Clear filters
    @reactive.effect
    @reactive.event(input.gl2_clear_filters)
    def _clear_filters():
        ui.update_selectize("gl2_account_filter", selected="")
        ui.update_selectize("gl2_category_filter", selected="")
        ui.update_text("gl2_quick_search", value="")

    # Start edit mode
    @reactive.effect
    @reactive.event(input.gl2_start_edit)
    def _start_edit():
        selected = input.gl2_journal_entries_table_selected_rows()
        if not selected:
            return

        df = filtered_journal_entries()
        if df.empty or len(selected) == 0:
            return

        idx = selected[0]
        if idx >= len(df):
            return

        row = df.iloc[idx]
        row_key = str(row.get('row_key', ''))

        if row_key:
            edit_mode_active.set(True)
            editing_row_key.set(row_key)
            logger.info(f"[GL2] Started editing entry: {row_key}")

    # Cancel edit mode
    @reactive.effect
    @reactive.event(input.gl2_cancel_edit)
    def _cancel_edit():
        edit_mode_active.set(False)
        editing_row_key.set(None)
        logger.info("[GL2] Cancelled editing")

    # Save edited entry to S3
    @reactive.effect
    @reactive.event(input.gl2_save_edit)
    def _save_edit():
        row_key = editing_row_key()
        if not row_key:
            ui.notification_show("No entry selected for editing", type="error")
            return

        # Get the full GL2 data
        full_df = load_GL2_file()
        if full_df.empty:
            ui.notification_show("No GL2 data found", type="error")
            return

        # Normalize columns for lookup
        full_df = normalize_gl2_columns(full_df)

        # Find the row to update
        if 'row_key' not in full_df.columns:
            ui.notification_show("row_key column not found in GL2 data", type="error")
            return

        mask = full_df['row_key'] == row_key
        if not mask.any():
            ui.notification_show(f"Entry not found: {row_key}", type="error")
            return

        try:
            # Get edited values from form inputs
            edit_date = input.gl2_edit_date()
            edit_transaction_type = input.gl2_edit_transaction_type()
            edit_fund_id = input.gl2_edit_fund_id()
            edit_gl_acct_number = input.gl2_edit_gl_acct_number()
            edit_cryptocurrency = input.gl2_edit_cryptocurrency()
            edit_debit_crypto = input.gl2_edit_debit_crypto() or 0
            edit_credit_crypto = input.gl2_edit_credit_crypto() or 0
            edit_eth_usd_price = input.gl2_edit_eth_usd_price() or 0
            edit_debit_usd = input.gl2_edit_debit_usd() or 0
            edit_credit_usd = input.gl2_edit_credit_usd() or 0
            edit_loan_id = input.gl2_edit_loan_id()
            edit_contract_address = input.gl2_edit_contract_address()
            edit_lender = input.gl2_edit_lender()
            edit_borrower = input.gl2_edit_borrower()
            edit_principal_crypto = input.gl2_edit_principal_crypto() or 0
            edit_annual_interest_rate = input.gl2_edit_annual_interest_rate() or 0

            # Build account_name from COA lookup
            gl_acct_name = ""
            coa = coa_data()
            if not coa.empty and edit_gl_acct_number:
                match = coa[coa['GL_Acct_Number'].astype(str) == str(edit_gl_acct_number)]
                if not match.empty:
                    gl_acct_name = match.iloc[0]['GL_Acct_Name']

            # Update the row
            row_idx = mask.idxmax()

            # Update date - convert to timestamp
            if edit_date:
                full_df.loc[row_idx, 'date'] = pd.Timestamp(edit_date, tz='UTC')

            # Update fields using 37-column format
            full_df.loc[row_idx, 'transaction_type'] = edit_transaction_type
            full_df.loc[row_idx, 'fund_id'] = edit_fund_id
            # Store as account_name in "100.30 - ETH Wallet" format
            if gl_acct_name:
                full_df.loc[row_idx, 'account_name'] = f"{edit_gl_acct_number} - {gl_acct_name}"
            elif edit_gl_acct_number:
                full_df.loc[row_idx, 'account_name'] = edit_gl_acct_number
            full_df.loc[row_idx, 'cryptocurrency'] = edit_cryptocurrency
            full_df.loc[row_idx, 'debit_crypto'] = float(edit_debit_crypto)
            full_df.loc[row_idx, 'credit_crypto'] = float(edit_credit_crypto)
            full_df.loc[row_idx, 'eth_usd_price'] = float(edit_eth_usd_price)
            full_df.loc[row_idx, 'debit_USD'] = float(edit_debit_usd)
            full_df.loc[row_idx, 'credit_USD'] = float(edit_credit_usd)
            full_df.loc[row_idx, 'loan_id'] = edit_loan_id if edit_loan_id else None
            full_df.loc[row_idx, 'contract_address'] = edit_contract_address if edit_contract_address else None
            full_df.loc[row_idx, 'lender'] = edit_lender if edit_lender else None
            full_df.loc[row_idx, 'borrower'] = edit_borrower if edit_borrower else None
            full_df.loc[row_idx, 'principal_crypto'] = float(edit_principal_crypto) if edit_principal_crypto else None
            full_df.loc[row_idx, 'annual_interest_rate'] = float(edit_annual_interest_rate) if edit_annual_interest_rate else None

            # Save to S3
            if save_GL2_file(full_df):
                ui.notification_show("Entry updated successfully", type="message")
                edit_mode_active.set(False)
                editing_row_key.set(None)
                clear_GL2_cache()
                gl2_data_version.set(gl2_data_version() + 1)
                logger.info(f"[GL2] Saved edited entry: {row_key}")
            else:
                ui.notification_show("Failed to save changes to S3", type="error")

        except Exception as e:
            logger.error(f"[GL2] Error saving edit: {e}")
            ui.notification_show(f"Error saving: {str(e)}", type="error")

    # Delete selected entries
    @reactive.effect
    @reactive.event(input.gl2_delete_selected)
    def _delete_selected():
        selected = input.gl2_journal_entries_table_selected_rows()
        if not selected:
            ui.notification_show("No entries selected", type="warning")
            return

        df = filtered_journal_entries()
        full_df = gl2_data()

        if df.empty:
            return

        # Get row_keys to delete
        row_keys_to_delete = []
        for idx in selected:
            if idx < len(df) and 'row_key' in df.columns:
                row_keys_to_delete.append(df.iloc[idx]['row_key'])

        if not row_keys_to_delete:
            ui.notification_show("Could not identify entries to delete", type="error")
            return

        # Remove from full dataset
        updated_df = full_df[~full_df['row_key'].isin(row_keys_to_delete)]

        # Save
        try:
            if save_GL2_file(updated_df):
                ui.notification_show(f"Deleted {len(row_keys_to_delete)} entries", type="message")
                clear_GL2_cache()
                gl2_data_version.set(gl2_data_version() + 1)
            else:
                ui.notification_show("Failed to save changes", type="error")
        except Exception as e:
            ui.notification_show(f"Error: {str(e)}", type="error")

    # Reverse selected entries
    @reactive.effect
    @reactive.event(input.gl2_reverse_selected)
    def _reverse_selected():
        selected = input.gl2_journal_entries_table_selected_rows()
        if not selected:
            ui.notification_show("No entries selected", type="warning")
            return

        df = filtered_journal_entries()
        full_df = gl2_data()

        if df.empty:
            return

        # Create reversing entries using new schema
        reversal_records = []
        timestamp = datetime.now(timezone.utc)

        for idx in selected:
            if idx >= len(df):
                continue

            row = df.iloc[idx]
            row_key_val = row.get('row_key', '')
            tx_hash = f"reversal_{hashlib.md5(f'{row_key_val}{timestamp}'.encode()).hexdigest()[:16]}"

            # Swap debits and credits using 37-column format
            reversal = {
                'date': timestamp,
                'transaction_type': 'reversal',
                'platform': row.get('platform', ''),
                'fund_id': row.get('fund_id', ''),
                'counterparty_fund_id': row.get('counterparty_fund_id', ''),
                'wallet_id': row.get('wallet_id', ''),
                'cryptocurrency': row.get('cryptocurrency', 'ETH'),
                'account_name': row.get('account_name', ''),
                'debit_crypto': float(row.get('credit_crypto', 0)),
                'credit_crypto': float(row.get('debit_crypto', 0)),
                'eth_usd_price': float(row.get('eth_usd_price', 0)),
                'debit_USD': float(row.get('credit_USD', 0)),
                'credit_USD': float(row.get('debit_USD', 0)),
                'hash': tx_hash,
                'event': 'Reversal',
                'loan_id': row.get('loan_id'),
                'lender': row.get('lender'),
                'borrower': row.get('borrower'),
                'from': row.get('from'),
                'to': row.get('to'),
                'contract_address': row.get('contract_address'),
                'payable_currency': row.get('payable_currency'),
                'collateral_address': row.get('collateral_address'),
                'token_id': row.get('token_id'),
                'principal_crypto': row.get('principal_crypto'),
                'principal_USD': row.get('principal_USD'),
                'payoff_amount_crypto': row.get('payoff_amount_crypto'),
                'payoff_amount_USD': row.get('payoff_amount_USD'),
                'annual_interest_rate': row.get('annual_interest_rate'),
                'loan_due_date': row.get('loan_due_date'),
                'tranche_floor': row.get('tranche_floor'),
                'tranche_index': row.get('tranche_index'),
                'fund_role': row.get('fund_role'),
                'origination_fee': row.get('origination_fee'),
                'net_origination_fee': row.get('net_origination_fee'),
                'source_file': '',
                'notes': f"Reversal of {row_key_val}",
                'row_key': f"{tx_hash}:{row.get('account_name', '')}:reversal"
            }
            reversal_records.append(reversal)

        if not reversal_records:
            ui.notification_show("No entries to reverse", type="warning")
            return

        # Add reversals to data
        try:
            reversal_df = pd.DataFrame(reversal_records)
            combined = pd.concat([full_df, reversal_df], ignore_index=True)

            if save_GL2_file(combined):
                ui.notification_show(f"Created {len(reversal_records)} reversing entries", type="message")
                clear_GL2_cache()
                gl2_data_version.set(gl2_data_version() + 1)
            else:
                ui.notification_show("Failed to save reversals", type="error")
        except Exception as e:
            ui.notification_show(f"Error: {str(e)}", type="error")

    # =========================================================================
    # ACCOUNT LEDGER TAB
    # =========================================================================

    @reactive.effect
    def _update_ledger_account_dropdown():
        """Update ledger account dropdown when data changes."""
        choices = account_choices()
        if "" in choices:
            del choices[""]
        choices = {"": "Select an account...", **choices}
        ui.update_selectize("gl2_ledger_account", choices=choices, selected="")

    @reactive.calc
    def account_ledger_data():
        """Get ledger data for selected account."""
        df = gl2_data()
        account = input.gl2_ledger_account()

        if df.empty or not account:
            return pd.DataFrame()

        df = df[df['GL_Acct_Number'].astype(str) == str(account)].copy()

        if df.empty:
            return df

        # Date filter
        try:
            date_range = input.gl2_ledger_date_range()
            if date_range and len(date_range) == 2:
                start_date = pd.Timestamp(date_range[0], tz='UTC')
                end_date = pd.Timestamp(date_range[1], tz='UTC') + pd.Timedelta(days=1)
                if 'date' in df.columns:
                    df = df[(df['date'] >= start_date) & (df['date'] < end_date)]
        except:
            pass

        # Sort and calculate running balance
        if 'date' in df.columns:
            df = df.sort_values('date')

        running = 0.0
        balances = []
        for _, row in df.iterrows():
            debit = float(row['debit_crypto']) if pd.notna(row.get('debit_crypto')) else 0
            credit = float(row['credit_crypto']) if pd.notna(row.get('credit_crypto')) else 0
            running += debit - credit
            balances.append(running)

        df['running_balance'] = balances
        return df

    @output
    @render.text
    def gl2_ledger_account_name():
        account = input.gl2_ledger_account()
        if not account:
            return "Select an Account"

        coa = coa_data()
        if not coa.empty:
            match = coa[coa['GL_Acct_Number'].astype(str) == account]
            if not match.empty:
                return f"{account} - {match.iloc[0]['GL_Acct_Name']}"

        return f"Account {account}"

    @output
    @render.ui
    def gl2_account_summary():
        df = account_ledger_data()
        if df.empty:
            return ui.div(
                ui.p("Select an account to view ledger.", class_="text-muted text-center py-2"),
            )

        total_debits = sum(float(x) if pd.notna(x) else 0 for x in df['debit_crypto'])
        total_credits = sum(float(x) if pd.notna(x) else 0 for x in df['credit_crypto'])
        balance = total_debits - total_credits
        entry_count = len(df)

        # Compact inline summary instead of large value boxes
        return ui.div(
            ui.div(
                ui.span(f"Entries: ", class_="text-muted"),
                ui.strong(f"{entry_count:,}", class_="me-4"),
                ui.span(f"Debits: ", class_="text-muted"),
                ui.strong(f"{total_debits:,.6f}", class_="text-success me-4"),
                ui.span(f"Credits: ", class_="text-muted"),
                ui.strong(f"{total_credits:,.6f}", style="color: #4f46e5;", class_="me-4"),
                ui.span(f"Balance: ", class_="text-muted"),
                ui.strong(f"{balance:,.6f}", class_="text-info"),
                class_="py-2 px-3 bg-light rounded d-flex align-items-center flex-wrap gap-2"
            ),
            class_="mb-2 mt-2 px-3"
        )

    @output
    @render.data_frame
    def gl2_account_ledger_table():
        df = account_ledger_data()
        if df.empty:
            return render.DataGrid(
                pd.DataFrame({"Message": ["Select an account to view entries."]}),
                width="100%"
            )

        # Use same columns as Journal Entries table plus running_balance
        display_cols = ['date', 'hash', 'event', 'account_name',
                       'transaction_type', 'debit_crypto', 'credit_crypto',
                       'running_balance', 'cryptocurrency', 'fund_id', 'fund_role', 'row_key']

        display_df = df[[c for c in display_cols if c in df.columns]].copy()

        # Sort by date descending
        if 'date' in display_df.columns:
            display_df = display_df.sort_values('date', ascending=False)
            display_df['date'] = pd.to_datetime(display_df['date']).dt.strftime('%Y-%m-%d %H:%M')

        # Recalculate running balance after sort (in chronological order for balance)
        if 'running_balance' in display_df.columns:
            # We need to recalculate since we sorted
            temp_df = df.sort_values('date', ascending=True)
            running = 0.0
            balances = {}
            for idx, row in temp_df.iterrows():
                debit = float(row['debit_crypto']) if pd.notna(row.get('debit_crypto')) else 0
                credit = float(row['credit_crypto']) if pd.notna(row.get('credit_crypto')) else 0
                running += debit - credit
                balances[idx] = running
            display_df['running_balance'] = display_df.index.map(balances)

        # Format numeric columns
        for col in ['debit_crypto', 'credit_crypto', 'running_balance']:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(
                    lambda x: f"{float(x):,.6f}" if pd.notna(x) else ""
                )

        # Shorten hash
        if 'hash' in display_df.columns:
            display_df['hash'] = display_df['hash'].apply(
                lambda x: f"{x[:12]}..." if pd.notna(x) and len(str(x)) > 12 else x
            )

        # Rename columns for display
        col_rename = {
            'date': 'Date',
            'hash': 'TX Hash',
            'event': 'Event',
            'account_name': 'Account',
            'transaction_type': 'Type',
            'debit_crypto': 'Debit',
            'credit_crypto': 'Credit',
            'running_balance': 'Balance',
            'cryptocurrency': 'Asset',
            'fund_id': 'Fund',
            'fund_role': 'Role',
            'row_key': 'Row Key'
        }
        display_df = display_df.rename(columns={k: v for k, v in col_rename.items() if k in display_df.columns})

        # Calculate height based on rows (same logic as journal entries)
        try:
            rows_per_page = int(input.gl2_rows_per_page())
        except:
            rows_per_page = 100
        table_height = f"{min(rows_per_page * 40 + 60, 2000)}px"

        return render.DataGrid(
            display_df,
            filters=True,
            selection_mode="rows",
            width="100%",
            height=table_height,
            row_selection_mode="single"
        )

    # =========================================================================
    # TRIAL BALANCE TAB
    # Ported from production notebook: realworld_nav_drip_capital_TB_production_v1.ipynb
    # =========================================================================

    def _compute_month_end_tb(df, group_cols):
        """Compute cumulative trial balance at each month end.
        Ported from production notebook cell 478."""
        if df.empty or 'date' not in df.columns:
            return pd.DataFrame()

        df = df.copy()

        # Convert Decimal columns to float for aggregation
        for col in ['debit_crypto', 'credit_crypto', 'debit_USD', 'credit_USD']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

        # Fill empty fund_id
        if 'fund_id' in df.columns:
            df['fund_id'] = df['fund_id'].fillna('(No Fund)').replace('', '(No Fund)')

        if df['date'].dt.tz is not None:
            df['date'] = df['date'].dt.tz_localize(None)
        df['year_month'] = df['date'].dt.to_period('M')
        unique_months = sorted(df['year_month'].unique())

        trial_balances = []
        for year_month in unique_months:
            next_month_start = (year_month + 1).to_timestamp(how='start')
            month_end_label = year_month.to_timestamp(how='end')

            df_through_month = df[df['date'] < next_month_start]

            # Only group by columns that exist
            valid_group_cols = [c for c in group_cols if c in df_through_month.columns]
            if not valid_group_cols:
                continue

            tb = df_through_month.groupby(valid_group_cols).agg({
                'debit_crypto': 'sum',
                'credit_crypto': 'sum',
                'debit_USD': 'sum',
                'credit_USD': 'sum'
            }).reset_index()

            tb['net_debit_credit_crypto'] = tb['debit_crypto'] - tb['credit_crypto']
            tb['net_debit_credit_USD'] = tb['debit_USD'] - tb['credit_USD']
            tb['month_end'] = month_end_label

            trial_balances.append(tb)

        if not trial_balances:
            return pd.DataFrame()

        result = pd.concat(trial_balances, ignore_index=True)
        sort_cols = ['month_end'] + [c for c in group_cols if c in result.columns]
        result = result.sort_values(sort_cols).reset_index(drop=True)
        return result

    def _get_category(acct_num):
        """Categorize account by first digit of GL account number.
        1xxxx=Assets, 2xxxx=Liabilities, 3xxxx=Equity, 4xxxx=Revenue,
        5xxxx/6xxxx/8xxxx=Expenses, 9xxxx=Other Income."""
        try:
            acct_str = str(acct_num).split('.')[0].strip()
            if not acct_str:
                return "9. Other"
            first_digit = acct_str[0]
            if first_digit == '1': return "1. Assets"
            elif first_digit == '2': return "2. Liabilities"
            elif first_digit == '3': return "3. Equity"
            elif first_digit == '4': return "4. Revenue"
            elif first_digit in ('5', '6', '8'): return "5. Expenses"
            elif first_digit == '9': return "6. Other Income"
            else: return "9. Other"
        except:
            return "9. Other"

    def _enrich_gl_acct_number(df):
        """Map account_name to GL_Acct_Number using COA.
        Ported from production notebook cell 281.
        The GL2 37-col format stores account_name as snake_case (e.g. 'digital_assets_eth')
        without a GL number prefix, so we must look it up from the COA."""
        coa = coa_data()
        if coa.empty or 'account_name' not in coa.columns or 'GL_Acct_Number' not in coa.columns:
            return df

        df = df.copy()

        # Build mapping: account_name -> GL_Acct_Number (as string)
        gl_number_map = coa.drop_duplicates('account_name').set_index('account_name')['GL_Acct_Number']
        gl_number_map = gl_number_map.astype(str).to_dict()

        # Fill GL_Acct_Number where it's missing or empty
        if 'GL_Acct_Number' not in df.columns:
            df['GL_Acct_Number'] = ''

        empty_mask = (df['GL_Acct_Number'].isna()) | (df['GL_Acct_Number'] == '') | (df['GL_Acct_Number'] == 'nan')
        if empty_mask.any() and 'account_name' in df.columns:
            df.loc[empty_mask, 'GL_Acct_Number'] = df.loc[empty_mask, 'account_name'].map(gl_number_map)
            # Fill any still-missing with empty string
            df['GL_Acct_Number'] = df['GL_Acct_Number'].fillna('')

        return df

    @reactive.calc
    def all_month_end_trial_balances():
        """Compute cumulative trial balance at each month end.
        Groups by fund_id, GL_Acct_Number, account_name.
        Ported from production notebook cell 478."""
        df = gl2_data()
        if df.empty:
            return pd.DataFrame()
        df = _enrich_gl_acct_number(df)
        return _compute_month_end_tb(df, ['fund_id', 'GL_Acct_Number', 'account_name'])

    @reactive.calc
    def all_month_end_tb_by_wallet():
        """Compute cumulative trial balance at each month end, broken out by wallet.
        Groups by fund_id, wallet_id, GL_Acct_Number, account_name.
        Ported from production notebook cell 480."""
        df = gl2_data()
        if df.empty:
            return pd.DataFrame()
        df = _enrich_gl_acct_number(df)
        return _compute_month_end_tb(df, ['fund_id', 'wallet_id', 'GL_Acct_Number', 'account_name'])

    @reactive.calc
    def tb_period_choices():
        """Extract unique month-end periods for the period selector."""
        df_tb = all_month_end_trial_balances()
        if df_tb.empty or 'month_end' not in df_tb.columns:
            return {}
        periods = sorted(df_tb['month_end'].unique(), reverse=True)
        return {str(p): pd.Timestamp(p).strftime('%B %Y') for p in periods}

    @reactive.calc
    def tb_fund_choices():
        """Extract unique fund_ids for the fund filter."""
        df_tb = all_month_end_trial_balances()
        if df_tb.empty or 'fund_id' not in df_tb.columns:
            return {"": "All Funds"}
        funds = sorted(df_tb['fund_id'].dropna().unique())
        choices = {"": "All Funds"}
        choices.update({f: f for f in funds})
        return choices

    @reactive.effect
    def _update_tb_period_choices():
        choices = tb_period_choices()
        if choices:
            ui.update_select("gl2_tb_period", choices=choices)

    @reactive.effect
    def _update_tb_fund_choices():
        choices = tb_fund_choices()
        if choices:
            ui.update_select("gl2_tb_fund", choices=choices)

    @reactive.calc
    def filtered_trial_balance():
        """Filter TB by selected period and fund.
        Returns the trial balance DataFrame for the selected view."""
        view_mode = "pivot"
        try:
            view_mode = input.gl2_tb_view()
        except:
            pass

        # Choose data source based on view mode
        if view_mode == "wallet":
            df_tb = all_month_end_tb_by_wallet()
        else:
            df_tb = all_month_end_trial_balances()

        if df_tb.empty:
            return pd.DataFrame()

        # Filter by selected period
        try:
            selected_period = input.gl2_tb_period()
            if selected_period:
                df_tb = df_tb[df_tb['month_end'] == pd.Timestamp(selected_period)]
        except:
            pass

        # Default to most recent period if filter produced nothing or no selection
        if df_tb.empty:
            if view_mode == "wallet":
                df_tb = all_month_end_tb_by_wallet()
            else:
                df_tb = all_month_end_trial_balances()
            if not df_tb.empty and 'month_end' in df_tb.columns:
                latest = df_tb['month_end'].max()
                df_tb = df_tb[df_tb['month_end'] == latest]

        if df_tb.empty:
            return pd.DataFrame()

        # Filter by selected fund (optional)
        try:
            selected_fund = input.gl2_tb_fund()
            if selected_fund:
                df_tb = df_tb[df_tb['fund_id'] == selected_fund]
        except:
            pass

        return df_tb

    @reactive.calc
    def tb_fund_balance_summary():
        """Check if each fund's TB is balanced.
        Ported from production notebook cell 537."""
        tb = filtered_trial_balance()
        if tb.empty or 'fund_id' not in tb.columns:
            return []

        currency = "crypto"
        try:
            currency = input.gl2_tb_currency()
        except:
            pass

        net_col = 'net_debit_credit_USD' if currency == 'usd' else 'net_debit_credit_crypto'
        threshold = 0.01 if currency == 'usd' else 0.000001

        if net_col not in tb.columns:
            return []

        results = []
        for fund in sorted(tb['fund_id'].unique()):
            fund_total = tb[tb['fund_id'] == fund][net_col].sum()
            results.append({
                'fund_id': fund,
                'net_total': fund_total,
                'is_balanced': abs(fund_total) < threshold
            })
        return results

    @reactive.calc
    def tb_unnatural_balances():
        """Check for accounts with unnatural debit/credit balances.
        Merges with COA natural_balance column.
        Ported from production notebook cell 484."""
        tb = filtered_trial_balance()
        if tb.empty:
            return []

        coa = coa_data()
        if coa.empty or 'natural_balance' not in coa.columns:
            return []

        currency = "crypto"
        try:
            currency = input.gl2_tb_currency()
        except:
            pass

        balance_col = 'net_debit_credit_USD' if currency == 'usd' else 'net_debit_credit_crypto'
        threshold = 0.01 if currency == 'usd' else 0.0001

        if balance_col not in tb.columns:
            return []

        # Join on account_name if available in COA, else skip
        if 'account_name' in coa.columns and 'account_name' in tb.columns:
            tb_with_coa = tb.merge(
                coa[['account_name', 'natural_balance']].drop_duplicates(subset='account_name'),
                on='account_name',
                how='left'
            )
        else:
            return []

        unnatural = []
        for _, row in tb_with_coa.iterrows():
            balance = row.get(balance_col, 0)
            natural = str(row.get('natural_balance', '')).lower().strip()

            if abs(balance) < threshold:
                continue
            if natural == 'debit_credit':
                continue

            is_unnatural = False
            issue = ""

            if natural == 'debit' and balance < 0:
                is_unnatural = True
                issue = "Expected DEBIT (+), got CREDIT"
            elif natural == 'credit' and balance > 0:
                is_unnatural = True
                issue = "Expected CREDIT (-), got DEBIT"

            if is_unnatural:
                unnatural.append({
                    'fund_id': row.get('fund_id', ''),
                    'GL_Acct_Number': row.get('GL_Acct_Number', ''),
                    'account_name': row.get('account_name', ''),
                    'natural_balance': natural,
                    'actual_balance': balance,
                    'issue': issue
                })

        return unnatural

    # --- Summary Outputs ---

    @output
    @render.text
    def gl2_tb_total_debits():
        tb = filtered_trial_balance()
        if tb.empty:
            return "0.000000"
        currency = "crypto"
        try:
            currency = input.gl2_tb_currency()
        except:
            pass
        col = 'debit_USD' if currency == 'usd' else 'debit_crypto'
        if col not in tb.columns:
            return "0.000000"
        total = tb[col].sum()
        fmt = ",.2f" if currency == 'usd' else ",.6f"
        return f"{total:{fmt}}"

    @output
    @render.text
    def gl2_tb_total_credits():
        tb = filtered_trial_balance()
        if tb.empty:
            return "0.000000"
        currency = "crypto"
        try:
            currency = input.gl2_tb_currency()
        except:
            pass
        col = 'credit_USD' if currency == 'usd' else 'credit_crypto'
        if col not in tb.columns:
            return "0.000000"
        total = tb[col].sum()
        fmt = ",.2f" if currency == 'usd' else ",.6f"
        return f"{total:{fmt}}"

    @output
    @render.text
    def gl2_tb_date_display():
        try:
            selected_period = input.gl2_tb_period()
            if selected_period:
                ts = pd.Timestamp(selected_period)
                return f"As of {ts.strftime('%B %d, %Y')} (11:59:59 PM UTC)"
        except:
            pass
        return ""

    @output
    @render.ui
    def gl2_tb_balance_status():
        """Render overall balance status with styling."""
        tb = filtered_trial_balance()
        if tb.empty:
            return ui.div("--", class_="tb-summary-value")

        currency = "crypto"
        try:
            currency = input.gl2_tb_currency()
        except:
            pass
        net_col = 'net_debit_credit_USD' if currency == 'usd' else 'net_debit_credit_crypto'
        threshold = 0.01 if currency == 'usd' else 0.000001

        if net_col not in tb.columns:
            return ui.div("--", class_="tb-summary-value")

        total_net = abs(tb[net_col].sum())
        fmt = ",.2f" if currency == 'usd' else ",.6f"

        if total_net < threshold:
            return ui.div(
                ui.HTML('<i class="bi bi-check-circle-fill me-2"></i>'),
                "Balanced",
                class_="tb-summary-value balanced"
            )
        else:
            return ui.div(
                ui.HTML('<i class="bi bi-exclamation-triangle-fill me-2"></i>'),
                f"Off by {total_net:{fmt}}",
                class_="tb-summary-value unbalanced"
            )

    @output
    @render.ui
    def gl2_tb_balance_badge():
        """Render balance badge in footer."""
        tb = filtered_trial_balance()
        if tb.empty:
            return ui.span()

        currency = "crypto"
        try:
            currency = input.gl2_tb_currency()
        except:
            pass
        net_col = 'net_debit_credit_USD' if currency == 'usd' else 'net_debit_credit_crypto'
        threshold = 0.01 if currency == 'usd' else 0.000001

        if net_col not in tb.columns:
            return ui.span()

        total_net = abs(tb[net_col].sum())
        fmt = ",.2f" if currency == 'usd' else ",.6f"

        if total_net < threshold:
            return ui.span(
                ui.HTML('<i class="bi bi-check-circle-fill"></i>'),
                " Trial Balance is in Balance",
                class_="tb-balanced-badge balanced"
            )
        else:
            return ui.span(
                ui.HTML('<i class="bi bi-exclamation-triangle-fill"></i>'),
                f" Out of Balance by {total_net:{fmt}}",
                class_="tb-balanced-badge unbalanced"
            )

    # --- Fund Balance Cards ---

    @output
    @render.ui
    def gl2_tb_fund_balance_cards():
        """Render per-fund balance status cards.
        Ported from production notebook cell 537."""
        summary = tb_fund_balance_summary()
        if not summary:
            return ui.div()

        currency = "crypto"
        try:
            currency = input.gl2_tb_currency()
        except:
            pass
        fmt = ",.2f" if currency == 'usd' else ",.6f"
        prefix = "$" if currency == 'usd' else ""

        cards = []
        for item in summary:
            icon = "bi-check-circle-fill" if item['is_balanced'] else "bi-exclamation-triangle-fill"
            color = "#059669" if item['is_balanced'] else "#b45309"
            bg = "#d1fae5" if item['is_balanced'] else "#fef3c7"
            val_str = f"{prefix}{item['net_total']:{fmt}}"

            cards.append(
                ui.div(
                    ui.div(
                        ui.HTML(f'<i class="bi {icon}" style="color: {color}; margin-right: 6px;"></i>'),
                        ui.span(item['fund_id'], style="font-weight: 600; font-size: 0.8rem;"),
                        style="margin-bottom: 4px;"
                    ),
                    ui.div(
                        val_str,
                        style=f"font-family: 'SF Mono', 'Consolas', monospace; font-size: 0.85rem; color: {color};"
                    ),
                    style=f"background: {bg}; border-radius: 8px; padding: 10px 14px; min-width: 180px;"
                )
            )

        return ui.div(
            *cards,
            style="display: flex; gap: 12px; padding: 16px 32px; flex-wrap: wrap; border-bottom: 1px solid #e2e8f0; background: #f8fafc;"
        )

    # --- Unnatural Balance Alerts ---

    @output
    @render.ui
    def gl2_tb_unnatural_alerts():
        """Render unnatural balance check warnings.
        Ported from production notebook cell 484."""
        unnatural = tb_unnatural_balances()
        if not unnatural:
            return ui.div()

        currency = "crypto"
        try:
            currency = input.gl2_tb_currency()
        except:
            pass
        fmt = ",.2f" if currency == 'usd' else ",.6f"
        prefix = "$" if currency == 'usd' else ""

        alert_rows = []
        for item in unnatural:
            alert_rows.append(
                ui.tags.tr(
                    ui.tags.td(str(item['fund_id']), style="font-size: 0.8rem;"),
                    ui.tags.td(str(item['GL_Acct_Number']), style="font-family: monospace; font-size: 0.8rem;"),
                    ui.tags.td(str(item['account_name']), style="font-size: 0.8rem;"),
                    ui.tags.td(item['natural_balance'], style="font-size: 0.8rem; text-transform: capitalize;"),
                    ui.tags.td(f"{prefix}{item['actual_balance']:{fmt}}", style="font-family: monospace; font-size: 0.8rem; text-align: right;"),
                    ui.tags.td(item['issue'], style="font-size: 0.8rem; color: #b45309;"),
                )
            )

        return ui.div(
            ui.div(
                ui.HTML('<i class="bi bi-exclamation-triangle-fill me-2" style="color: #f59e0b;"></i>'),
                ui.span(f"Unnatural Balance Check: {len(unnatural)} account(s) flagged", style="font-weight: 600;"),
                style="padding: 12px 20px; background: #fffbeb; border-bottom: 1px solid #fde68a;"
            ),
            ui.tags.table(
                ui.tags.thead(
                    ui.tags.tr(
                        ui.tags.th("Fund"),
                        ui.tags.th("Acct #"),
                        ui.tags.th("Account Name"),
                        ui.tags.th("Natural"),
                        ui.tags.th("Balance", style="text-align: right;"),
                        ui.tags.th("Issue"),
                    ),
                    style="background: #fef3c7;"
                ),
                ui.tags.tbody(*alert_rows),
                style="width: 100%; border-collapse: collapse; font-size: 0.85rem;",
                class_="tb-table"
            ),
            style="margin: 0; border: 1px solid #fde68a; border-radius: 0;"
        )

    # --- Main Trial Balance Display ---

    @output
    @render.ui
    def gl2_trial_balance_display():
        """Render trial balance as HTML table.
        Supports pivot, flat, and wallet views.
        Ported from production notebook cells 444, 484, 490, 537."""
        tb = filtered_trial_balance()
        if tb.empty:
            return ui.div(
                ui.div(
                    ui.HTML('<i class="bi bi-inbox" style="font-size: 3rem; color: #cbd5e1;"></i>'),
                    ui.p("No journal entries found", class_="mt-3 mb-1 fw-semibold"),
                    ui.p("Post some journal entries to see your trial balance.", class_="text-muted small"),
                    class_="text-center py-5"
                )
            )

        view_mode = "pivot"
        try:
            view_mode = input.gl2_tb_view()
        except:
            pass

        currency = "crypto"
        try:
            currency = input.gl2_tb_currency()
        except:
            pass

        if view_mode == "pivot":
            return _render_pivot_view(tb, currency)
        elif view_mode == "wallet":
            return _render_wallet_view(tb, currency)
        else:
            return _render_flat_view(tb, currency)

    def _render_pivot_view(tb, currency):
        """Render pivot table with funds as columns.
        Ported from production notebook cells 484/497/537."""
        value_col = 'net_debit_credit_USD' if currency == 'usd' else 'net_debit_credit_crypto'

        if value_col not in tb.columns or 'fund_id' not in tb.columns:
            return ui.div("Missing required columns for pivot view.", class_="text-center py-3 text-muted")

        pivot = tb.pivot_table(
            index=['GL_Acct_Number', 'account_name'],
            columns='fund_id',
            values=value_col,
            aggfunc='sum',
            fill_value=0,
            margins=True,
            margins_name='TOTAL'
        )

        fund_cols = [c for c in pivot.columns if c != 'TOTAL']
        if 'TOTAL' in pivot.columns:
            display_cols = fund_cols + ['TOTAL']
        else:
            display_cols = fund_cols

        fmt = ",.2f" if currency == 'usd' else ",.6f"
        prefix = "$" if currency == 'usd' else ""

        # Build header
        header_cells = [
            ui.tags.th("Acct #"),
            ui.tags.th("Account Name"),
        ]
        for col in display_cols:
            style = "text-align: right; font-weight: 700;" if col == 'TOTAL' else "text-align: right;"
            header_cells.append(ui.tags.th(str(col), style=style))
        header_row = ui.tags.tr(*header_cells)

        # Build rows with category grouping
        rows = []
        current_category = None

        for (acct_num, acct_name), row_data in pivot.iterrows():
            # Skip the TOTAL margin row in the index — we'll add it separately
            if acct_num == 'TOTAL':
                continue

            # Category header
            cat = _get_category(acct_num)
            if cat != current_category:
                current_category = cat
                cat_name = current_category.split('. ', 1)[1] if '. ' in current_category else current_category
                rows.append(
                    ui.tags.tr(
                        ui.tags.td(
                            ui.HTML(f'<i class="bi bi-folder2 me-2"></i>{cat_name}'),
                            colspan=str(len(display_cols) + 2)
                        ),
                        class_="tb-category-row"
                    )
                )

            cells = [
                ui.tags.td(str(acct_num), class_="acct-num"),
                ui.tags.td(str(acct_name), class_="acct-name"),
            ]
            for col in display_cols:
                val = row_data[col] if col in row_data.index else 0
                if abs(val) < (0.005 if currency == 'usd' else 0.0000005):
                    cells.append(ui.tags.td("-", class_="text-right tb-empty-cell"))
                else:
                    val_class = "text-right debit-value" if val > 0 else "text-right credit-value"
                    if col == 'TOTAL':
                        val_class += " fw-bold"
                    cells.append(ui.tags.td(f"{prefix}{val:{fmt}}", class_=val_class))
            rows.append(ui.tags.tr(*cells, class_="tb-account-row"))

        # TOTAL row from pivot margins
        if 'TOTAL' in pivot.index.get_level_values(0):
            total_data = pivot.loc[('TOTAL', 'TOTAL')] if ('TOTAL', 'TOTAL') in pivot.index else pivot.loc['TOTAL']
            total_cells = [ui.tags.td("TOTAL", colspan="2")]
            for col in display_cols:
                val = total_data[col] if col in total_data.index else 0
                total_cells.append(ui.tags.td(f"{prefix}{val:{fmt}}", class_="text-right"))
            rows.append(ui.tags.tr(*total_cells, class_="tb-total-row"))
        else:
            # Compute totals manually
            total_cells = [ui.tags.td("TOTAL", colspan="2")]
            for col in display_cols:
                val = pivot[col].sum() if col in pivot.columns else 0
                # If margins were added, the TOTAL row is already in the sum, so use the column excluding TOTAL index
                non_total = pivot.loc[pivot.index.get_level_values(0) != 'TOTAL']
                val = non_total[col].sum() if col in non_total.columns else 0
                total_cells.append(ui.tags.td(f"{prefix}{val:{fmt}}", class_="text-right"))
            rows.append(ui.tags.tr(*total_cells, class_="tb-total-row"))

        return ui.tags.table(
            ui.tags.thead(header_row),
            ui.tags.tbody(*rows),
            class_="tb-table"
        )

    def _render_flat_view(tb, currency):
        """Render flat account-level detail with category headers."""
        if currency == 'usd':
            debit_col, credit_col, net_col = 'debit_USD', 'credit_USD', 'net_debit_credit_USD'
            fmt, prefix = ",.2f", "$"
        else:
            debit_col, credit_col, net_col = 'debit_crypto', 'credit_crypto', 'net_debit_credit_crypto'
            fmt, prefix = ",.6f", ""

        # Aggregate across funds for flat view
        group_cols = ['GL_Acct_Number', 'account_name']
        valid_cols = [c for c in group_cols if c in tb.columns]
        agg_cols = {c: 'sum' for c in [debit_col, credit_col, net_col] if c in tb.columns}
        if not valid_cols or not agg_cols:
            return ui.div("Missing columns for flat view.", class_="text-center py-3 text-muted")

        flat = tb.groupby(valid_cols).agg(agg_cols).reset_index()
        flat['category'] = flat['GL_Acct_Number'].apply(_get_category)
        flat = flat.sort_values(['category', 'GL_Acct_Number'])

        header_row = ui.tags.tr(
            ui.tags.th("Acct #"),
            ui.tags.th("Account Name"),
            ui.tags.th("Debits", style="text-align: right;"),
            ui.tags.th("Credits", style="text-align: right;"),
            ui.tags.th("Net Debit (Credit)", style="text-align: right;"),
        )

        rows = []
        current_category = None
        threshold = 0.005 if currency == 'usd' else 0.0000005

        for _, row in flat.iterrows():
            cat = row.get('category', '')
            if cat != current_category:
                current_category = cat
                cat_name = current_category.split('. ', 1)[1] if '. ' in current_category else current_category
                rows.append(
                    ui.tags.tr(
                        ui.tags.td(
                            ui.HTML(f'<i class="bi bi-folder2 me-2"></i>{cat_name}'),
                            colspan="5"
                        ),
                        class_="tb-category-row"
                    )
                )

            dr_val = row.get(debit_col, 0)
            cr_val = row.get(credit_col, 0)
            net_val = row.get(net_col, 0)

            dr_str = f"{prefix}{dr_val:{fmt}}" if abs(dr_val) > threshold else "-"
            cr_str = f"{prefix}{cr_val:{fmt}}" if abs(cr_val) > threshold else "-"
            net_str = f"{prefix}{net_val:{fmt}}" if abs(net_val) > threshold else "-"

            dr_class = "text-right debit-value" if abs(dr_val) > threshold else "text-right tb-empty-cell"
            cr_class = "text-right credit-value" if abs(cr_val) > threshold else "text-right tb-empty-cell"
            net_class = "text-right debit-value" if net_val > threshold else ("text-right credit-value" if net_val < -threshold else "text-right tb-empty-cell")

            rows.append(
                ui.tags.tr(
                    ui.tags.td(str(row.get('GL_Acct_Number', '')), class_="acct-num"),
                    ui.tags.td(str(row.get('account_name', '')), class_="acct-name"),
                    ui.tags.td(dr_str, class_=dr_class),
                    ui.tags.td(cr_str, class_=cr_class),
                    ui.tags.td(net_str, class_=net_class),
                    class_="tb-account-row"
                )
            )

        # Total row
        total_dr = flat[debit_col].sum() if debit_col in flat.columns else 0
        total_cr = flat[credit_col].sum() if credit_col in flat.columns else 0
        total_net = flat[net_col].sum() if net_col in flat.columns else 0
        rows.append(
            ui.tags.tr(
                ui.tags.td("TOTAL", colspan="2"),
                ui.tags.td(f"{prefix}{total_dr:{fmt}}", class_="text-right"),
                ui.tags.td(f"{prefix}{total_cr:{fmt}}", class_="text-right"),
                ui.tags.td(f"{prefix}{total_net:{fmt}}", class_="text-right"),
                class_="tb-total-row"
            )
        )

        return ui.tags.table(
            ui.tags.thead(header_row),
            ui.tags.tbody(*rows),
            class_="tb-table"
        )

    def _render_wallet_view(tb, currency):
        """Render by-wallet breakdown.
        Ported from production notebook cell 490."""
        value_col = 'net_debit_credit_USD' if currency == 'usd' else 'net_debit_credit_crypto'
        fmt = ",.2f" if currency == 'usd' else ",.6f"
        prefix = "$" if currency == 'usd' else ""

        if value_col not in tb.columns or 'wallet_id' not in tb.columns:
            return ui.div("Wallet data not available. Select 'Pivot' or 'Flat' view.", class_="text-center py-3 text-muted")

        # Pivot by wallet
        index_cols = ['wallet_id', 'GL_Acct_Number', 'account_name']
        valid_idx = [c for c in index_cols if c in tb.columns]

        pivot = tb.pivot_table(
            index=valid_idx,
            columns='fund_id',
            values=value_col,
            aggfunc='sum',
            fill_value=0,
            margins=True,
            margins_name='TOTAL'
        )

        fund_cols = [c for c in pivot.columns if c != 'TOTAL']
        display_cols = fund_cols + ['TOTAL'] if 'TOTAL' in pivot.columns else fund_cols

        header_cells = [ui.tags.th("Wallet"), ui.tags.th("Acct #"), ui.tags.th("Account")]
        for col in display_cols:
            style = "text-align: right; font-weight: 700;" if col == 'TOTAL' else "text-align: right;"
            header_cells.append(ui.tags.th(str(col), style=style))
        header_row = ui.tags.tr(*header_cells)

        rows = []
        current_wallet = None
        threshold = 0.005 if currency == 'usd' else 0.0000005

        for idx, row_data in pivot.iterrows():
            if not isinstance(idx, tuple):
                idx = (idx,)

            wallet = str(idx[0]) if len(idx) > 0 else ''
            acct_num = str(idx[1]) if len(idx) > 1 else ''
            acct_name = str(idx[2]) if len(idx) > 2 else ''

            if wallet == 'TOTAL':
                continue

            # Wallet header
            if wallet != current_wallet:
                current_wallet = wallet
                short_wallet = f"{wallet[:8]}...{wallet[-6:]}" if len(wallet) > 20 else wallet
                rows.append(
                    ui.tags.tr(
                        ui.tags.td(
                            ui.HTML(f'<i class="bi bi-wallet2 me-2"></i>{short_wallet}'),
                            colspan=str(len(display_cols) + 3)
                        ),
                        class_="tb-category-row"
                    )
                )

            cells = [
                ui.tags.td("", class_="acct-num"),
                ui.tags.td(str(acct_num), class_="acct-num"),
                ui.tags.td(str(acct_name), class_="acct-name"),
            ]
            for col in display_cols:
                val = row_data[col] if col in row_data.index else 0
                if abs(val) < threshold:
                    cells.append(ui.tags.td("-", class_="text-right tb-empty-cell"))
                else:
                    val_class = "text-right debit-value" if val > 0 else "text-right credit-value"
                    cells.append(ui.tags.td(f"{prefix}{val:{fmt}}", class_=val_class))
            rows.append(ui.tags.tr(*cells, class_="tb-account-row"))

        # TOTAL row
        if 'TOTAL' in pivot.index.get_level_values(0):
            total_idx = [i for i in pivot.index if (i[0] if isinstance(i, tuple) else i) == 'TOTAL']
            if total_idx:
                total_data = pivot.loc[total_idx[-1]]
                total_cells = [ui.tags.td("TOTAL", colspan="3")]
                for col in display_cols:
                    val = total_data[col] if col in total_data.index else 0
                    total_cells.append(ui.tags.td(f"{prefix}{val:{fmt}}", class_="text-right"))
                rows.append(ui.tags.tr(*total_cells, class_="tb-total-row"))

        return ui.tags.table(
            ui.tags.thead(header_row),
            ui.tags.tbody(*rows),
            class_="tb-table"
        )

    # =========================================================================
    # NEW ENTRY TAB
    # =========================================================================

    @output
    @render.ui
    def gl2_entry_lines_container():
        """Render entry line inputs."""
        lines = entry_lines()
        choices = account_choices()
        if "" in choices:
            choices = {k: v for k, v in choices.items() if k}
        choices = {"": "Select account...", **choices}

        line_elements = []
        for i, line in enumerate(lines):
            line_ui = ui.layout_columns(
                ui.input_selectize(
                    f"gl2_line_{i}_account",
                    None,
                    choices=choices,
                    selected=line.get('account', ''),
                    width="100%"
                ),
                ui.input_numeric(
                    f"gl2_line_{i}_debit",
                    None,
                    value=line.get('debit', 0.0),
                    min=0,
                    step=0.000001
                ),
                ui.input_numeric(
                    f"gl2_line_{i}_credit",
                    None,
                    value=line.get('credit', 0.0),
                    min=0,
                    step=0.000001
                ),
                ui.input_action_button(
                    f"gl2_remove_line_{i}",
                    ui.HTML('<i class="bi bi-x"></i>'),
                    class_="btn-outline-dark btn-sm"
                ) if len(lines) > 2 else ui.div(),
                col_widths=[6, 2, 2, 2],
                class_="mb-2 align-items-center"
            )
            line_elements.append(line_ui)

        # Add header row
        header = ui.layout_columns(
            ui.tags.small("Account", class_="text-muted"),
            ui.tags.small("Debit", class_="text-muted"),
            ui.tags.small("Credit", class_="text-muted"),
            ui.tags.small("", class_="text-muted"),
            col_widths=[6, 2, 2, 2],
            class_="mb-1"
        )

        return ui.div(header, *line_elements)

    @output
    @render.text
    def gl2_new_entry_total_debits():
        lines = entry_lines()
        total = sum(line.get('debit', 0) or 0 for line in lines)
        return f"{total:,.6f}"

    @output
    @render.text
    def gl2_new_entry_total_credits():
        lines = entry_lines()
        total = sum(line.get('credit', 0) or 0 for line in lines)
        return f"{total:,.6f}"

    @output
    @render.text
    def gl2_new_entry_difference():
        lines = entry_lines()
        debits = sum(line.get('debit', 0) or 0 for line in lines)
        credits = sum(line.get('credit', 0) or 0 for line in lines)
        diff = debits - credits
        return "Balanced" if abs(diff) < 0.000001 else f"{diff:,.6f}"

    @output
    @render.ui
    def gl2_entry_validation_message():
        lines = entry_lines()
        debits = sum(line.get('debit', 0) or 0 for line in lines)
        credits = sum(line.get('credit', 0) or 0 for line in lines)

        if abs(debits - credits) > 0.000001:
            return ui.div(
                ui.HTML('<i class="bi bi-exclamation-triangle"></i> '),
                "Entry is not balanced. Debits must equal credits.",
                class_="text-warning mt-3"
            )

        accounts_selected = [line.get('account') for line in lines if line.get('account')]
        if len(accounts_selected) < 2:
            return ui.div(
                ui.HTML('<i class="bi bi-info-circle"></i> '),
                "Select at least two accounts.",
                class_="text-info mt-3"
            )

        return ui.div(
            ui.HTML('<i class="bi bi-check-circle"></i> '),
            "Entry is valid and ready to post.",
            class_="text-success mt-3"
        )

    # Add line
    @reactive.effect
    @reactive.event(input.gl2_add_entry_line)
    def _add_entry_line():
        current = entry_lines()
        current.append({"account": "", "debit": 0.0, "credit": 0.0})
        entry_lines.set(current)

    # Clear form
    @reactive.effect
    @reactive.event(input.gl2_clear_entry)
    def _clear_entry_form():
        entry_lines.set([
            {"account": "", "debit": 0.0, "credit": 0.0},
            {"account": "", "debit": 0.0, "credit": 0.0}
        ])
        ui.update_text("gl2_new_entry_description", value="")

    # Post entry
    @reactive.effect
    @reactive.event(input.gl2_post_entry)
    def _post_manual_entry():
        lines_data = []
        line_count = len(entry_lines())

        for i in range(line_count):
            try:
                account = getattr(input, f"gl2_line_{i}_account")()
                debit = getattr(input, f"gl2_line_{i}_debit")() or 0
                credit = getattr(input, f"gl2_line_{i}_credit")() or 0

                if account and (debit > 0 or credit > 0):
                    lines_data.append({
                        "account": account,
                        "debit": float(debit),
                        "credit": float(credit)
                    })
            except:
                continue

        # Validate
        total_debits = sum(l['debit'] for l in lines_data)
        total_credits = sum(l['credit'] for l in lines_data)

        if abs(total_debits - total_credits) > 0.000001:
            ui.notification_show("Entry is not balanced!", type="error")
            return

        if len(lines_data) < 2:
            ui.notification_show("Entry must have at least 2 lines", type="error")
            return

        # Get metadata
        entry_date = input.gl2_new_entry_date()
        description = input.gl2_new_entry_description() or "Manual Journal Entry"
        category = input.gl2_new_entry_category()

        timestamp = datetime.now(timezone.utc)
        if entry_date:
            timestamp = pd.Timestamp(entry_date, tz='UTC')

        tx_hash = f"manual_{hashlib.md5(f'{timestamp}{description}'.encode()).hexdigest()[:16]}"

        coa = coa_data()

        # Create records using new schema format
        records = []
        for line in lines_data:
            gl_acct_name = ""
            if not coa.empty:
                match = coa[coa['GL_Acct_Number'].astype(str) == line['account']]
                if not match.empty:
                    gl_acct_name = match.iloc[0]['GL_Acct_Name']

            # Build account_name in "100.30 - ETH Wallet" format
            acct_name_str = f"{line['account']} - {gl_acct_name}" if gl_acct_name else line['account']
            row_key = f"{tx_hash}:{acct_name_str}:manual:{'DR' if line['debit'] > 0 else 'CR'}"

            records.append({
                'date': timestamp,
                'transaction_type': category or 'manual_entry',
                'platform': '',
                'fund_id': '',
                'counterparty_fund_id': '',
                'wallet_id': '',
                'cryptocurrency': 'ETH',
                'account_name': acct_name_str,
                'debit_crypto': line['debit'],
                'credit_crypto': line['credit'],
                'eth_usd_price': 0.0,
                'debit_USD': 0.0,
                'credit_USD': 0.0,
                'hash': tx_hash,
                'event': 'Manual Entry',
                'loan_id': None,
                'lender': None,
                'borrower': None,
                'from': None,
                'to': None,
                'contract_address': None,
                'payable_currency': None,
                'collateral_address': None,
                'token_id': None,
                'principal_crypto': None,
                'principal_USD': None,
                'payoff_amount_crypto': None,
                'payoff_amount_USD': None,
                'annual_interest_rate': None,
                'loan_due_date': None,
                'tranche_floor': None,
                'tranche_index': None,
                'fund_role': '',
                'origination_fee': None,
                'net_origination_fee': None,
                'source_file': '',
                'notes': description,
                'row_key': row_key,
            })

        # Save
        try:
            existing_df = load_GL2_file()
            new_df = pd.DataFrame(records)
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)

            if save_GL2_file(combined_df):
                ui.notification_show(f"Posted {len(records)} entries", type="message")
                entry_lines.set([
                    {"account": "", "debit": 0.0, "credit": 0.0},
                    {"account": "", "debit": 0.0, "credit": 0.0}
                ])
                ui.update_text("gl2_new_entry_description", value="")
                clear_GL2_cache()
                gl2_data_version.set(gl2_data_version() + 1)
            else:
                ui.notification_show("Failed to save entry", type="error")
        except Exception as e:
            ui.notification_show(f"Error: {str(e)}", type="error")

    @output
    @render.data_frame
    def gl2_recent_manual_entries():
        df = gl2_data()
        if df.empty:
            return render.DataGrid(
                pd.DataFrame({"Message": ["No manual entries yet."]}),
                width="100%"
            )

        # Filter for manual entries
        manual_df = df[df['transaction_type'].astype(str).str.contains('manual', case=False, na=False)].copy()
        if manual_df.empty:
            return render.DataGrid(
                pd.DataFrame({"Message": ["No manual entries yet."]}),
                width="100%"
            )

        if 'date' in manual_df.columns:
            manual_df = manual_df.sort_values('date', ascending=False).head(20)

        display_cols = ['date', 'account_name', 'debit_crypto', 'credit_crypto']
        display_df = manual_df[[c for c in display_cols if c in manual_df.columns]].copy()

        if 'date' in display_df.columns:
            display_df['date'] = pd.to_datetime(display_df['date']).dt.strftime('%Y-%m-%d')

        for col in ['debit_crypto', 'credit_crypto']:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(
                    lambda x: f"{float(x):,.6f}" if pd.notna(x) and float(x) != 0 else ""
                )

        col_rename = {'date': 'Date', 'account_name': 'Account',
                      'debit_crypto': 'Debit', 'credit_crypto': 'Credit'}
        display_df = display_df.rename(columns={k: v for k, v in col_rename.items() if k in display_df.columns})
        return render.DataGrid(display_df, width="100%", height="300px")

    # =========================================================================
    # DOWNLOADS
    # =========================================================================

    @render.download(filename=lambda: f"gl2_journal_entries_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
    async def gl2_download_je():
        """Export filtered journal entries to Excel."""
        df = filtered_journal_entries()
        if df.empty:
            df = pd.DataFrame({"Message": ["No data to export"]})

        from io import BytesIO
        buffer = BytesIO()

        # Create Excel writer with formatting
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            # Prepare data for export
            export_df = df.copy()

            # Format date column if exists
            if 'date' in export_df.columns:
                export_df['date'] = pd.to_datetime(export_df['date']).dt.strftime('%Y-%m-%d %H:%M:%S')

            # Write to Excel
            export_df.to_excel(writer, sheet_name='Journal Entries', index=False)

            # Auto-adjust column widths
            worksheet = writer.sheets['Journal Entries']
            for idx, col in enumerate(export_df.columns):
                max_length = max(
                    export_df[col].astype(str).map(len).max(),
                    len(str(col))
                ) + 2
                worksheet.column_dimensions[chr(65 + idx) if idx < 26 else f'A{chr(65 + idx - 26)}'].width = min(max_length, 50)

        buffer.seek(0)
        return buffer.getvalue()

    @render.download(filename=lambda: f"gl2_account_ledger_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
    async def gl2_download_ledger():
        """Export account ledger to Excel."""
        df = account_ledger_data()
        if df.empty:
            df = pd.DataFrame({"Message": ["No data to export - select an account first"]})

        from io import BytesIO
        buffer = BytesIO()

        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            export_df = df.copy()
            if 'date' in export_df.columns:
                export_df['date'] = pd.to_datetime(export_df['date']).dt.strftime('%Y-%m-%d %H:%M:%S')

            export_df.to_excel(writer, sheet_name='Account Ledger', index=False)

            worksheet = writer.sheets['Account Ledger']
            for idx, col in enumerate(export_df.columns):
                max_length = max(
                    export_df[col].astype(str).map(len).max(),
                    len(str(col))
                ) + 2
                worksheet.column_dimensions[chr(65 + idx) if idx < 26 else f'A{chr(65 + idx - 26)}'].width = min(max_length, 50)

        buffer.seek(0)
        return buffer.getvalue()

    @render.download(filename=lambda: f"gl2_trial_balance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
    async def gl2_download_tb():
        """Export trial balance to Excel with multiple sheets."""
        from io import BytesIO
        buffer = BytesIO()

        tb = filtered_trial_balance()
        currency = "crypto"
        try:
            currency = input.gl2_tb_currency()
        except:
            pass

        view_mode = "pivot"
        try:
            view_mode = input.gl2_tb_view()
        except:
            pass

        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            # Sheet 1: Trial Balance (pivot or flat)
            if tb.empty:
                pd.DataFrame({"Message": ["No data to export"]}).to_excel(
                    writer, sheet_name='Trial Balance', index=False)
            elif view_mode == "pivot" and 'fund_id' in tb.columns:
                value_col = 'net_debit_credit_USD' if currency == 'usd' else 'net_debit_credit_crypto'
                if value_col in tb.columns:
                    pivot = tb.pivot_table(
                        index=['GL_Acct_Number', 'account_name'],
                        columns='fund_id',
                        values=value_col,
                        aggfunc='sum',
                        fill_value=0,
                        margins=True,
                        margins_name='TOTAL'
                    )
                    pivot.to_excel(writer, sheet_name='Trial Balance')
                else:
                    tb.to_excel(writer, sheet_name='Trial Balance', index=False)
            else:
                tb.to_excel(writer, sheet_name='Trial Balance', index=False)

            # Sheet 2: Unnatural Balance Check
            unnatural = tb_unnatural_balances()
            if unnatural:
                pd.DataFrame(unnatural).to_excel(
                    writer, sheet_name='Unnatural Balances', index=False)

            # Sheet 3: Fund Balance Summary
            fund_summary = tb_fund_balance_summary()
            if fund_summary:
                pd.DataFrame(fund_summary).to_excel(
                    writer, sheet_name='Fund Balance Check', index=False)

            # Auto-adjust column widths for all sheets
            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
                for column_cells in worksheet.columns:
                    max_length = 0
                    col_letter = column_cells[0].column_letter
                    for cell in column_cells:
                        try:
                            if cell.value:
                                max_length = max(max_length, len(str(cell.value)))
                        except:
                            pass
                    worksheet.column_dimensions[col_letter].width = min(max_length + 2, 50)

        buffer.seek(0)
        return buffer.getvalue()

    @render.download(filename=lambda: f"gl2_full_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
    async def gl2_download_full():
        """Export all GL2 data to Excel with multiple sheets."""
        from io import BytesIO
        buffer = BytesIO()

        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            # Sheet 1: All Journal Entries
            je_df = gl2_data()
            if not je_df.empty:
                export_je = je_df.copy()
                if 'date' in export_je.columns:
                    export_je['date'] = pd.to_datetime(export_je['date']).dt.strftime('%Y-%m-%d %H:%M:%S')
                export_je.to_excel(writer, sheet_name='All Journal Entries', index=False)

            # Sheet 2: Trial Balance
            tb_df = filtered_trial_balance()
            if not tb_df.empty:
                tb_df.to_excel(writer, sheet_name='Trial Balance', index=False)

            # Sheet 3: Summary
            summary_data = {
                'Metric': ['Total Entries', 'Unique Accounts', 'Total Debits', 'Total Credits', 'Balance Check'],
                'Value': [
                    len(je_df) if not je_df.empty else 0,
                    (je_df['GL_Acct_Number'].nunique() if 'GL_Acct_Number' in je_df.columns else je_df['account_name'].nunique() if 'account_name' in je_df.columns else 0) if not je_df.empty else 0,
                    sum(float(x) if pd.notna(x) else 0 for x in je_df.get('debit_crypto', [])) if not je_df.empty else 0,
                    sum(float(x) if pd.notna(x) else 0 for x in je_df.get('credit_crypto', [])) if not je_df.empty else 0,
                    'Balanced' if abs(sum(float(x) if pd.notna(x) else 0 for x in je_df.get('debit_crypto', [])) -
                                     sum(float(x) if pd.notna(x) else 0 for x in je_df.get('credit_crypto', []))) < 0.000001 else 'Not Balanced'
                ]
            }
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)

        buffer.seek(0)
        return buffer.getvalue()
