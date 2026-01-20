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
    Normalize column names to handle both old and new schema.
    New schema uses: date, hash, GL_Acct_Number, GL_Acct_Name, transaction_type
    Old schema uses: timestamp, tx_hash, account_number, account_name, entry_type
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

                # Fix empty GL_Acct_Number
                if 'GL_Acct_Number' in df.columns and 'GL_Acct_Name' in df.columns:
                    empty_mask = (df['GL_Acct_Number'].isna()) | (df['GL_Acct_Number'] == '')
                    if empty_mask.any():
                        df.loc[empty_mask, 'GL_Acct_Number'] = df.loc[empty_mask, 'GL_Acct_Name'].apply(extract_account_number)

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
        if df.empty or 'GL_Acct_Number' not in df.columns:
            return "0"
        return str(df['GL_Acct_Number'].nunique())

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

        # Select display columns using new schema names
        display_cols = ['date', 'hash', 'GL_Acct_Number', 'GL_Acct_Name',
                       'transaction_type', 'debit_crypto', 'credit_crypto',
                       'cryptocurrency', 'fund_id', 'row_key']

        display_df = df[[c for c in display_cols if c in df.columns]].copy()

        # Sort by date descending
        if 'date' in display_df.columns:
            display_df = display_df.sort_values('date', ascending=False)
            display_df['date'] = pd.to_datetime(display_df['date']).dt.strftime('%Y-%m-%d %H:%M')

        # Format numeric columns
        for col in ['debit_crypto', 'credit_crypto']:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(
                    lambda x: f"{float(x):,.6f}" if pd.notna(x) and float(x) != 0 else ""
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
            'GL_Acct_Number': 'Acct #',
            'GL_Acct_Name': 'Account Name',
            'transaction_type': 'Type',
            'debit_crypto': 'Debit',
            'credit_crypto': 'Credit',
            'cryptocurrency': 'Asset',
            'fund_id': 'Fund',
            'row_key': 'Row Key'
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
                            ui.strong("Account: "), f"{row.get('GL_Acct_Number', '')} - {row.get('GL_Acct_Name', '')}",
                        ),
                        ui.div(
                            ui.strong("Type: "), str(row.get('transaction_type', 'N/A')),
                        ),
                        ui.div(
                            ui.strong("Wallet: "), str(row.get('wallet_id', 'N/A'))[:12] + "..." if row.get('wallet_id') else 'N/A',
                        ),
                        col_widths=[6, 3, 3]
                    ),
                    ui.hr(),
                    ui.layout_columns(
                        ui.div(
                            ui.strong("Debit: "), f"{float(row.get('debit_crypto', 0) or 0):,.6f}",
                            class_="text-success"
                        ),
                        ui.div(
                            ui.strong("Credit: "), f"{float(row.get('credit_crypto', 0) or 0):,.6f}",
                            class_="text-danger"
                        ),
                        ui.div(
                            ui.strong("Asset: "), str(row.get('cryptocurrency', 'N/A')),
                        ),
                        col_widths=[4, 4, 4]
                    ),
                    ui.hr(),
                    ui.layout_columns(
                        ui.div(
                            ui.strong("Loan ID: "), str(row.get('loan_id', 'N/A') or 'N/A'),
                        ),
                        ui.div(
                            ui.strong("ETH/USD: "), f"${float(row.get('eth_usd_price', 0) or 0):,.2f}",
                        ),
                        ui.div(
                            ui.strong("Principal: "), f"{float(row.get('principal_crypto', 0) or 0):,.6f}" if row.get('principal_crypto') else 'N/A',
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

            # Get GL_Acct_Name from COA
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

            # Update other fields
            full_df.loc[row_idx, 'transaction_type'] = edit_transaction_type
            full_df.loc[row_idx, 'fund_id'] = edit_fund_id
            full_df.loc[row_idx, 'GL_Acct_Number'] = edit_gl_acct_number
            full_df.loc[row_idx, 'GL_Acct_Name'] = gl_acct_name
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

            # Swap debits and credits with new schema
            reversal = {
                # Core identifiers (1-4)
                'date': timestamp,
                'fund_id': row.get('fund_id', ''),
                'limited_partner_ID': None,
                'wallet_id': row.get('wallet_id', ''),
                # Transaction info (5-7)
                'transaction_type': 'reversal',
                'cryptocurrency': row.get('cryptocurrency', 'ETH'),
                'account_name': row.get('account_name', ''),
                # GL Account info (8-9)
                'GL_Acct_Number': row.get('GL_Acct_Number', ''),
                'GL_Acct_Name': row.get('GL_Acct_Name', ''),
                # Amounts - swapped (10-14)
                'debit_crypto': float(row.get('credit_crypto', 0)),
                'credit_crypto': float(row.get('debit_crypto', 0)),
                'eth_usd_price': float(row.get('eth_usd_price', 0)),
                'debit_USD': float(row.get('credit_USD', 0)),
                'credit_USD': float(row.get('debit_USD', 0)),
                # Event info (15-17)
                'event': 'Reversal',
                'function': None,
                'hash': tx_hash,
                # Loan details (18-31)
                'loan_id': row.get('loan_id'),
                'lender': row.get('lender'),
                'borrower': row.get('borrower'),
                'from': row.get('from'),
                'to': row.get('to'),
                'contract_address': row.get('contract_address'),
                'collateral_address': row.get('collateral_address'),
                'token_id': row.get('token_id'),
                'principal_crypto': row.get('principal_crypto'),
                'principal_USD': row.get('principal_USD'),
                'annual_interest_rate': row.get('annual_interest_rate'),
                'payoff_amount_crypto': row.get('payoff_amount_crypto'),
                'payoff_amount_USD': row.get('payoff_amount_USD'),
                'loan_due_date': row.get('loan_due_date'),
                # End of day price (37)
                'end_of_day_ETH_USD': row.get('end_of_day_ETH_USD'),
                # Internal deduplication key
                'row_key': f"{tx_hash}:{row.get('GL_Acct_Number', '')}:reversal"
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
                ui.strong(f"{total_credits:,.6f}", class_="text-danger me-4"),
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
        display_cols = ['date', 'hash', 'GL_Acct_Number', 'GL_Acct_Name',
                       'transaction_type', 'debit_crypto', 'credit_crypto',
                       'running_balance', 'cryptocurrency', 'fund_id', 'row_key']

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
            'GL_Acct_Number': 'Acct #',
            'GL_Acct_Name': 'Account Name',
            'transaction_type': 'Type',
            'debit_crypto': 'Debit',
            'credit_crypto': 'Credit',
            'running_balance': 'Balance',
            'cryptocurrency': 'Asset',
            'fund_id': 'Fund',
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
    # =========================================================================

    @reactive.calc
    def trial_balance_data():
        """Generate trial balance from GL2 data."""
        df = gl2_data()

        if df.empty:
            return pd.DataFrame()

        # Filter by as-of date
        try:
            as_of_date = input.gl2_tb_as_of_date()
            if as_of_date:
                end_date = pd.Timestamp(as_of_date, tz='UTC') + pd.Timedelta(days=1)
                if 'date' in df.columns:
                    df = df[df['date'] < end_date]
        except:
            pass

        if df.empty:
            return pd.DataFrame()

        # Determine which currency to use
        currency = "crypto"
        try:
            currency = input.gl2_tb_currency()
        except:
            pass

        debit_col = 'debit_USD' if currency == 'usd' else 'debit_crypto'
        credit_col = 'credit_USD' if currency == 'usd' else 'credit_crypto'

        # Group by account
        grouped = df.groupby('GL_Acct_Number').agg({
            'GL_Acct_Name': 'first',
            debit_col: lambda x: sum(float(v) if pd.notna(v) else 0 for v in x),
            credit_col: lambda x: sum(float(v) if pd.notna(v) else 0 for v in x)
        }).reset_index()

        grouped.columns = ['account_number', 'account_name', 'total_debits', 'total_credits']

        # Calculate net balance
        grouped['net_balance'] = grouped['total_debits'] - grouped['total_credits']
        grouped['debit_balance'] = grouped['net_balance'].apply(lambda x: x if x > 0 else 0)
        grouped['credit_balance'] = grouped['net_balance'].apply(lambda x: abs(x) if x < 0 else 0)

        # Add category based on first digit of account number
        # Account scheme: 1xxxx = Assets, 2xxxx = Liabilities, 3xxxx = Equity,
        # 4xxxx = Revenue, 5xxxx/6xxxx/8xxxx = Expenses, 9xxxx = Other Income
        def get_category(acct_num):
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

        grouped['category'] = grouped['account_number'].apply(get_category)
        grouped = grouped.sort_values(['category', 'account_number'])

        return grouped

    @output
    @render.text
    def gl2_tb_total_debits():
        df = trial_balance_data()
        if df.empty:
            return "0.000000"
        total = df['debit_balance'].sum()
        return f"{total:,.6f}"

    @output
    @render.text
    def gl2_tb_total_credits():
        df = trial_balance_data()
        if df.empty:
            return "0.000000"
        total = df['credit_balance'].sum()
        return f"{total:,.6f}"

    @output
    @render.text
    def gl2_tb_difference():
        df = trial_balance_data()
        if df.empty:
            return "Balanced"
        debits = df['debit_balance'].sum()
        credits = df['credit_balance'].sum()
        diff = debits - credits
        return "Balanced" if abs(diff) < 0.000001 else f"Off: {diff:,.6f}"

    @output
    @render.text
    def gl2_tb_date_display():
        try:
            as_of = input.gl2_tb_as_of_date()
            return f"As of {as_of}" if as_of else ""
        except:
            return ""

    @output
    @render.ui
    def gl2_tb_balance_status():
        """Render balance status with styling."""
        df = trial_balance_data()
        if df.empty:
            return ui.div("--", class_="tb-summary-value")

        debits = df['debit_balance'].sum()
        credits = df['credit_balance'].sum()
        diff = abs(debits - credits)

        if diff < 0.000001:
            return ui.div(
                ui.HTML('<i class="bi bi-check-circle-fill me-2"></i>'),
                "Balanced",
                class_="tb-summary-value balanced"
            )
        else:
            return ui.div(
                ui.HTML('<i class="bi bi-exclamation-triangle-fill me-2"></i>'),
                f"{diff:,.6f}",
                class_="tb-summary-value unbalanced"
            )

    @output
    @render.ui
    def gl2_tb_balance_badge():
        """Render balance badge in footer."""
        df = trial_balance_data()
        if df.empty:
            return ui.span()

        debits = df['debit_balance'].sum()
        credits = df['credit_balance'].sum()
        diff = abs(debits - credits)

        if diff < 0.000001:
            return ui.span(
                ui.HTML('<i class="bi bi-check-circle-fill"></i>'),
                " Trial Balance is in Balance",
                class_="tb-balanced-badge balanced"
            )
        else:
            return ui.span(
                ui.HTML('<i class="bi bi-exclamation-triangle-fill"></i>'),
                f" Out of Balance by {diff:,.6f}",
                class_="tb-balanced-badge unbalanced"
            )

    @output
    @render.ui
    def gl2_trial_balance_display():
        """Render beautiful trial balance as HTML table."""
        df = trial_balance_data()
        if df.empty:
            return ui.div(
                ui.div(
                    ui.HTML('<i class="bi bi-inbox" style="font-size: 3rem; color: #cbd5e1;"></i>'),
                    ui.p("No journal entries found", class_="mt-3 mb-1 fw-semibold"),
                    ui.p("Post some journal entries to see your trial balance.", class_="text-muted small"),
                    class_="text-center py-5"
                )
            )

        grouping = "account"
        try:
            grouping = input.gl2_tb_grouping()
        except:
            pass

        rows = []

        if grouping == "category":
            # Group by category - summary view
            summary = df.groupby('category').agg({
                'debit_balance': 'sum',
                'credit_balance': 'sum'
            }).reset_index()

            for _, row in summary.iterrows():
                dr = f"{row['debit_balance']:,.6f}" if row['debit_balance'] > 0.000001 else ""
                cr = f"{row['credit_balance']:,.6f}" if row['credit_balance'] > 0.000001 else ""
                dr_class = "text-right debit-value" if dr else "text-right tb-empty-cell"
                cr_class = "text-right credit-value" if cr else "text-right tb-empty-cell"

                rows.append(
                    ui.tags.tr(
                        ui.tags.td(row['category'], class_="acct-name", colspan="2"),
                        ui.tags.td(dr or "-", class_=dr_class),
                        ui.tags.td(cr or "-", class_=cr_class),
                        class_="tb-account-row"
                    )
                )
        else:
            # Individual accounts with category headers
            current_category = None

            for _, row in df.iterrows():
                # Category header row
                if row['category'] != current_category:
                    current_category = row['category']
                    # Extract just the category name (remove number prefix)
                    cat_name = current_category.split('. ', 1)[1] if '. ' in current_category else current_category
                    rows.append(
                        ui.tags.tr(
                            ui.tags.td(
                                ui.HTML(f'<i class="bi bi-folder2 me-2"></i>{cat_name}'),
                                colspan="4"
                            ),
                            class_="tb-category-row"
                        )
                    )

                # Account row
                dr = f"{row['debit_balance']:,.6f}" if row['debit_balance'] > 0.000001 else ""
                cr = f"{row['credit_balance']:,.6f}" if row['credit_balance'] > 0.000001 else ""
                dr_class = "text-right debit-value" if dr else "text-right tb-empty-cell"
                cr_class = "text-right credit-value" if cr else "text-right tb-empty-cell"

                rows.append(
                    ui.tags.tr(
                        ui.tags.td(row['account_number'], class_="acct-num"),
                        ui.tags.td(row['account_name'], class_="acct-name"),
                        ui.tags.td(dr or "-", class_=dr_class),
                        ui.tags.td(cr or "-", class_=cr_class),
                        class_="tb-account-row"
                    )
                )

        # Total row
        total_dr = df['debit_balance'].sum()
        total_cr = df['credit_balance'].sum()
        rows.append(
            ui.tags.tr(
                ui.tags.td("TOTAL", colspan="2"),
                ui.tags.td(f"{total_dr:,.6f}", class_="text-right"),
                ui.tags.td(f"{total_cr:,.6f}", class_="text-right"),
                class_="tb-total-row"
            )
        )

        # Build table header
        if grouping == "category":
            header_row = ui.tags.tr(
                ui.tags.th("Category", colspan="2"),
                ui.tags.th("Debit Balance", class_="text-right"),
                ui.tags.th("Credit Balance", class_="text-right"),
            )
        else:
            header_row = ui.tags.tr(
                ui.tags.th("Account #"),
                ui.tags.th("Account Name"),
                ui.tags.th("Debit Balance", class_="text-right"),
                ui.tags.th("Credit Balance", class_="text-right"),
            )

        return ui.tags.table(
            ui.tags.thead(header_row),
            ui.tags.tbody(*rows),
            class_="tb-table"
        )

    @output
    @render.data_frame
    def gl2_trial_balance_table():
        """Alternative DataGrid view of trial balance."""
        df = trial_balance_data()
        if df.empty:
            return render.DataGrid(
                pd.DataFrame({"Message": ["No data for trial balance."]}),
                width="100%"
            )

        display_df = df[['account_number', 'account_name', 'debit_balance', 'credit_balance']].copy()
        display_df['debit_balance'] = display_df['debit_balance'].apply(lambda x: f"{x:,.6f}" if x > 0 else "")
        display_df['credit_balance'] = display_df['credit_balance'].apply(lambda x: f"{x:,.6f}" if x > 0 else "")
        display_df.columns = ['Account #', 'Account Name', 'Debit Balance', 'Credit Balance']

        return render.DataGrid(display_df, width="100%", height="400px")

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
                    class_="btn-outline-danger btn-sm"
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

            row_key = f"{tx_hash}:{line['account']}:manual:{'DR' if line['debit'] > 0 else 'CR'}"

            records.append({
                # Core identifiers (1-4)
                'date': timestamp,
                'fund_id': '',
                'limited_partner_ID': None,
                'wallet_id': '',
                # Transaction info (5-7)
                'transaction_type': category or 'manual_entry',
                'cryptocurrency': 'ETH',
                'account_name': gl_acct_name.lower().replace(' ', '_').replace('-', '_') if gl_acct_name else '',
                # GL Account info (8-9)
                'GL_Acct_Number': line['account'],
                'GL_Acct_Name': gl_acct_name or f"Account {line['account']}",
                # Amounts (10-14)
                'debit_crypto': line['debit'],
                'credit_crypto': line['credit'],
                'eth_usd_price': 0.0,
                'debit_USD': 0.0,
                'credit_USD': 0.0,
                # Event info (15-17)
                'event': None,
                'function': None,
                'hash': tx_hash,
                # Loan details (18-31)
                'loan_id': None,
                'lender': None,
                'borrower': None,
                'from': None,
                'to': None,
                'contract_address': None,
                'collateral_address': None,
                'token_id': None,
                'principal_crypto': None,
                'principal_USD': None,
                'annual_interest_rate': None,
                'payoff_amount_crypto': None,
                'payoff_amount_USD': None,
                'loan_due_date': None,
                # End of day price (37)
                'end_of_day_ETH_USD': None,
                # Internal deduplication key
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

        display_cols = ['date', 'GL_Acct_Name', 'GL_Acct_Number', 'debit_crypto', 'credit_crypto']
        display_df = manual_df[[c for c in display_cols if c in manual_df.columns]].copy()

        if 'date' in display_df.columns:
            display_df['date'] = pd.to_datetime(display_df['date']).dt.strftime('%Y-%m-%d')

        for col in ['debit_crypto', 'credit_crypto']:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(
                    lambda x: f"{float(x):,.6f}" if pd.notna(x) and float(x) != 0 else ""
                )

        col_rename = {'date': 'Date', 'GL_Acct_Name': 'Account', 'GL_Acct_Number': 'Acct #',
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
        """Export trial balance to Excel."""
        df = trial_balance_data()
        if df.empty:
            df = pd.DataFrame({"Message": ["No data to export"]})

        from io import BytesIO
        buffer = BytesIO()

        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            export_df = df.copy()
            export_df.to_excel(writer, sheet_name='Trial Balance', index=False)

            worksheet = writer.sheets['Trial Balance']
            for idx, col in enumerate(export_df.columns):
                max_length = max(
                    export_df[col].astype(str).map(len).max(),
                    len(str(col))
                ) + 2
                worksheet.column_dimensions[chr(65 + idx) if idx < 26 else f'A{chr(65 + idx - 26)}'].width = min(max_length, 50)

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
            tb_df = trial_balance_data()
            if not tb_df.empty:
                tb_df.to_excel(writer, sheet_name='Trial Balance', index=False)

            # Sheet 3: Summary
            summary_data = {
                'Metric': ['Total Entries', 'Unique Accounts', 'Total Debits', 'Total Credits', 'Balance Check'],
                'Value': [
                    len(je_df) if not je_df.empty else 0,
                    je_df['GL_Acct_Number'].nunique() if not je_df.empty and 'GL_Acct_Number' in je_df.columns else 0,
                    sum(float(x) if pd.notna(x) else 0 for x in je_df.get('debit_crypto', [])) if not je_df.empty else 0,
                    sum(float(x) if pd.notna(x) else 0 for x in je_df.get('credit_crypto', [])) if not je_df.empty else 0,
                    'Balanced' if abs(sum(float(x) if pd.notna(x) else 0 for x in je_df.get('debit_crypto', [])) -
                                     sum(float(x) if pd.notna(x) else 0 for x in je_df.get('credit_crypto', []))) < 0.000001 else 'Not Balanced'
                ]
            }
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)

        buffer.seek(0)
        return buffer.getvalue()
