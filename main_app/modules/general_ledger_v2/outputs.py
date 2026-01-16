"""
General Ledger 2 Outputs Module

Server-side render functions for the GL2 module:
- Journal entries viewing and filtering
- Account ledger with running balances
- Trial balance generation
- Manual journal entry creation
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
    # Match patterns like "100.30" or "10030" at the start
    match = re.match(r'^(\d+\.?\d*)', str(account_name_str))
    if match:
        return match.group(1)
    return ''


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

    # Entry lines for new manual entry (list of dicts)
    entry_lines = reactive.value([
        {"account": "", "debit": 0.0, "credit": 0.0},
        {"account": "", "debit": 0.0, "credit": 0.0}
    ])

    # GL2 data refresh trigger
    gl2_data_version = reactive.value(0)

    # =========================================================================
    # DATA LOADING
    # =========================================================================

    @reactive.calc
    def gl2_data():
        """Load GL2 data from S3."""
        # Depend on version for refresh
        _ = gl2_data_version()
        try:
            df = load_GL2_file()
            logger.info(f"[GL2] Loaded {len(df)} rows from GL2")

            if not df.empty:
                # Fix empty account_number by extracting from account_name
                if 'account_number' in df.columns and 'account_name' in df.columns:
                    empty_acct_mask = (df['account_number'].isna()) | (df['account_number'] == '')
                    if empty_acct_mask.any():
                        logger.info(f"[GL2] Fixing {empty_acct_mask.sum()} empty account numbers")
                        df.loc[empty_acct_mask, 'account_number'] = df.loc[empty_acct_mask, 'account_name'].apply(extract_account_number)

                # Fix empty categories - derive from entry_type if possible
                if 'category' in df.columns:
                    empty_cat_mask = (df['category'].isna()) | (df['category'] == '')
                    if empty_cat_mask.any():
                        logger.info(f"[GL2] {empty_cat_mask.sum()} entries have empty category")
                        # Try to derive category from entry_type or set to "uncategorized"
                        df.loc[empty_cat_mask, 'category'] = df.loc[empty_cat_mask, 'entry_type'].apply(
                            lambda x: x if x and x not in ['DEBIT', 'CREDIT'] else 'uncategorized'
                        )

                # Log sample data
                logger.info(f"[GL2] Columns: {list(df.columns)}")
                logger.info(f"[GL2] Sample account_numbers: {df['account_number'].unique()[:5].tolist()}")
                logger.info(f"[GL2] Sample categories: {df['category'].unique()[:5].tolist() if 'category' in df.columns else 'N/A'}")

            return df
        except Exception as e:
            logger.error(f"[GL2] Error loading GL2 data: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame(columns=get_gl2_schema_columns())

    @reactive.calc
    def coa_data():
        """Load Chart of Accounts data."""
        try:
            df = load_COA_file()
            logger.info(f"[GL2] Loaded COA with {len(df)} accounts")
            if not df.empty:
                logger.info(f"[GL2] COA columns: {list(df.columns)}")
            return df
        except Exception as e:
            logger.error(f"[GL2] Error loading COA: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    @reactive.calc
    def account_choices():
        """Generate account choices from GL2 data and COA."""
        choices = {"": "All Accounts"}

        # First, get unique accounts from GL2 data
        gl2_df = gl2_data()
        if not gl2_df.empty and 'account_number' in gl2_df.columns and 'account_name' in gl2_df.columns:
            gl2_accounts = gl2_df.groupby('account_number').agg({'account_name': 'first'}).reset_index()
            for _, row in gl2_accounts.iterrows():
                acct_num = str(row['account_number'])
                acct_name = str(row['account_name'])
                if acct_num and acct_num != '':
                    # Use the full account name if available
                    choices[acct_num] = acct_name if acct_name else acct_num
            logger.info(f"[GL2] Added {len(gl2_accounts)} accounts from GL2 data")

        # Also add COA accounts
        coa_df = coa_data()
        if not coa_df.empty:
            for _, row in coa_df.iterrows():
                try:
                    acct_num = str(int(row['GL_Acct_Number']))
                    acct_name = row['GL_Acct_Name']
                    if acct_num not in choices:
                        choices[acct_num] = f"{acct_num} - {acct_name}"
                except Exception as e:
                    continue

        logger.info(f"[GL2] Generated {len(choices)} total account choices")
        return choices

    @reactive.calc
    def category_choices():
        """Generate category choices from GL2 data."""
        choices = {"": "All Categories"}

        gl2_df = gl2_data()
        if not gl2_df.empty and 'category' in gl2_df.columns:
            categories = gl2_df['category'].dropna().unique()
            for cat in sorted(categories):
                if cat and str(cat).strip():
                    choices[str(cat)] = str(cat)
            logger.info(f"[GL2] Generated {len(choices)} category choices: {list(choices.keys())[:10]}")

        return choices

    # =========================================================================
    # JOURNAL ENTRIES TAB
    # =========================================================================

    @reactive.calc
    def filtered_journal_entries():
        """Filter journal entries based on user selections."""
        df = gl2_data()
        logger.info(f"[GL2] filtered_journal_entries: starting with {len(df)} rows")

        if df.empty:
            logger.info("[GL2] filtered_journal_entries: DataFrame is empty")
            return df

        # Date filter
        try:
            date_range = input.gl2_date_range()
            if date_range and len(date_range) == 2:
                start_date = pd.Timestamp(date_range[0], tz='UTC')
                end_date = pd.Timestamp(date_range[1], tz='UTC') + pd.Timedelta(days=1)
                logger.info(f"[GL2] Date filter: {start_date} to {end_date}")

                if 'timestamp' in df.columns:
                    before_count = len(df)
                    df = df[(df['timestamp'] >= start_date) & (df['timestamp'] < end_date)]
                    logger.info(f"[GL2] Date filter: {before_count} -> {len(df)} rows")
        except Exception as e:
            logger.warning(f"[GL2] Date filter error: {e}")

        # Account filter
        try:
            account_filter = input.gl2_account_filter()
            if account_filter:
                logger.info(f"[GL2] Account filter: '{account_filter}'")
                before_count = len(df)
                df = df[df['account_number'] == account_filter]
                logger.info(f"[GL2] Account filter: {before_count} -> {len(df)} rows")
        except Exception as e:
            logger.warning(f"[GL2] Account filter error: {e}")

        # Category filter
        try:
            category_filter = input.gl2_category_filter()
            if category_filter:
                logger.info(f"[GL2] Category filter: '{category_filter}'")
                before_count = len(df)
                df = df[df['category'] == category_filter]
                logger.info(f"[GL2] Category filter: {before_count} -> {len(df)} rows")
        except Exception as e:
            logger.warning(f"[GL2] Category filter error: {e}")

        # Search filter
        try:
            search = input.gl2_search()
            if search:
                logger.info(f"[GL2] Search filter: '{search}'")
                search_lower = search.lower()
                mask = (
                    df['tx_hash'].astype(str).str.lower().str.contains(search_lower, na=False) |
                    df['description'].astype(str).str.lower().str.contains(search_lower, na=False)
                )
                before_count = len(df)
                df = df[mask]
                logger.info(f"[GL2] Search filter: {before_count} -> {len(df)} rows")
        except Exception as e:
            logger.warning(f"[GL2] Search filter error: {e}")

        logger.info(f"[GL2] filtered_journal_entries: returning {len(df)} rows")
        return df

    @output
    @render.text
    def gl2_total_entries():
        df = filtered_journal_entries()
        return f"{len(df):,}"

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

        if diff < 0.000001:
            return "Balanced"
        return f"Off by {diff:,.6f}"

    @output
    @render.data_frame
    def gl2_journal_entries_table():
        df = filtered_journal_entries()
        if df.empty:
            return render.DataGrid(
                pd.DataFrame({"Message": ["No journal entries found. Post entries from Decoded Transactions or create manual entries."]}),
                width="100%"
            )

        # Select and format display columns
        display_cols = ['timestamp', 'tx_hash', 'account_number', 'account_name',
                       'entry_type', 'debit_crypto', 'credit_crypto', 'description', 'category']

        display_df = df[[c for c in display_cols if c in df.columns]].copy()

        # Format timestamp
        if 'timestamp' in display_df.columns:
            display_df['timestamp'] = pd.to_datetime(display_df['timestamp']).dt.strftime('%Y-%m-%d %H:%M')

        # Format numeric columns
        for col in ['debit_crypto', 'credit_crypto']:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(
                    lambda x: f"{float(x):,.6f}" if pd.notna(x) and float(x) != 0 else ""
                )

        # Shorten tx_hash for display
        if 'tx_hash' in display_df.columns:
            display_df['tx_hash'] = display_df['tx_hash'].apply(
                lambda x: f"{x[:10]}..." if pd.notna(x) and len(str(x)) > 10 else x
            )

        return render.DataGrid(
            display_df,
            filters=True,
            width="100%",
            height="500px"
        )

    @output
    @render.ui
    def gl2_update_account_dropdown():
        """Update account dropdown with data from GL2 and COA."""
        choices = account_choices()
        logger.info(f"[GL2] Updating account dropdown with {len(choices)} choices")
        return ui.update_selectize(
            "gl2_account_filter",
            choices=choices,
            selected=""
        )

    @output
    @render.ui
    def gl2_update_category_dropdown():
        """Update category dropdown with actual categories from GL2 data."""
        choices = category_choices()
        logger.info(f"[GL2] Updating category dropdown with {len(choices)} choices")
        return ui.update_selectize(
            "gl2_category_filter",
            choices=choices,
            selected=""
        )

    # Refresh button handler
    @reactive.effect
    @reactive.event(input.gl2_refresh)
    def _refresh_gl2_data():
        clear_GL2_cache()
        gl2_data_version.set(gl2_data_version() + 1)
        ui.notification_show("GL2 data refreshed", type="message")

    # Clear filters handler
    @reactive.effect
    @reactive.event(input.gl2_clear_filters)
    def _clear_filters():
        ui.update_selectize("gl2_account_filter", selected="")
        ui.update_selectize("gl2_category_filter", selected="")
        ui.update_text("gl2_search", value="")

    # =========================================================================
    # ACCOUNT LEDGER TAB
    # =========================================================================

    @output
    @render.ui
    def gl2_update_ledger_account_dropdown():
        """Update ledger account dropdown with COA data."""
        choices = account_choices()
        # Remove "All Accounts" option for ledger view
        if "" in choices:
            del choices[""]
        choices = {"": "Select an account...", **choices}

        return ui.update_selectize(
            "gl2_ledger_account",
            choices=choices,
            selected=""
        )

    @reactive.calc
    def account_ledger_data():
        """Get ledger data for selected account."""
        df = gl2_data()
        account = input.gl2_ledger_account()

        if df.empty or not account:
            return pd.DataFrame()

        # Filter by account
        df = df[df['account_number'] == account].copy()

        if df.empty:
            return df

        # Date filter
        try:
            date_range = input.gl2_ledger_date_range()
            if date_range and len(date_range) == 2:
                start_date = pd.Timestamp(date_range[0], tz='UTC')
                end_date = pd.Timestamp(date_range[1], tz='UTC') + pd.Timedelta(days=1)

                if 'timestamp' in df.columns:
                    df = df[(df['timestamp'] >= start_date) & (df['timestamp'] < end_date)]
        except:
            pass

        # Sort by timestamp
        if 'timestamp' in df.columns:
            df = df.sort_values('timestamp')

        # Calculate running balance
        df['running_balance'] = 0.0
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
            return "Account Ledger"

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
                ui.p("Select an account to view its ledger.", class_="text-muted"),
                class_="mb-3"
            )

        total_debits = sum(float(x) if pd.notna(x) else 0 for x in df['debit_crypto'])
        total_credits = sum(float(x) if pd.notna(x) else 0 for x in df['credit_crypto'])
        balance = total_debits - total_credits

        return ui.layout_columns(
            ui.value_box(
                title="Total Debits",
                value=f"{total_debits:,.6f}",
                theme="success"
            ),
            ui.value_box(
                title="Total Credits",
                value=f"{total_credits:,.6f}",
                theme="info"
            ),
            ui.value_box(
                title="Balance",
                value=f"{balance:,.6f}",
                theme="primary"
            ),
            col_widths=[4, 4, 4],
            class_="mb-3"
        )

    @output
    @render.data_frame
    def gl2_account_ledger_table():
        df = account_ledger_data()
        if df.empty:
            return render.DataGrid(
                pd.DataFrame({"Message": ["Select an account and click 'Load Ledger' to view entries."]}),
                width="100%"
            )

        # Select display columns
        display_cols = ['timestamp', 'description', 'debit_crypto', 'credit_crypto', 'running_balance']
        display_df = df[[c for c in display_cols if c in df.columns]].copy()

        # Format timestamp
        if 'timestamp' in display_df.columns:
            display_df['timestamp'] = pd.to_datetime(display_df['timestamp']).dt.strftime('%Y-%m-%d')

        # Format numeric columns
        for col in ['debit_crypto', 'credit_crypto', 'running_balance']:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(
                    lambda x: f"{float(x):,.6f}" if pd.notna(x) else ""
                )

        # Rename columns for display
        display_df.columns = ['Date', 'Description', 'Debit', 'Credit', 'Balance']

        return render.DataGrid(
            display_df,
            width="100%",
            height="400px"
        )

    # =========================================================================
    # TRIAL BALANCE TAB
    # =========================================================================

    @reactive.calc
    def trial_balance_data():
        """Generate trial balance from GL2 data."""
        df = gl2_data()
        coa = coa_data()

        logger.info(f"[GL2] trial_balance_data: starting with {len(df)} rows")

        if df.empty:
            logger.info("[GL2] trial_balance_data: DataFrame is empty")
            return pd.DataFrame()

        # Filter by as-of date
        try:
            as_of_date = input.gl2_tb_as_of_date()
            if as_of_date:
                end_date = pd.Timestamp(as_of_date, tz='UTC') + pd.Timedelta(days=1)
                logger.info(f"[GL2] TB as-of date filter: < {end_date}")
                if 'timestamp' in df.columns:
                    before_count = len(df)
                    df = df[df['timestamp'] < end_date]
                    logger.info(f"[GL2] TB date filter: {before_count} -> {len(df)} rows")
        except Exception as e:
            logger.warning(f"[GL2] TB date filter error: {e}")

        if df.empty:
            logger.info("[GL2] trial_balance_data: DataFrame empty after date filter")
            return pd.DataFrame()

        logger.info(f"[GL2] Unique account_numbers in GL2: {df['account_number'].unique().tolist()[:10]}")

        # Group by account
        try:
            grouped = df.groupby('account_number').agg({
                'account_name': 'first',
                'debit_crypto': lambda x: sum(float(v) if pd.notna(v) else 0 for v in x),
                'credit_crypto': lambda x: sum(float(v) if pd.notna(v) else 0 for v in x)
            }).reset_index()
            logger.info(f"[GL2] TB grouped to {len(grouped)} accounts")
        except Exception as e:
            logger.error(f"[GL2] TB groupby error: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

        # Calculate net balance
        grouped['net_balance'] = grouped['debit_crypto'] - grouped['credit_crypto']

        # Determine debit/credit balances based on account type
        # Assets and Expenses have debit balances, Liabilities/Equity/Income have credit balances
        grouped['debit_balance'] = grouped.apply(
            lambda row: row['net_balance'] if row['net_balance'] > 0 else 0, axis=1
        )
        grouped['credit_balance'] = grouped.apply(
            lambda row: abs(row['net_balance']) if row['net_balance'] < 0 else 0, axis=1
        )

        # Add account category based on account number
        def get_category(acct_num):
            try:
                # Handle account numbers like "100.30" by extracting integer part
                num_str = str(acct_num).split('.')[0]
                num = int(num_str)
                if num < 200:
                    return "Assets"
                elif num < 300:
                    return "Liabilities"
                elif num < 400:
                    return "Capital/Equity"
                elif num < 500:
                    return "Other Income"
                elif num < 900:
                    return "Expenses"
                else:
                    return "Income"
            except Exception as e:
                logger.warning(f"[GL2] get_category error for '{acct_num}': {e}")
                return "Other"

        grouped['category'] = grouped['account_number'].apply(get_category)
        logger.info(f"[GL2] TB categories: {grouped['category'].value_counts().to_dict()}")

        # Sort by account number
        grouped = grouped.sort_values('account_number')

        logger.info(f"[GL2] trial_balance_data: returning {len(grouped)} accounts")
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
            return "0.000000"
        debits = df['debit_balance'].sum()
        credits = df['credit_balance'].sum()
        diff = debits - credits
        if abs(diff) < 0.000001:
            return "Balanced"
        return f"{diff:,.6f}"

    @output
    @render.data_frame
    def gl2_trial_balance_table():
        df = trial_balance_data()
        if df.empty:
            return render.DataGrid(
                pd.DataFrame({"Message": ["No data for trial balance. Post some journal entries first."]}),
                width="100%"
            )

        grouping = input.gl2_tb_grouping()

        if grouping == "category":
            # Group by category
            summary = df.groupby('category').agg({
                'debit_balance': 'sum',
                'credit_balance': 'sum'
            }).reset_index()

            # Format numbers
            summary['debit_balance'] = summary['debit_balance'].apply(lambda x: f"{x:,.6f}" if x > 0 else "")
            summary['credit_balance'] = summary['credit_balance'].apply(lambda x: f"{x:,.6f}" if x > 0 else "")

            summary.columns = ['Account Category', 'Debit Balance', 'Credit Balance']
        else:
            # Individual accounts
            summary = df[['account_number', 'account_name', 'debit_balance', 'credit_balance']].copy()
            summary['debit_balance'] = summary['debit_balance'].apply(lambda x: f"{x:,.6f}" if x > 0 else "")
            summary['credit_balance'] = summary['credit_balance'].apply(lambda x: f"{x:,.6f}" if x > 0 else "")
            summary.columns = ['Account #', 'Account Name', 'Debit Balance', 'Credit Balance']

        return render.DataGrid(
            summary,
            width="100%",
            height="400px"
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

        # Remove "All Accounts" option
        if "" in choices:
            choices = {k: v for k, v in choices.items() if k}
        choices = {"": "Select account...", **choices}

        line_elements = []
        for i, line in enumerate(lines):
            line_ui = ui.layout_columns(
                ui.input_selectize(
                    f"gl2_line_{i}_account",
                    f"Account {i+1}",
                    choices=choices,
                    selected=line.get('account', '')
                ),
                ui.input_numeric(
                    f"gl2_line_{i}_debit",
                    "Debit",
                    value=line.get('debit', 0.0),
                    min=0,
                    step=0.000001
                ),
                ui.input_numeric(
                    f"gl2_line_{i}_credit",
                    "Credit",
                    value=line.get('credit', 0.0),
                    min=0,
                    step=0.000001
                ),
                ui.input_action_button(
                    f"gl2_remove_line_{i}",
                    "Remove",
                    class_="btn-outline-danger btn-sm mt-4"
                ) if len(lines) > 2 else ui.div(),
                col_widths=[5, 3, 3, 1],
                class_="mb-2"
            )
            line_elements.append(line_ui)

        return ui.div(*line_elements)

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
        if abs(diff) < 0.000001:
            return "Balanced"
        return f"{diff:,.6f}"

    @output
    @render.ui
    def gl2_entry_validation_message():
        lines = entry_lines()
        debits = sum(line.get('debit', 0) or 0 for line in lines)
        credits = sum(line.get('credit', 0) or 0 for line in lines)

        # Check if balanced
        if abs(debits - credits) > 0.000001:
            return ui.div(
                ui.HTML('<i class="bi bi-exclamation-triangle text-warning"></i> '),
                "Entry is not balanced. Debits must equal credits.",
                class_="text-warning mt-2"
            )

        # Check if any accounts selected
        accounts_selected = [line.get('account') for line in lines if line.get('account')]
        if len(accounts_selected) < 2:
            return ui.div(
                ui.HTML('<i class="bi bi-info-circle text-info"></i> '),
                "Select at least two accounts.",
                class_="text-info mt-2"
            )

        # All good
        return ui.div(
            ui.HTML('<i class="bi bi-check-circle text-success"></i> '),
            "Entry is valid and ready to post.",
            class_="text-success mt-2"
        )

    # Add line button handler
    @reactive.effect
    @reactive.event(input.gl2_add_entry_line)
    def _add_entry_line():
        current = entry_lines()
        current.append({"account": "", "debit": 0.0, "credit": 0.0})
        entry_lines.set(current)

    # Clear form handler
    @reactive.effect
    @reactive.event(input.gl2_clear_entry)
    def _clear_entry_form():
        entry_lines.set([
            {"account": "", "debit": 0.0, "credit": 0.0},
            {"account": "", "debit": 0.0, "credit": 0.0}
        ])
        ui.update_text("gl2_new_entry_description", value="")

    # Post entry handler
    @reactive.effect
    @reactive.event(input.gl2_post_entry)
    def _post_manual_entry():
        # Collect entry data from inputs
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
            ui.notification_show("Entry must have at least 2 lines with amounts", type="error")
            return

        # Get entry metadata
        entry_date = input.gl2_new_entry_date()
        description = input.gl2_new_entry_description() or "Manual Journal Entry"
        category = input.gl2_new_entry_category()

        # Create timestamp
        timestamp = datetime.now(timezone.utc)
        if entry_date:
            timestamp = pd.Timestamp(entry_date, tz='UTC')

        # Generate unique tx_hash for manual entry
        tx_hash = f"manual_{hashlib.md5(f'{timestamp}{description}'.encode()).hexdigest()[:16]}"

        # Get COA for account names
        coa = coa_data()

        # Create GL records
        records = []
        for line in lines_data:
            # Get account name from COA
            account_name = ""
            if not coa.empty:
                match = coa[coa['GL_Acct_Number'].astype(str) == line['account']]
                if not match.empty:
                    account_name = match.iloc[0]['GL_Acct_Name']

            # Create row key
            row_key = f"{tx_hash}:{line['account']}:{'DEBIT' if line['debit'] > 0 else 'CREDIT'}:{line['debit']}:{line['credit']}"

            record = {
                'tx_hash': tx_hash,
                'entry_type': 'DEBIT' if line['debit'] > 0 else 'CREDIT',
                'account_number': line['account'],
                'account_name': account_name,
                'debit_crypto': line['debit'],
                'credit_crypto': line['credit'],
                'debit_USD': 0.0,
                'credit_USD': 0.0,
                'asset': 'ETH',
                'description': description,
                'category': category,
                'platform': 'manual',
                'timestamp': timestamp,
                'posted_date': datetime.now(timezone.utc),
                'row_key': row_key
            }
            records.append(record)

        # Load existing data and append
        try:
            existing_df = load_GL2_file()
            new_df = pd.DataFrame(records)
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)

            # Save
            if save_GL2_file(combined_df):
                ui.notification_show(f"Posted {len(records)} entries successfully!", type="message")

                # Clear form
                entry_lines.set([
                    {"account": "", "debit": 0.0, "credit": 0.0},
                    {"account": "", "debit": 0.0, "credit": 0.0}
                ])
                ui.update_text("gl2_new_entry_description", value="")

                # Refresh data
                gl2_data_version.set(gl2_data_version() + 1)
            else:
                ui.notification_show("Failed to save entry", type="error")

        except Exception as e:
            ui.notification_show(f"Error: {str(e)}", type="error")
            import traceback
            traceback.print_exc()

    @output
    @render.data_frame
    def gl2_recent_manual_entries():
        df = gl2_data()
        if df.empty:
            return render.DataGrid(
                pd.DataFrame({"Message": ["No manual entries yet."]}),
                width="100%"
            )

        # Filter to manual entries only
        manual_df = df[df['platform'] == 'manual'].copy()

        if manual_df.empty:
            return render.DataGrid(
                pd.DataFrame({"Message": ["No manual entries yet."]}),
                width="100%"
            )

        # Get most recent 20 entries
        if 'timestamp' in manual_df.columns:
            manual_df = manual_df.sort_values('timestamp', ascending=False).head(20)

        # Select display columns
        display_cols = ['timestamp', 'description', 'account_number', 'debit_crypto', 'credit_crypto']
        display_df = manual_df[[c for c in display_cols if c in manual_df.columns]].copy()

        # Format timestamp
        if 'timestamp' in display_df.columns:
            display_df['timestamp'] = pd.to_datetime(display_df['timestamp']).dt.strftime('%Y-%m-%d')

        # Format numbers
        for col in ['debit_crypto', 'credit_crypto']:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(
                    lambda x: f"{float(x):,.6f}" if pd.notna(x) and float(x) != 0 else ""
                )

        display_df.columns = ['Date', 'Description', 'Account', 'Debit', 'Credit']

        return render.DataGrid(
            display_df,
            width="100%",
            height="300px"
        )

    # =========================================================================
    # DOWNLOAD HANDLERS
    # =========================================================================

    @render.download(filename="gl2_journal_entries.xlsx")
    def gl2_download_je():
        df = filtered_journal_entries()
        from io import BytesIO
        buffer = BytesIO()
        df.to_excel(buffer, index=False)
        buffer.seek(0)
        return buffer.getvalue()

    @render.download(filename="gl2_account_ledger.xlsx")
    def gl2_download_ledger():
        df = account_ledger_data()
        from io import BytesIO
        buffer = BytesIO()
        df.to_excel(buffer, index=False)
        buffer.seek(0)
        return buffer.getvalue()

    @render.download(filename="gl2_trial_balance.xlsx")
    def gl2_download_tb():
        df = trial_balance_data()
        from io import BytesIO
        buffer = BytesIO()
        df.to_excel(buffer, index=False)
        buffer.seek(0)
        return buffer.getvalue()
