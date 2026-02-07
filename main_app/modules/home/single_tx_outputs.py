"""
Single Transaction Decoder Outputs

Server-side logic for the Single TX decoder tab.
Decodes a single transaction by hash and allows GL posting.
"""

from shiny import reactive, render, ui
from typing import Dict, List, Any, Optional
import logging
import pandas as pd
from datetime import datetime, timezone
from decimal import Decimal

logger = logging.getLogger(__name__)


def _generate_gl_row_key(row: Dict) -> str:
    """
    Generate deterministic unique key for GL row deduplication.
    """
    return (
        f"{row.get('hash', '')}:"
        f"{row.get('account_name', '')}:"
        f"{row.get('transaction_type', '')}:"
        f"{row.get('debit_crypto', 0)}:"
        f"{row.get('credit_crypto', 0)}"
    )


def register_single_tx_outputs(input, output, session, selected_fund):
    """
    Register server outputs for the Single TX decoder tab.

    This reuses the existing Gondi decoder to decode individual transactions.
    """

    # Reactive values for state management
    decoded_single_tx = reactive.value(None)  # Stores the decoded transaction result
    decoding_status = reactive.value("idle")  # idle, decoding, success, error
    error_message = reactive.value("")
    posting_status = reactive.value("idle")  # idle, posting, success, error

    # =========================================================================
    # DECODE BUTTON HANDLER
    # =========================================================================

    @reactive.effect
    @reactive.event(input.decode_single_tx_btn)
    def handle_decode_single_tx():
        """Decode a single transaction by hash using the Gondi decoder"""
        tx_hash = input.single_tx_hash_input()

        if not tx_hash:
            ui.notification_show("Please enter a transaction hash", type="warning")
            return

        # Normalize hash
        tx_hash = tx_hash.strip()
        if not tx_hash.startswith('0x'):
            tx_hash = '0x' + tx_hash

        if len(tx_hash) != 66:
            ui.notification_show("Invalid transaction hash (must be 66 characters with 0x prefix)", type="error")
            return

        decoding_status.set("decoding")
        error_message.set("")
        decoded_single_tx.set(None)

        try:
            # Import and initialize the Gondi decoder
            from ...services.decoders.gondi_decoder import GondiEventDecoder, GondiJournalEntryGenerator
            from ...services.decoders.abis import load_abi
            from ...s3_utils import load_WALLET_file
            import os
            from web3 import Web3
            from dotenv import load_dotenv

            load_dotenv()

            # Get Web3 connection
            infura_key = os.getenv("INFURA_API_KEY")
            if not infura_key:
                raise ValueError("INFURA_API_KEY not set in environment")

            w3 = Web3(Web3.HTTPProvider(f"https://mainnet.infura.io/v3/{infura_key}"))
            if not w3.is_connected():
                raise ValueError("Could not connect to Ethereum node")

            # Load wallet metadata for fund identification
            wallet_df = load_WALLET_file()
            wallet_metadata = {}
            fund_wallets = []

            if wallet_df is not None and not wallet_df.empty:
                current_fund = selected_fund() if selected_fund else None
                for _, row in wallet_df.iterrows():
                    addr = str(row.get('wallet_address', '')).strip().lower()
                    fund_id = str(row.get('fund_id', '')).strip()
                    friendly_name = str(row.get('friendly_name', '')).strip()
                    category = str(row.get('category', '')).strip()

                    if addr:
                        wallet_metadata[addr] = {
                            'fund_id': fund_id,
                            'friendly_name': friendly_name,
                            'category': category,
                        }
                        # Add to fund wallets if category is 'fund' and matches selected fund
                        if category.lower() == 'fund':
                            if not current_fund or fund_id == current_fund:
                                fund_wallets.append(addr)

            logger.info(f"Loaded {len(wallet_metadata)} wallet mappings, {len(fund_wallets)} fund wallets")

            # Load Gondi contract ABIs
            GONDI_CONTRACTS = {
                "0xf41b389e0c1950dc0b16c9498eae77131cc08a56": "v1",
                "0x478f6f994c6fb3cf3e444a489b3ad9edb8ccae16": "v2",
                "0xf65b99ce6dc5f6c556172bcc0ff27d3665a7d9a8": "v3",
                "0x59e0b87e3dcfb5d34c06c71c3fbf7f6b7d77a4ff": "multi_source",
            }

            contracts = {}
            for addr, version in GONDI_CONTRACTS.items():
                abi = load_abi(addr, f"gondi_{version}")
                if abi:
                    contracts[addr] = w3.eth.contract(
                        address=Web3.to_checksum_address(addr),
                        abi=abi
                    )
                    logger.info(f"Loaded Gondi {version} ABI for {addr[:16]}...")

            if not contracts:
                raise ValueError("Could not load any Gondi contract ABIs")

            # Initialize the decoder and journal generator
            decoder = GondiEventDecoder(
                w3=w3,
                contracts=contracts,
                wallet_metadata=wallet_metadata
            )

            journal_generator = GondiJournalEntryGenerator(
                wallet_metadata=wallet_metadata
            )

            # Decode the transaction (decoder fetches tx/receipt internally)
            logger.info(f"Decoding transaction {tx_hash[:16]}...")
            decoded_events = decoder.decode_transaction(tx_hash)

            if not decoded_events:
                logger.warning("No events decoded from transaction")

            # Fetch transaction details for display
            tx = w3.eth.get_transaction(tx_hash)
            receipt = w3.eth.get_transaction_receipt(tx_hash)
            block = w3.eth.get_block(tx['blockNumber'])

            # Get ETH price from CoinGecko API (historical price at block timestamp)
            eth_price = 2500.0  # Default fallback
            try:
                import requests
                from datetime import datetime
                block_date = datetime.utcfromtimestamp(block['timestamp']).strftime('%d-%m-%Y')
                resp = requests.get(
                    f"https://api.coingecko.com/api/v3/coins/ethereum/history?date={block_date}",
                    timeout=5
                )
                if resp.status_code == 200:
                    data = resp.json()
                    eth_price = data.get('market_data', {}).get('current_price', {}).get('usd', 2500.0)
                    logger.info(f"ETH price on {block_date}: ${eth_price:.2f}")
            except Exception as e:
                logger.warning(f"Could not get ETH price from CoinGecko: {e}")

            # Generate journal entries
            logger.info(f"Generating journal entries for {len(decoded_events)} events...")
            journal_results = journal_generator.process_events(
                decoded_events,
                generate_accruals=False,  # Single TX doesn't need accruals
                generate_reversals=False,
                validate_reversals=False
            )

            # Collect all journal entries
            all_journal_entries = []
            for key, df in journal_results.items():
                if isinstance(df, pd.DataFrame) and not df.empty:
                    all_journal_entries.extend(df.to_dict('records'))

            # Build result dictionary
            result = {
                'tx_hash': tx_hash,
                'block_number': tx['blockNumber'],
                'timestamp': datetime.fromtimestamp(block['timestamp'], tz=timezone.utc).isoformat(),
                'from_address': tx['from'].lower(),
                'to_address': (tx.get('to') or '').lower(),
                'value_eth': float(tx['value']) / 1e18,
                'gas_used': receipt['gasUsed'],
                'gas_price_gwei': float(tx['gasPrice']) / 1e9,
                'eth_price': eth_price,
                'status': 'success' if receipt['status'] == 1 else 'failed',
                'decoded_events': [
                    {
                        'event_type': e.event_type,
                        'loan_id': e.loan_id,
                        'tx_hash': e.tx_hash,
                        'block_number': e.block_number,
                        'log_index': e.log_index,
                        'timestamp': e.timestamp.isoformat() if e.timestamp else None,
                        'fund_tranches': len(e.fund_tranches) if e.fund_tranches else 0,
                        'is_fund_borrower': e.is_fund_borrower,
                        'is_fund_originator': getattr(e, 'is_fund_originator', False),
                        'loan': _loan_to_dict(e.loan) if e.loan else None,
                        'liquidator': getattr(e, 'liquidator', None),
                    }
                    for e in decoded_events
                ],
                'journal_entries': all_journal_entries,
                'journal_count': len(all_journal_entries),
            }

            decoded_single_tx.set(result)
            decoding_status.set("success")

            if all_journal_entries:
                ui.notification_show(
                    f"Decoded {len(decoded_events)} events, generated {len(all_journal_entries)} journal entries",
                    type="success"
                )
            else:
                ui.notification_show(
                    f"Decoded {len(decoded_events)} events, no journal entries generated (fund may not be involved)",
                    type="warning"
                )

        except Exception as e:
            logger.error(f"Error decoding transaction: {e}", exc_info=True)
            decoding_status.set("error")
            error_message.set(str(e))
            ui.notification_show(f"Decoding failed: {str(e)}", type="error")

    # =========================================================================
    # POST TO GL BUTTON HANDLER
    # =========================================================================

    @reactive.effect
    @reactive.event(input.post_single_tx_to_gl)
    def handle_post_single_tx():
        """Post the decoded transaction's journal entries to GL"""
        result = decoded_single_tx.get()
        if not result or not result.get('journal_entries'):
            ui.notification_show("No journal entries to post", type="warning")
            return

        posting_status.set("posting")

        try:
            from ...s3_utils import load_WALLET_file, load_COA_file, load_GL_file, save_GL_file

            journal_entries = result['journal_entries']

            # Load wallet mapping for fund_id lookup
            wallet_df = load_WALLET_file()
            wallet_to_fund_map = {}
            if wallet_df is not None and not wallet_df.empty:
                for _, row in wallet_df.iterrows():
                    addr = str(row.get('wallet_address', '')).strip().lower()
                    fund = str(row.get('fund_id', '')).strip()
                    if addr and fund:
                        wallet_to_fund_map[addr] = fund

            # Load COA for GL account number lookup
            coa_df = load_COA_file()
            coa_map = {}
            if coa_df is not None and not coa_df.empty:
                for _, row in coa_df.iterrows():
                    try:
                        acct_num = int(row.get('GL_Acct_Number', 0))
                        acct_name = str(row.get('GL_Acct_Name', '')).strip()
                        if acct_num and acct_name:
                            coa_map[acct_name] = (acct_num, acct_name)
                            coa_map[acct_name.lower()] = (acct_num, acct_name)
                    except:
                        continue

            # Create DataFrame with dedup keys
            df_new = pd.DataFrame(journal_entries)
            df_new['_row_key'] = df_new.apply(
                lambda row: _generate_gl_row_key(row.to_dict()),
                axis=1
            )

            # Get fresh GL data
            load_GL_file.cache_clear()
            existing_gl = load_GL_file()

            # Add dedup keys to existing GL
            existing_gl['_row_key'] = existing_gl.apply(
                lambda row: _generate_gl_row_key(row.to_dict()), axis=1
            )
            existing_keys = set(existing_gl['_row_key'].tolist())

            # Filter out duplicates
            df_to_add = df_new[~df_new['_row_key'].isin(existing_keys)]
            duplicates_count = len(df_new) - len(df_to_add)

            if df_to_add.empty:
                posting_status.set("success")
                ui.notification_show(
                    f"All {duplicates_count} entries already in GL (idempotent)",
                    type="message"
                )
                return

            # Drop the temp key column before saving
            df_to_add = df_to_add.drop(columns=['_row_key'])

            # Append to existing GL
            df_combined = pd.concat([existing_gl.drop(columns=['_row_key'], errors='ignore'), df_to_add], ignore_index=True)

            # Save to S3
            save_GL_file(df_combined)

            # Clear cache
            load_GL_file.cache_clear()

            posting_status.set("success")

            msg = f"Posted {len(df_to_add)} entries to GL"
            if duplicates_count > 0:
                msg += f" ({duplicates_count} duplicates skipped)"
            ui.notification_show(msg, type="success")

        except Exception as e:
            logger.error(f"Error posting to GL: {e}", exc_info=True)
            posting_status.set("error")
            ui.notification_show(f"Posting failed: {str(e)}", type="error")

    # =========================================================================
    # CLEAR BUTTON HANDLER
    # =========================================================================

    @reactive.effect
    @reactive.event(input.clear_single_tx)
    def handle_clear_single_tx():
        """Clear the current decoded transaction"""
        decoded_single_tx.set(None)
        decoding_status.set("idle")
        error_message.set("")
        posting_status.set("idle")

    # =========================================================================
    # EXPORT CSV HANDLER
    # =========================================================================

    @render.download(filename=lambda: f"journal_entries_{(decoded_single_tx.get() or {}).get('tx_hash', 'export')[:10]}.csv")
    def download_single_tx_csv():
        """Download journal entries from decoded single transaction as CSV"""
        result = decoded_single_tx.get()
        if not result or not result.get('journal_entries'):
            yield "tx_hash,date,account_name,debit_crypto,credit_crypto,cryptocurrency,debit_usd,credit_usd,eth_price,gl_acct_number,gl_acct_name,in_coa\n"
            return

        try:
            from ...services.decoders.accounts import COA

            journal_entries = result['journal_entries']
            tx_hash = result.get('tx_hash', '')
            timestamp = result.get('timestamp', '')
            eth_price = float(result.get('eth_price', 0))

            rows = []
            for je in journal_entries:
                account_name = je.get('account_name', '')
                debit_val = float(je.get('debit_crypto', 0) or je.get('debit', 0) or 0)
                credit_val = float(je.get('credit_crypto', 0) or je.get('credit', 0) or 0)
                crypto = je.get('cryptocurrency', je.get('asset', 'ETH'))

                # COA lookup
                gl_acct_number = ''
                gl_acct_name = ''
                in_coa = False
                for key, (num, name) in COA.items():
                    if name == account_name:
                        gl_acct_number = num
                        gl_acct_name = name
                        in_coa = True
                        break

                # Calculate USD amounts
                stablecoins = {'USDC', 'USDT', 'DAI', 'FRAX', 'LUSD'}
                if crypto.upper() in stablecoins:
                    debit_usd = debit_val
                    credit_usd = credit_val
                else:
                    debit_usd = debit_val * eth_price
                    credit_usd = credit_val * eth_price

                rows.append({
                    'tx_hash': tx_hash,
                    'date': timestamp,
                    'account_name': account_name,
                    'debit_crypto': debit_val if debit_val > 0 else '',
                    'credit_crypto': credit_val if credit_val > 0 else '',
                    'cryptocurrency': crypto,
                    'debit_usd': f"{debit_usd:.2f}" if debit_val > 0 else '',
                    'credit_usd': f"{credit_usd:.2f}" if credit_val > 0 else '',
                    'eth_price': eth_price,
                    'gl_acct_number': gl_acct_number,
                    'gl_acct_name': gl_acct_name,
                    'in_coa': in_coa,
                })

            df = pd.DataFrame(rows)
            yield df.to_csv(index=False)
            logger.info(f"Exported {len(rows)} journal entries from single TX to CSV")

        except Exception as e:
            logger.error(f"Failed to export single TX CSV: {e}", exc_info=True)
            yield f"Error exporting: {str(e)}\n"

    # =========================================================================
    # UI OUTPUTS
    # =========================================================================

    @output
    @render.ui
    def single_tx_status():
        """Show decoding status"""
        status = decoding_status.get()

        if status == "idle":
            return ui.div()
        elif status == "decoding":
            return ui.div(
                ui.HTML('<i class="bi bi-hourglass-split spin-animation me-2"></i>'),
                "Decoding transaction...",
                class_="alert alert-info d-flex align-items-center"
            )
        elif status == "error":
            return ui.div(
                ui.HTML('<i class="bi bi-exclamation-triangle me-2"></i>'),
                f"Error: {error_message.get()}",
                class_="alert alert-danger"
            )
        return ui.div()

    @output
    @render.ui
    def single_tx_results():
        """Display decoded transaction summary"""
        result = decoded_single_tx.get()
        if not result:
            return ui.div()

        # Transaction summary card
        return ui.card(
            ui.card_header(
                ui.div(
                    ui.HTML('<i class="bi bi-check-circle-fill text-success me-2"></i>'),
                    "Transaction Summary",
                    class_="d-flex align-items-center"
                )
            ),
            ui.card_body(
                ui.layout_columns(
                    ui.div(
                        ui.tags.small("TX Hash", class_="text-muted d-block"),
                        ui.tags.code(result['tx_hash'][:20] + "..."),
                    ),
                    ui.div(
                        ui.tags.small("Block", class_="text-muted d-block"),
                        ui.tags.span(str(result['block_number'])),
                    ),
                    ui.div(
                        ui.tags.small("Status", class_="text-muted d-block"),
                        ui.tags.span(
                            result['status'].upper(),
                            class_=f"badge bg-{'success' if result['status'] == 'success' else 'danger'}"
                        ),
                    ),
                    ui.div(
                        ui.tags.small("ETH Price", class_="text-muted d-block"),
                        ui.tags.span(f"${result['eth_price']:,.2f}"),
                    ),
                    col_widths=[4, 2, 2, 4]
                ),
                ui.hr(),
                ui.div(
                    ui.tags.small("From", class_="text-muted d-block"),
                    ui.tags.code(result['from_address'], class_="small"),
                    class_="mb-2"
                ),
                ui.div(
                    ui.tags.small("To", class_="text-muted d-block"),
                    ui.tags.code(result['to_address'] or "Contract Creation", class_="small"),
                ),
            ),
            class_="mt-3"
        )

    @output
    @render.ui
    def single_tx_decoded_events():
        """Display decoded events"""
        result = decoded_single_tx.get()
        if not result or not result.get('decoded_events'):
            return ui.div()

        events = result['decoded_events']

        event_cards = []
        for i, event in enumerate(events):
            event_cards.append(
                ui.div(
                    ui.div(
                        ui.span(
                            event['event_type'],
                            class_="badge bg-primary me-2"
                        ),
                        ui.span(f"log index {event['log_index']}", class_="text-muted small"),
                        class_="d-flex align-items-center mb-2"
                    ),
                    ui.div(
                        ui.tags.small(f"loan_id: {event['loan_id']}", class_="d-block"),
                        ui.tags.small(f"fund_tranches: {event['fund_tranches']}", class_="d-block"),
                        ui.tags.small(f"is_fund_borrower: {event['is_fund_borrower']}", class_="d-block"),
                        ui.tags.small(f"is_fund_originator: {event.get('is_fund_originator', False)}", class_="d-block"),
                        ui.tags.small(f"liquidator: {event.get('liquidator', 'N/A')}", class_="d-block") if event.get('liquidator') else None,
                    ),
                    class_="border rounded p-3 mb-2"
                )
            )

        return ui.card(
            ui.card_header(f"Decoded Events ({len(events)})"),
            ui.card_body(*event_cards) if event_cards else ui.card_body("No events decoded"),
            class_="mt-3"
        )

    @output
    @render.ui
    def single_tx_journal_entries():
        """Display journal entries preview"""
        result = decoded_single_tx.get()
        if not result:
            return ui.div()

        journal_entries = result.get('journal_entries', [])

        if not journal_entries:
            return ui.card(
                ui.card_header("Journal Entries (0)"),
                ui.card_body(
                    ui.div(
                        ui.HTML('<i class="bi bi-info-circle text-warning me-2"></i>'),
                        "No journal entries generated. This may mean the fund is not involved in this transaction.",
                        class_="alert alert-warning"
                    )
                ),
                class_="mt-3"
            )

        # Build a simple table of journal entries
        rows = []
        for je in journal_entries:
            # Handle both 'debit'/'credit' and 'debit_crypto'/'credit_crypto' column names
            debit_val = je.get('debit_crypto') or je.get('debit', 0)
            credit_val = je.get('credit_crypto') or je.get('credit', 0)
            # Format the values
            debit_str = f"{float(debit_val):.6f}" if debit_val and float(debit_val) > 0 else ''
            credit_str = f"{float(credit_val):.6f}" if credit_val and float(credit_val) > 0 else ''
            rows.append(
                ui.tags.tr(
                    ui.tags.td(je.get('account_name', ''), class_="small"),
                    ui.tags.td(debit_str, class_="text-end"),
                    ui.tags.td(credit_str, class_="text-end"),
                    ui.tags.td(je.get('cryptocurrency', ''), class_="text-center"),
                )
            )

        return ui.card(
            ui.card_header(f"Journal Entries ({len(journal_entries)})"),
            ui.card_body(
                ui.tags.table(
                    ui.tags.thead(
                        ui.tags.tr(
                            ui.tags.th("Account"),
                            ui.tags.th("Debit", class_="text-end"),
                            ui.tags.th("Credit", class_="text-end"),
                            ui.tags.th("Currency", class_="text-center"),
                        )
                    ),
                    ui.tags.tbody(*rows),
                    class_="table table-sm table-striped"
                )
            ),
            class_="mt-3"
        )

    @output
    @render.ui
    def single_tx_actions():
        """Display action buttons after decoding"""
        result = decoded_single_tx.get()
        if not result:
            return ui.div()

        journal_count = result.get('journal_count', 0)
        status = posting_status.get()

        buttons = []

        # Post to GL button (only if there are journal entries)
        if journal_count > 0:
            if status == "posting":
                buttons.append(
                    ui.input_action_button(
                        "post_single_tx_to_gl",
                        ui.HTML('<i class="bi bi-hourglass-split spin-animation me-2"></i>Posting...'),
                        class_="btn-success me-2",
                        disabled=True
                    )
                )
            elif status == "success":
                buttons.append(
                    ui.span(
                        ui.HTML('<i class="bi bi-check-circle me-2"></i>Posted to GL'),
                        class_="badge bg-success p-2 me-2"
                    )
                )
            else:
                buttons.append(
                    ui.input_action_button(
                        "post_single_tx_to_gl",
                        ui.HTML('<i class="bi bi-journal-arrow-up me-2"></i>Post to General Ledger'),
                        class_="btn-success me-2"
                    )
                )

            # Export CSV button
            buttons.append(
                ui.download_button(
                    "download_single_tx_csv",
                    ui.HTML('<i class="bi bi-download me-2"></i>Export to CSV'),
                    class_="btn-outline-primary me-2"
                )
            )

        # Clear button
        buttons.append(
            ui.input_action_button(
                "clear_single_tx",
                ui.HTML('<i class="bi bi-arrow-counterclockwise me-2"></i>Decode Another'),
                class_="btn-outline-primary"
            )
        )

        # Etherscan link
        buttons.append(
            ui.a(
                ui.HTML('<i class="bi bi-box-arrow-up-right me-2"></i>View on Etherscan'),
                href=f"https://etherscan.io/tx/{result['tx_hash']}",
                target="_blank",
                class_="btn btn-outline-secondary ms-2"
            )
        )

        return ui.div(*buttons, class_="mt-4 d-flex align-items-center")


def _loan_to_dict(loan) -> Optional[Dict]:
    """Convert Loan object to dictionary for display"""
    if not loan:
        return None
    return {
        'borrower': getattr(loan, 'borrower', None),
        'principalAmount': getattr(loan, 'principalAmount', 0),
        'principalAddress': getattr(loan, 'principalAddress', None),
        'nftCollateralAddress': getattr(loan, 'nftCollateralAddress', None),
        'nftCollateralTokenId': getattr(loan, 'nftCollateralTokenId', None),
        'tranches_count': len(loan.tranches) if hasattr(loan, 'tranches') and loan.tranches else 0,
    }
