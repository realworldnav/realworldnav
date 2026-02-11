"""
Crypto Subledger UI module — Shiny integration for the subledger service.

Provides ingestion controls, balance/movement views, and on-chain reconciliation
proof mode via a tab under Investments.
"""

import asyncio
import concurrent.futures
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd
from shiny import reactive, render, ui

logger = logging.getLogger(__name__)

_executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)

# Cache for latest block number (avoid hammering Etherscan)
_latest_block_cache: Dict[str, object] = {"block": None, "fetched_at": 0.0}
_CACHE_TTL = 60  # seconds

SAFE_MODE_MAX_BLOCKS = 100_000
SAFE_MODE_MAX_WALLETS = 5

def _get_registry():
    """Lazy-load a shared AssetRegistry for symbol/decimal lookups."""
    from ...services.subledger.asset_registry import AssetRegistry
    if not hasattr(_get_registry, "_instance"):
        _get_registry._instance = AssetRegistry(chain_id=1)
    return _get_registry._instance


def _format_amount(raw_value, asset_id: str, registry=None, show_sign: bool = True) -> str:
    """Convert raw wei/base-unit amount to human-readable string with +/- sign."""
    try:
        val = int(raw_value)
    except (ValueError, TypeError):
        return str(raw_value)
    if registry is None:
        registry = _get_registry()
    decimals = registry.get_decimals(asset_id)
    human = val / (10 ** decimals)
    # Format number
    if abs(human) < 0.000001 and human != 0:
        formatted = f"{abs(human):.18f}".rstrip("0").rstrip(".")
    else:
        formatted = f"{abs(human):,.6f}".rstrip("0").rstrip(".")
    # Add explicit sign
    if not show_sign:
        return formatted if human >= 0 else f"-{formatted}"
    if human > 0:
        return f"+{formatted}"
    elif human < 0:
        return f"-{formatted}"
    return formatted


def _asset_symbol(asset_id: str, registry=None) -> str:
    """Resolve asset_id to a human-readable symbol."""
    if registry is None:
        registry = _get_registry()
    meta = registry.get(asset_id)
    if meta and meta.symbol:
        return meta.symbol
    # Fallback: extract contract and shorten
    from ...services.subledger.asset_registry import is_native, extract_contract
    if is_native(asset_id):
        return "ETH"
    contract = extract_contract(asset_id)
    return f"{contract[:6]}...{contract[-4:]}"


def _short_wallet(addr: str) -> str:
    """Shorten wallet address for display."""
    addr = str(addr)
    if len(addr) > 10:
        return f"{addr[:6]}...{addr[-4:]}"
    return addr


def _build_wallet_lookup() -> dict:
    """Build wallet_address -> friendly_name mapping from S3 wallet file."""
    try:
        from ...s3_utils import load_WALLET_file
        wallet_df = load_WALLET_file()
        if wallet_df.empty:
            return {}
        lookup = {}
        for _, row in wallet_df.iterrows():
            addr = str(row.get("wallet_address", "")).strip().lower()
            name = str(row.get("friendly_name", "")).strip()
            if addr and name:
                lookup[addr] = name
        return lookup
    except Exception:
        return {}


def _display_wallet(addr: str, lookup: dict) -> str:
    """Display wallet as friendly name or full address."""
    addr_lower = str(addr).strip().lower()
    name = lookup.get(addr_lower)
    if name:
        return f"{name} ({addr_lower})"
    return addr_lower


def _display_asset(asset_id: str, registry=None) -> str:
    """Display full asset name/symbol (not truncated)."""
    if registry is None:
        registry = _get_registry()
    meta = registry.get(asset_id)
    if meta:
        if meta.symbol:
            return meta.symbol
    from ...services.subledger.asset_registry import is_native, extract_contract
    if is_native(asset_id):
        return "ETH"
    contract = extract_contract(asset_id)
    return f"ERC-20: {contract}"


def _detail_field(label: str, value: str):
    """Render a styled label/value pair for detail panels."""
    return ui.div(
        ui.div(label, class_="subledger-field-label"),
        ui.div(str(value) if value else "N/A", class_="subledger-field-value"),
        class_="mb-2",
    )


TRACE_COMPLETENESS_DESCRIPTIONS = {
    "internals_only": (
        "Etherscan internals_only — top-level ETH from tx.value, "
        "internal ETH from traces. Full call tree not available."
    ),
    "full_call_tree": (
        "Full call tree — ALL ETH movements from traces, tx.value ignored. "
        "Most accurate but requires archive node."
    ),
    "none": (
        "No traces — only tx.value ETH is captured. "
        "Internal transfers are missing; ETH balances will undercount."
    ),
}


def _get_latest_block() -> Optional[int]:
    """Fetch latest Ethereum block number from Etherscan, cached for 60s."""
    now = time.time()
    if (
        _latest_block_cache["block"] is not None
        and (now - _latest_block_cache["fetched_at"]) < _CACHE_TTL
    ):
        return _latest_block_cache["block"]

    try:
        import os
        import requests
        from dotenv import load_dotenv
        load_dotenv()

        api_key = os.getenv("ETHERSCAN_API_KEY", "")
        logger.info(f"Fetching latest block (API key: {'set' if api_key else 'MISSING'})")
        resp = requests.get(
            "https://api.etherscan.io/v2/api",
            params={
                "chainid": "1",
                "module": "proxy",
                "action": "eth_blockNumber",
                "apikey": api_key,
            },
            timeout=10,
        )
        data = resp.json()
        logger.info(f"Etherscan eth_blockNumber response: {data}")
        block_hex = data.get("result", "")
        if block_hex and isinstance(block_hex, str) and block_hex.startswith("0x"):
            block_num = int(block_hex, 16)
            _latest_block_cache["block"] = block_num
            _latest_block_cache["fetched_at"] = now
            return block_num
        else:
            logger.warning(f"Unexpected eth_blockNumber result: {block_hex}")
    except Exception as e:
        logger.warning(f"Failed to fetch latest block: {e}")
    return _latest_block_cache.get("block")


# =============================================================================
# UI
# =============================================================================

def crypto_subledger_ui():
    return ui.page_fluid(
        ui.tags.style("""
            .subledger-table-wrapper {
                overflow-x: auto;
                overflow-y: auto;
                max-width: 100%;
                max-height: 800px;
            }
            .subledger-table-wrapper .shiny-data-grid {
                min-width: 900px;
            }
            .subledger-table-wrapper .shiny-data-grid,
            .subledger-table-wrapper .shiny-data-grid-grid {
                overflow-y: auto !important;
                -webkit-overflow-scrolling: touch;
            }
            .subledger-detail-panel {
                margin-top: 12px;
                border: 1px solid #dee2e6;
                border-radius: 8px;
                overflow: hidden;
            }
            .subledger-detail-header {
                background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
                color: white;
                padding: 10px 15px;
            }
            .subledger-detail-body {
                padding: 15px;
                background: #f8f9fa;
            }
            .subledger-field-label {
                font-weight: 600;
                font-size: 0.8rem;
                color: #555;
                margin-bottom: 2px;
            }
            .subledger-field-value {
                font-family: 'SF Mono', 'Consolas', monospace;
                font-size: 0.9rem;
                padding: 4px 8px;
                background: white;
                border: 1px solid #e9ecef;
                border-radius: 4px;
                word-break: break-all;
                margin-bottom: 4px;
            }
        """),
        ui.div(
            ui.h2("Crypto Subledger", style="color: var(--bs-primary); font-weight: 600;"),
            ui.p(
                "On-chain ingestion, normalized movements, and balance reconciliation.",
                class_="text-muted mb-3",
            ),
        ),
        # Controls card
        ui.card(
            ui.card_header("Ingestion Controls"),
            ui.card_body(
                ui.layout_columns(
                    ui.output_ui("subledger_wallet_selector"),
                    ui.input_select(
                        "subledger_chain",
                        "Chain",
                        {"1": "Ethereum Mainnet"},
                        selected="1",
                    ),
                    ui.input_numeric(
                        "subledger_start_block",
                        "Start Block",
                        value=0,
                        min=0,
                    ),
                    ui.input_numeric(
                        "subledger_end_block",
                        "End Block",
                        value=0,
                        min=0,
                    ),
                    col_widths=[4, 2, 3, 3],
                ),
                ui.layout_columns(
                    ui.div(
                        ui.input_action_button(
                            "subledger_last_10k", "Last 10K", class_="btn-sm btn-outline-secondary me-1"
                        ),
                        ui.input_action_button(
                            "subledger_last_100k", "Last 100K", class_="btn-sm btn-outline-secondary me-1"
                        ),
                        ui.input_action_button(
                            "subledger_last_500k", "Last 500K", class_="btn-sm btn-outline-secondary"
                        ),
                        class_="d-flex align-items-end pb-2",
                    ),
                    ui.input_checkbox(
                        "subledger_safe_mode",
                        "Safe Mode (cap 100K blocks, 5 wallets)",
                        value=True,
                    ),
                    ui.input_action_button(
                        "subledger_run_ingestion",
                        "Run Ingestion",
                        class_="btn-primary",
                    ),
                    ui.input_action_button(
                        "subledger_run_reconciliation",
                        "Run Reconciliation",
                        class_="btn-warning",
                    ),
                    col_widths=[4, 3, 3, 2],
                ),
            ),
        ),
        # Status row
        ui.layout_column_wrap(
            ui.value_box("Run Status", ui.output_ui("subledger_ingestion_status")),
            ui.value_box("Raw Data", ui.output_ui("subledger_raw_tx_count")),
            ui.value_box("Movements", ui.output_ui("subledger_movement_count")),
            ui.value_box("Reconciliation", ui.output_ui("subledger_recon_summary")),
            fill=False,
        ),
        # Data tables
        ui.card(
            ui.card_header("Subledger Data"),
            ui.card_body(
                ui.navset_tab(
                    ui.nav_panel(
                        "Balances",
                        ui.div(
                            ui.div(
                                ui.input_checkbox(
                                    "subledger_range_only_balances",
                                    "Range-only balances",
                                    value=False,
                                ),
                                ui.output_ui("subledger_balance_mode_label"),
                                ui.input_checkbox(
                                    "subledger_show_spam",
                                    "Show filtered tokens",
                                    value=False,
                                ),
                                ui.output_ui("subledger_filter_summary"),
                                ui.download_button(
                                    "subledger_download_balances",
                                    ui.HTML('<i class="bi bi-download"></i> Export CSV'),
                                    class_="btn-outline-success btn-sm mb-2",
                                ),
                                class_="d-flex justify-content-end align-items-center gap-3",
                            ),
                            ui.div(
                                ui.output_data_frame("subledger_balances_table"),
                                class_="subledger-table-wrapper",
                            ),
                            ui.output_ui("subledger_balances_detail_panel"),
                        ),
                    ),
                    ui.nav_panel(
                        "Movements",
                        ui.div(
                            ui.div(
                                ui.download_button(
                                    "subledger_download_movements",
                                    ui.HTML('<i class="bi bi-download"></i> Export CSV'),
                                    class_="btn-outline-success btn-sm mb-2",
                                ),
                                class_="d-flex justify-content-end",
                            ),
                            ui.div(
                                ui.output_data_frame("subledger_movements_table"),
                                class_="subledger-table-wrapper",
                            ),
                            ui.output_ui("subledger_movements_detail_panel"),
                        ),
                    ),
                    ui.nav_panel(
                        "Reconciliation",
                        ui.div(
                            ui.div(
                                ui.download_button(
                                    "subledger_download_recon",
                                    ui.HTML('<i class="bi bi-download"></i> Export CSV'),
                                    class_="btn-outline-success btn-sm mb-2",
                                ),
                                class_="d-flex justify-content-end",
                            ),
                            ui.div(
                                ui.output_data_frame("subledger_recon_table"),
                                class_="subledger-table-wrapper",
                            ),
                            ui.output_ui("subledger_recon_detail_panel"),
                        ),
                    ),
                ),
                style="padding: 0.5rem;",
            ),
            full_screen=True,
            style="min-height: 500px;",
        ),
    )


# =============================================================================
# Server
# =============================================================================

def register_crypto_subledger_outputs(output, input, session, selected_fund):
    """Register all server outputs for the Crypto Subledger tab."""

    # Reactive state
    ingestion_status = reactive.value("idle")
    recon_status = reactive.value("idle")
    last_run_id = reactive.value("")
    last_trace_completeness = reactive.value("")
    last_run_result = reactive.value({})

    cached_balances = reactive.value(pd.DataFrame())
    cached_range_balances = reactive.value(pd.DataFrame())
    cached_movements = reactive.value(pd.DataFrame())
    cached_recon = reactive.value(pd.DataFrame())

    # Price map: lowercase contract address -> Decimal USD price
    price_map = reactive.value({})

    def _get_token_filter():
        """Lazy-load a TokenFilter with current price_map."""
        from ...services.subledger.token_filter import TokenFilter
        return TokenFilter(_get_registry(), price_map.get())

    def _active_balances():
        """Return range-only or cumulative balances based on toggle."""
        if input.subledger_range_only_balances():
            return cached_range_balances.get()
        return cached_balances.get()

    # ------------------------------------------------------------------
    # Balance mode label
    # ------------------------------------------------------------------

    @output
    @render.ui
    def subledger_balance_mode_label():
        range_only = input.subledger_range_only_balances()
        start_block = int(input.subledger_start_block() or 0)
        end_block = int(input.subledger_end_block() or 0)
        if range_only:
            return ui.span(
                f"Range: blocks {start_block:,}\u2013{end_block:,}",
                class_="text-info",
                style="font-size:0.85em;",
            )
        return ui.span(
            f"Cumulative: all blocks up to {end_block:,}",
            class_="text-muted",
            style="font-size:0.85em;",
        )

    # ------------------------------------------------------------------
    # Wallet selector
    # ------------------------------------------------------------------

    @output
    @render.ui
    def subledger_wallet_selector():
        current_fund = selected_fund()
        wallet_choices = {}
        try:
            from ...s3_utils import load_WALLET_file

            wallet_df = load_WALLET_file()
            if not wallet_df.empty:
                fund_wallets = wallet_df[wallet_df["fund_id"] == current_fund]
                for _, row in fund_wallets.iterrows():
                    addr = str(row.get("wallet_address", "")).strip().lower()
                    name = str(row.get("friendly_name", "")).strip()
                    if addr:
                        label = f"{name} ({addr[:6]}...{addr[-4:]})" if name else f"{addr[:6]}...{addr[-4:]}"
                        wallet_choices[addr] = label
        except Exception as e:
            logger.warning(f"Could not load wallet mappings: {e}")

        # Default test wallet
        test_addr = "0xf9b64dc47dbe8c75f6ffc573cbc7599404bfe5a7"
        if test_addr not in wallet_choices:
            wallet_choices[test_addr] = f"Test Wallet ({test_addr[:6]}...{test_addr[-4:]})"

        return ui.input_selectize(
            "subledger_wallets",
            "Wallets",
            choices=wallet_choices,
            selected=[test_addr],
            multiple=True,
        )

    # ------------------------------------------------------------------
    # Quick-range buttons
    # ------------------------------------------------------------------

    @reactive.effect
    @reactive.event(input.subledger_last_10k)
    def _set_10k():
        _set_quick_range(10_000)

    @reactive.effect
    @reactive.event(input.subledger_last_100k)
    def _set_100k():
        _set_quick_range(100_000)

    @reactive.effect
    @reactive.event(input.subledger_last_500k)
    def _set_500k():
        _set_quick_range(500_000)

    def _set_quick_range(n: int):
        latest = _get_latest_block()
        if latest is None:
            ui.notification_show(
                "Could not fetch latest block from Etherscan. Check API key.",
                type="error",
            )
            return
        ui.update_numeric("subledger_end_block", value=latest)
        ui.update_numeric("subledger_start_block", value=max(0, latest - n))

    # ------------------------------------------------------------------
    # Ingestion task
    # ------------------------------------------------------------------

    @reactive.extended_task
    async def ingestion_task(wallets, start_block, end_block, fund_id, chain_id):
        loop = asyncio.get_event_loop()

        def _run():
            from ...services.subledger.storage import SubledgerStorage
            from ...services.subledger.ingestion import SubledgerIngester
            from ...services.subledger.replay import SubledgerReplay

            storage = SubledgerStorage(fund_id)
            ingester = SubledgerIngester(chain_id=chain_id)
            replay = SubledgerReplay(
                storage=storage,
                ingester=ingester,
                our_wallets=set(wallets),
                fund_id=fund_id,
                chain_id=chain_id,
            )

            movement_count = replay.ingest_block_range(
                start_block, end_block, wallet_addresses=wallets
            )

            # Register discovered token metadata for accurate decimal conversion
            if hasattr(ingester, "discovered_tokens") and ingester.discovered_tokens:
                from ...services.subledger.asset_registry import resolve_asset_id as _resolve
                _registry = _get_registry()
                for contract_addr, meta in ingester.discovered_tokens.items():
                    aid = _resolve(chain_id, contract_addr, 0, "erc20")
                    _registry.register_if_better(
                        asset_id=aid,
                        symbol=meta.get("symbol", ""),
                        name=meta.get("name", ""),
                        decimals=meta.get("decimals", 18),
                    )
                logger.info(
                    f"Registered {len(ingester.discovered_tokens)} token metadata entries"
                )

            # Count raw data
            raw_txs = storage.load_raw_transactions(start_block, end_block)
            raw_logs = storage.load_raw_logs(start_block, end_block)
            raw_traces = storage.load_raw_traces(start_block, end_block)

            # Determine trace completeness from raw txs
            tc = "none"
            if not raw_txs.empty and "trace_completeness" in raw_txs.columns:
                tc_vals = raw_txs["trace_completeness"].unique().tolist()
                if "full_call_tree" in tc_vals:
                    tc = "full_call_tree"
                elif "internals_only" in tc_vals:
                    tc = "internals_only"

            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            run_id = f"{fund_id}_{chain_id}_{start_block}_{end_block}_{ts}"

            return {
                "run_id": run_id,
                "movement_count": movement_count,
                "raw_tx_count": len(raw_txs),
                "raw_log_count": len(raw_logs),
                "raw_trace_count": len(raw_traces),
                "trace_completeness": tc,
                "wallets": wallets,
                "start": start_block,
                "end": end_block,
            }

        return await loop.run_in_executor(_executor, _run)

    @reactive.effect
    @reactive.event(input.subledger_run_ingestion)
    def _start_ingestion():
        wallets = list(input.subledger_wallets()) if input.subledger_wallets() else []
        # Normalize
        wallets = list(dict.fromkeys(w.strip().lower() for w in wallets if w and w != "none"))
        start_block = int(input.subledger_start_block() or 0)
        end_block = int(input.subledger_end_block() or 0)
        fund_id = selected_fund()
        chain_id = int(input.subledger_chain())

        if not wallets:
            ui.notification_show("No wallets selected.", type="warning")
            return
        if end_block <= start_block:
            ui.notification_show("End block must be greater than start block.", type="warning")
            return

        # Safe mode enforcement
        if input.subledger_safe_mode():
            block_range = end_block - start_block
            if block_range > SAFE_MODE_MAX_BLOCKS:
                ui.notification_show(
                    f"Safe Mode: block range {block_range:,} exceeds cap of {SAFE_MODE_MAX_BLOCKS:,}. "
                    f"Disable Safe Mode to proceed.",
                    type="error",
                )
                return
            if len(wallets) > SAFE_MODE_MAX_WALLETS:
                ui.notification_show(
                    f"Safe Mode: {len(wallets)} wallets exceeds cap of {SAFE_MODE_MAX_WALLETS}. "
                    f"Disable Safe Mode to proceed.",
                    type="error",
                )
                return

        ingestion_status.set("running")
        ingestion_task(wallets, start_block, end_block, fund_id, chain_id)

    @reactive.effect
    def _handle_ingestion_complete():
        status = ingestion_task.status()
        if status == "initial" or status == "running":
            return
        if status == "error":
            ingestion_status.set("error")
            err = ingestion_task.error()
            ui.notification_show(f"Ingestion failed: {err}", type="error", duration=10)
            return
        if status == "success":
            result = ingestion_task.result()
            ingestion_status.set("complete")
            last_run_id.set(result["run_id"])
            last_trace_completeness.set(result.get("trace_completeness", ""))
            last_run_result.set(result)
            _refresh_cached_data()
            ui.notification_show(
                f"Ingestion complete: {result['movement_count']} movements from "
                f"{result['raw_tx_count']} txs.",
                type="message",
            )

    # ------------------------------------------------------------------
    # Reconciliation task
    # ------------------------------------------------------------------

    @reactive.extended_task
    async def reconciliation_task(wallets, fund_id, chain_id, start_block, end_block):
        loop = asyncio.get_event_loop()

        def _run():
            from ...services.subledger.storage import SubledgerStorage
            from ...services.subledger.balance_engine import BalanceEngine
            from ...services.subledger.reconciler import SubledgerReconciler
            from ...services.subledger.asset_registry import native_eth_asset_id

            storage = SubledgerStorage(fund_id)
            balance_engine = BalanceEngine(storage, fund_id)
            reconciler = SubledgerReconciler(storage, balance_engine, fund_id=fund_id)

            # Range-scoped asset selection
            movements = storage.load_movements(start_block=start_block, end_block=end_block)
            asset_ids = set()
            if not movements.empty and "asset_id" in movements.columns:
                asset_ids = set(movements["asset_id"].unique())
            asset_ids.add(native_eth_asset_id(chain_id))
            asset_ids = sorted(asset_ids)

            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            proof_run_id = f"proof_{fund_id}_{ts}"

            results = reconciler.run_proof(
                wallets=wallets,
                asset_ids=asset_ids,
                block_numbers=[end_block],
                proof_run_id=proof_run_id,
            )

            matches = sum(1 for r in results if r.is_match)
            errors = sum(1 for r in results if r.onchain_balance_raw == "ERROR")

            return {
                "run_id": proof_run_id,
                "total": len(results),
                "matches": matches,
                "mismatches": len(results) - matches,
                "errors": errors,
                "asset_ids": asset_ids,
            }

        return await loop.run_in_executor(_executor, _run)

    @reactive.effect
    @reactive.event(input.subledger_run_reconciliation)
    def _start_reconciliation():
        wallets = list(input.subledger_wallets()) if input.subledger_wallets() else []
        wallets = list(dict.fromkeys(w.strip().lower() for w in wallets if w and w != "none"))
        start_block = int(input.subledger_start_block() or 0)
        end_block = int(input.subledger_end_block() or 0)
        fund_id = selected_fund()
        chain_id = int(input.subledger_chain())

        if not wallets:
            ui.notification_show("No wallets selected.", type="warning")
            return
        if end_block <= 0:
            ui.notification_show("Set an end block for reconciliation.", type="warning")
            return

        # Safe mode enforcement
        if input.subledger_safe_mode():
            block_range = end_block - start_block
            if block_range > SAFE_MODE_MAX_BLOCKS:
                ui.notification_show(
                    f"Safe Mode: block range {block_range:,} exceeds cap of {SAFE_MODE_MAX_BLOCKS:,}. "
                    f"Disable Safe Mode to proceed.",
                    type="error",
                )
                return
            if len(wallets) > SAFE_MODE_MAX_WALLETS:
                ui.notification_show(
                    f"Safe Mode: {len(wallets)} wallets exceeds cap of {SAFE_MODE_MAX_WALLETS}. "
                    f"Disable Safe Mode to proceed.",
                    type="error",
                )
                return

        recon_status.set("running")
        reconciliation_task(wallets, fund_id, chain_id, start_block, end_block)

    @reactive.effect
    def _handle_recon_complete():
        status = reconciliation_task.status()
        if status == "initial" or status == "running":
            return
        if status == "error":
            recon_status.set("error")
            err = reconciliation_task.error()
            ui.notification_show(f"Reconciliation failed: {err}", type="error", duration=10)
            return
        if status == "success":
            result = reconciliation_task.result()
            recon_status.set("complete")
            _refresh_recon_data(result.get("run_id", ""))
            ui.notification_show(
                f"Reconciliation: {result['matches']}/{result['total']} matched, "
                f"{result['mismatches']} mismatches, {result['errors']} errors.",
                type="message",
            )

    # ------------------------------------------------------------------
    # Data refresh
    # ------------------------------------------------------------------

    def _refresh_cached_data():
        """Reload balances and movements from S3 using current block range."""
        try:
            from ...services.subledger.storage import SubledgerStorage
            from ...services.subledger.balance_engine import BalanceEngine

            fund_id = selected_fund()
            start_block = int(input.subledger_start_block() or 0)
            end_block = int(input.subledger_end_block() or 0)

            storage = SubledgerStorage(fund_id)
            balance_engine = BalanceEngine(storage, fund_id)

            # Range-scoped movement load
            mvs = storage.load_movements(start_block=start_block, end_block=end_block)
            cached_movements.set(mvs)

            # Range-only balances (from movements in selected range)
            if not mvs.empty:
                mvs_copy = mvs.copy()
                mvs_copy["delta_int"] = mvs_copy["amount_delta_raw"].apply(lambda x: int(x))
                range_bals = mvs_copy.groupby(["wallet_address", "asset_id"]).agg(
                    balance_raw=("delta_int", "sum"),
                    movement_count=("movement_id", "count"),
                    last_block=("block_number", "max"),
                ).reset_index()
                range_bals["balance_raw"] = range_bals["balance_raw"].apply(str)
                cached_range_balances.set(range_bals)
            else:
                cached_range_balances.set(pd.DataFrame())

            # Cumulative balances up to end_block
            if end_block > 0:
                bals = balance_engine.get_all_balances(up_to_block=end_block)
            else:
                bals = balance_engine.get_all_balances()
            cached_balances.set(bals)

            # Fetch USD prices for all tokens (use cumulative for broadest coverage)
            _refresh_prices(bals)
        except Exception as e:
            logger.error(f"Failed to refresh subledger data: {e}")

    def _refresh_prices(bals: pd.DataFrame):
        """Fetch current USD prices for verified tokens in the balances DataFrame.

        Only prices tokens we can look up by CoinGecko symbol (verified tokens).
        Contract-based pricing requires a CoinGecko API key (not available on free tier).
        """
        from decimal import Decimal as D
        from ...services.subledger.asset_registry import extract_contract, is_native
        new_map: Dict[str, D] = {}
        try:
            from ...services.price_service import get_price_service
            svc = get_price_service()

            registry = _get_registry()
            verified_symbols = []
            symbol_to_contract: Dict[str, str] = {}

            if not bals.empty:
                for aid in bals["asset_id"].unique():
                    if is_native(aid):
                        verified_symbols.append("ETH")
                        continue
                    contract = extract_contract(aid)
                    meta = registry.get(aid)
                    if meta and meta.is_verified and meta.symbol:
                        verified_symbols.append(meta.symbol)
                        symbol_to_contract[meta.symbol] = contract

            if verified_symbols:
                sym_prices = svc.get_current_prices(list(set(verified_symbols)), ["usd"])
                for sym, pdata in sym_prices.items():
                    usd = pdata.get("usd")
                    if usd is not None:
                        contract = symbol_to_contract.get(sym)
                        if contract:
                            new_map[contract] = usd
                        if sym == "ETH":
                            new_map["__ETH__"] = usd
                            weth_addr = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
                            new_map[weth_addr] = usd

            logger.info(f"Price map: {len(new_map)} verified tokens priced")
        except Exception as e:
            logger.warning(f"Price fetch failed (non-fatal): {e}")

        price_map.set(new_map)

    def _refresh_recon_data(proof_run_id: str):
        """Reload reconciliation results for a specific proof run."""
        try:
            from ...services.subledger.storage import SubledgerStorage

            fund_id = selected_fund()
            storage = SubledgerStorage(fund_id)
            recon = storage.load_reconciliation(proof_run_id=proof_run_id)
            cached_recon.set(recon)
        except Exception as e:
            logger.error(f"Failed to refresh recon data: {e}")

    # ------------------------------------------------------------------
    # Status outputs
    # ------------------------------------------------------------------

    @output
    @render.ui
    def subledger_ingestion_status():
        status = ingestion_status.get()
        run_id = last_run_id.get()
        tc = last_trace_completeness.get()

        if status == "idle":
            return ui.p("Idle — configure and run ingestion.", class_="text-muted mb-0")
        if status == "running":
            return ui.p("Running...", class_="text-info mb-0", style="font-weight:600;")
        if status == "error":
            return ui.p("Error — check notifications.", class_="text-warning mb-0")

        # Complete
        tc_desc = TRACE_COMPLETENESS_DESCRIPTIONS.get(tc, tc or "unknown")
        return ui.div(
            ui.p(f"Run: {run_id}", class_="mb-1", style="font-size:0.85em;"),
            ui.p(f"Traces: {tc_desc}", class_="text-muted mb-0", style="font-size:0.8em;"),
        )

    @output
    @render.ui
    def subledger_raw_tx_count():
        result = last_run_result.get()
        if not result:
            return ui.p("--", class_="text-muted mb-0")
        return ui.div(
            ui.p(f"{result.get('raw_tx_count', 0):,} txs", class_="mb-0"),
            ui.p(
                f"{result.get('raw_log_count', 0):,} logs, "
                f"{result.get('raw_trace_count', 0):,} traces",
                class_="text-muted mb-0",
                style="font-size:0.85em;",
            ),
        )

    @output
    @render.ui
    def subledger_movement_count():
        result = last_run_result.get()
        if not result:
            return ui.p("--", class_="text-muted mb-0")
        return ui.p(f"{result.get('movement_count', 0):,}", class_="mb-0")

    @output
    @render.ui
    def subledger_recon_summary():
        rs = recon_status.get()
        if rs == "idle":
            return ui.p("--", class_="text-muted mb-0")
        if rs == "running":
            return ui.p("Running...", class_="text-info mb-0")
        if rs == "error":
            return ui.p("Error", class_="text-warning mb-0")
        recon_df = cached_recon.get()
        if recon_df.empty:
            return ui.p("No results", class_="text-muted mb-0")
        matches = int((recon_df["is_match"] == True).sum()) if "is_match" in recon_df.columns else 0
        total = len(recon_df)
        return ui.p(f"{matches}/{total} matched", class_="mb-0")

    # ------------------------------------------------------------------
    # Filter summary
    # ------------------------------------------------------------------

    @output
    @render.ui
    def subledger_filter_summary():
        df = _active_balances()
        if df.empty:
            return ui.span()
        token_filter = _get_token_filter()
        classifications = token_filter.classify_balances(df)
        total = len(classifications)
        visible = sum(1 for r in classifications.values() if r.is_visible)
        hidden = total - visible
        show_spam = input.subledger_show_spam()
        if show_spam:
            return ui.span(
                f"{total} tokens (all shown)",
                class_="text-muted",
                style="font-size:0.85em;",
            )
        return ui.span(
            f"{visible} shown / {hidden} filtered",
            class_="text-muted",
            style="font-size:0.85em;",
        )

    # ------------------------------------------------------------------
    # Data tables
    # ------------------------------------------------------------------

    @output
    @render.data_frame
    def subledger_balances_table():
        df = _active_balances()
        if df.empty:
            return render.DataGrid(pd.DataFrame({"info": ["No data — run ingestion first"]}))

        registry = _get_registry()
        wallet_lookup = _build_wallet_lookup()
        token_filter = _get_token_filter()
        show_spam = input.subledger_show_spam()

        # Classify and filter
        classifications = token_filter.classify_balances(df)
        display_df = token_filter.filter_dataframe(df, classifications, show_spam)
        if display_df.empty:
            return render.DataGrid(pd.DataFrame({"info": ["All tokens filtered — toggle 'Show filtered tokens'"]}))

        from ...services.subledger.asset_registry import extract_contract, is_native
        from decimal import Decimal as D
        pm = price_map.get()

        out = pd.DataFrame()
        out["Wallet"] = display_df["wallet_address"].apply(lambda x: _display_wallet(x, wallet_lookup))
        out["Asset"] = display_df["asset_id"].apply(lambda x: _display_asset(x, registry))
        out["Direction"] = display_df["balance_raw"].apply(
            lambda x: "NET IN" if int(x) > 0 else ("NET OUT" if int(x) < 0 else "ZERO")
        )
        out["Balance"] = display_df.apply(
            lambda r: _format_amount(r["balance_raw"], r["asset_id"], registry), axis=1
        )

        # USD columns
        def _usd_price(asset_id):
            if is_native(asset_id):
                p = pm.get("__ETH__")
            else:
                p = pm.get(extract_contract(asset_id))
            return f"${float(p):,.4f}" if p else ""

        def _usd_value(row):
            aid = row["asset_id"]
            if is_native(aid):
                p = pm.get("__ETH__")
            else:
                p = pm.get(extract_contract(aid))
            if not p:
                return ""
            dec = registry.get_decimals(aid)
            bal = int(row["balance_raw"]) / (10 ** dec)
            return f"${float(D(str(bal)) * p):,.2f}"

        out["USD Price"] = display_df["asset_id"].apply(_usd_price)
        out["USD Value"] = display_df.apply(_usd_value, axis=1)

        if "movement_count" in display_df.columns:
            out["Movements"] = display_df["movement_count"]
        if "last_block" in display_df.columns:
            out["Last Block"] = display_df["last_block"]

        # Show filter metadata when toggled on
        if show_spam:
            out["Status"] = display_df["asset_id"].apply(
                lambda x: classifications.get(x).visibility if x in classifications else ""
            )
            out["Spam Score"] = display_df["asset_id"].apply(
                lambda x: classifications.get(x).spam_score if x in classifications else 0
            )

        return render.DataGrid(
            out, filters=True, width="100%", height="600px",
            selection_mode="rows", row_selection_mode="single",
        )

    @output
    @render.data_frame
    def subledger_movements_table():
        df = cached_movements.get()
        if df.empty:
            return render.DataGrid(pd.DataFrame({"info": ["No data — run ingestion first"]}))
        registry = _get_registry()
        wallet_lookup = _build_wallet_lookup()

        # Filter out spam tokens (movements for hidden assets)
        token_filter = _get_token_filter()
        bals = cached_balances.get()
        show_spam = input.subledger_show_spam()
        if not bals.empty:
            classifications = token_filter.classify_balances(bals)
            visible_assets = {aid for aid, r in classifications.items() if r.is_visible} if not show_spam else set(df["asset_id"].unique())
            df = df[df["asset_id"].isin(visible_assets)].reset_index(drop=True)
        if df.empty:
            return render.DataGrid(pd.DataFrame({"info": ["All movements filtered — toggle 'Show filtered tokens'"]}))

        from ...services.subledger.asset_registry import extract_contract, is_native
        from decimal import Decimal as D
        pm = price_map.get()

        out = pd.DataFrame()
        out["Block"] = df["block_number"]
        if "block_timestamp" in df.columns:
            out["Time"] = pd.to_datetime(df["block_timestamp"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M")
        if "tx_hash" in df.columns:
            out["Tx"] = df["tx_hash"].astype(str).str[:10] + "..."
        out["Wallet"] = df["wallet_address"].apply(lambda x: _display_wallet(x, wallet_lookup))
        out["Asset"] = df["asset_id"].apply(lambda x: _display_asset(x, registry))
        if "movement_kind" in df.columns:
            out["Activity"] = df["movement_kind"]
        out["Direction"] = df["amount_delta_raw"].apply(
            lambda x: "IN" if int(x) > 0 else ("OUT" if int(x) < 0 else "---")
        )
        out["Amount"] = df.apply(
            lambda r: _format_amount(r["amount_delta_raw"], r["asset_id"], registry), axis=1
        )

        # USD Value column
        def _movement_usd(row):
            aid = row["asset_id"]
            p = pm.get("__ETH__") if is_native(aid) else pm.get(extract_contract(aid))
            if not p:
                return ""
            dec = registry.get_decimals(aid)
            amt = int(row["amount_delta_raw"]) / (10 ** dec)
            return f"${float(D(str(amt)) * p):,.2f}"

        out["USD Value"] = df.apply(_movement_usd, axis=1)

        if "source_type" in df.columns:
            out["Source"] = df["source_type"]
        if "counterparty" in df.columns:
            out["Counterparty"] = df["counterparty"].apply(
                lambda x: _display_wallet(str(x), wallet_lookup) if pd.notna(x) and str(x) else ""
            )
        return render.DataGrid(
            out, filters=True, width="100%", height="600px",
            selection_mode="rows", row_selection_mode="single",
        )

    @output
    @render.data_frame
    def subledger_recon_table():
        df = cached_recon.get()
        if df.empty:
            return render.DataGrid(pd.DataFrame({"info": ["No data — run reconciliation first"]}))
        registry = _get_registry()
        wallet_lookup = _build_wallet_lookup()

        # Filter out spam tokens from recon view
        token_filter = _get_token_filter()
        bals = cached_balances.get()
        show_spam = input.subledger_show_spam()
        if not bals.empty:
            classifications = token_filter.classify_balances(bals)
            visible_assets = {aid for aid, r in classifications.items() if r.is_visible} if not show_spam else set(df["asset_id"].unique())
            df = df[df["asset_id"].isin(visible_assets)].reset_index(drop=True)
        if df.empty:
            return render.DataGrid(pd.DataFrame({"info": ["All tokens filtered — toggle 'Show filtered tokens'"]}))

        from ...services.subledger.asset_registry import extract_contract, is_native
        from decimal import Decimal as D
        pm = price_map.get()

        out = pd.DataFrame()
        out["Wallet"] = df["wallet_address"].apply(lambda x: _display_wallet(x, wallet_lookup))
        out["Asset"] = df["asset_id"].apply(lambda x: _display_asset(x, registry))
        out["Block"] = df["block_number"]
        out["Derived"] = df.apply(
            lambda r: _format_amount(r["derived_balance_raw"], r["asset_id"], registry), axis=1
        )
        out["On-Chain"] = df.apply(
            lambda r: _format_amount(r["onchain_balance_raw"], r["asset_id"], registry)
            if str(r.get("onchain_balance_raw")) != "ERROR" else "ERROR", axis=1
        )
        out["Variance"] = df.apply(
            lambda r: _format_amount(r["variance_raw"], r["asset_id"], registry)
            if str(r.get("variance_raw")) != "ERROR" else "ERROR", axis=1
        )

        # USD Variance column
        def _usd_variance(row):
            var_raw = row.get("variance_raw")
            if str(var_raw) == "ERROR":
                return "ERROR"
            aid = row["asset_id"]
            p = pm.get("__ETH__") if is_native(aid) else pm.get(extract_contract(aid))
            if not p:
                return ""
            dec = registry.get_decimals(aid)
            var = int(var_raw) / (10 ** dec)
            return f"${float(D(str(var)) * p):,.2f}"

        out["USD Variance"] = df.apply(_usd_variance, axis=1)

        if "is_match" in df.columns:
            out["Match"] = df["is_match"].apply(lambda x: "Yes" if x else "No")
        if "diagnosis" in df.columns:
            out["Diagnosis"] = df["diagnosis"].fillna("")
        return render.DataGrid(
            out, filters=True, width="100%", height="600px",
            selection_mode="rows", row_selection_mode="single",
        )

    # ------------------------------------------------------------------
    # Detail panels (click row to expand)
    # ------------------------------------------------------------------

    @output
    @render.ui
    def subledger_balances_detail_panel():
        selected = input.subledger_balances_table_selected_rows()
        if not selected:
            return ui.div()

        # When filtering is active, the selected index maps to the filtered df
        # We need to get the filtered view to find the correct row
        bals_df = _active_balances()
        if bals_df.empty or len(selected) == 0:
            return ui.div()

        token_filter = _get_token_filter()
        show_spam = input.subledger_show_spam()
        classifications = token_filter.classify_balances(bals_df)
        display_df = token_filter.filter_dataframe(bals_df, classifications, show_spam)

        idx = selected[0]
        if idx >= len(display_df):
            return ui.div()
        row = display_df.iloc[idx]
        registry = _get_registry()
        wallet_lookup = _build_wallet_lookup()

        wallet_addr = str(row.get("wallet_address", ""))
        asset_id = str(row.get("asset_id", ""))

        # USD info
        from ...services.subledger.asset_registry import extract_contract, is_native
        from decimal import Decimal as D
        pm = price_map.get()
        p = pm.get("__ETH__") if is_native(asset_id) else pm.get(extract_contract(asset_id))
        usd_price_str = f"${float(p):,.4f}" if p else "N/A"
        usd_value_str = "N/A"
        if p:
            dec = registry.get_decimals(asset_id)
            bal = int(row.get("balance_raw", 0)) / (10 ** dec)
            usd_value_str = f"${float(D(str(bal)) * p):,.2f}"

        # Filter classification
        clf = classifications.get(asset_id)
        filter_status = clf.visibility if clf else "unknown"
        spam_score = clf.spam_score if clf else 0
        reasons = ", ".join(clf.reasons) if clf and clf.reasons else "N/A"

        return ui.card(
            ui.div(
                ui.strong("Balance Detail"),
                ui.span(
                    filter_status.upper(),
                    class_=f"badge ms-2 {'bg-success' if filter_status == 'verified' else 'bg-warning text-dark' if filter_status in ('spam', 'user_rejected') else 'bg-secondary'}",
                ),
                class_="subledger-detail-header d-flex align-items-center",
            ),
            ui.div(
                ui.layout_columns(
                    _detail_field("Wallet Name", _display_wallet(wallet_addr, wallet_lookup)),
                    _detail_field("Wallet Address", wallet_addr),
                    _detail_field("Asset", _display_asset(asset_id, registry)),
                    col_widths=[4, 4, 4],
                ),
                ui.layout_columns(
                    _detail_field("Asset ID (canonical)", asset_id),
                    _detail_field("Balance (raw wei)", str(row.get("balance_raw", ""))),
                    _detail_field(
                        "Balance (formatted)",
                        _format_amount(row.get("balance_raw", "0"), asset_id, registry),
                    ),
                    col_widths=[4, 4, 4],
                ),
                ui.layout_columns(
                    _detail_field("USD Price", usd_price_str),
                    _detail_field("USD Value", usd_value_str),
                    _detail_field("Movement Count", str(row.get("movement_count", ""))),
                    col_widths=[4, 4, 4],
                ),
                ui.layout_columns(
                    _detail_field("Filter Status", filter_status),
                    _detail_field("Spam Score", str(spam_score)),
                    _detail_field("Filter Reasons", reasons),
                    col_widths=[4, 4, 4],
                ),
                ui.layout_columns(
                    _detail_field("Last Block", str(row.get("last_block", ""))),
                    ui.div(
                        ui.tags.a(
                            ui.HTML('<i class="bi bi-box-arrow-up-right"></i> View Wallet on Etherscan'),
                            href=f"https://etherscan.io/address/{wallet_addr}",
                            target="_blank",
                            class_="btn btn-outline-primary btn-sm mt-3",
                        ),
                    ),
                    ui.div(),
                    col_widths=[4, 4, 4],
                ),
                class_="subledger-detail-body",
            ),
            class_="subledger-detail-panel",
        )

    @output
    @render.ui
    def subledger_movements_detail_panel():
        selected = input.subledger_movements_table_selected_rows()
        if not selected:
            return ui.div()
        df = cached_movements.get()
        if df.empty or len(selected) == 0:
            return ui.div()

        # Apply same filter as table so index matches
        token_filter = _get_token_filter()
        bals = cached_balances.get()
        show_spam = input.subledger_show_spam()
        if not bals.empty:
            classifications = token_filter.classify_balances(bals)
            visible_assets = {aid for aid, r in classifications.items() if r.is_visible} if not show_spam else set(df["asset_id"].unique())
            df = df[df["asset_id"].isin(visible_assets)].reset_index(drop=True)

        idx = selected[0]
        if idx >= len(df):
            return ui.div()
        row = df.iloc[idx]
        registry = _get_registry()
        wallet_lookup = _build_wallet_lookup()

        wallet_addr = str(row.get("wallet_address", ""))
        asset_id = str(row.get("asset_id", ""))
        tx_hash = str(row.get("tx_hash", ""))
        counterparty = str(row.get("counterparty", "")) if pd.notna(row.get("counterparty")) else ""
        kind = str(row.get("movement_kind", ""))
        direction = "IN" if int(row.get("amount_delta_raw", 0)) > 0 else "OUT"

        # USD value
        from ...services.subledger.asset_registry import extract_contract, is_native
        from decimal import Decimal as D
        pm = price_map.get()
        p = pm.get("__ETH__") if is_native(asset_id) else pm.get(extract_contract(asset_id))
        usd_value_str = "N/A"
        if p:
            dec = registry.get_decimals(asset_id)
            amt = int(row.get("amount_delta_raw", 0)) / (10 ** dec)
            usd_value_str = f"${float(D(str(amt)) * p):,.2f}"

        badge_class = {
            "TRANSFER": "bg-info", "FEE": "bg-secondary", "MINT": "bg-success",
            "BURN": "bg-dark", "WRAP": "bg-info", "UNWRAP": "bg-warning",
            "SELFDESTRUCT": "bg-dark", "CONTRACT_CREATION": "bg-dark",
        }.get(kind, "bg-secondary")

        return ui.card(
            ui.div(
                ui.strong("Movement Detail"),
                ui.span(kind, class_=f"badge {badge_class} ms-2"),
                ui.span(direction, class_=f"badge {'bg-success' if direction == 'IN' else 'bg-secondary'} ms-1"),
                class_="subledger-detail-header d-flex align-items-center",
            ),
            ui.div(
                ui.layout_columns(
                    _detail_field("Movement ID", str(row.get("movement_id", ""))),
                    _detail_field("Transaction Hash", tx_hash),
                    _detail_field("Block Number", str(row.get("block_number", ""))),
                    col_widths=[4, 4, 4],
                ),
                ui.layout_columns(
                    _detail_field("Block Timestamp", str(row.get("block_timestamp", ""))),
                    _detail_field("Wallet Name", _display_wallet(wallet_addr, wallet_lookup)),
                    _detail_field("Wallet Address", wallet_addr),
                    col_widths=[4, 4, 4],
                ),
                ui.layout_columns(
                    _detail_field("Asset", _display_asset(asset_id, registry)),
                    _detail_field("Asset ID", asset_id),
                    _detail_field("Amount (raw)", str(row.get("amount_delta_raw", ""))),
                    col_widths=[4, 4, 4],
                ),
                ui.layout_columns(
                    _detail_field(
                        "Amount (formatted)",
                        _format_amount(row.get("amount_delta_raw", "0"), asset_id, registry),
                    ),
                    _detail_field("USD Value", usd_value_str),
                    _detail_field("Direction", direction),
                    col_widths=[4, 4, 4],
                ),
                ui.layout_columns(
                    _detail_field("Activity Type", kind),
                    _detail_field("Source Type", str(row.get("source_type", ""))),
                    _detail_field("Source ID", str(row.get("source_id", ""))),
                    col_widths=[4, 4, 4],
                ),
                ui.layout_columns(
                    _detail_field(
                        "Counterparty",
                        _display_wallet(counterparty, wallet_lookup) if counterparty else "N/A",
                    ),
                    _detail_field("Counterparty Address", counterparty if counterparty else "N/A"),
                    _detail_field("Finality Status", str(row.get("finality_status", ""))),
                    col_widths=[4, 4, 4],
                ),
                ui.layout_columns(
                    _detail_field("Fund ID", str(row.get("fund_id", ""))),
                    ui.div(),
                    ui.div(),
                    col_widths=[4, 4, 4],
                ),
                ui.div(
                    ui.tags.a(
                        ui.HTML('<i class="bi bi-box-arrow-up-right"></i> View Transaction'),
                        href=f"https://etherscan.io/tx/{tx_hash}",
                        target="_blank",
                        class_="btn btn-outline-primary btn-sm me-2",
                    ),
                    ui.tags.a(
                        ui.HTML('<i class="bi bi-box-arrow-up-right"></i> View Wallet'),
                        href=f"https://etherscan.io/address/{wallet_addr}",
                        target="_blank",
                        class_="btn btn-outline-secondary btn-sm",
                    ),
                    class_="mt-3",
                ),
                class_="subledger-detail-body",
            ),
            class_="subledger-detail-panel",
        )

    @output
    @render.ui
    def subledger_recon_detail_panel():
        selected = input.subledger_recon_table_selected_rows()
        if not selected:
            return ui.div()
        df = cached_recon.get()
        if df.empty or len(selected) == 0:
            return ui.div()

        # Apply same filter as table so index matches
        token_filter = _get_token_filter()
        bals = cached_balances.get()
        show_spam = input.subledger_show_spam()
        if not bals.empty:
            classifications = token_filter.classify_balances(bals)
            visible_assets = {aid for aid, r in classifications.items() if r.is_visible} if not show_spam else set(df["asset_id"].unique())
            df = df[df["asset_id"].isin(visible_assets)].reset_index(drop=True)

        idx = selected[0]
        if idx >= len(df):
            return ui.div()
        row = df.iloc[idx]
        registry = _get_registry()
        wallet_lookup = _build_wallet_lookup()

        wallet_addr = str(row.get("wallet_address", ""))
        asset_id = str(row.get("asset_id", ""))
        is_match = bool(row.get("is_match", False))
        tc = str(row.get("trace_completeness", ""))
        tc_desc = TRACE_COMPLETENESS_DESCRIPTIONS.get(tc, tc)

        from ...services.subledger.asset_registry import is_native, extract_contract
        from decimal import Decimal as D
        contract = ""
        if not is_native(asset_id):
            contract = extract_contract(asset_id)

        # USD values for variance
        pm = price_map.get()
        p = pm.get("__ETH__") if is_native(asset_id) else pm.get(contract)
        usd_variance_str = "N/A"
        if p and str(row.get("variance_raw")) != "ERROR":
            dec = registry.get_decimals(asset_id)
            var = int(row.get("variance_raw", 0)) / (10 ** dec)
            usd_variance_str = f"${float(D(str(var)) * p):,.2f}"

        match_badge = ui.span(
            "MATCHED" if is_match else "MISMATCH",
            class_=f"badge {'bg-success' if is_match else 'bg-warning text-dark'} ms-2",
        )

        etherscan_buttons = [
            ui.tags.a(
                ui.HTML('<i class="bi bi-box-arrow-up-right"></i> View Wallet'),
                href=f"https://etherscan.io/address/{wallet_addr}",
                target="_blank",
                class_="btn btn-outline-primary btn-sm me-2",
            ),
        ]
        if contract:
            etherscan_buttons.append(
                ui.tags.a(
                    ui.HTML('<i class="bi bi-box-arrow-up-right"></i> View Token Contract'),
                    href=f"https://etherscan.io/token/{contract}",
                    target="_blank",
                    class_="btn btn-outline-secondary btn-sm",
                ),
            )

        return ui.card(
            ui.div(
                ui.strong("Reconciliation Detail"),
                match_badge,
                class_="subledger-detail-header d-flex align-items-center",
            ),
            ui.div(
                ui.layout_columns(
                    _detail_field("Recon ID", str(row.get("recon_id", ""))),
                    _detail_field("Wallet Name", _display_wallet(wallet_addr, wallet_lookup)),
                    _detail_field("Wallet Address", wallet_addr),
                    col_widths=[4, 4, 4],
                ),
                ui.layout_columns(
                    _detail_field("Asset", _display_asset(asset_id, registry)),
                    _detail_field("Asset ID", asset_id),
                    _detail_field("Block Number", str(row.get("block_number", ""))),
                    col_widths=[4, 4, 4],
                ),
                ui.layout_columns(
                    _detail_field("Derived Balance (raw)", str(row.get("derived_balance_raw", ""))),
                    _detail_field("On-Chain Balance (raw)", str(row.get("onchain_balance_raw", ""))),
                    _detail_field("Variance (raw)", str(row.get("variance_raw", ""))),
                    col_widths=[4, 4, 4],
                ),
                ui.layout_columns(
                    _detail_field(
                        "Derived (formatted)",
                        _format_amount(row.get("derived_balance_raw", "0"), asset_id, registry),
                    ),
                    _detail_field(
                        "On-Chain (formatted)",
                        _format_amount(row.get("onchain_balance_raw", "0"), asset_id, registry)
                        if str(row.get("onchain_balance_raw")) != "ERROR" else "ERROR",
                    ),
                    _detail_field(
                        "Variance (formatted)",
                        _format_amount(row.get("variance_raw", "0"), asset_id, registry)
                        if str(row.get("variance_raw")) != "ERROR" else "ERROR",
                    ),
                    col_widths=[4, 4, 4],
                ),
                ui.layout_columns(
                    _detail_field("USD Variance", usd_variance_str),
                    _detail_field("Match Status", "Yes" if is_match else "No"),
                    _detail_field("Diagnosis", str(row.get("diagnosis", "")) or "N/A"),
                    col_widths=[4, 4, 4],
                ),
                ui.layout_columns(
                    _detail_field("Proof Run ID", str(row.get("proof_run_id", ""))),
                    _detail_field("Trace Completeness", tc_desc),
                    _detail_field("Checked At", str(row.get("checked_at", ""))),
                    col_widths=[4, 4, 4],
                ),
                ui.div(*etherscan_buttons, class_="mt-3"),
                class_="subledger-detail-body",
            ),
            class_="subledger-detail-panel",
        )

    # ------------------------------------------------------------------
    # CSV exports
    # ------------------------------------------------------------------

    @render.download(
        filename=lambda: f"subledger_balances_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
    )
    def subledger_download_balances():
        df = _active_balances()
        if df.empty:
            yield "info\nNo data\n"
            return
        registry = _get_registry()
        wallet_lookup = _build_wallet_lookup()

        from ...services.subledger.asset_registry import extract_contract, is_native
        from decimal import Decimal as D
        pm = price_map.get()

        # Include filter classifications
        token_filter = _get_token_filter()
        classifications = token_filter.classify_balances(df)

        export = df.copy()
        export["wallet_name"] = export["wallet_address"].apply(
            lambda x: _display_wallet(x, wallet_lookup)
        )
        export["asset_name"] = export["asset_id"].apply(
            lambda x: _display_asset(x, registry)
        )
        export["balance_formatted"] = export.apply(
            lambda r: _format_amount(r["balance_raw"], r["asset_id"], registry, show_sign=False),
            axis=1,
        )
        export["direction"] = export["balance_raw"].apply(
            lambda x: "NET IN" if int(x) > 0 else ("NET OUT" if int(x) < 0 else "ZERO")
        )

        def _csv_usd_price(asset_id):
            p = pm.get("__ETH__") if is_native(asset_id) else pm.get(extract_contract(asset_id))
            return float(p) if p else ""

        def _csv_usd_value(row):
            aid = row["asset_id"]
            p = pm.get("__ETH__") if is_native(aid) else pm.get(extract_contract(aid))
            if not p:
                return ""
            dec = registry.get_decimals(aid)
            bal = int(row["balance_raw"]) / (10 ** dec)
            return float(D(str(bal)) * p)

        export["usd_price"] = export["asset_id"].apply(_csv_usd_price)
        export["usd_value"] = export.apply(_csv_usd_value, axis=1)
        export["filter_status"] = export["asset_id"].apply(
            lambda x: classifications[x].visibility if x in classifications else ""
        )
        export["spam_score"] = export["asset_id"].apply(
            lambda x: classifications[x].spam_score if x in classifications else 0
        )
        yield export.to_csv(index=False)

    @render.download(
        filename=lambda: f"subledger_movements_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
    )
    def subledger_download_movements():
        df = cached_movements.get()
        if df.empty:
            yield "info\nNo data\n"
            return
        registry = _get_registry()
        wallet_lookup = _build_wallet_lookup()

        from ...services.subledger.asset_registry import extract_contract, is_native
        from decimal import Decimal as D
        pm = price_map.get()

        export = df.copy()
        export["wallet_name"] = export["wallet_address"].apply(
            lambda x: _display_wallet(x, wallet_lookup)
        )
        export["asset_name"] = export["asset_id"].apply(
            lambda x: _display_asset(x, registry)
        )
        export["amount_formatted"] = export.apply(
            lambda r: _format_amount(r["amount_delta_raw"], r["asset_id"], registry),
            axis=1,
        )
        export["direction"] = export["amount_delta_raw"].apply(
            lambda x: "IN" if int(x) > 0 else "OUT"
        )

        def _csv_movement_usd(row):
            aid = row["asset_id"]
            p = pm.get("__ETH__") if is_native(aid) else pm.get(extract_contract(aid))
            if not p:
                return ""
            dec = registry.get_decimals(aid)
            amt = int(row["amount_delta_raw"]) / (10 ** dec)
            return float(D(str(amt)) * p)

        export["usd_value"] = export.apply(_csv_movement_usd, axis=1)

        if "counterparty" in export.columns:
            export["counterparty_name"] = export["counterparty"].apply(
                lambda x: _display_wallet(str(x), wallet_lookup) if pd.notna(x) and str(x) else ""
            )
        yield export.to_csv(index=False)

    @render.download(
        filename=lambda: f"subledger_recon_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
    )
    def subledger_download_recon():
        df = cached_recon.get()
        if df.empty:
            yield "info\nNo data\n"
            return
        registry = _get_registry()
        wallet_lookup = _build_wallet_lookup()

        from ...services.subledger.asset_registry import extract_contract, is_native
        from decimal import Decimal as D
        pm = price_map.get()

        export = df.copy()
        export["wallet_name"] = export["wallet_address"].apply(
            lambda x: _display_wallet(x, wallet_lookup)
        )
        export["asset_name"] = export["asset_id"].apply(
            lambda x: _display_asset(x, registry)
        )
        for col in ["derived_balance_raw", "onchain_balance_raw", "variance_raw"]:
            export[f"{col}_formatted"] = export.apply(
                lambda r, c=col: _format_amount(r[c], r["asset_id"], registry)
                if str(r[c]) != "ERROR" else "ERROR",
                axis=1,
            )

        def _csv_usd_variance(row):
            var_raw = row.get("variance_raw")
            if str(var_raw) == "ERROR":
                return "ERROR"
            aid = row["asset_id"]
            p = pm.get("__ETH__") if is_native(aid) else pm.get(extract_contract(aid))
            if not p:
                return ""
            dec = registry.get_decimals(aid)
            var = int(var_raw) / (10 ** dec)
            return float(D(str(var)) * p)

        export["usd_variance"] = export.apply(_csv_usd_variance, axis=1)
        yield export.to_csv(index=False)
