"""
Raw data ingestion from Etherscan APIs.

Extends the existing EtherscanClient with:
  - txlistinternal (internal/trace transactions)
  - Block header fetching for reorg backbone
  - Structured conversion to raw_blocks, raw_transactions, raw_logs, raw_traces

All raw data is stored verbatim — no interpretation, no filtering by wallet.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

import requests

from main_app.services.subledger.models import (
    RawBlock,
    RawTransaction,
    RawLog,
    RawTrace,
    FinalityStatus,
    TraceProvider,
    TraceCompleteness,
)
from main_app.services.subledger.asset_registry import (
    TRANSFER_TOPIC,
    DEPOSIT_TOPIC,
    WITHDRAWAL_TOPIC,
    TRANSFER_SINGLE_TOPIC,
    TRANSFER_BATCH_TOPIC,
)

logger = logging.getLogger(__name__)


class SubledgerIngester:
    """Fetches raw blockchain data from Etherscan and structures it for the subledger."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        chain_id: int = 1,
        confirmation_blocks: int = 3,
    ):
        import os
        from dotenv import load_dotenv
        load_dotenv()

        self.api_key = api_key or os.getenv("ETHERSCAN_API_KEY", "")
        self.chain_id = chain_id
        self.confirmation_blocks = confirmation_blocks

        # Etherscan API URLs (V2)
        self.base_url = "https://api.etherscan.io/v2/api"
        self.rate_limit_delay = 0.21  # 5 req/s max
        self._last_request_time = 0.0
        self.discovered_tokens: Dict[str, Dict] = {}  # contract -> {symbol, name, decimals}

        if self.api_key:
            masked = f"{self.api_key[:4]}...{self.api_key[-4:]}"
            logger.info(f"SubledgerIngester initialized with key: {masked}")

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------

    def _rate_limit(self) -> None:
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self._last_request_time = time.time()

    def _get(self, params: Dict) -> Optional[Dict]:
        """Make a rate-limited GET request to Etherscan V2 API."""
        self._rate_limit()
        params["apikey"] = self.api_key
        params["chainid"] = str(self.chain_id)
        try:
            resp = requests.get(self.base_url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            # Proxy/JSON-RPC responses (module=proxy)
            if "jsonrpc" in data:
                return data
            # Account module responses
            if data.get("status") == "1" or data.get("message") == "OK":
                return data
            if data.get("message") == "No transactions found":
                return data
            # Rate-limit error: retry once after delay
            if "rate limit" in str(data.get("result", "")).lower():
                logger.warning("Etherscan rate limit hit, waiting 1s...")
                time.sleep(1.0)
                resp = requests.get(self.base_url, params=params, timeout=30)
                return resp.json()
            logger.warning(f"Etherscan API: {data.get('message')} - {data.get('result')}")
            return data
        except Exception as e:
            logger.error(f"Etherscan request failed: {e}")
            return None

    # ------------------------------------------------------------------
    # Block header fetching
    # ------------------------------------------------------------------

    def get_block_header(self, block_number: int) -> Optional[RawBlock]:
        """Fetch a single block header from Etherscan proxy API."""
        params = {
            "module": "proxy",
            "action": "eth_getBlockByNumber",
            "tag": hex(block_number),
            "boolean": "false",  # don't include full txs
        }
        data = self._get(params)
        if data is None or data.get("result") is None:
            return None
        blk = data["result"]
        try:
            return RawBlock(
                block_number=int(blk["number"], 16),
                block_hash=blk["hash"].lower(),
                parent_hash=blk["parentHash"].lower(),
                block_timestamp=datetime.fromtimestamp(
                    int(blk["timestamp"], 16), tz=timezone.utc
                ),
                finality_status=FinalityStatus.CONFIRMED.value,
                tx_count=len(blk.get("transactions", [])),
            )
        except (KeyError, ValueError) as e:
            logger.error(f"Failed to parse block {block_number}: {e}")
            return None

    def get_block_headers(self, block_numbers: List[int]) -> List[RawBlock]:
        """Fetch multiple block headers."""
        blocks = []
        for bn in block_numbers:
            blk = self.get_block_header(bn)
            if blk:
                blocks.append(blk)
        return blocks

    # ------------------------------------------------------------------
    # Normal transactions (txlist)
    # ------------------------------------------------------------------

    def get_transactions(
        self,
        address: str,
        start_block: int = 0,
        end_block: int = 99999999,
    ) -> List[Dict]:
        """Fetch normal transactions for an address."""
        all_txs = []
        page = 1
        while True:
            params = {
                "module": "account",
                "action": "txlist",
                "address": address,
                "startblock": start_block,
                "endblock": end_block,
                "page": page,
                "offset": 10000,
                "sort": "asc",
            }
            data = self._get(params)
            if data is None:
                break
            result = data.get("result", [])
            if not isinstance(result, list) or not result:
                break
            all_txs.extend(result)
            if len(result) < 10000:
                break
            page += 1
        logger.info(f"Fetched {len(all_txs)} normal txs for {address[:10]}...")
        return all_txs

    # ------------------------------------------------------------------
    # Internal transactions (txlistinternal)
    # ------------------------------------------------------------------

    def get_internal_transactions(
        self,
        address: str,
        start_block: int = 0,
        end_block: int = 99999999,
    ) -> List[Dict]:
        """Fetch internal (trace) transactions for an address."""
        all_txs = []
        page = 1
        while True:
            params = {
                "module": "account",
                "action": "txlistinternal",
                "address": address,
                "startblock": start_block,
                "endblock": end_block,
                "page": page,
                "offset": 10000,
                "sort": "asc",
            }
            data = self._get(params)
            if data is None:
                break
            result = data.get("result", [])
            if not isinstance(result, list) or not result:
                break
            all_txs.extend(result)
            if len(result) < 10000:
                break
            page += 1
        logger.info(f"Fetched {len(all_txs)} internal txs for {address[:10]}...")
        return all_txs

    # ------------------------------------------------------------------
    # ERC-20 token transfers (tokentx)
    # ------------------------------------------------------------------

    def get_token_transfers(
        self,
        address: str,
        start_block: int = 0,
        end_block: int = 99999999,
    ) -> List[Dict]:
        """Fetch ERC-20 token transfers for an address."""
        all_txs = []
        page = 1
        while True:
            params = {
                "module": "account",
                "action": "tokentx",
                "address": address,
                "startblock": start_block,
                "endblock": end_block,
                "page": page,
                "offset": 10000,
                "sort": "asc",
            }
            data = self._get(params)
            if data is None:
                break
            result = data.get("result", [])
            if not isinstance(result, list) or not result:
                break
            all_txs.extend(result)
            if len(result) < 10000:
                break
            page += 1
        logger.info(f"Fetched {len(all_txs)} ERC-20 transfers for {address[:10]}...")
        return all_txs

    # ------------------------------------------------------------------
    # ERC-721 NFT transfers (tokennfttx)
    # ------------------------------------------------------------------

    def get_nft_transfers(
        self,
        address: str,
        start_block: int = 0,
        end_block: int = 99999999,
    ) -> List[Dict]:
        """Fetch ERC-721 NFT transfers for an address."""
        all_txs = []
        page = 1
        while True:
            params = {
                "module": "account",
                "action": "tokennfttx",
                "address": address,
                "startblock": start_block,
                "endblock": end_block,
                "page": page,
                "offset": 10000,
                "sort": "asc",
            }
            data = self._get(params)
            if data is None:
                break
            result = data.get("result", [])
            if not isinstance(result, list) or not result:
                break
            all_txs.extend(result)
            if len(result) < 10000:
                break
            page += 1
        logger.info(f"Fetched {len(all_txs)} ERC-721 transfers for {address[:10]}...")
        return all_txs

    # ------------------------------------------------------------------
    # Conversion: Etherscan JSON -> Raw dataclasses
    # ------------------------------------------------------------------

    def _parse_timestamp(self, ts_str: str) -> datetime:
        return datetime.fromtimestamp(int(ts_str), tz=timezone.utc)

    def convert_normal_tx(self, tx: Dict) -> RawTransaction:
        """Convert an Etherscan txlist result to RawTransaction."""
        block_ts = self._parse_timestamp(tx.get("timeStamp", "0"))
        gas_price = tx.get("gasPrice", "0")
        # Etherscan doesn't always provide effective gas price; use gasPrice as fallback
        effective_gas_price = tx.get("effectiveGasPrice", gas_price)

        return RawTransaction(
            tx_hash=tx["hash"].lower(),
            block_number=int(tx.get("blockNumber", 0)),
            block_hash=tx.get("blockHash", "").lower(),
            block_timestamp=block_ts,
            from_address=tx.get("from", "").lower(),
            to_address=tx.get("to", "").lower() if tx.get("to") else None,
            value_wei=tx.get("value", "0"),
            gas_used=int(tx.get("gasUsed", 0)),
            gas_price_wei=gas_price,
            effective_gas_price_wei=effective_gas_price,
            tx_status=0 if tx.get("isError") == "1" else 1,
            tx_type=int(tx.get("type", "0"), 16) if tx.get("type", "").startswith("0x") else 0,
            nonce=int(tx.get("nonce", 0)),
            input_data=tx.get("input", "")[:10],
            contract_created=tx.get("contractAddress", "").lower() or None,
            log_count=0,  # populated later when we fetch logs
            trace_count=None,
            trace_provider=TraceProvider.NONE.value,
            trace_completeness=TraceCompleteness.NONE.value,
            finality_status=FinalityStatus.CONFIRMED.value,
        )

    def convert_internal_tx(self, itx: Dict, tx_hash: str) -> RawTrace:
        """Convert an Etherscan txlistinternal result to RawTrace."""
        block_ts = self._parse_timestamp(itx.get("timeStamp", "0"))
        # Etherscan txlistinternal has 'traceId' but it's not the trace_address format
        # Use a combination of from/to/value for uniqueness within a tx
        trace_addr = itx.get("traceId", "0")

        return RawTrace(
            trace_id=f"{tx_hash.lower()}:trace:{trace_addr}",
            tx_hash=tx_hash.lower(),
            trace_address=str(trace_addr),
            block_number=int(itx.get("blockNumber", 0)),
            block_timestamp=block_ts,
            call_type=itx.get("type", "call").lower(),
            from_address=itx.get("from", "").lower(),
            to_address=itx.get("to", "").lower(),
            value_wei=itx.get("value", "0"),
            gas_used=int(itx.get("gasUsed", 0)),
            input_data=itx.get("input", "")[:10] if itx.get("input") else "",
            error=itx.get("errCode") if itx.get("isError") == "1" else None,
            trace_provider=TraceProvider.ETHERSCAN_INTERNAL.value,
            finality_status=FinalityStatus.CONFIRMED.value,
        )

    def convert_token_transfer_to_log(self, ttx: Dict) -> RawLog:
        """Convert an Etherscan tokentx result to a synthetic RawLog.

        Etherscan's tokentx returns decoded Transfer events; we reconstruct
        the log structure for uniform processing.
        """
        tx_hash = ttx["hash"].lower()
        block_ts = self._parse_timestamp(ttx.get("timeStamp", "0"))
        log_index = int(ttx.get("transactionIndex", 0))
        contract = ttx.get("contractAddress", "").lower()
        from_addr = ttx.get("from", "").lower()
        to_addr = ttx.get("to", "").lower()
        value = ttx.get("value", "0")
        token_id = ttx.get("tokenID", "0")

        # Determine if ERC-20 or ERC-721 based on tokenDecimal presence
        is_erc721 = ttx.get("tokenDecimal") == "" or ttx.get("tokenDecimal") is None

        # Pad addresses to topic format (32 bytes = 64 hex chars + 0x)
        topic1 = "0x" + from_addr[2:].zfill(64)
        topic2 = "0x" + to_addr[2:].zfill(64)

        if is_erc721:
            # ERC-721: topic3 = token_id
            topic3 = "0x" + hex(int(token_id))[2:].zfill(64)
            data = "0x"
            event_name = "Transfer(ERC-721)"
        else:
            # ERC-20: value in data
            topic3 = None
            data = "0x" + hex(int(value))[2:].zfill(64) if value != "0" else "0x" + "0" * 64
            event_name = "Transfer(ERC-20)"

        return RawLog(
            log_id=f"{tx_hash}:{log_index}",
            tx_hash=tx_hash,
            log_index=log_index,
            block_number=int(ttx.get("blockNumber", 0)),
            block_timestamp=block_ts,
            contract_address=contract,
            topic0=TRANSFER_TOPIC,
            topic1=topic1,
            topic2=topic2,
            topic3=topic3,
            data=data,
            decoded_event_name=event_name,
            finality_status=FinalityStatus.CONFIRMED.value,
        )

    def convert_nft_transfer_to_log(self, ntx: Dict) -> RawLog:
        """Convert an Etherscan tokennfttx result to a synthetic RawLog."""
        tx_hash = ntx["hash"].lower()
        block_ts = self._parse_timestamp(ntx.get("timeStamp", "0"))
        log_index = int(ntx.get("transactionIndex", 0))
        contract = ntx.get("contractAddress", "").lower()
        from_addr = ntx.get("from", "").lower()
        to_addr = ntx.get("to", "").lower()
        token_id = ntx.get("tokenID", "0")

        topic1 = "0x" + from_addr[2:].zfill(64)
        topic2 = "0x" + to_addr[2:].zfill(64)
        topic3 = "0x" + hex(int(token_id))[2:].zfill(64)

        return RawLog(
            log_id=f"{tx_hash}:{log_index}",
            tx_hash=tx_hash,
            log_index=log_index,
            block_number=int(ntx.get("blockNumber", 0)),
            block_timestamp=block_ts,
            contract_address=contract,
            topic0=TRANSFER_TOPIC,
            topic1=topic1,
            topic2=topic2,
            topic3=topic3,
            data="0x",
            decoded_event_name="Transfer(ERC-721)",
            finality_status=FinalityStatus.CONFIRMED.value,
        )

    # ------------------------------------------------------------------
    # Full ingestion pipeline for a wallet
    # ------------------------------------------------------------------

    def ingest_wallet(
        self,
        wallet_address: str,
        start_block: int = 0,
        end_block: int = 99999999,
    ) -> Tuple[List[RawTransaction], List[RawLog], List[RawTrace], Set[int]]:
        """Ingest all raw data for a single wallet address.

        Returns:
            (raw_transactions, raw_logs, raw_traces, block_numbers_touched)
        """
        addr = wallet_address.lower()
        logger.info(f"Ingesting wallet {addr[:10]}... blocks {start_block}-{end_block}")

        # 1. Normal transactions
        normal_txs_raw = self.get_transactions(addr, start_block, end_block)
        raw_txs: Dict[str, RawTransaction] = {}
        for tx in normal_txs_raw:
            rtx = self.convert_normal_tx(tx)
            raw_txs[rtx.tx_hash] = rtx

        # 2. Internal transactions -> traces
        internal_txs_raw = self.get_internal_transactions(addr, start_block, end_block)
        raw_traces: List[RawTrace] = []
        tx_hashes_with_internals: Set[str] = set()
        for itx in internal_txs_raw:
            tx_hash = itx.get("hash", "").lower()
            trace = self.convert_internal_tx(itx, tx_hash)
            raw_traces.append(trace)
            tx_hashes_with_internals.add(tx_hash)

        # Update trace_completeness on transactions that have internals
        for tx_hash in tx_hashes_with_internals:
            if tx_hash in raw_txs:
                raw_txs[tx_hash].trace_provider = TraceProvider.ETHERSCAN_INTERNAL.value
                raw_txs[tx_hash].trace_completeness = TraceCompleteness.INTERNALS_ONLY.value
                raw_txs[tx_hash].trace_count = sum(
                    1 for t in raw_traces if t.tx_hash == tx_hash
                )

        # 3. ERC-20 token transfers -> synthetic logs
        token_txs_raw = self.get_token_transfers(addr, start_block, end_block)
        raw_logs: List[RawLog] = []
        for ttx in token_txs_raw:
            log = self.convert_token_transfer_to_log(ttx)
            raw_logs.append(log)
            # Ensure we have a RawTransaction for this tx_hash
            tx_hash = ttx["hash"].lower()
            if tx_hash not in raw_txs:
                # Create a minimal RawTransaction (we only saw this via token transfer)
                raw_txs[tx_hash] = RawTransaction(
                    tx_hash=tx_hash,
                    block_number=int(ttx.get("blockNumber", 0)),
                    block_hash=ttx.get("blockHash", "").lower(),
                    block_timestamp=self._parse_timestamp(ttx.get("timeStamp", "0")),
                    from_address=ttx.get("from", "").lower(),
                    to_address=ttx.get("to", "").lower() if ttx.get("to") else None,
                    value_wei="0",
                    gas_used=int(ttx.get("gasUsed", 0)),
                    gas_price_wei=ttx.get("gasPrice", "0"),
                    effective_gas_price_wei=ttx.get("gasPrice", "0"),
                    tx_status=1,
                    finality_status=FinalityStatus.CONFIRMED.value,
                )

        # 4. ERC-721 NFT transfers -> synthetic logs
        nft_txs_raw = self.get_nft_transfers(addr, start_block, end_block)
        for ntx in nft_txs_raw:
            log = self.convert_nft_transfer_to_log(ntx)
            raw_logs.append(log)
            tx_hash = ntx["hash"].lower()
            if tx_hash not in raw_txs:
                raw_txs[tx_hash] = RawTransaction(
                    tx_hash=tx_hash,
                    block_number=int(ntx.get("blockNumber", 0)),
                    block_hash=ntx.get("blockHash", "").lower(),
                    block_timestamp=self._parse_timestamp(ntx.get("timeStamp", "0")),
                    from_address=ntx.get("from", "").lower(),
                    to_address=ntx.get("to", "").lower() if ntx.get("to") else None,
                    value_wei="0",
                    gas_used=int(ntx.get("gasUsed", 0)),
                    gas_price_wei=ntx.get("gasPrice", "0"),
                    effective_gas_price_wei=ntx.get("gasPrice", "0"),
                    tx_status=1,
                    finality_status=FinalityStatus.CONFIRMED.value,
                )

        # Capture token metadata from Etherscan responses (decimals, symbol, name)
        for ttx in token_txs_raw:
            contract = ttx.get("contractAddress", "").lower()
            if not contract or contract in self.discovered_tokens:
                continue
            td = ttx.get("tokenDecimal", "")
            if not td:
                continue
            try:
                self.discovered_tokens[contract] = {
                    "symbol": ttx.get("tokenSymbol", ""),
                    "name": ttx.get("tokenName", ""),
                    "decimals": int(td),
                }
            except (ValueError, TypeError):
                pass

        for ntx in nft_txs_raw:
            contract = ntx.get("contractAddress", "").lower()
            if not contract or contract in self.discovered_tokens:
                continue
            self.discovered_tokens[contract] = {
                "symbol": ntx.get("tokenSymbol", "") or ntx.get("tokenName", ""),
                "name": ntx.get("tokenName", ""),
                "decimals": 0,  # NFTs don't have decimals
            }

        # Update log_count on transactions
        log_counts: Dict[str, int] = {}
        for log in raw_logs:
            log_counts[log.tx_hash] = log_counts.get(log.tx_hash, 0) + 1
        for tx_hash, count in log_counts.items():
            if tx_hash in raw_txs:
                raw_txs[tx_hash].log_count = count

        # Collect all block numbers touched
        block_numbers: Set[int] = set()
        for rtx in raw_txs.values():
            block_numbers.add(rtx.block_number)
        for trace in raw_traces:
            block_numbers.add(trace.block_number)

        tx_list = list(raw_txs.values())
        logger.info(
            f"Ingested {len(tx_list)} txs, {len(raw_logs)} logs, "
            f"{len(raw_traces)} traces across {len(block_numbers)} blocks"
        )
        return tx_list, raw_logs, raw_traces, block_numbers

    def ingest_wallets(
        self,
        wallet_addresses: List[str],
        start_block: int = 0,
        end_block: int = 99999999,
    ) -> Tuple[List[RawTransaction], List[RawLog], List[RawTrace], Set[int]]:
        """Ingest raw data for multiple wallets, deduplicating by primary key."""
        all_txs: Dict[str, RawTransaction] = {}
        all_logs: Dict[str, RawLog] = {}
        all_traces: Dict[str, RawTrace] = {}
        all_blocks: Set[int] = set()

        for addr in wallet_addresses:
            txs, logs, traces, blocks = self.ingest_wallet(addr, start_block, end_block)
            for tx in txs:
                # Keep the one with more trace info
                if tx.tx_hash in all_txs:
                    existing = all_txs[tx.tx_hash]
                    if tx.trace_completeness != TraceCompleteness.NONE.value:
                        all_txs[tx.tx_hash] = tx
                else:
                    all_txs[tx.tx_hash] = tx
            for log in logs:
                all_logs[log.log_id] = log
            for trace in traces:
                all_traces[trace.trace_id] = trace
            all_blocks.update(blocks)

        return list(all_txs.values()), list(all_logs.values()), list(all_traces.values()), all_blocks
