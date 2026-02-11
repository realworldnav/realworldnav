"""
S3 persistence for subledger parquet tables.

Follows the atomic write pattern from persistence_manager.py:
  staging → production → backup

All tables are partitioned by block range and stored as parquet.
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from main_app.services.subledger.models import (
    RAW_BLOCK_COLUMNS,
    RAW_TRANSACTION_COLUMNS,
    RAW_LOG_COLUMNS,
    RAW_TRACE_COLUMNS,
    MOVEMENT_COLUMNS,
    BALANCE_SNAPSHOT_COLUMNS,
    RECONCILIATION_COLUMNS,
    _records_to_df,
    Checkpoint,
)

logger = logging.getLogger(__name__)

BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "realworldnav-beta-1")


def _coerce_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure all columns are parquet-safe types.

    - datetime -> ISO string
    - object columns -> string
    - bool columns preserved
    - int/float columns preserved
    """
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) else "")
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].apply(
                lambda x: x.isoformat() if pd.notna(x) else ""
            )
    return df


def _write_parquet(df: pd.DataFrame, buffer: io.BytesIO) -> None:
    """Write DataFrame to parquet in a BytesIO buffer."""
    df = _coerce_for_parquet(df)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, buffer)
    buffer.seek(0)


def _read_parquet(body: bytes) -> pd.DataFrame:
    """Read parquet bytes into a DataFrame."""
    table = pq.read_table(io.BytesIO(body))
    return table.to_pandas()


_BLOCK_RANGE_RE = re.compile(r"blocks?_(\d+)_(\d+)\.parquet$")


def _key_overlaps_range(key: str, start_block: Optional[int], end_block: Optional[int]) -> bool:
    """Check if a parquet file's block range overlaps the query range.

    Filenames follow pattern: block_{start}_{end}.parquet or blocks_{start}_{end}.parquet.
    If the filename doesn't match the pattern, we conservatively return True (load it).
    """
    if start_block is None and end_block is None:
        return True
    m = _BLOCK_RANGE_RE.search(key)
    if not m:
        return True  # can't parse — load it to be safe
    file_start, file_end = int(m.group(1)), int(m.group(2))
    if start_block is not None and file_end < start_block:
        return False  # file ends before query starts
    if end_block is not None and file_start > end_block:
        return False  # file starts after query ends
    return True


class SubledgerStorage:
    """S3 read/write for all subledger parquet tables."""

    def __init__(self, fund_id: str, bucket: str = BUCKET_NAME):
        self.fund_id = fund_id
        self.bucket = bucket
        try:
            from main_app.s3_utils import get_s3_client
            self.s3 = get_s3_client()
        except ImportError:
            import boto3
            self.s3 = boto3.client("s3")
        self._base = f"subledger/{fund_id}"

    # ------------------------------------------------------------------
    # S3 helpers
    # ------------------------------------------------------------------

    def _key_exists(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except Exception:
            return False

    def _put(self, key: str, data: bytes) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=data)

    def _get(self, key: str) -> Optional[bytes]:
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            return obj["Body"].read()
        except Exception:
            return None

    def _delete(self, key: str) -> None:
        try:
            self.s3.delete_object(Bucket=self.bucket, Key=key)
        except Exception:
            pass

    def _list_keys(self, prefix: str) -> List[str]:
        """List all keys under a prefix."""
        keys = []
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys

    def _atomic_write(self, production_key: str, data: bytes) -> None:
        """Atomic write: staging -> production."""
        staging_key = f"{self._base}/staging/{production_key.split('/')[-1]}"
        self._put(staging_key, data)
        self.s3.copy_object(
            Bucket=self.bucket,
            CopySource={"Bucket": self.bucket, "Key": staging_key},
            Key=production_key,
        )
        self._delete(staging_key)

    def _save_df(self, df: pd.DataFrame, key: str) -> None:
        """Serialize DataFrame to parquet and write atomically to S3."""
        if df.empty:
            logger.debug(f"Empty DataFrame, skipping save to {key}")
            return
        buf = io.BytesIO()
        _write_parquet(df, buf)
        self._atomic_write(key, buf.getvalue())
        logger.info(f"Saved {len(df)} rows to {key}")

    def _load_df(self, key: str) -> pd.DataFrame:
        """Load a parquet file from S3 into DataFrame."""
        data = self._get(key)
        if data is None:
            return pd.DataFrame()
        return _read_parquet(data)

    # ------------------------------------------------------------------
    # Key generation
    # ------------------------------------------------------------------

    def _block_range_key(self, table: str, start: int, end: int, subfolder: str = "") -> str:
        """Build S3 key for a block-range parquet file."""
        if subfolder:
            return f"{self._base}/{table}/{subfolder}/block_{start}_{end}.parquet"
        return f"{self._base}/{table}/block_{start}_{end}.parquet"

    def _monthly_key(self, table: str, year: int, month: int, start: int, end: int) -> str:
        return f"{self._base}/raw/{table}/{year:04d}/{month:02d}/block_{start}_{end}.parquet"

    # ------------------------------------------------------------------
    # Raw blocks
    # ------------------------------------------------------------------

    def save_raw_blocks(self, blocks: List, start_block: int, end_block: int) -> None:
        df = _records_to_df(blocks, RAW_BLOCK_COLUMNS)
        key = f"{self._base}/raw/blocks/blocks_{start_block}_{end_block}.parquet"
        self._save_df(df, key)

    def load_raw_blocks(self, start_block: Optional[int] = None, end_block: Optional[int] = None) -> pd.DataFrame:
        """Load raw blocks, optionally filtering by range."""
        prefix = f"{self._base}/raw/blocks/"
        keys = self._list_keys(prefix)
        if not keys:
            return pd.DataFrame(columns=RAW_BLOCK_COLUMNS)
        frames = []
        for key in keys:
            if not _key_overlaps_range(key, start_block, end_block):
                continue
            df = self._load_df(key)
            if not df.empty:
                if start_block is not None:
                    df = df[df["block_number"] >= start_block]
                if end_block is not None:
                    df = df[df["block_number"] <= end_block]
                if not df.empty:
                    frames.append(df)
        if not frames:
            return pd.DataFrame(columns=RAW_BLOCK_COLUMNS)
        return pd.concat(frames, ignore_index=True).sort_values("block_number")

    # ------------------------------------------------------------------
    # Raw transactions
    # ------------------------------------------------------------------

    def save_raw_transactions(self, txs: List, start_block: int, end_block: int) -> None:
        df = _records_to_df(txs, RAW_TRANSACTION_COLUMNS)
        if df.empty:
            return
        # Partition by year/month of first tx
        ts = pd.to_datetime(df["block_timestamp"].iloc[0])
        key = self._monthly_key("transactions", ts.year, ts.month, start_block, end_block)
        self._save_df(df, key)

    def load_raw_transactions(self, start_block: Optional[int] = None, end_block: Optional[int] = None) -> pd.DataFrame:
        prefix = f"{self._base}/raw/transactions/"
        keys = self._list_keys(prefix)
        if not keys:
            return pd.DataFrame(columns=RAW_TRANSACTION_COLUMNS)
        frames = []
        for key in keys:
            if not _key_overlaps_range(key, start_block, end_block):
                continue
            df = self._load_df(key)
            if not df.empty:
                if start_block is not None:
                    df = df[df["block_number"] >= start_block]
                if end_block is not None:
                    df = df[df["block_number"] <= end_block]
                if not df.empty:
                    frames.append(df)
        if not frames:
            return pd.DataFrame(columns=RAW_TRANSACTION_COLUMNS)
        return pd.concat(frames, ignore_index=True).sort_values("block_number")

    # ------------------------------------------------------------------
    # Raw logs
    # ------------------------------------------------------------------

    def save_raw_logs(self, logs: List, start_block: int, end_block: int) -> None:
        df = _records_to_df(logs, RAW_LOG_COLUMNS)
        if df.empty:
            return
        ts = pd.to_datetime(df["block_timestamp"].iloc[0])
        key = self._monthly_key("logs", ts.year, ts.month, start_block, end_block)
        self._save_df(df, key)

    def load_raw_logs(self, start_block: Optional[int] = None, end_block: Optional[int] = None) -> pd.DataFrame:
        prefix = f"{self._base}/raw/logs/"
        keys = self._list_keys(prefix)
        if not keys:
            return pd.DataFrame(columns=RAW_LOG_COLUMNS)
        frames = []
        for key in keys:
            if not _key_overlaps_range(key, start_block, end_block):
                continue
            df = self._load_df(key)
            if not df.empty:
                if start_block is not None:
                    df = df[df["block_number"] >= start_block]
                if end_block is not None:
                    df = df[df["block_number"] <= end_block]
                if not df.empty:
                    frames.append(df)
        if not frames:
            return pd.DataFrame(columns=RAW_LOG_COLUMNS)
        return pd.concat(frames, ignore_index=True).sort_values("block_number")

    # ------------------------------------------------------------------
    # Raw traces
    # ------------------------------------------------------------------

    def save_raw_traces(self, traces: List, start_block: int, end_block: int) -> None:
        df = _records_to_df(traces, RAW_TRACE_COLUMNS)
        if df.empty:
            return
        ts = pd.to_datetime(df["block_timestamp"].iloc[0])
        key = self._monthly_key("traces", ts.year, ts.month, start_block, end_block)
        self._save_df(df, key)

    def load_raw_traces(self, start_block: Optional[int] = None, end_block: Optional[int] = None) -> pd.DataFrame:
        prefix = f"{self._base}/raw/traces/"
        keys = self._list_keys(prefix)
        if not keys:
            return pd.DataFrame(columns=RAW_TRACE_COLUMNS)
        frames = []
        for key in keys:
            if not _key_overlaps_range(key, start_block, end_block):
                continue
            df = self._load_df(key)
            if not df.empty:
                if start_block is not None:
                    df = df[df["block_number"] >= start_block]
                if end_block is not None:
                    df = df[df["block_number"] <= end_block]
                if not df.empty:
                    frames.append(df)
        if not frames:
            return pd.DataFrame(columns=RAW_TRACE_COLUMNS)
        return pd.concat(frames, ignore_index=True).sort_values("block_number")

    # ------------------------------------------------------------------
    # Movements
    # ------------------------------------------------------------------

    def save_movements(self, movements: List, start_block: int, end_block: int) -> None:
        df = _records_to_df(movements, MOVEMENT_COLUMNS)
        if df.empty:
            return
        ts = pd.to_datetime(df["block_timestamp"].iloc[0])
        key = f"{self._base}/movements/{ts.year:04d}/{ts.month:02d}/block_{start_block}_{end_block}.parquet"
        self._save_df(df, key)

    def load_movements(
        self,
        start_block: Optional[int] = None,
        end_block: Optional[int] = None,
        wallet_address: Optional[str] = None,
        asset_id: Optional[str] = None,
    ) -> pd.DataFrame:
        prefix = f"{self._base}/movements/"
        keys = self._list_keys(prefix)
        if not keys:
            return pd.DataFrame(columns=MOVEMENT_COLUMNS)
        frames = []
        for key in keys:
            if not _key_overlaps_range(key, start_block, end_block):
                continue
            df = self._load_df(key)
            if not df.empty:
                if start_block is not None:
                    df = df[df["block_number"] >= start_block]
                if end_block is not None:
                    df = df[df["block_number"] <= end_block]
                if wallet_address:
                    df = df[df["wallet_address"] == wallet_address.lower()]
                if asset_id:
                    df = df[df["asset_id"] == asset_id]
                if not df.empty:
                    frames.append(df)
        if not frames:
            return pd.DataFrame(columns=MOVEMENT_COLUMNS)
        return pd.concat(frames, ignore_index=True).sort_values("block_number")

    # ------------------------------------------------------------------
    # Snapshots
    # ------------------------------------------------------------------

    def save_snapshots(self, snapshots: List, block_number: int) -> None:
        df = _records_to_df(snapshots, BALANCE_SNAPSHOT_COLUMNS)
        ts = pd.to_datetime(df["block_timestamp"].iloc[0]) if not df.empty else datetime.now(timezone.utc)
        year = ts.year if hasattr(ts, "year") else datetime.now(timezone.utc).year
        month = ts.month if hasattr(ts, "month") else datetime.now(timezone.utc).month
        key = f"{self._base}/snapshots/{year:04d}/{month:02d}/snap_{block_number}.parquet"
        self._save_df(df, key)

    def load_snapshots(self, block_number: Optional[int] = None) -> pd.DataFrame:
        prefix = f"{self._base}/snapshots/"
        keys = self._list_keys(prefix)
        if not keys:
            return pd.DataFrame(columns=BALANCE_SNAPSHOT_COLUMNS)
        if block_number is not None:
            # Look for exact snapshot
            for key in keys:
                if f"snap_{block_number}.parquet" in key:
                    return self._load_df(key)
            return pd.DataFrame(columns=BALANCE_SNAPSHOT_COLUMNS)
        # Load all
        frames = [self._load_df(k) for k in keys]
        frames = [f for f in frames if not f.empty]
        if not frames:
            return pd.DataFrame(columns=BALANCE_SNAPSHOT_COLUMNS)
        return pd.concat(frames, ignore_index=True)

    # ------------------------------------------------------------------
    # Reconciliation results
    # ------------------------------------------------------------------

    def save_reconciliation(self, results: List, proof_run_id: str, block_number: int) -> None:
        df = _records_to_df(results, RECONCILIATION_COLUMNS)
        key = f"{self._base}/reconciliation/{proof_run_id}/recon_{block_number}.parquet"
        self._save_df(df, key)

    def load_reconciliation(self, proof_run_id: Optional[str] = None) -> pd.DataFrame:
        if proof_run_id:
            prefix = f"{self._base}/reconciliation/{proof_run_id}/"
        else:
            prefix = f"{self._base}/reconciliation/"
        keys = self._list_keys(prefix)
        if not keys:
            return pd.DataFrame(columns=RECONCILIATION_COLUMNS)
        frames = [self._load_df(k) for k in keys]
        frames = [f for f in frames if not f.empty]
        if not frames:
            return pd.DataFrame(columns=RECONCILIATION_COLUMNS)
        return pd.concat(frames, ignore_index=True)

    # ------------------------------------------------------------------
    # Checkpoints
    # ------------------------------------------------------------------

    def save_checkpoint(self, checkpoint: Checkpoint) -> None:
        key = f"{self._base}/checkpoints/checkpoint_{checkpoint.block_number}.json"
        data = json.dumps(checkpoint.to_dict(), indent=2).encode("utf-8")
        self._atomic_write(key, data)
        logger.info(f"Saved checkpoint at block {checkpoint.block_number}")

    def load_checkpoint(self, block_number: int) -> Optional[Checkpoint]:
        key = f"{self._base}/checkpoints/checkpoint_{block_number}.json"
        data = self._get(key)
        if data is None:
            return None
        d = json.loads(data)
        d["created_at"] = datetime.fromisoformat(d["created_at"])
        return Checkpoint(**d)

    def list_checkpoints(self) -> List[int]:
        """Return list of block numbers that have checkpoints."""
        prefix = f"{self._base}/checkpoints/"
        keys = self._list_keys(prefix)
        blocks = []
        for key in keys:
            fname = key.rsplit("/", 1)[-1]
            if fname.startswith("checkpoint_") and fname.endswith(".json"):
                try:
                    bn = int(fname.replace("checkpoint_", "").replace(".json", ""))
                    blocks.append(bn)
                except ValueError:
                    pass
        return sorted(blocks)

    # ------------------------------------------------------------------
    # Block-range deletion (for idempotent reprocessing)
    # ------------------------------------------------------------------

    def delete_block_range(self, start_block: int, end_block: int) -> int:
        """Delete all data (raw + movements + snapshots) for a block range.

        Returns count of keys deleted.
        """
        deleted = 0
        # Check all data prefixes
        prefixes = [
            f"{self._base}/raw/blocks/",
            f"{self._base}/raw/transactions/",
            f"{self._base}/raw/logs/",
            f"{self._base}/raw/traces/",
            f"{self._base}/movements/",
            f"{self._base}/snapshots/",
        ]
        for prefix in prefixes:
            keys = self._list_keys(prefix)
            for key in keys:
                # Load, filter out the block range, re-save (or delete if empty)
                df = self._load_df(key)
                if df.empty:
                    continue
                if "block_number" not in df.columns:
                    continue
                before = len(df)
                df = df[
                    (df["block_number"] < start_block) | (df["block_number"] > end_block)
                ]
                if len(df) == 0:
                    # Entire file is within the range — delete it
                    self._delete(key)
                    deleted += before
                elif len(df) < before:
                    # Some rows removed — re-save
                    self._save_df(df, key)
                    deleted += before - len(df)
        logger.info(
            f"Deleted {deleted} rows for blocks {start_block}-{end_block} "
            f"from fund {self.fund_id}"
        )
        return deleted

    # ------------------------------------------------------------------
    # Asset registry persistence
    # ------------------------------------------------------------------

    def save_asset_registry(self, registry_json: str) -> None:
        key = f"{self._base}/asset_registry.json"
        self._atomic_write(key, registry_json.encode("utf-8"))

    def load_asset_registry(self) -> Optional[str]:
        key = f"{self._base}/asset_registry.json"
        data = self._get(key)
        return data.decode("utf-8") if data else None
