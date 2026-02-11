"""
Token spam/scam filter for the crypto subledger.

Display-layer filtering only -- all raw data is preserved in storage.
Three-layer approach:
  1. Whitelist (VERIFIED_TOKENS)    -> always show
  2. Blacklist + Unicode + No symbol + Symbol impersonation -> always hide
  3. Heuristic scoring (with USD)   -> threshold-based (named tokens only)

No network calls at classify time.  Pricing data is passed in by the caller.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

import pandas as pd
from decimal import Decimal

from main_app.config.blockchain_config import (
    VERIFIED_TOKENS,
    BLACKLISTED_TOKENS,
    SUSPICIOUS_PATTERNS,
)
from main_app.services.subledger.asset_registry import (
    AssetRegistry,
    extract_contract,
    is_native,
)

logger = logging.getLogger(__name__)

SPAM_THRESHOLD = 40  # score >= this => hidden by default


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class TokenFilterResult:
    asset_id: str
    visibility: str          # "verified" | "spam" | "suspicious" | "unknown" | "user_approved" | "user_rejected"
    spam_score: int          # 0-100
    usd_price: Optional[Decimal] = None
    usd_value: Optional[Decimal] = None
    reasons: List[str] = field(default_factory=list)

    @property
    def is_visible(self) -> bool:
        return self.visibility in ("verified", "unknown", "suspicious", "user_approved")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _has_non_ascii(text: str) -> bool:
    """Any character with ord > 127 is suspicious in a token symbol/name."""
    if not text:
        return False
    return any(ord(c) > 127 for c in text)


def _matches_scam_keywords(symbol: str) -> bool:
    upper = symbol.upper()
    for kw in SUSPICIOUS_PATTERNS.get("scam_keywords", []):
        if kw in upper:
            return True
    return False


# ---------------------------------------------------------------------------
# Main filter
# ---------------------------------------------------------------------------

class TokenFilter:
    """Stateless token filter.  Instantiate with an AssetRegistry and an
    optional price map, then call ``classify_balances()``."""

    def __init__(
        self,
        registry: AssetRegistry,
        price_map: Optional[Dict[str, Decimal]] = None,
    ):
        """
        Args:
            registry:  AssetRegistry with token metadata.
            price_map: Mapping of **lowercase contract address** -> USD price.
                       Built by the UI layer from PriceService results.
        """
        self.registry = registry
        self.price_map: Dict[str, Decimal] = price_map or {}

        self._verified_contracts: Set[str] = {
            addr.lower() for addr in VERIFIED_TOKENS.values()
        }
        # Reverse map: symbol -> verified contract (for impersonation detection)
        self._verified_symbol_contracts: Dict[str, str] = {
            sym.upper(): addr.lower() for sym, addr in VERIFIED_TOKENS.items()
        }
        self._blacklisted_contracts: Set[str] = {
            addr.lower() for addr in BLACKLISTED_TOKENS.values()
        }

        # User overrides (best-effort load from S3)
        self._user_approved: Set[str] = set()
        self._user_rejected: Set[str] = set()
        self._load_user_overrides()

    # ------------------------------------------------------------------
    def _load_user_overrides(self) -> None:
        try:
            from main_app.s3_utils import load_approved_tokens_file, load_rejected_tokens_file
            self._user_approved = {a.lower() for a in load_approved_tokens_file()}
            self._user_rejected = {a.lower() for a in load_rejected_tokens_file()}
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def classify_balances(
        self, balances_df: pd.DataFrame
    ) -> Dict[str, TokenFilterResult]:
        """Classify every unique ``asset_id`` in *balances_df*.

        Returns a dict keyed by asset_id.
        """
        results: Dict[str, TokenFilterResult] = {}
        if balances_df.empty:
            return results

        # Aggregate per asset across all wallets
        agg = (
            balances_df.groupby("asset_id")
            .agg(
                total_movements=("movement_count", "sum"),
                max_abs_balance=("balance_raw", lambda s: max(abs(int(v)) for v in s)),
            )
            .reset_index()
        )

        for _, row in agg.iterrows():
            asset_id = row["asset_id"]
            results[asset_id] = self._classify_single(
                asset_id,
                int(row["total_movements"]),
                int(row["max_abs_balance"]),
            )
        return results

    def filter_dataframe(
        self,
        df: pd.DataFrame,
        classifications: Dict[str, TokenFilterResult],
        show_spam: bool = False,
    ) -> pd.DataFrame:
        """Return *df* with spam rows removed (unless *show_spam*)."""
        if df.empty or show_spam:
            return df
        visible = {aid for aid, r in classifications.items() if r.is_visible}
        return df[df["asset_id"].isin(visible)].reset_index(drop=True)

    # ------------------------------------------------------------------
    # Classification logic
    # ------------------------------------------------------------------

    def _classify_single(
        self,
        asset_id: str,
        total_movements: int,
        max_abs_balance: int,
    ) -> TokenFilterResult:

        # Native ETH → always show
        if is_native(asset_id):
            return TokenFilterResult(asset_id, "verified", 0, reasons=["Native ETH"])

        contract = extract_contract(asset_id)
        meta = self.registry.get(asset_id)
        symbol = meta.symbol if meta else ""
        name = meta.name if meta else ""
        decimals = meta.decimals if meta else 18

        # Compute USD price / value
        usd_price = self.price_map.get(contract)
        formatted_balance = abs(max_abs_balance) / (10 ** decimals)
        usd_value = Decimal(str(formatted_balance)) * usd_price if usd_price else None

        # ---- Layer 1: Whitelist ----
        if contract in self._verified_contracts or (meta and meta.is_verified):
            return TokenFilterResult(
                asset_id, "verified", 0,
                usd_price=usd_price, usd_value=usd_value,
                reasons=["Verified token"],
            )

        # ---- User overrides ----
        if contract in self._user_approved:
            return TokenFilterResult(
                asset_id, "user_approved", 0,
                usd_price=usd_price, usd_value=usd_value,
                reasons=["User approved"],
            )
        if contract in self._user_rejected:
            return TokenFilterResult(
                asset_id, "user_rejected", 100,
                usd_price=usd_price, usd_value=usd_value,
                reasons=["User rejected"],
            )

        # ---- Layer 2: Blacklist + Unicode ----
        if contract in self._blacklisted_contracts:
            return TokenFilterResult(
                asset_id, "spam", 100,
                usd_price=usd_price, usd_value=usd_value,
                reasons=["Blacklisted contract"],
            )
        if _has_non_ascii(symbol) or _has_non_ascii(name):
            return TokenFilterResult(
                asset_id, "spam", 90,
                usd_price=usd_price, usd_value=usd_value,
                reasons=[f"Non-ASCII characters in symbol/name: {symbol!r}"],
            )
        if not symbol or symbol.startswith("ERC-20:"):
            return TokenFilterResult(
                asset_id, "spam", 80,
                usd_price=usd_price, usd_value=usd_value,
                reasons=["No Etherscan symbol (unrecognized contract)"],
            )
        # Symbol impersonation: claims a verified symbol but wrong contract
        verified_addr = self._verified_symbol_contracts.get(symbol.upper())
        if verified_addr and contract != verified_addr:
            return TokenFilterResult(
                asset_id, "spam", 85,
                usd_price=usd_price, usd_value=usd_value,
                reasons=[f"Impersonates {symbol} (real contract: {verified_addr[:10]}...)"],
            )

        # ---- Layer 3: Heuristic scoring ----
        # (only tokens WITH an Etherscan symbol reach this point)
        score = 0
        reasons: List[str] = []

        # --- Positive (spam signals) ---
        if total_movements == 1:
            score += 30
            reasons.append("Single movement (likely airdrop)")
        elif total_movements <= 2:
            score += 15
            reasons.append("Very low movement count")

        if 0 < formatted_balance < 0.00001:
            score += 20
            reasons.append(f"Dust amount ({formatted_balance:.10g})")

        if symbol and _matches_scam_keywords(symbol):
            score += 25
            reasons.append("Symbol matches scam keyword")

        if symbol and len(symbol) > 20:
            score += 10
            reasons.append(f"Symbol unusually long ({len(symbol)} chars)")

        # --- Negative (legitimacy signals) ---
        if total_movements >= 50:
            score -= 40
            reasons.append(f"High activity ({total_movements} movements)")
        elif total_movements >= 10:
            score -= 25
            reasons.append(f"Moderate activity ({total_movements} movements)")

        if formatted_balance >= 1.0:
            score -= 25
            reasons.append(f"Significant crypto balance ({formatted_balance:.6g})")

        if usd_value is not None:
            fv = float(usd_value)
            if fv >= 100:
                score -= 35
                reasons.append(f"Substantial USD value (${fv:,.2f})")
            elif fv >= 10:
                score -= 20
                reasons.append(f"Has USD value (${fv:,.2f})")

        score = max(0, min(100, score))

        if score >= SPAM_THRESHOLD:
            vis = "spam"
        elif score >= 20:
            vis = "suspicious"
        else:
            vis = "unknown"

        return TokenFilterResult(
            asset_id, vis, score,
            usd_price=usd_price, usd_value=usd_value,
            reasons=reasons,
        )
