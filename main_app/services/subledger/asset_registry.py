"""
Asset identity resolution and metadata cache.

Canonical asset_id format: {chain_id}:{asset_type}:{contract_address}:{token_id}

Examples:
  Native ETH:    1:native:0x0000000000000000000000000000000000000000:0
  WETH ERC-20:   1:erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2:0
  ERC-721 NFT:   1:erc721:0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d:1234
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Dict, Optional

logger = logging.getLogger(__name__)

ZERO_ADDRESS = "0x" + "0" * 40

# Well-known WETH contract on mainnet
WETH_CONTRACT = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

# Standard event topic0 hashes
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
DEPOSIT_TOPIC = "0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c"
WITHDRAWAL_TOPIC = "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65"

# ERC-1155 event topics
TRANSFER_SINGLE_TOPIC = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
TRANSFER_BATCH_TOPIC = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"


def _normalize_address(addr: Optional[str]) -> str:
    """Lowercase, 0x-prefixed, 42-char address."""
    if addr is None:
        return ZERO_ADDRESS
    addr = addr.strip().lower()
    if not addr.startswith("0x"):
        addr = "0x" + addr
    # Pad if short (e.g. "0x0" -> full zero address)
    if len(addr) < 42:
        addr = "0x" + addr[2:].zfill(40)
    return addr


def resolve_asset_id(
    chain_id: int,
    contract_address: Optional[str],
    token_id: int = 0,
    asset_type: Optional[str] = None,
) -> str:
    """Build canonical asset_id string.

    Args:
        chain_id: Network id (1=mainnet, 10=optimism, 42161=arbitrum).
        contract_address: Token contract. None or zero address => native ETH.
        token_id: Token id for NFTs. 0 for fungible assets.
        asset_type: Override type (erc20/erc721/erc1155). Defaults to erc20.

    Returns:
        Canonical asset_id, e.g. "1:native:0x0000...0000:0"
    """
    addr = _normalize_address(contract_address)
    if addr == ZERO_ADDRESS:
        return f"{chain_id}:native:{ZERO_ADDRESS}:0"
    atype = asset_type or "erc20"
    return f"{chain_id}:{atype}:{addr}:{token_id}"


def native_eth_asset_id(chain_id: int = 1) -> str:
    """Shortcut for native ETH asset id."""
    return f"{chain_id}:native:{ZERO_ADDRESS}:0"


def is_native(asset_id: str) -> bool:
    """Check if asset_id refers to native ETH."""
    return ":native:" in asset_id


def extract_contract(asset_id: str) -> str:
    """Extract contract address from asset_id."""
    parts = asset_id.split(":")
    if len(parts) >= 3:
        return parts[2]
    return ZERO_ADDRESS


def extract_chain_id(asset_id: str) -> int:
    """Extract chain_id from asset_id."""
    parts = asset_id.split(":")
    return int(parts[0]) if parts else 1


# ---------------------------------------------------------------------------
# Asset metadata cache
# ---------------------------------------------------------------------------

@dataclass
class AssetMetadata:
    asset_id: str
    symbol: str = ""
    name: str = ""
    decimals: int = 18
    asset_type: str = "erc20"
    contract_address: str = ""
    chain_id: int = 1
    is_verified: bool = False
    discovered_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class AssetRegistry:
    """In-memory asset metadata cache, seeded from blockchain_config.py constants."""

    def __init__(self, chain_id: int = 1):
        self.chain_id = chain_id
        self._cache: Dict[str, AssetMetadata] = {}
        self._seed_from_config()

    def _seed_from_config(self) -> None:
        """Populate cache from VERIFIED_TOKENS and TOKEN_DECIMALS."""
        try:
            from main_app.config.blockchain_config import VERIFIED_TOKENS, TOKEN_DECIMALS
        except ImportError:
            logger.warning("Could not import blockchain_config; registry will be empty")
            return

        # Register native ETH
        eth_id = native_eth_asset_id(self.chain_id)
        self._cache[eth_id] = AssetMetadata(
            asset_id=eth_id,
            symbol="ETH",
            name="Ether",
            decimals=18,
            asset_type="native",
            contract_address=ZERO_ADDRESS,
            chain_id=self.chain_id,
            is_verified=True,
        )

        # Register known ERC-20 tokens
        for symbol, address in VERIFIED_TOKENS.items():
            addr = _normalize_address(address)
            asset_id = resolve_asset_id(self.chain_id, addr, 0, "erc20")
            decimals = TOKEN_DECIMALS.get(symbol, 18)
            self._cache[asset_id] = AssetMetadata(
                asset_id=asset_id,
                symbol=symbol,
                name=symbol,
                decimals=decimals,
                asset_type="erc20",
                contract_address=addr,
                chain_id=self.chain_id,
                is_verified=True,
            )

        logger.info(f"AssetRegistry seeded with {len(self._cache)} assets")

    def get(self, asset_id: str) -> Optional[AssetMetadata]:
        return self._cache.get(asset_id)

    def get_or_create(
        self,
        asset_id: str,
        symbol: str = "",
        name: str = "",
        decimals: int = 18,
        asset_type: str = "erc20",
    ) -> AssetMetadata:
        """Return cached metadata or create a new unverified entry."""
        if asset_id in self._cache:
            return self._cache[asset_id]
        meta = AssetMetadata(
            asset_id=asset_id,
            symbol=symbol,
            name=name,
            decimals=decimals,
            asset_type=asset_type,
            contract_address=extract_contract(asset_id),
            chain_id=extract_chain_id(asset_id),
            is_verified=False,
        )
        self._cache[asset_id] = meta
        logger.debug(f"AssetRegistry: new unverified asset {asset_id} ({symbol})")
        return meta

    def register_if_better(
        self,
        asset_id: str,
        symbol: str = "",
        name: str = "",
        decimals: int = 18,
        asset_type: str = "erc20",
    ) -> AssetMetadata:
        """Register or update metadata, but never overwrite verified entries.

        Used to feed Etherscan's tokenDecimal/tokenSymbol into the registry
        after ingestion. Safe to call multiple times — only improves data.
        """
        existing = self._cache.get(asset_id)
        if existing and existing.is_verified:
            return existing  # Never override verified tokens
        if existing:
            updated = False
            if symbol and not existing.symbol:
                existing.symbol = symbol
                updated = True
            if name and not existing.name:
                existing.name = name
                updated = True
            if decimals != 18 and existing.decimals == 18:
                existing.decimals = decimals
                updated = True
            if updated:
                logger.info(
                    f"AssetRegistry: updated {asset_id} -> "
                    f"{existing.symbol} decimals={existing.decimals}"
                )
            return existing
        return self.get_or_create(asset_id, symbol, name, decimals, asset_type)

    def get_decimals(self, asset_id: str) -> int:
        """Return decimals for an asset, defaulting to 18."""
        meta = self._cache.get(asset_id)
        return meta.decimals if meta else 18

    def lookup_by_contract(self, contract_address: str) -> Optional[AssetMetadata]:
        """Find asset metadata by contract address."""
        addr = _normalize_address(contract_address)
        for meta in self._cache.values():
            if meta.contract_address == addr:
                return meta
        return None

    def all_assets(self) -> Dict[str, AssetMetadata]:
        return dict(self._cache)

    def to_json(self) -> str:
        """Serialize registry for persistence."""
        data = {}
        for aid, meta in self._cache.items():
            d = asdict(meta)
            d["discovered_at"] = meta.discovered_at.isoformat()
            data[aid] = d
        return json.dumps(data, indent=2)

    @classmethod
    def from_json(cls, json_str: str, chain_id: int = 1) -> "AssetRegistry":
        """Deserialize registry from JSON."""
        registry = cls(chain_id=chain_id)
        data = json.loads(json_str)
        for aid, d in data.items():
            d["discovered_at"] = datetime.fromisoformat(d["discovered_at"])
            registry._cache[aid] = AssetMetadata(**d)
        return registry
