"""
Blockchain Configuration Module

Contains all blockchain-related constants, configurations, and settings
for Web3 integration with Ethereum mainnet.
"""

from decimal import Decimal

# Infura API Configuration
INFURA_URL = "https://mainnet.infura.io/v3/16f12641c1db46beb60e95cf4c88cbe1"
INFURA_API_KEY = "16f12641c1db46beb60e95cf4c88cbe1"

# Etherscan API Configuration (for fallback or additional data)
ETHERSCAN_API_KEY = "P13CVTCP43NWU9GX5D9VBA2QMUTJDDS941"
ETHERSCAN_BASE_URL = "https://api.etherscan.io/api"

# Event Topic0 Hash Mappings
TOPIC0_HASH_MAP = {
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef": "Transfer",
    "0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c": "Deposit",
    "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65": "Withdraw"
}

# Verified Token Contract Addresses - Fully Trusted
VERIFIED_TOKENS = {
    # Major stablecoins
    "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
    "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
    "DAI": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
    "FRAX": "0x853d955aCEf822Db058eb8505911ED77F175b99e",
    "LUSD": "0x5f98805A4E8be255a32880FDeC7F6728C6568bA0",
    
    # ETH and wETH variants
    "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
    "WSTETH": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
    "STETH": "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84",
    "CBETH": "0xBe9895146f7AF43049ca1c1AE358B0541Ea49704",
    "RETH": "0xae78736Cd615f374D3085123A210448E74Fc6393",
    
    # Major cryptocurrencies
    "WBTC": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
    "LINK": "0x514910771AF9Ca656af840dff83E8264EcF986CA",
    "UNI": "0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984",
    "AAVE": "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9",
    "CRV": "0xD533a949740bb3306d119CC777fa900bA034cd52",
    "SNX": "0xC011a73ee8576Fb46F5E1c5751cA3B9Fe0af2a6F",
    "COMP": "0xc00e94Cb662C3520282E6f5717214004A7f26888",
    "MKR": "0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2",
    
    # Popular tokens
    "SHIB": "0x95aD61b0a150d79219dCF64E1E6Cc01f0B64C4cE",
    "PEPE": "0x6982508145454Ce325dDbE47a25d4ec3d2311933",
    "BLUR": "0x5283D291DBCF85356A21bA090E6db59121208b44",
    "BLUR_POOL": "0x0000000000A39bb272e79075ade125fd351887Ac",
    "MEME": "0xb131f4A55907B10d1F0A50d8ab8FA09EC342cd74",
    
    # DeFi protocol tokens
    "SUSHI": "0x6B3595068778DD592e39A122f4f5a5cF09C90fE2",
    "LDO": "0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32",
    "RPL": "0xD33526068D116cE69F19A9ee46F0bd304F21A51f",
    "FXS": "0x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0"
}

# Legacy support - keep for backward compatibility
TOKEN_ADDRESSES = VERIFIED_TOKENS

# Token Decimals Mapping
TOKEN_DECIMALS = {
    "ETH": 18,
    "WETH": 18,
    "USDC": 6,
    "USDT": 6,
    "WSTETH": 18,
    "BLUR_POOL": 18,
    "MEME": 18,
    "DAI": 18,
}

# Stablecoin List
STABLECOINS = {"USDC", "USDT", "DAI", "VARIABLEDEBTETHUSDC", "VARIABLEDEBTETHUSDT"}

# ETH One-for-One Assets
ETH_ONE_FOR_ONE = {"ETH", "BLUR POOL", "BLUR", "WETH", "MWETH-PPG:5", "AETHWETH"}

# wstETH Related Assets
WSTETH_ASSETS = {"WSTETH", "stETH", "wstETH", "rsETH", "rstETH"}
WSTETH_RATE_DEFAULT = Decimal("1.18045433553113")

# Meme Coins
MEME_COINS = {"MEME"}
MEME_RATE = Decimal("0.000001")

# Special Addresses
MINT_OR_BURN_ADDRESS = "0x0000000000000000000000000000000000000000"
COW_PROTOCOL_ETH_FLOW = "0x40A50cf069e992AA4536211B23F286eF88752187"

# Coinbase Addresses
COINBASE_ADDRESSES = {
    "coinbase_prime": "0xCD531Ae9EFCCE479654c4926dec5F6209531Ca7b",
    "coinbase_prime_2": "0xceB69F6342eCE283b2F5c9088Ff249B5d0Ae66ea",
    "coinbase_10": "0xA9D1e08C7793af67e9d92fe308d5697FB81d3E43",
}

# Lending Platform Smart Contracts
LENDING_CONTRACTS = {
    "gondi": "0xf65B99CE6DC5F6c556172BCC0Ff27D3665a7d9A8",
    "gondi_v2": "0x478f6F994C6fb3cf3e444a489b3AD9edB8cCaE16",
    "p2p_lending": "0x5F19431BC8A3eb21222771c6C867a63a119DeDA7",
    "blur_pool": "0x0000000000A39bb272e79075ade125fd351887Ac",
}

# Chainlink Price Feed Address (ETH/USD)
CHAINLINK_ETH_USD_FEED = "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"

# Chainlink ABI for Price Feed
CHAINLINK_AGGREGATOR_V3_ABI = [{
    "inputs": [],
    "name": "latestRoundData",
    "outputs": [
        {"internalType": "uint80", "name": "roundId", "type": "uint80"},
        {"internalType": "int256", "name": "answer", "type": "int256"},
        {"internalType": "uint256", "name": "startedAt", "type": "uint256"},
        {"internalType": "uint256", "name": "updatedAt", "type": "uint256"},
        {"internalType": "uint80", "name": "answeredInRound", "type": "uint80"},
    ],
    "stateMutability": "view",
    "type": "function",
}]

# Query Configuration
BLOCK_CHUNK_SIZE = 10000  # Number of blocks to query at once
MAX_RETRIES = 3
RETRY_DELAY = 0.5  # seconds
REQUEST_TIMEOUT = 30  # seconds

# Cache Configuration
CACHE_SIZES = {
    "block_by_timestamp": 1024,
    "eth_price_at_block": 512,
    "decoded_logs": 2048,
    "wallet_fund_mapping": 256,
    "token_info": 128,
}

# Known Blacklisted Tokens (Scams/Phishing)
BLACKLISTED_TOKENS = {
    # Common phishing attempts - add addresses as discovered
    "FAKE_USDC": "0x0000000000000000000000000000000000000001",  # Example placeholder
    # Note: Real blacklist addresses should be added when discovered
}

# Suspicious Token Patterns
SUSPICIOUS_PATTERNS = {
    "emoji_symbols": ["🚀", "💎", "🔥", "⚡", "🌙", "💰", "🎯", "⭐"],
    "scam_keywords": [
        "AIRDROP", "FREE", "BONUS", "REWARD", "CLAIM", "GIFT", "WIN", "LOTTERY",
        "1000X", "MOON", "LAMBO", "DIAMOND", "ROCKET", "PUMP", "DOGE", "ELON"
    ],
    "impersonation_attempts": [
        "USSDC", "USDCC", "USDCT", "TETHER", "TETHHER", "ETHERRUM", "ETHEREM",
        "BITCOIN", "BITCON", "UNISWAP", "CHAINLINK", "BINANCE"
    ]
}

# Token Risk Assessment Thresholds
TOKEN_RISK_THRESHOLDS = {
    "high_risk": {
        "contract_age_days": 7,      # Tokens created within 7 days
        "holder_count": 100,         # Fewer than 100 holders
        "transaction_count": 1000,   # Fewer than 1000 transactions
    },
    "medium_risk": {
        "contract_age_days": 30,     # Tokens created within 30 days
        "holder_count": 1000,        # Fewer than 1000 holders
        "transaction_count": 10000,  # Fewer than 10000 transactions
    }
}

# Token Approval Status
TOKEN_STATUS = {
    "VERIFIED": "verified",         # Pre-approved, always show
    "PENDING": "pending",           # Needs manual review
    "APPROVED": "approved",         # User approved
    "REJECTED": "rejected",         # User rejected
    "BLACKLISTED": "blacklisted"    # Auto-rejected
}

# Decimal Precision Settings
CRYPTO_PRECISION = 18  # decimal places for crypto amounts
USD_PRECISION = 2  # decimal places for USD amounts
SCALE_CRYPTO = Decimal('0.000000000000000001')  # 18 decimal places
SCALE_USD = Decimal('0.01')  # 2 decimal places