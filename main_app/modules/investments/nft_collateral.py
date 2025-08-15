from shiny import ui, reactive, render, req
from shiny import App, Inputs, Outputs, Session
from shiny.render import DataGrid

import pandas as pd
import requests
import json
from decimal import Decimal, InvalidOperation
from functools import lru_cache
import time
from typing import Dict, Optional

# NFTScan API Configuration
NFTSCAN_API_KEY = "L47BjfbSQELgYhlQvU2FecI4"
NFTSCAN_BASE_URL = "https://api.nftscan.com"

# Alternative API configurations
OPENSEA_API_KEY = "your_opensea_api_key_here"  # You'd need to get this
OPENSEA_BASE_URL = "https://api.opensea.io/api/v1"

# Alchemy API Configuration
ALCHEMY_BASE_URL = "https://eth-mainnet.g.alchemy.com/v2"
ALCHEMY_API_KEY = "mmhv6R8fVVR6JnSoHSHD3KRsAWxuTPJV"

# Cache configuration
CACHE_EXPIRY_HOURS = 24  # Cache NFT metadata for 24 hours
_metadata_cache: Dict[str, Dict] = {}
_collection_cache: Dict[str, Dict] = {}

def test_api_connectivity():
    """Test connectivity to different NFT APIs"""
    import requests
    
    apis_to_test = [
        ("NFTScan", f"{NFTSCAN_BASE_URL}/v2/collections/0x60e4d786628fea6478f785a6d7e704777c86a7c6"),
        ("OpenSea", f"{OPENSEA_BASE_URL}/assets/0x60e4d786628fea6478f785a6d7e704777c86a7c6/1"),
        ("Alchemy", f"{ALCHEMY_BASE_URL}/getNFTMetadata?contractAddress=0x60e4d786628fea6478f785a6d7e704777c86a7c6&tokenId=1")
    ]
    
    results = {}
    for name, url in apis_to_test:
        try:
            headers = {"Accept": "application/json"}
            if name == "NFTScan":
                headers["X-API-KEY"] = NFTSCAN_API_KEY
            
            response = requests.get(url, headers=headers, timeout=5)
            results[name] = {
                "status": "✅ Connected",
                "status_code": response.status_code,
                "error": None
            }
        except Exception as e:
            results[name] = {
                "status": "❌ Failed",
                "status_code": None,
                "error": str(e)
            }
    
    return results

def diagnose_network_issue():
    """Diagnose network connectivity issues"""
    import requests
    import socket
    
    diagnostics = {
        "dns_resolution": {},
        "basic_connectivity": {},
        "api_endpoints": {}
    }
    
    # Test DNS resolution
    domains = ["api.nftscan.com", "api.opensea.io", "eth-mainnet.g.alchemy.com"]
    for domain in domains:
        try:
            ip = socket.gethostbyname(domain)
            diagnostics["dns_resolution"][domain] = f"✅ Resolved to {ip}"
        except socket.gaierror as e:
            diagnostics["dns_resolution"][domain] = f"❌ DNS resolution failed: {e}"
    
    # Test basic HTTP connectivity
    test_urls = [
        ("Google", "https://www.google.com"),
        ("GitHub", "https://api.github.com"),
        ("NFTScan", "https://api.nftscan.com")
    ]
    
    for name, url in test_urls:
        try:
            response = requests.get(url, timeout=5)
            diagnostics["basic_connectivity"][name] = f"✅ HTTP {response.status_code}"
        except Exception as e:
            diagnostics["basic_connectivity"][name] = f"❌ Failed: {e}"
    
    return diagnostics

# === Helpers ===
def safe_decimal(val):
    try:
        return Decimal(str(val))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")

def safe_str(v):
    return "" if pd.isna(v) or v is None else str(v)

def format_eth_amount(amount):
    """Format ETH amounts with proper styling"""
    try:
        if pd.isna(amount) or amount is None:
            return "0.00 ETH"
        return f"{float(amount):.2f} ETH"
    except:
        return "0.00 ETH"

def format_contract_address(address):
    """Format contract addresses for better readability"""
    if not address or len(address) < 10:
        return address
    return f"{address[:6]}...{address[-4:]}"

def get_collection_name(contract_address):
    """Get human-readable collection names"""
    collection_map = {
        "0x60e4d786628fea6478f785a6d7e704777c86a7c6": "Bored Ape Yacht Club",
        "0x7d8820fa92eb1584636f64f219cb160d353d2a3e": "Doodles",
        "0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d": "Bored Ape Yacht Club",
        "0x8a90cab2b38dba80c64b7734e58ee1db38b8992e": "Doodles",
        "0x1a92f7381b9f03921564a437210bb9396471050c": "Cool Cats"
    }
    return collection_map.get(contract_address, "Unknown Collection")


def get_nft_metadata(contract_address, token_id):
    """Fetch NFT metadata from NFTScan API"""
    try:
        headers = {
            "X-API-KEY": NFTSCAN_API_KEY,
            "Accept": "application/json"
        }
        
        url = f"{NFTSCAN_BASE_URL}/v2/assets/{contract_address}/{token_id}"
        response = requests.get(url, headers=headers, timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            return data.get("data", {})
        else:
            print(f"NFTScan API error: {response.status_code}")
            return {}
            
    except requests.exceptions.ConnectionError as e:
        print(f"Connection error fetching NFT metadata: {e}")
        return {}
    except requests.exceptions.Timeout as e:
        print(f"Timeout error fetching NFT metadata: {e}")
        return {}
    except Exception as e:
        print(f"Error fetching NFT metadata: {e}")
        return {}

def get_nft_metadata_opensea(contract_address, token_id):
    """Fetch NFT metadata from OpenSea public endpoint (no API key required)"""
    try:
        headers = {
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        # Use OpenSea's public endpoint
        url = f"https://api.opensea.io/api/v1/asset/{contract_address}/{token_id}/"
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            return {
                "name": data.get("name", "Unknown"),
                "description": data.get("description", "No description available"),
                "image": data.get("image_url", ""),
                "collection": data.get("collection", {}).get("name", "Unknown Collection")
            }
        elif response.status_code == 403:
            print("OpenSea rate limited - trying alternative approach")
            return {}
        else:
            print(f"OpenSea API error: {response.status_code}")
            return {}
            
    except requests.exceptions.ConnectionError as e:
        print(f"Connection error fetching OpenSea metadata: {e}")
        return {}
    except requests.exceptions.Timeout as e:
        print(f"Timeout error fetching OpenSea metadata: {e}")
        return {}
    except Exception as e:
        print(f"Error fetching OpenSea metadata: {e}")
        return {}

def get_nft_image_from_ipfs(ipfs_hash):
    """Try to get NFT image from public IPFS gateways"""
    if not ipfs_hash or not ipfs_hash.startswith('ipfs://'):
        return None
    
    # Remove ipfs:// prefix
    hash_only = ipfs_hash.replace('ipfs://', '')
    
    # Try different public IPFS gateways
    gateways = [
        f"https://ipfs.io/ipfs/{hash_only}",
        f"https://gateway.pinata.cloud/ipfs/{hash_only}",
        f"https://cloudflare-ipfs.com/ipfs/{hash_only}",
        f"https://dweb.link/ipfs/{hash_only}"
    ]
    
    for gateway_url in gateways:
        try:
            response = requests.head(gateway_url, timeout=5)
            if response.status_code == 200:
                return gateway_url
        except:
            continue
    
    return None

def get_nft_metadata_public(contract_address, token_id):
    """Fetch NFT metadata using public blockchain data (no API keys)"""
    try:
        # Try to get basic info from blockchain
        # This is a simplified approach that doesn't require API keys
        return {
            "name": f"NFT #{token_id}",
            "description": f"NFT from contract {contract_address}",
            "image": "",  # Would need IPFS gateway or other public service
            "collection": "Unknown Collection"
        }
    except Exception as e:
        print(f"Error in public NFT metadata: {e}")
        return {}

@lru_cache(maxsize=1000)
def get_nft_metadata_alchemy(contract_address: str, token_id: str) -> Dict:
    """Fetch NFT metadata from Alchemy API with caching"""
    cache_key = f"{contract_address.lower()}_{token_id}"
    
    # Check memory cache first
    if cache_key in _metadata_cache:
        cached_data = _metadata_cache[cache_key]
        # Check if cache is still valid (within expiry time)
        if time.time() - cached_data.get('timestamp', 0) < (CACHE_EXPIRY_HOURS * 3600):
            print(f"Using cached metadata for {contract_address}:{token_id}")
            return cached_data.get('data', {})
        else:
            # Cache expired, remove it
            del _metadata_cache[cache_key]
    
    try:
        headers = {
            "Accept": "application/json"
        }
        
        # Use Alchemy's API with your key
        url = f"{ALCHEMY_BASE_URL}/{ALCHEMY_API_KEY}/getNFTMetadata?contractAddress={contract_address}&tokenId={token_id}"
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            
            # Extract and normalize the metadata
            metadata = {
                "name": data.get("title", f"NFT #{token_id}"),
                "description": data.get("description", "No description available"),
                "image": "",
                "collection": data.get("contract", {}).get("name", "Unknown Collection"),
                "contract_address": contract_address.lower(),
                "token_id": str(token_id),
                "raw_data": data  # Store raw data for debugging
            }
            
            # Handle image URL from Alchemy's media array
            if data.get("media"):
                for media_item in data["media"]:
                    if media_item.get("gateway"):
                        metadata["image"] = media_item["gateway"]
                        break
                    elif media_item.get("raw"):
                        # Try to convert IPFS URLs to gateway URLs
                        raw_url = media_item["raw"]
                        if raw_url.startswith("ipfs://"):
                            ipfs_hash = raw_url.replace("ipfs://", "")
                            metadata["image"] = f"https://ipfs.io/ipfs/{ipfs_hash}"
                        else:
                            metadata["image"] = raw_url
                        break
            
            # Cache the result with timestamp
            _metadata_cache[cache_key] = {
                'data': metadata,
                'timestamp': time.time()
            }
            
            print(f"Fetched and cached metadata for {contract_address}:{token_id}")
            return metadata
        else:
            print(f"Alchemy API error: {response.status_code} for {contract_address}:{token_id}")
            return {}
            
    except requests.exceptions.ConnectionError as e:
        print(f"Connection error fetching Alchemy metadata: {e}")
        return {}
    except requests.exceptions.Timeout as e:
        print(f"Timeout error fetching Alchemy metadata: {e}")
        return {}
    except Exception as e:
        print(f"Error fetching Alchemy metadata: {e}")
        return {}



def get_nft_metadata_with_fallback(contract_address: str, token_id: str) -> Dict:
    """Get NFT metadata using only Alchemy API with caching"""
    # Normalize inputs
    contract_address = contract_address.lower().strip()
    token_id = str(token_id).strip()
    
    if not contract_address or not token_id:
        return get_nft_metadata_public(contract_address, token_id)
    
    # Use only Alchemy API as requested
    metadata = get_nft_metadata_alchemy(contract_address, token_id)
    
    if metadata and metadata.get("name"):
        return metadata
    
    # Fallback to basic info if Alchemy fails
    print(f"Alchemy API failed for {contract_address}:{token_id}, using basic info...")
    return get_nft_metadata_public(contract_address, token_id)

@lru_cache(maxsize=500)
def get_nft_collection_info(contract_address: str) -> Dict:
    """Fetch NFT collection information using Alchemy API with caching"""
    contract_address = contract_address.lower().strip()
    cache_key = f"collection_{contract_address}"
    
    # Check memory cache first
    if cache_key in _collection_cache:
        cached_data = _collection_cache[cache_key]
        # Check if cache is still valid (collections change less frequently, cache longer)
        if time.time() - cached_data.get('timestamp', 0) < (CACHE_EXPIRY_HOURS * 3600 * 7):  # 7 days
            print(f"Using cached collection info for {contract_address}")
            return cached_data.get('data', {})
        else:
            del _collection_cache[cache_key]
    
    try:
        headers = {
            "Accept": "application/json"
        }
        
        # Use Alchemy's contract metadata endpoint
        url = f"{ALCHEMY_BASE_URL}/{ALCHEMY_API_KEY}/getContractMetadata?contractAddress={contract_address}"
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            
            collection_info = {
                "name": data.get("name", "Unknown Collection"),
                "symbol": data.get("symbol", ""),
                "total_supply": data.get("totalSupply", ""),
                "contract_type": data.get("contractMetadata", {}).get("tokenType", "ERC721"),
                "contract_address": contract_address
            }
            
            # Cache the result
            _collection_cache[cache_key] = {
                'data': collection_info,
                'timestamp': time.time()
            }
            
            print(f"Fetched and cached collection info for {contract_address}")
            return collection_info
        else:
            print(f"Alchemy collection API error: {response.status_code} for {contract_address}")
            return {"name": "Unknown Collection"}
            
    except requests.exceptions.ConnectionError as e:
        print(f"Connection error fetching collection info: {e}")
        return {"name": "Unknown Collection"}
    except requests.exceptions.Timeout as e:
        print(f"Timeout error fetching collection info: {e}")
        return {"name": "Unknown Collection"}
    except Exception as e:
        print(f"Error fetching collection info: {e}")
        return {"name": "Unknown Collection"}

# === UI ===
def nft_ui():
    return ui.page_fluid(
        ui.h2("🖼️ NFT Collateral", class_="mt-3"),
        ui.layout_sidebar(
            ui.sidebar(
                ui.input_selectize("nft_contract_filter", "Filter by Contract:", ["All Contracts"], options={"create": False}),
                ui.input_selectize("nft_collection_filter", "Filter by Collection:", ["All Collections"], options={"create": False}),
                ui.input_numeric("nft_token_id", "Token ID:", value=None, min=0),
                ui.input_action_button("nft_refresh", "Refresh NFT Data", class_="btn-primary")
            ),
            ui.card(
                ui.card_header("NFT Collateral Summary"),
                ui.output_ui("nft_summary"),
            ),
            ui.card(
                ui.card_header("NFT Collateral Table"),
                ui.output_data_frame("nft_table"),
                full_screen=True,
            ),
            ui.card(
                ui.card_header("Selected NFT Details"),
                ui.output_ui("nft_details"),
            ),
            ui.card(
                ui.card_header("NFT Image Preview"),
                ui.output_ui("nft_image"),
            ),
        )
    )

# === Server ===
def register_nft_outputs(output: Outputs, input: Inputs, session: Session, selected_fund):
    selected_nft_store = reactive.Value(None)
    nft_data_store = reactive.Value(pd.DataFrame())

    @reactive.calc
    def raw_nft_data():
        """Get NFT collateral data from loan portfolio"""
        try:
            # This would typically come from your loan data
            # For now, we'll create sample data
            sample_data = {
                "loan_id": ["LOAN001", "LOAN002", "LOAN003"],
                "collateral_address": [
                    "0x60e4d786628fea6478f785a6d7e704777c86a7c6",  # BAYC
                    "0x7d8820fa92eb1584636f64f219cb160d353d2a3e",  # Doodles
                    "0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d"   # BAYC
                ],
                "token_id": ["1234", "5678", "9999"],
                "loan_amount": [10.5, 5.2, 15.0],
                "status": ["Active", "Active", "Repaid"]
            }
            
            df = pd.DataFrame(sample_data)
            return df
            
        except Exception as e:
            print(f"Error loading NFT data: {e}")
            return pd.DataFrame()

    @reactive.effect
    def update_nft_filters():
        """Update filter choices based on available data"""
        try:
            df = raw_nft_data()
            if df.empty:
                return
            
            # Update contract filter
            contracts = ["All Contracts"] + sorted(df["collateral_address"].dropna().unique())
            ui.update_selectize("nft_contract_filter", choices=contracts)
            
            # Update collection filter (you could fetch collection names from API)
            collections = ["All Collections"] + ["Bored Ape Yacht Club", "Doodles", "Other"]
            ui.update_selectize("nft_collection_filter", choices=collections)
            
        except Exception as e:
            print(f"Error updating NFT filters: {e}")

    @reactive.calc
    def filtered_nft_data():
        """Filter NFT data based on user selections"""
        try:
            df = raw_nft_data()
            contract_filter = input.nft_contract_filter()
            collection_filter = input.nft_collection_filter()
            token_id_filter = input.nft_token_id()
            
            if contract_filter and contract_filter != "All Contracts":
                df = df[df["collateral_address"] == contract_filter]
                
            if collection_filter and collection_filter != "All Collections":
                # This would need to be implemented based on your collection mapping
                pass
                
            if token_id_filter:
                df = df[df["token_id"] == str(token_id_filter)]
                
            return df
            
        except Exception as e:
            print(f"Error filtering NFT data: {e}")
            return pd.DataFrame()

    @output
    @render.data_frame
    def nft_table():
        """Display NFT collateral table"""
        df = filtered_nft_data()
        if df.empty:
            return pd.DataFrame({"Message": ["No NFT collateral data available"]})
        
        # Add collection names for display
        df_display = df.copy()
        df_display["collection"] = df_display["collateral_address"].map({
            "0x60e4d786628fea6478f785a6d7e704777c86a7c6": "Bored Ape Yacht Club",
            "0x7d8820fa92eb1584636f64f219cb160d353d2a3e": "Doodles",
            "0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d": "Bored Ape Yacht Club"
        }).fillna("Unknown")
        
        return DataGrid(df_display, selection_mode="row")

    @reactive.effect
    def capture_selected_nft():
        """Capture selected NFT row"""
        selection = nft_table.cell_selection()
        df = filtered_nft_data()
        
        if not selection or "rows" not in selection or not selection["rows"]:
            selected_nft_store.set(None)
            return
        
        row_idx = selection["rows"][0]
        if df is None or row_idx >= len(df):
            selected_nft_store.set(None)
            return
        
        row = df.iloc[row_idx].to_dict()
        selected_nft_store.set(row)

    @output
    @render.ui
    def nft_summary():
        """Display NFT collateral summary"""
        df = raw_nft_data()
        if df.empty:
            return ui.p("No NFT collateral data available.")
        
        total_nfts = len(df)
        total_value = df["loan_amount"].sum()
        active_loans = len(df[df["status"] == "Active"])
        
        return ui.div(
            ui.tags.p(f"Total NFT Collateral: {total_nfts}"),
            ui.tags.p(f"Total Collateral Value: {total_value:.2f} ETH"),
            ui.tags.p(f"Active Loans: {active_loans}")
        )

    @output
    @render.ui
    def nft_details():
        """Display detailed NFT information"""
        nft = selected_nft_store.get()
        if not nft:
            return ui.p("⚠️ Select an NFT row to view details.")
        
        try:
            # Fetch NFT metadata
            contract_address = nft.get("collateral_address", "")
            token_id = nft.get("token_id", "")
            
            if not contract_address or not token_id:
                return ui.p("⚠️ Invalid NFT data.")
            
            metadata = get_nft_metadata_with_fallback(contract_address, token_id)
            collection_info = get_nft_collection_info(contract_address)
            
            # Use fallback data if API fails
            if not metadata:
                metadata = get_fallback_nft_data(contract_address, token_id)
                elements = []
                elements.append(ui.tags.div(
                    ui.tags.span("⚠️ API Unavailable", class_="badge bg-warning text-dark me-2"),
                    ui.tags.span("Using cached data", class_="text-muted small")
                ))
            else:
                elements = []
            
            # Basic loan info
            elements.append(ui.tags.h4("Loan Information"))
            elements.append(ui.tags.p(f"Loan ID: {safe_str(nft.get('loan_id'))}"))
            elements.append(ui.tags.p(f"Loan Amount: {safe_str(nft.get('loan_amount'))} ETH"))
            elements.append(ui.tags.p(f"Status: {safe_str(nft.get('status'))}"))
            
            # NFT info
            elements.append(ui.tags.h4("NFT Information"))
            elements.append(ui.tags.p(f"Contract: {contract_address}"))
            elements.append(ui.tags.p(f"Token ID: {token_id}"))
            
            if metadata:
                name = metadata.get("name", "Unknown")
                description = metadata.get("description", "No description available")
                elements.append(ui.tags.p(f"Name: {name}"))
                elements.append(ui.tags.p(f"Description: {description[:200]}..."))
            
            if collection_info:
                collection_name = collection_info.get("name", "Unknown Collection")
                elements.append(ui.tags.p(f"Collection: {collection_name}"))
            
            # External links
            elements.append(ui.tags.h4("External Links"))
            nftscan_url = f"https://nftscan.com/{contract_address}/{token_id}"
            opensea_url = f"https://opensea.io/assets/{contract_address}/{token_id}"
            
            elements.append(ui.tags.p(
                ui.tags.a("View on NFTScan", href=nftscan_url, target="_blank", class_="btn btn-sm btn-outline-primary me-2"),
                ui.tags.a("View on OpenSea", href=opensea_url, target="_blank", class_="btn btn-sm btn-outline-secondary")
            ))
            
            return ui.div(*elements)
            
        except Exception as e:
            print(f"Error displaying NFT details: {e}")
            return ui.p(f"Error loading NFT details: {e}")

    @output
    @render.ui
    def nft_image():
        """Display NFT image preview"""
        nft = selected_nft_store.get()
        if not nft:
            return ui.p("⚠️ Select an NFT row to view image.")
        
        try:
            contract_address = nft.get("collateral_address", "")
            token_id = nft.get("token_id", "")
            
            if not contract_address or not token_id:
                return ui.p("⚠️ Invalid NFT data.")
            
            metadata = get_nft_metadata_with_fallback(contract_address, token_id)
            
            if metadata and "image" in metadata:
                image_url = metadata["image"]
                return ui.div(
                    ui.tags.img(
                        src=image_url,
                        alt=f"NFT {token_id}",
                        style="max-width: 100%; height: auto; border-radius: 8px;",
                        class_="img-fluid"
                    ),
                    ui.tags.p(f"Token ID: {token_id}", class_="text-center mt-2")
                )
            else:
                return ui.div(
                    ui.tags.div(
                        "🖼️",
                        style="font-size: 4rem; text-align: center; color: #ccc;",
                        class_="d-flex align-items-center justify-content-center"
                    ),
                    ui.tags.p("Image not available", class_="text-center text-muted")
                )
                
        except Exception as e:
            print(f"Error displaying NFT image: {e}")
            return ui.p(f"Error loading NFT image: {e}")

    @reactive.effect
    def refresh_nft_data():
        """Refresh NFT data when button is clicked"""
        input.nft_refresh()
        # Clear the caches to force fresh data
        _metadata_cache.clear()
        _collection_cache.clear()
        # Clear LRU caches
        get_nft_metadata_alchemy.cache_clear()
        get_nft_collection_info.cache_clear()
        print("NFT data refresh requested - caches cleared")

def clear_nft_cache():
    """Utility function to clear all NFT caches"""
    global _metadata_cache, _collection_cache
    _metadata_cache.clear()
    _collection_cache.clear()
    get_nft_metadata_alchemy.cache_clear()
    get_nft_collection_info.cache_clear()
    print("All NFT caches cleared")

def get_cache_stats():
    """Get statistics about the current cache state"""
    return {
        "metadata_cache_size": len(_metadata_cache),
        "collection_cache_size": len(_collection_cache),
        "lru_metadata_info": get_nft_metadata_alchemy.cache_info(),
        "lru_collection_info": get_nft_collection_info.cache_info()
    }
