"""
PCAP (Partner Capital Account Projections) Module - Simplified Excel-based Version

This module loads PCAP Excel files from S3 and generates PDF statements.
All complex calculations have been moved to external Excel generation.
"""

# Import the new Excel-based PCAP processor
from .pcap_excel_loader import (
    PCAPExcelProcessor,
    load_and_parse_pcap_excel,
    generate_pcap_pdf
)

# Keep some utility functions for compatibility
from .excess import (
    ensure_timezone_aware,
    normalize_to_eod_utc,
)

# Export the simplified API
__all__ = [
    # Main PCAP processor class
    'PCAPExcelProcessor',
    
    # Convenience functions
    'load_and_parse_pcap_excel',
    'generate_pcap_pdf',
    
    # Utility functions
    'ensure_timezone_aware',
    'normalize_to_eod_utc',
]