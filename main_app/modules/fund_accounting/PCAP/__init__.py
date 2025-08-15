"""
PCAP (Partner Capital Account Projections) Module

This module contains the core business logic for calculating partner capital accounts,
profit/loss allocations, NAV calculations, and waterfall distributions for investment funds.
"""

# Import only the functions that are actually used in the application
from .pcap import (
    # Core PCAP functions - used in fund_accounting.py
    create_complete_fund_pcap_with_gp,
    run_partner_capital_pcap_allocation,
    
    # PDF generation functions - used in fund_accounting.py
    generate_all_lp_statements_pdf,
    generate_investor_statement_pdf,
    
    # Other potentially used functions (keeping for safety)
    SimplifiedCapitalTiming,
    process_capital_with_partner_accounting,
    validate_fund_allocation_timing_only,
    create_pnl_breakdown_columns,
    all_partners_daily_accounts_decimal_USD,
    allocate_daily_pl_to_partners_USD_with_prior_nav,
    allocate_daily_nav_USD,
    all_partners_daily_accounts_decimal_crypto,
    allocate_daily_pl_to_partners_crypto_with_prior_nav,
    allocate_daily_nav_crypto,
    compute_waterfall_for_lp,
    compute_waterfall_audit,
    generate_gp_incentive_journal_entries,
    get_lp_terms,
    process_gp_incentive_audit_details,
    get_lp_net_irr_from_audit,
    create_combined_pcap_excel
)

from .excess import (
    # Utility functions - used in fund_accounting.py and pcap.py
    ensure_timezone_aware,
    
    # Other utility functions (keeping for pcap.py dependencies)
    normalize_to_eod_utc,
    localize_date_columns_to_utc,
    safe_decimal,
    safe_decimal_mul,
    safe_pct_change,
    get_account_classification,
    get_normal_balance,
    format_accounting_amount,
    validate_account_balance_nature,
    xnpv,
    xirr,
    compound_forward,
    save_cash_flow_waterfall_image_from_row,
    build_lp_pdf_report_clean
)

# Export only the functions that are actively used
__all__ = [
    # Core PCAP functions
    'create_complete_fund_pcap_with_gp',
    'run_partner_capital_pcap_allocation',
    'generate_all_lp_statements_pdf', 
    'generate_investor_statement_pdf',
    
    # Utility functions
    'ensure_timezone_aware',
    
    # Additional functions (keeping for internal module use)
    'SimplifiedCapitalTiming',
    'process_capital_with_partner_accounting',
    'validate_fund_allocation_timing_only',
    'create_pnl_breakdown_columns',
    'all_partners_daily_accounts_decimal_USD',
    'allocate_daily_pl_to_partners_USD_with_prior_nav',
    'allocate_daily_nav_USD',
    'all_partners_daily_accounts_decimal_crypto',
    'allocate_daily_pl_to_partners_crypto_with_prior_nav',
    'allocate_daily_nav_crypto',
    'compute_waterfall_for_lp',
    'compute_waterfall_audit',
    'generate_gp_incentive_journal_entries',
    'get_lp_terms',
    'process_gp_incentive_audit_details',
    'get_lp_net_irr_from_audit',
    'create_combined_pcap_excel',
    'normalize_to_eod_utc',
    'localize_date_columns_to_utc',
    'safe_decimal',
    'safe_decimal_mul',
    'safe_pct_change',
    'get_account_classification',
    'get_normal_balance',
    'format_accounting_amount',
    'validate_account_balance_nature',
    'xnpv',
    'xirr',
    'compound_forward',
    'save_cash_flow_waterfall_image_from_row',
    'build_lp_pdf_report_clean'
]