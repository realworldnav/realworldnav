"""
General Ledger 2 Module

A fresh implementation of the General Ledger with full accounting functionality:
- Journal Entries viewing and filtering
- Account-based ledger views with running balances
- Trial Balance generation
- Manual journal entry creation connected to Chart of Accounts
"""

from .ui import general_ledger_v2_ui
from .outputs import register_gl2_outputs

__all__ = ['general_ledger_v2_ui', 'register_gl2_outputs']
