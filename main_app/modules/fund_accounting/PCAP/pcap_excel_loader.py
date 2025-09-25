"""
Simplified PCAP Module - Excel Loading and PDF Generation
This module loads PCAP Excel files from S3 and generates PDF statements
"""

import pandas as pd
import numpy as np
from datetime import datetime
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from jinja2 import Environment, FileSystemLoader
try:
    from weasyprint import HTML
    HAS_WEASYPRINT = True
except (ImportError, OSError):
    # OSError occurs when WeasyPrint can't find system libraries (pango, cairo, etc.)
    HAS_WEASYPRINT = False
    HTML = None
import tempfile
import shutil

# Import S3 utilities
from ....s3_utils import (
    list_pcap_excel_files,
    load_pcap_excel_file,
    parse_pcap_excel_to_json
)

class PCAPExcelProcessor:
    """Processes PCAP Excel files and generates reports"""
    
    def __init__(self):
        self.excel_data = None
        self.current_file = None
        self.available_files = []
        self.available_lps = []
        
        # Fund name lookup table
        self.fund_name_lookup = {
            "fund_i_class_B_ETH": "ETH Lending Fund I, LP",
            "fund_ii_class_B_ETH": "ETH Lending Fund II, LP",
            "holdings_class_B_ETH": "Drip Capital Holdings, LLC"
        }
        
        # LP display name lookup table
        self.lp_display_name_lookup = {
            "fund_i_class_B_ETH": {
                "1": "Fund I Limited Partner",  # Default for Fund I
                "00001": "Fund I Limited Partner"
            },
            "fund_ii_class_B_ETH": {
                "2": "Artha Investment Partners",
                "3": "Mohak Agarwal",
                "00002": "Artha Investment Partners",
                "00003": "Mohak Agarwal"
            },
            "holdings_class_B_ETH": {
                "1": "ETH Lending Fund I LP",  # Holdings_00001_fund_i_class_B_ETH
                "00001": "ETH Lending Fund I LP",  # Holdings_00001_fund_i_class_B_ETH
                "2": "ETH Lending Fund II LP",  # Holdings_00002_fund_ii_class_B_ETH
                "00002": "ETH Lending Fund II LP",  # Holdings_00002_fund_ii_class_B_ETH
                "2001": "ETH Lending Fund I LP",  # Reporting ID 2001
                "2002": "ETH Lending Fund II LP"  # Reporting ID 2002
            }
        }
    
    def get_fund_name_from_lp(self, lp_id: str) -> str:
        """Extract fund name from LP ID with priority-based detection"""
        # First priority: Use current file context
        if self.current_file and self.current_file.get('fund_id'):
            fund_id = self.current_file['fund_id']
            if fund_id in self.fund_name_lookup:
                print(f"Using fund from file context: {fund_id}")
                return self.fund_name_lookup[fund_id]
        
        # Second priority: Check for specific fund patterns in order of specificity
        lp_id_lower = lp_id.lower()
        
        # Check for holdings first (most specific)
        if 'holdings' in lp_id_lower:
            print(f"Detected Holdings fund from LP: {lp_id}")
            return self.fund_name_lookup['holdings_class_B_ETH']
        
        # Check for fund_ii before fund_i (more specific first)
        if 'fund_ii' in lp_id_lower or 'fund_2' in lp_id_lower:
            print(f"Detected Fund II from LP: {lp_id}")
            return self.fund_name_lookup['fund_ii_class_B_ETH']
        
        if 'fund_i' in lp_id_lower or 'fund_1' in lp_id_lower:
            print(f"Detected Fund I from LP: {lp_id}")
            return self.fund_name_lookup['fund_i_class_B_ETH']
        
        # Third priority: Try exact substring match
        for fund_id in self.fund_name_lookup.keys():
            if fund_id in lp_id:
                print(f"Found exact fund match: {fund_id}")
                return self.fund_name_lookup[fund_id]
        
        # Default fallback
        print(f"No fund detected for LP {lp_id}, using default")
        return "ETH Lending Fund, LP"
    
    def get_lp_display_name(self, lp_id: str) -> str:
        """Get display name for an LP with improved fund detection"""
        # Extract fund ID and partner number from LP ID
        # LP ID format: LP_00001_fund_i_class_B_ETH or Holdings_00002_fund_ii_class_B_
        
        # Determine which fund this LP belongs to using same priority logic
        fund_id = None
        lp_id_lower = lp_id.lower()
        
        # First priority: Use current file context
        if self.current_file and self.current_file.get('fund_id'):
            fund_id = self.current_file['fund_id']
            print(f"Using fund from file context for display name: {fund_id}")
        
        # Second priority: Check for specific patterns
        if not fund_id:
            if 'holdings' in lp_id_lower:
                fund_id = 'holdings_class_B_ETH'
            elif 'fund_ii' in lp_id_lower or 'fund_2' in lp_id_lower:
                fund_id = 'fund_ii_class_B_ETH'
            elif 'fund_i' in lp_id_lower or 'fund_1' in lp_id_lower:
                fund_id = 'fund_i_class_B_ETH'
            else:
                # Try exact match
                for fid in self.fund_name_lookup.keys():
                    if fid in lp_id:
                        fund_id = fid
                        break
        
        if fund_id and fund_id in self.lp_display_name_lookup:
            # Extract partner number from LP ID
            # Look for patterns like LP_00001, Holdings_00002, etc.
            import re
            
            # Try to extract the number part (more flexible pattern)
            # First check for reporting IDs (2001, 2002, etc)
            if lp_id.startswith('20'):
                number_match = re.match(r'^(\d+)$', lp_id)
            else:
                number_match = re.search(r'(?:LP|Holdings|Partner)[_-]?(\d+)', lp_id, re.IGNORECASE)
                if not number_match:
                    # Try just finding any sequence of digits
                    number_match = re.search(r'(\d+)', lp_id)
            
            if number_match:
                partner_num = number_match.group(1).lstrip('0')  # Remove leading zeros
                full_partner_num = number_match.group(1)  # Keep with zeros
                
                # Check if this partner has a specific name
                fund_partners = self.lp_display_name_lookup[fund_id]
                
                # Try different variations of the partner number
                if partner_num in fund_partners:
                    return fund_partners[partner_num]
                elif full_partner_num in fund_partners:
                    return fund_partners[full_partner_num]
                elif "1" in fund_partners:  # Default for fund
                    return fund_partners["1"]
        
        # If no specific name found, return the LP ID as is
        return lp_id
        
    def get_available_pcap_files(self) -> List[Dict]:
        """Get list of available PCAP files from S3"""
        self.available_files = list_pcap_excel_files()
        return self.available_files
    
    def load_pcap_file(self, date: str = None, fund_id: str = None, key: str = None) -> bool:
        """
        Load a PCAP Excel file from S3
        
        Args:
            date: Date in YYYYMMDD format
            fund_id: Fund identifier
            key: Direct S3 key to file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.excel_data = load_pcap_excel_file(key=key, date=date, fund_id=fund_id)
            
            if self.excel_data:
                self.current_file = {
                    'date': date,
                    'fund_id': fund_id,
                    'key': key
                }
                # Extract available LPs from sheet names
                self.available_lps = self._extract_lp_list()
                print(f"Successfully loaded PCAP file with {len(self.excel_data)} sheets")
                return True
            else:
                print("Failed to load PCAP file")
                return False
                
        except Exception as e:
            print(f"Error loading PCAP file: {e}")
            return False
    
    def _extract_lp_list(self) -> List[str]:
        """Extract list of LPs from All_LPs_Combined sheet"""
        if not self.excel_data:
            return []
        
        lp_list = []
        
        # Look for the All_LPs_Combined sheet
        combined_sheet = None
        for sheet_name in ['All_LPs_Combined', 'All LPs Combined', 'All_Partners', 'All Partners']:
            if sheet_name in self.excel_data:
                combined_sheet = self.excel_data[sheet_name]
                print(f"Found combined sheet: {sheet_name}")
                break
        
        if combined_sheet is not None:
            # Look for limited_partner_ID column
            if 'limited_partner_ID' in combined_sheet.columns:
                # Extract unique LP IDs
                lp_ids = combined_sheet['limited_partner_ID'].dropna().unique()
                print(f"Found {len(lp_ids)} unique LPs in limited_partner_ID column")
                
                # Only add LPs that have corresponding sheets
                for lp_id in lp_ids:
                    # Check if this LP has a sheet
                    if lp_id in self.excel_data:
                        lp_list.append(lp_id)
                    # Also check with different naming conventions
                    elif f"LP_{lp_id}" in self.excel_data:
                        lp_list.append(f"LP_{lp_id}")
                    elif lp_id.replace('_', '-') in self.excel_data:
                        lp_list.append(lp_id.replace('_', '-'))
            else:
                # If no limited_partner_ID column, look for LP patterns in first column
                print("No limited_partner_ID column found, checking first column for LP patterns")
                first_col = combined_sheet.iloc[:, 0] if len(combined_sheet.columns) > 0 else pd.Series()
                
                for val in first_col.dropna().unique():
                    val_str = str(val)
                    # Look for LP patterns
                    if 'LP_' in val_str or val_str.startswith('LP'):
                        # Check if this LP has a sheet
                        if val_str in self.excel_data:
                            lp_list.append(val_str)
        
        # If we couldn't find LPs from All_LPs_Combined, fall back to sheet names
        if not lp_list:
            print("Falling back to sheet name extraction")
            for sheet_name in self.excel_data.keys():
                # Look for LP-specific sheets
                if any(pattern in sheet_name for pattern in ['LP_', 'LP-', '_fund_']):
                    # Skip summary sheets
                    if sheet_name.lower() not in ['summary', 'all_lps_combined', 'general_partner', 'gp_mgmt_fees']:
                        lp_list.append(sheet_name)
        
        print(f"Final LP list: {lp_list}")
        return sorted(lp_list)
    
    def get_lp_data(self, lp_id: str) -> Optional[pd.DataFrame]:
        """Get data for a specific LP"""
        if not self.excel_data:
            return None
        
        # Check if LP has a dedicated sheet
        if lp_id in self.excel_data:
            return self.excel_data[lp_id]
        
        # Otherwise, try to filter from combined sheet
        for sheet_name in ['All_LPs_Combined', 'All LPs Combined', 'All Partners', 'Summary']:
            if sheet_name in self.excel_data:
                df = self.excel_data[sheet_name]
                
                # Try to filter by limited_partner_ID column first
                if 'limited_partner_ID' in df.columns:
                    lp_data = df[df['limited_partner_ID'] == lp_id]
                    if not lp_data.empty:
                        return lp_data
                
                # Try other column names
                for col_name in ['LP', 'Partner', 'LP_ID', 'Partner_ID']:
                    if col_name in df.columns:
                        lp_data = df[df[col_name] == lp_id]
                        if not lp_data.empty:
                            return lp_data
        
        return None
    
    def parse_excel_to_json(self, lp_id: str = None) -> Dict:
        """
        Parse Excel data to JSON format for PDF generation
        
        Args:
            lp_id: Optional LP identifier to filter data
            
        Returns:
            JSON structure formatted for PDF template
        """
        if not self.excel_data:
            return None
        
        try:
            # Get the appropriate sheet
            if lp_id and lp_id in self.excel_data:
                df = self.excel_data[lp_id]
            else:
                # Try to find the main summary sheet
                df = None
                for sheet_name in ['Summary', 'All Partners', 'PCAP', 'Fund Summary']:
                    if sheet_name in self.excel_data:
                        df = self.excel_data[sheet_name]
                        break
                
                if df is None:
                    # Use first sheet if no standard sheet found
                    df = self.excel_data[list(self.excel_data.keys())[0]]
            
            # Extract date from filename or data
            date_str = self._extract_date_string()
            
            # Parse the DataFrame to extract statement data
            statement_of_changes = self._parse_statement_of_changes(df, lp_id)
            commitment_summary = self._parse_commitment_summary(df, lp_id)
            performance_metrics = self._parse_performance_metrics(df, lp_id)
            
            # Get display name for the LP
            lp_display_name = self.get_lp_display_name(lp_id) if lp_id else 'All Partners'
            
            # Build JSON structure
            json_data = {
                'main_date': date_str,
                'currency': 'ETH',
                'lp_name': lp_display_name,  # Use display name instead of LP ID
                'statement_of_changes': statement_of_changes,
                'commitment_summary': commitment_summary,
                'performance_metrics': performance_metrics
            }
            
            return json_data
            
        except Exception as e:
            print(f"Error parsing Excel to JSON: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _extract_date_string(self) -> str:
        """Extract formatted date from filename or Excel data"""
        # First try to get from filename
        if self.current_file and self.current_file['date']:
            try:
                date_obj = pd.to_datetime(self.current_file['date'], format='%Y%m%d')
                return date_obj.strftime('%B %d, %Y')
            except:
                pass
        
        # Try to extract from Excel data - look for a date cell
        if self.excel_data:
            for sheet_name, df in self.excel_data.items():
                # Look for date patterns in the first few rows and columns
                for row_idx in range(min(5, len(df))):
                    for col_idx in range(min(3, len(df.columns))):
                        try:
                            cell_val = df.iloc[row_idx, col_idx]
                            if pd.notna(cell_val):
                                # Check if it's already a datetime
                                if pd.api.types.is_datetime64_any_dtype(type(cell_val)):
                                    return pd.to_datetime(cell_val).strftime('%B %d, %Y')
                                # Check if it looks like a date string
                                cell_str = str(cell_val)
                                if any(month in cell_str for month in ['January', 'February', 'March', 'April', 'May', 'June', 
                                                                        'July', 'August', 'September', 'October', 'November', 'December']):
                                    return cell_str
                                # Try parsing as date
                                if '/' in cell_str or '-' in cell_str:
                                    parsed_date = pd.to_datetime(cell_str, errors='coerce')
                                    if pd.notna(parsed_date):
                                        return parsed_date.strftime('%B %d, %Y')
                        except:
                            continue
        
        return datetime.now().strftime('%B %d, %Y')
    
    def _parse_statement_of_changes(self, df: pd.DataFrame, lp_id: str = None) -> List[Dict]:
        """Parse statement of changes from DataFrame"""
        statement_items = []
        
        print(f"Parsing statement of changes from DataFrame with columns: {list(df.columns)}")
        print(f"DataFrame shape: {df.shape}")
        print(f"First few rows:\n{df.head()}")
        
        # Check if DataFrame has column headers or if we need to use first row
        # Common column patterns in PCAP Excel files
        period_columns = ['MTD', 'QTD', 'YTD', 'ITD', 'Month to Date', 'Quarter to Date', 'Year to Date', 'Inception to Date']
        
        # Find which columns contain the period data
        mtd_col = None
        qtd_col = None
        ytd_col = None
        itd_col = None
        
        for col in df.columns:
            col_upper = str(col).upper()
            if 'MTD' in col_upper or 'MONTH' in col_upper:
                mtd_col = col
            elif 'QTD' in col_upper or 'QUARTER' in col_upper:
                qtd_col = col
            elif 'YTD' in col_upper or 'YEAR' in col_upper:
                ytd_col = col
            elif 'ITD' in col_upper or 'INCEPTION' in col_upper or 'CUMULATIVE' in col_upper:
                itd_col = col
        
        # If no period columns found in headers, check if first row contains headers
        if not any([mtd_col, qtd_col, ytd_col, itd_col]) and len(df) > 0:
            first_row = df.iloc[0]
            for idx, val in enumerate(first_row):
                val_upper = str(val).upper()
                if 'MTD' in val_upper or 'MONTH' in val_upper:
                    mtd_col = df.columns[idx]
                elif 'QTD' in val_upper or 'QUARTER' in val_upper:
                    qtd_col = df.columns[idx]
                elif 'YTD' in val_upper or 'YEAR' in val_upper:
                    ytd_col = df.columns[idx]
                elif 'ITD' in val_upper or 'INCEPTION' in val_upper:
                    itd_col = df.columns[idx]
        
        # Define the expected line items with possible variations
        line_items_map = {
            'Beginning Balance': ['beginning balance', 'opening balance', 'starting balance', 'beginning_balance', 'beg_bal'],
            'Capital contributions': ['capital contribution', 'contribution', 'capital_contribution', 'cap_contrib'],
            'Management fees': ['management fee', 'mgmt fee', 'management_fee', 'mgmt_fee'],
            'Interest expense': ['interest expense', 'interest_expense'],
            'Capital distributions': ['capital distribution', 'distribution', 'capital_distribution', 'cap_dist'],
            'Other income': ['other income', 'other_income'],
            'Operating expenses': ['operating expense', 'operating_expense', 'op_expense'],
            'Interest income': ['interest income', 'interest_income'],
            'Provision for bad debt': ['provision for bad debt', 'bad debt', 'provision_bad_debt'],
            'Income allocated from investments': ['income allocated', 'investment income', 'income_from_investments'],
            'Realized gain (loss)': ['realized gain', 'realized loss', 'realized_gain_loss'],
            'Change in unrealized gain (loss)': ['unrealized gain', 'unrealized loss', 'unrealized_gain_loss', 'change_unrealized'],
            'Ending Capital': ['ending capital', 'ending balance', 'closing balance', 'ending_balance', 'end_bal']
        }
        
        # Process each expected line item
        for label, search_terms in line_items_map.items():
            row_data = {'label': label, 'mtd': 0.0, 'qtd': 0.0, 'ytd': 0.0, 'itd': 0.0}
            
            # Search for the item in the DataFrame
            found = False
            for idx, row in df.iterrows():
                # Get the description (usually first column)
                desc = str(row.iloc[0]).lower() if len(row) > 0 else ''
                
                # Check if this row matches any of our search terms
                if any(term in desc for term in search_terms):
                    found = True
                    print(f"Found '{label}' at row {idx}: {desc}")
                    
                    # Extract values from identified columns
                    try:
                        if mtd_col and mtd_col in df.columns:
                            val = row[mtd_col]
                            if pd.notna(val) and val != '-':
                                row_data['mtd'] = float(val)
                        
                        if qtd_col and qtd_col in df.columns:
                            val = row[qtd_col]
                            if pd.notna(val) and val != '-':
                                row_data['qtd'] = float(val)
                        
                        if ytd_col and ytd_col in df.columns:
                            val = row[ytd_col]
                            if pd.notna(val) and val != '-':
                                row_data['ytd'] = float(val)
                        
                        if itd_col and itd_col in df.columns:
                            val = row[itd_col]
                            if pd.notna(val) and val != '-':
                                row_data['itd'] = float(val)
                    except (ValueError, TypeError) as e:
                        print(f"Error parsing values for {label}: {e}")
                    
                    break
            
            if not found:
                print(f"Warning: Could not find line item '{label}' in DataFrame")

            statement_items.append(row_data)

        # Filter out empty line items (all zeros) except for Beginning Balance and Ending Capital
        filtered_items = []
        for item in statement_items:
            # Always keep Beginning Balance and Ending Capital
            if item['label'] in ['Beginning Balance', 'Ending Capital']:
                filtered_items.append(item)
            else:
                # Check if at least one period has a non-zero value
                if any([item['mtd'] != 0.0, item['qtd'] != 0.0, item['ytd'] != 0.0, item['itd'] != 0.0]):
                    filtered_items.append(item)
                else:
                    print(f"Filtering out empty line item: {item['label']}")

        return filtered_items
    
    def _parse_commitment_summary(self, df: pd.DataFrame, lp_id: str = None) -> Dict:
        """Parse commitment summary from DataFrame"""
        summary = {
            'Total commitments': '-',
            'Capital called': '-',
            'Remaining commitments': '-'
        }
        
        # Look for commitment data in the DataFrame
        commitment_search_terms = {
            'Total commitments': ['total commitment', 'commitment amount', 'committed capital'],
            'Capital called': ['capital called', 'called capital', 'capital contribution', 'paid in capital'],
            'Remaining commitments': ['remaining commitment', 'uncalled capital', 'unfunded commitment']
        }
        
        for label, search_terms in commitment_search_terms.items():
            for idx, row in df.iterrows():
                desc = str(row.iloc[0]).lower() if len(row) > 0 else ''
                
                if any(term in desc for term in search_terms):
                    # Found the row, get the value (usually in the last column or second column)
                    try:
                        # Try last column first (common for summary values)
                        val = row.iloc[-1]
                        if pd.isna(val) or val == '-':
                            # Try second column
                            val = row.iloc[1] if len(row) > 1 else 0
                        
                        if pd.notna(val) and val != '-':
                            summary[label] = f"{float(val):.6f}"
                    except (ValueError, TypeError, IndexError):
                        pass
                    break
        
        # If we found capital contributions in statement_of_changes, use that as capital called
        if summary['Capital called'] == '-' and self.excel_data:
            # Try to get from the ITD column of Capital contributions
            for idx, row in df.iterrows():
                desc = str(row.iloc[0]).lower() if len(row) > 0 else ''
                if 'capital contribution' in desc:
                    try:
                        # Get ITD value (usually last or 5th column)
                        if len(row) > 4:
                            val = row.iloc[4]
                            if pd.notna(val) and val != '-':
                                summary['Capital called'] = f"{abs(float(val)):.6f}"
                    except (ValueError, TypeError):
                        pass
                    break
        
        return summary
    
    def _parse_performance_metrics(self, df: pd.DataFrame, lp_id: str = None) -> Dict:
        """Parse performance metrics from DataFrame"""
        metrics = {
            'Net IRR': '-',
            'Gross MOIC': '-',
            'NAV per unit': '-'
        }
        
        # Look for performance metrics in the DataFrame
        metrics_search_terms = {
            'Net IRR': ['net irr', 'irr', 'internal rate', 'rate of return'],
            'Gross MOIC': ['gross moic', 'moic', 'multiple', 'multiple on invested'],
            'NAV per unit': ['nav per unit', 'net asset value', 'unit price', 'price per unit']
        }
        
        for label, search_terms in metrics_search_terms.items():
            for idx, row in df.iterrows():
                desc = str(row.iloc[0]).lower() if len(row) > 0 else ''
                
                if any(term in desc for term in search_terms):
                    # Found the row, get the value
                    try:
                        # Try last column first
                        val = row.iloc[-1]
                        if pd.isna(val) or val == '-':
                            # Try second column
                            val = row.iloc[1] if len(row) > 1 else 0
                        
                        if pd.notna(val) and val != '-':
                            if 'IRR' in label:
                                # Format as percentage
                                if isinstance(val, str) and '%' in val:
                                    metrics[label] = val
                                else:
                                    metrics[label] = f"{float(val):.2f}%"
                            else:
                                # Format as decimal
                                metrics[label] = f"{float(val):.6f}"
                    except (ValueError, TypeError, IndexError):
                        pass
                    break
        
        # Calculate Gross MOIC if we have the data
        if metrics['Gross MOIC'] == '-' and self.excel_data:
            try:
                # MOIC = (Distributions + Current NAV) / Contributions
                distributions = 0
                contributions = 0
                ending_capital = 0
                
                for idx, row in df.iterrows():
                    desc = str(row.iloc[0]).lower() if len(row) > 0 else ''
                    
                    # Get ITD values
                    if 'capital distribution' in desc and len(row) > 4:
                        val = row.iloc[4]
                        if pd.notna(val) and val != '-':
                            distributions = abs(float(val))
                    elif 'capital contribution' in desc and len(row) > 4:
                        val = row.iloc[4]
                        if pd.notna(val) and val != '-':
                            contributions = abs(float(val))
                    elif 'ending capital' in desc or 'ending balance' in desc:
                        if len(row) > 4:
                            val = row.iloc[4]
                            if pd.notna(val) and val != '-':
                                ending_capital = float(val)
                
                if contributions > 0:
                    moic = (distributions + ending_capital) / contributions
                    metrics['Gross MOIC'] = f"{moic:.6f}"
                    
            except Exception as e:
                print(f"Error calculating MOIC: {e}")
        
        return metrics
    
    def generate_pdf(self, lp_id: str, fund_name: str = None, output_dir: str = None) -> str:
        """
        Generate PDF statement for an LP
        
        Args:
            lp_id: LP identifier
            fund_name: Name of the fund (optional, will auto-detect from LP ID)
            output_dir: Directory to save PDF (defaults to temp directory)
            
        Returns:
            Path to generated PDF file
        """
        try:
            # Get JSON data for the LP
            json_data = self.parse_excel_to_json(lp_id)
            
            if not json_data:
                print(f"No data available for LP: {lp_id}")
                return None
            
            # Auto-detect fund name if not provided
            if not fund_name:
                fund_name = self.get_fund_name_from_lp(lp_id)
                print(f"Auto-detected fund name: {fund_name}")
            
            # Set up paths
            base_path = Path(__file__).parent.parent / "PDF Creator"
            template_dir = base_path / "templates"
            
            # Set up Jinja2 environment
            env = Environment(loader=FileSystemLoader(str(template_dir)))
            template = env.get_template("report.html")
            
            # Format numbers to 6 decimal places
            json_data = self._format_numbers(json_data)
            
            # Render HTML
            # Note: lp_name is already in json_data, don't pass it again
            html_output = template.render(
                **json_data,
                fund_name=fund_name,
                css_path=str(base_path),
                generated_on=datetime.now().strftime("%B %d, %Y")
            )
            
            # Generate PDF filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            fund_id = self.current_file['fund_id'] if self.current_file else 'unknown'
            pdf_filename = f"PCAP_Statement_{fund_id}_{lp_id}_{timestamp}.pdf"
            
            # Determine output directory
            if output_dir:
                output_path = Path(output_dir) / pdf_filename
            else:
                output_path = Path(tempfile.gettempdir()) / pdf_filename
            
            # Generate PDF
            HTML(string=html_output, base_url=str(base_path)).write_pdf(str(output_path))
            
            print(f"PDF generated: {output_path}")
            return str(output_path)
            
        except Exception as e:
            print(f"Error generating PDF: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _format_numbers(self, data: Dict) -> Dict:
        """Format numbers in JSON data to 6 decimal places"""
        def format_value(val):
            if isinstance(val, (int, float)):
                if val == 0:
                    return "-"
                else:
                    return f"{val:.6f}"
            elif isinstance(val, str):
                return val
            elif isinstance(val, dict):
                return {k: format_value(v) for k, v in val.items()}
            elif isinstance(val, list):
                return [format_value(item) for item in val]
            else:
                return val
        
        return format_value(data)
    
    def generate_all_lp_pdfs(self, fund_name: str = None, output_dir: str = None) -> List[str]:
        """
        Generate PDF statements for all LPs
        
        Args:
            fund_name: Name of the fund (optional, will auto-detect from LP IDs)
            output_dir: Directory to save PDFs
            
        Returns:
            List of generated PDF file paths
        """
        pdf_files = []
        
        for lp_id in self.available_lps:
            # Auto-detect fund name for each LP if not provided
            lp_fund_name = fund_name if fund_name else self.get_fund_name_from_lp(lp_id)
            pdf_path = self.generate_pdf(lp_id, lp_fund_name, output_dir)
            if pdf_path:
                pdf_files.append(pdf_path)
        
        print(f"Generated {len(pdf_files)} PDF statements")
        return pdf_files

# Convenience functions for backward compatibility
def load_and_parse_pcap_excel(date: str = None, fund_id: str = None) -> Dict:
    """Load PCAP Excel and return parsed data"""
    processor = PCAPExcelProcessor()
    if processor.load_pcap_file(date=date, fund_id=fund_id):
        return {
            'processor': processor,
            'files': processor.available_files,
            'lps': processor.available_lps,
            'data': processor.excel_data
        }
    return None

def generate_pcap_pdf(date: str, fund_id: str, lp_id: str, fund_name: str) -> str:
    """Generate a single PCAP PDF statement"""
    processor = PCAPExcelProcessor()
    if processor.load_pcap_file(date=date, fund_id=fund_id):
        return processor.generate_pdf(lp_id, fund_name)
    return None