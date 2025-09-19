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
from weasyprint import HTML
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
        """Extract list of LPs from Excel sheet names"""
        if not self.excel_data:
            return []
        
        lp_sheets = []
        for sheet_name in self.excel_data.keys():
            # Skip summary/metadata sheets
            if sheet_name.lower() not in ['summary', 'all partners', 'pcap', 'metadata', 'totals']:
                # Assume other sheets are LP-specific
                lp_sheets.append(sheet_name)
        
        return sorted(lp_sheets)
    
    def get_lp_data(self, lp_id: str) -> Optional[pd.DataFrame]:
        """Get data for a specific LP"""
        if not self.excel_data:
            return None
        
        # Check if LP has a dedicated sheet
        if lp_id in self.excel_data:
            return self.excel_data[lp_id]
        
        # Otherwise, try to filter from main sheet
        for sheet_name in ['All Partners', 'Summary', 'PCAP']:
            if sheet_name in self.excel_data:
                df = self.excel_data[sheet_name]
                # Try to filter by LP column if it exists
                if 'LP' in df.columns or 'Partner' in df.columns:
                    lp_col = 'LP' if 'LP' in df.columns else 'Partner'
                    lp_data = df[df[lp_col] == lp_id]
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
            
            # Build JSON structure
            json_data = {
                'main_date': date_str,
                'currency': 'ETH',
                'lp_name': lp_id or 'All Partners',
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
        """Extract formatted date from filename or use current date"""
        if self.current_file and self.current_file['date']:
            try:
                date_obj = pd.to_datetime(self.current_file['date'], format='%Y%m%d')
                return date_obj.strftime('%B %d, %Y')
            except:
                pass
        
        return datetime.now().strftime('%B %d, %Y')
    
    def _parse_statement_of_changes(self, df: pd.DataFrame, lp_id: str = None) -> List[Dict]:
        """Parse statement of changes from DataFrame"""
        statement_items = []
        
        # Define the expected line items in order
        line_items = [
            'Beginning Balance',
            'Capital contributions',
            'Management fees',
            'Interest expense',
            'Capital distributions',
            'Other income',
            'Operating expenses',
            'Interest income',
            'Provision for bad debt',
            'Income allocated from investments',
            'Realized gain (loss)',
            'Change in unrealized gain (loss)',
            'Ending Capital'
        ]
        
        # Try to find each line item in the DataFrame
        for item in line_items:
            row_data = {'label': item, 'mtd': 0.0, 'qtd': 0.0, 'ytd': 0.0, 'itd': 0.0}
            
            # Search for the item in the first column
            for idx, row in df.iterrows():
                first_col = str(row.iloc[0]) if len(row) > 0 else ''
                if item.lower() in first_col.lower():
                    # Found the row, extract values
                    # Assuming columns are: Description, MTD, QTD, YTD, ITD
                    try:
                        if len(row) > 1 and pd.notna(row.iloc[1]):
                            row_data['mtd'] = float(row.iloc[1])
                        if len(row) > 2 and pd.notna(row.iloc[2]):
                            row_data['qtd'] = float(row.iloc[2])
                        if len(row) > 3 and pd.notna(row.iloc[3]):
                            row_data['ytd'] = float(row.iloc[3])
                        if len(row) > 4 and pd.notna(row.iloc[4]):
                            row_data['itd'] = float(row.iloc[4])
                    except (ValueError, TypeError):
                        pass
                    break
            
            statement_items.append(row_data)
        
        return statement_items
    
    def _parse_commitment_summary(self, df: pd.DataFrame, lp_id: str = None) -> Dict:
        """Parse commitment summary from DataFrame"""
        summary = {
            'Total commitments': '0.000000',
            'Capital called': '0.000000',
            'Remaining commitments': '0.000000'
        }
        
        # Try to find commitment data in the DataFrame
        # This will need to be customized based on actual Excel structure
        
        return summary
    
    def _parse_performance_metrics(self, df: pd.DataFrame, lp_id: str = None) -> Dict:
        """Parse performance metrics from DataFrame"""
        metrics = {
            'Net IRR': '0.00%',
            'Gross MOIC': '0.000000',
            'NAV per unit': '0.000000'
        }
        
        # Try to find performance data in the DataFrame
        # This will need to be customized based on actual Excel structure
        
        return metrics
    
    def generate_pdf(self, lp_id: str, fund_name: str, output_dir: str = None) -> str:
        """
        Generate PDF statement for an LP
        
        Args:
            lp_id: LP identifier
            fund_name: Name of the fund
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
            
            # Set up paths
            base_path = Path(__file__).parent.parent / "PDF Creator"
            template_dir = base_path / "templates"
            
            # Set up Jinja2 environment
            env = Environment(loader=FileSystemLoader(str(template_dir)))
            template = env.get_template("report.html")
            
            # Format numbers to 6 decimal places
            json_data = self._format_numbers(json_data)
            
            # Render HTML
            html_output = template.render(
                **json_data,
                fund_name=fund_name,
                lp_name=lp_id,
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
    
    def generate_all_lp_pdfs(self, fund_name: str, output_dir: str = None) -> List[str]:
        """
        Generate PDF statements for all LPs
        
        Args:
            fund_name: Name of the fund
            output_dir: Directory to save PDFs
            
        Returns:
            List of generated PDF file paths
        """
        pdf_files = []
        
        for lp_id in self.available_lps:
            pdf_path = self.generate_pdf(lp_id, fund_name, output_dir)
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