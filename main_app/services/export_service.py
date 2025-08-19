# -*- coding: utf-8 -*-
"""
Export Service Module

Comprehensive export and reporting capabilities for cryptocurrency portfolio data.
Supports PDF reports, CSV exports, tax reports, and custom formatting.
"""

import pandas as pd
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from decimal import Decimal
import logging
import json
import csv
from io import StringIO, BytesIO
import base64

# PDF generation
try:
    from weasyprint import HTML, CSS
    from jinja2 import Template
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.warning("PDF generation not available. Install weasyprint and jinja2.")

from .portfolio_valuation import get_valuation_engine
from .price_service import get_price_service
from .performance_metrics import get_performance_reporter

# Set up logging
logger = logging.getLogger(__name__)


class PortfolioReportGenerator:
    """Generate comprehensive portfolio reports in multiple formats"""
    
    def __init__(self):
        self.valuation_engine = get_valuation_engine()
        self.price_service = get_price_service()
        self.performance_reporter = get_performance_reporter()
    
    def generate_portfolio_summary_csv(self, filename: str = None) -> str:
        """Generate portfolio summary CSV report"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"portfolio_summary_{timestamp}.csv"
        
        try:
            # Get current portfolio data
            positions_df = self.valuation_engine.get_position_summary()
            
            if positions_df.empty:
                # Create empty template
                positions_df = pd.DataFrame({
                    'Symbol': ['No positions'],
                    'Quantity': [0.0],
                    'Avg Cost (USD)': [0.0],
                    'Current Price (USD)': [0.0],
                    'Market Value (USD)': [0.0],
                    'Unrealized P&L (USD)': [0.0],
                    'Unrealized P&L (%)': [0.0],
                    'Allocation (%)': [0.0]
                })
            else:
                # Calculate allocation percentages
                total_value = positions_df['market_value_usd'].sum()
                positions_df['allocation_pct'] = (positions_df['market_value_usd'] / total_value * 100) if total_value > 0 else 0
                
                # Create export DataFrame with formatted columns
                export_df = pd.DataFrame({
                    'Symbol': positions_df['symbol'],
                    'Quantity': positions_df['quantity'].round(8),
                    'Avg Cost (USD)': positions_df['avg_cost_usd'].round(2),
                    'Current Price (USD)': positions_df['current_price_usd'].round(2),
                    'Market Value (USD)': positions_df['market_value_usd'].round(2),
                    'Unrealized P&L (USD)': positions_df['unrealized_pnl_usd'].round(2),
                    'Unrealized P&L (%)': positions_df['unrealized_pnl_pct'].round(2),
                    'Allocation (%)': positions_df['allocation_pct'].round(2),
                    'Last Updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
                
                positions_df = export_df
            
            # Add summary row
            if len(positions_df) > 1 and positions_df.iloc[0]['Symbol'] != 'No positions':
                summary_row = {
                    'Symbol': 'TOTAL',
                    'Quantity': '',
                    'Avg Cost (USD)': '',
                    'Current Price (USD)': '',
                    'Market Value (USD)': positions_df['Market Value (USD)'].sum(),
                    'Unrealized P&L (USD)': positions_df['Unrealized P&L (USD)'].sum(),
                    'Unrealized P&L (%)': '',
                    'Allocation (%)': 100.0,
                    'Last Updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                positions_df = pd.concat([positions_df, pd.DataFrame([summary_row])], ignore_index=True)
            
            # Create export directory
            export_dir = "portfolio_exports"
            os.makedirs(export_dir, exist_ok=True)
            
            # Export to CSV
            file_path = os.path.join(export_dir, filename)
            positions_df.to_csv(file_path, index=False)
            
            logger.info(f"Portfolio summary exported to {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"Error generating portfolio summary CSV: {e}")
            raise
    
    def generate_transaction_history_csv(self, filename: str = None, 
                                       start_date: datetime = None, 
                                       end_date: datetime = None) -> str:
        """Generate transaction history CSV export"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"transaction_history_{timestamp}.csv"
        
        try:
            # Get transaction data
            from ..modules.general_ledger.crypto_token_fetch import get_staged_transactions_global
            staged_df = get_staged_transactions_global()
            
            if staged_df.empty:
                # Create empty template
                export_df = pd.DataFrame({
                    'Date': ['No transactions'],
                    'Token': ['-'],
                    'Type': ['-'],
                    'Quantity': [0.0],
                    'Price (USD)': [0.0],
                    'Value (USD)': [0.0],
                    'Value (ETH)': [0.0],
                    'Wallet': ['-'],
                    'Transaction Hash': ['-'],
                    'Intercompany': ['-']
                })
            else:
                # Filter by date range if specified
                if start_date or end_date:
                    date_col = pd.to_datetime(staged_df['date'])
                    if start_date:
                        staged_df = staged_df[date_col >= start_date]
                    if end_date:
                        staged_df = staged_df[date_col <= end_date]
                
                # Create export DataFrame
                export_df = pd.DataFrame({
                    'Date': pd.to_datetime(staged_df['date']).dt.strftime('%Y-%m-%d %H:%M:%S'),
                    'Token': staged_df.get('token_name', staged_df.get('asset', 'Unknown')),
                    'Type': staged_df.get('side', 'Unknown').str.title(),
                    'Quantity': staged_df.get('token_amount', 0.0).round(8),
                    'Price (USD)': (staged_df.get('token_value_usd', 0) / staged_df.get('token_amount', 1)).round(2),
                    'Value (USD)': staged_df.get('token_value_usd', 0.0).round(2),
                    'Value (ETH)': staged_df.get('token_value_eth', 0.0).round(8),
                    'Wallet': staged_df.get('wallet_id', 'Unknown'),
                    'Transaction Hash': staged_df.get('hash', 'Unknown'),
                    'Intercompany': staged_df.get('intercompany', False)
                })
                
                # Sort by date (newest first)
                export_df = export_df.sort_values('Date', ascending=False)
            
            # Create export directory
            export_dir = "portfolio_exports"
            os.makedirs(export_dir, exist_ok=True)
            
            # Export to CSV
            file_path = os.path.join(export_dir, filename)
            export_df.to_csv(file_path, index=False)
            
            logger.info(f"Transaction history exported to {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"Error generating transaction history CSV: {e}")
            raise
    
    def generate_tax_report_csv(self, tax_year: int = None, filename: str = None) -> str:
        """Generate tax report in Form 8949 format"""
        if tax_year is None:
            tax_year = datetime.now().year
            
        if filename is None:
            filename = f"tax_report_{tax_year}.csv"
        
        try:
            # This would typically use FIFO results for realized gains/losses
            # For now, create a template structure
            
            # Get FIFO results if available (placeholder)
            tax_df = pd.DataFrame({
                'Description': ['No realized transactions for tax year'],
                'Date Acquired': ['-'],
                'Date Sold': ['-'],
                'Proceeds': [0.0],
                'Cost Basis': [0.0],
                'Gain/Loss': [0.0],
                'Term': ['-']  # Short-term or Long-term
            })
            
            # Add summary totals
            summary_df = pd.DataFrame({
                'Description': ['TOTAL SHORT-TERM', 'TOTAL LONG-TERM', 'TOTAL ALL'],
                'Date Acquired': ['', '', ''],
                'Date Sold': ['', '', ''],
                'Proceeds': [0.0, 0.0, 0.0],
                'Cost Basis': [0.0, 0.0, 0.0],
                'Gain/Loss': [0.0, 0.0, 0.0],
                'Term': ['Short-term', 'Long-term', 'Combined']
            })
            
            # Combine DataFrames
            full_df = pd.concat([tax_df, summary_df], ignore_index=True)
            
            # Create export directory
            export_dir = "portfolio_exports"
            os.makedirs(export_dir, exist_ok=True)
            
            # Export to CSV
            file_path = os.path.join(export_dir, filename)
            full_df.to_csv(file_path, index=False)
            
            logger.info(f"Tax report exported to {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"Error generating tax report CSV: {e}")
            raise
    
    def generate_performance_report_json(self, filename: str = None) -> str:
        """Generate detailed performance report in JSON format"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"performance_report_{timestamp}.json"
        
        try:
            # Get portfolio performance data
            portfolio_data = {
                'positions': {},
                'historical_values': [],  # Would be populated with actual historical data
                'cash_flows': []  # Would be populated with actual cash flow data
            }
            
            # Get performance summary
            performance_summary = self.performance_reporter.generate_performance_summary(
                portfolio_data, 'ETH'
            )
            
            # Add current portfolio metrics
            performance_summary['current_portfolio'] = {
                'total_value_usd': float(self.valuation_engine.metrics.total_value_usd),
                'total_value_eth': float(self.valuation_engine.metrics.total_value_eth),
                'token_count': self.valuation_engine.metrics.token_count,
                'largest_position': self.valuation_engine.metrics.largest_position_symbol,
                'last_updated': datetime.now().isoformat()
            }
            
            # Add position details
            positions_df = self.valuation_engine.get_position_summary()
            if not positions_df.empty:
                performance_summary['positions'] = positions_df.to_dict('records')
            
            # Create export directory
            export_dir = "portfolio_exports"
            os.makedirs(export_dir, exist_ok=True)
            
            # Export to JSON
            file_path = os.path.join(export_dir, filename)
            with open(file_path, 'w') as f:
                json.dump(performance_summary, f, indent=2, default=str)
            
            logger.info(f"Performance report exported to {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"Error generating performance report JSON: {e}")
            raise


class PDFReportGenerator:
    """Generate PDF reports using HTML templates"""
    
    def __init__(self):
        if not PDF_AVAILABLE:
            raise ImportError("PDF generation requires weasyprint and jinja2")
        
        self.valuation_engine = get_valuation_engine()
        self.report_generator = PortfolioReportGenerator()
    
    def generate_portfolio_pdf_report(self, filename: str = None) -> str:
        """Generate comprehensive PDF portfolio report"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"portfolio_report_{timestamp}.pdf"
        
        try:
            # Get portfolio data
            positions_df = self.valuation_engine.get_position_summary()
            metrics = self.valuation_engine.metrics
            
            # Prepare template data
            template_data = {
                'report_title': 'Cryptocurrency Portfolio Report',
                'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'total_value_usd': f"${metrics.total_value_usd:,.2f}",
                'total_value_eth': f"{metrics.total_value_eth:.6f} ETH",
                'token_count': metrics.token_count,
                'total_unrealized_pnl': f"${metrics.total_unrealized_pnl_usd:+,.2f}",
                'total_unrealized_pnl_pct': f"{metrics.total_unrealized_pnl_pct:+.2f}%",
                'largest_position': metrics.largest_position_symbol,
                'positions': []
            }
            
            # Add position data
            if not positions_df.empty:
                for _, row in positions_df.iterrows():
                    template_data['positions'].append({
                        'symbol': row['symbol'],
                        'quantity': f"{row['quantity']:.6f}",
                        'market_value': f"${row['market_value_usd']:,.2f}",
                        'unrealized_pnl': f"${row['unrealized_pnl_usd']:+,.2f}",
                        'unrealized_pnl_pct': f"{row['unrealized_pnl_pct']:+.2f}%",
                        'allocation_pct': f"{(row['market_value_usd'] / metrics.total_value_usd * 100):.1f}%" if metrics.total_value_usd > 0 else "0.0%"
                    })
            
            # HTML template
            html_template = """
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="utf-8">
                <title>{{ report_title }}</title>
                <style>
                    body { font-family: Arial, sans-serif; margin: 40px; }
                    .header { text-align: center; margin-bottom: 30px; }
                    .summary-box { background: #f8f9fa; padding: 20px; border-radius: 8px; margin: 20px 0; }
                    .metrics-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin: 20px 0; }
                    .metric { text-align: center; padding: 15px; background: white; border-radius: 6px; }
                    .metric-value { font-size: 24px; font-weight: bold; color: #2563eb; }
                    .metric-label { font-size: 14px; color: #6b7280; margin-top: 5px; }
                    table { width: 100%; border-collapse: collapse; margin: 20px 0; }
                    th, td { padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }
                    th { background-color: #f8f9fa; font-weight: bold; }
                    .positive { color: #10b981; }
                    .negative { color: #ef4444; }
                    .footer { margin-top: 40px; text-align: center; font-size: 12px; color: #6b7280; }
                </style>
            </head>
            <body>
                <div class="header">
                    <h1>{{ report_title }}</h1>
                    <p>Generated on {{ generated_at }}</p>
                </div>
                
                <div class="summary-box">
                    <h2>Portfolio Summary</h2>
                    <div class="metrics-grid">
                        <div class="metric">
                            <div class="metric-value">{{ total_value_usd }}</div>
                            <div class="metric-label">Total Value (USD)</div>
                        </div>
                        <div class="metric">
                            <div class="metric-value">{{ token_count }}</div>
                            <div class="metric-label">Unique Tokens</div>
                        </div>
                        <div class="metric">
                            <div class="metric-value">{{ total_unrealized_pnl }}</div>
                            <div class="metric-label">Unrealized P&L</div>
                        </div>
                        <div class="metric">
                            <div class="metric-value">{{ largest_position }}</div>
                            <div class="metric-label">Largest Position</div>
                        </div>
                    </div>
                </div>
                
                {% if positions %}
                <h2>Portfolio Holdings</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Token</th>
                            <th>Quantity</th>
                            <th>Market Value</th>
                            <th>Unrealized P&L</th>
                            <th>Allocation</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for position in positions %}
                        <tr>
                            <td><strong>{{ position.symbol }}</strong></td>
                            <td>{{ position.quantity }}</td>
                            <td>{{ position.market_value }}</td>
                            <td class="{% if position.unrealized_pnl.startswith('+') %}positive{% else %}negative{% endif %}">
                                {{ position.unrealized_pnl }} ({{ position.unrealized_pnl_pct }})
                            </td>
                            <td>{{ position.allocation_pct }}</td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
                {% else %}
                <p>No positions to display.</p>
                {% endif %}
                
                <div class="footer">
                    <p>This report was generated by RealWorldNAV Crypto Tracker</p>
                    <p>Data as of {{ generated_at }} - For informational purposes only</p>
                </div>
            </body>
            </html>
            """
            
            # Render template
            template = Template(html_template)
            html_content = template.render(**template_data)
            
            # Create export directory
            export_dir = "portfolio_exports"
            os.makedirs(export_dir, exist_ok=True)
            
            # Generate PDF
            file_path = os.path.join(export_dir, filename)
            HTML(string=html_content).write_pdf(file_path)
            
            logger.info(f"PDF report generated: {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"Error generating PDF report: {e}")
            raise


class ExportService:
    """Main export service coordination"""
    
    def __init__(self):
        self.csv_generator = PortfolioReportGenerator()
        self.pdf_generator = PDFReportGenerator() if PDF_AVAILABLE else None
    
    def export_portfolio_summary(self, format: str = 'csv', filename: str = None) -> str:
        """Export portfolio summary in specified format"""
        if format.lower() == 'csv':
            return self.csv_generator.generate_portfolio_summary_csv(filename)
        elif format.lower() == 'pdf' and self.pdf_generator:
            return self.pdf_generator.generate_portfolio_pdf_report(filename)
        else:
            raise ValueError(f"Unsupported export format: {format}")
    
    def export_transaction_history(self, format: str = 'csv', filename: str = None,
                                 start_date: datetime = None, end_date: datetime = None) -> str:
        """Export transaction history"""
        if format.lower() == 'csv':
            return self.csv_generator.generate_transaction_history_csv(filename, start_date, end_date)
        else:
            raise ValueError(f"Unsupported format for transaction history: {format}")
    
    def export_tax_report(self, tax_year: int = None, filename: str = None) -> str:
        """Export tax report"""
        return self.csv_generator.generate_tax_report_csv(tax_year, filename)
    
    def export_performance_report(self, format: str = 'json', filename: str = None) -> str:
        """Export performance report"""
        if format.lower() == 'json':
            return self.csv_generator.generate_performance_report_json(filename)
        else:
            raise ValueError(f"Unsupported format for performance report: {format}")
    
    def get_available_formats(self) -> Dict[str, List[str]]:
        """Get available export formats for each report type"""
        formats = {
            'portfolio_summary': ['csv'],
            'transaction_history': ['csv'],
            'tax_report': ['csv'],
            'performance_report': ['json']
        }
        
        if self.pdf_generator:
            formats['portfolio_summary'].append('pdf')
        
        return formats


# Global export service instance
_export_service = None


def get_export_service() -> ExportService:
    """Get global export service instance"""
    global _export_service
    if _export_service is None:
        _export_service = ExportService()
    return _export_service