# -*- coding: utf-8 -*-
"""
Portfolio Valuation Engine

Real-time portfolio valuation with P&L calculations, risk metrics,
and performance analytics for cryptocurrency holdings.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union
from decimal import Decimal, ROUND_HALF_EVEN
import logging
from functools import lru_cache

from .price_service import get_price_service
from .fifo_tracker import FIFOTracker

# Set up logging
logger = logging.getLogger(__name__)

# Decimal precision
DECIMAL_PLACES = 8


class PortfolioPosition:
    """Individual position within a portfolio"""
    
    def __init__(self, symbol: str, quantity: Decimal, avg_cost_usd: Decimal = None, 
                 avg_cost_eth: Decimal = None, wallet_address: str = None):
        self.symbol = symbol.upper()
        self.quantity = Decimal(str(quantity))
        self.avg_cost_usd = Decimal(str(avg_cost_usd or 0))
        self.avg_cost_eth = Decimal(str(avg_cost_eth or 0))
        self.wallet_address = wallet_address
        
        # Market data (populated by valuation engine)
        self.current_price_usd = Decimal('0')
        self.current_price_eth = Decimal('0')
        self.market_value_usd = Decimal('0')
        self.market_value_eth = Decimal('0')
        self.unrealized_pnl_usd = Decimal('0')
        self.unrealized_pnl_eth = Decimal('0')
        self.unrealized_pnl_pct = Decimal('0')
        self.change_24h_pct = Decimal('0')
        self.last_updated = None
        
    def update_market_data(self, price_data: Dict[str, Any]) -> None:
        """Update position with current market data"""
        if 'usd' in price_data:
            self.current_price_usd = price_data['usd']
            self.market_value_usd = self.quantity * self.current_price_usd
            
            if self.avg_cost_usd > 0:
                cost_basis_usd = self.quantity * self.avg_cost_usd
                self.unrealized_pnl_usd = self.market_value_usd - cost_basis_usd
                self.unrealized_pnl_pct = (self.unrealized_pnl_usd / cost_basis_usd * 100) if cost_basis_usd > 0 else Decimal('0')
        
        if 'eth' in price_data:
            self.current_price_eth = price_data['eth']
            self.market_value_eth = self.quantity * self.current_price_eth
            
        if 'change_24h' in price_data:
            self.change_24h_pct = price_data['change_24h']
            
        if 'last_updated' in price_data:
            self.last_updated = price_data['last_updated']
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert position to dictionary"""
        return {
            'symbol': self.symbol,
            'quantity': float(self.quantity),
            'avg_cost_usd': float(self.avg_cost_usd),
            'avg_cost_eth': float(self.avg_cost_eth),
            'current_price_usd': float(self.current_price_usd),
            'current_price_eth': float(self.current_price_eth),
            'market_value_usd': float(self.market_value_usd),
            'market_value_eth': float(self.market_value_eth),
            'unrealized_pnl_usd': float(self.unrealized_pnl_usd),
            'unrealized_pnl_pct': float(self.unrealized_pnl_pct),
            'change_24h_pct': float(self.change_24h_pct),
            'wallet_address': self.wallet_address,
            'last_updated': self.last_updated.isoformat() if self.last_updated else None
        }


class PortfolioMetrics:
    """Portfolio-level metrics and analytics"""
    
    def __init__(self):
        self.total_value_usd = Decimal('0')
        self.total_value_eth = Decimal('0')
        self.total_cost_basis_usd = Decimal('0')
        self.total_cost_basis_eth = Decimal('0')
        self.total_unrealized_pnl_usd = Decimal('0')
        self.total_unrealized_pnl_eth = Decimal('0')
        self.total_unrealized_pnl_pct = Decimal('0')
        self.daily_change_usd = Decimal('0')
        self.daily_change_pct = Decimal('0')
        self.token_count = 0
        self.largest_position_pct = Decimal('0')
        self.largest_position_symbol = ''
        self.last_updated = None
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary"""
        return {
            'total_value_usd': float(self.total_value_usd),
            'total_value_eth': float(self.total_value_eth),
            'total_cost_basis_usd': float(self.total_cost_basis_usd),
            'total_cost_basis_eth': float(self.total_cost_basis_eth),
            'total_unrealized_pnl_usd': float(self.total_unrealized_pnl_usd),
            'total_unrealized_pnl_eth': float(self.total_unrealized_pnl_eth),
            'total_unrealized_pnl_pct': float(self.total_unrealized_pnl_pct),
            'daily_change_usd': float(self.daily_change_usd),
            'daily_change_pct': float(self.daily_change_pct),
            'token_count': self.token_count,
            'largest_position_pct': float(self.largest_position_pct),
            'largest_position_symbol': self.largest_position_symbol,
            'last_updated': self.last_updated.isoformat() if self.last_updated else None
        }


class PortfolioValuationEngine:
    """Main portfolio valuation engine"""
    
    def __init__(self):
        self.price_service = get_price_service()
        self.positions: Dict[str, PortfolioPosition] = {}
        self.metrics = PortfolioMetrics()
        
    def load_positions_from_fifo(self, fifo_positions_df: pd.DataFrame) -> None:
        """Load positions from FIFO tracker results"""
        if fifo_positions_df.empty:
            logger.warning("No FIFO positions to load")
            return
        
        self.positions.clear()
        
        for _, row in fifo_positions_df.iterrows():
            symbol = row['asset'].upper()
            quantity = Decimal(str(row['qty']))
            avg_cost_eth = Decimal(str(row.get('avg_unit_price_eth', 0)))
            wallet_address = row.get('wallet_address')
            
            position = PortfolioPosition(
                symbol=symbol,
                quantity=quantity,
                avg_cost_eth=avg_cost_eth,
                wallet_address=wallet_address
            )
            
            # If we have multiple positions for same token, combine them
            if symbol in self.positions:
                existing = self.positions[symbol]
                total_qty = existing.quantity + position.quantity
                
                # Weighted average cost basis
                if total_qty > 0:
                    existing.avg_cost_eth = (
                        (existing.quantity * existing.avg_cost_eth + 
                         position.quantity * position.avg_cost_eth) / total_qty
                    )
                    existing.quantity = total_qty
            else:
                self.positions[symbol] = position
        
        logger.info(f"Loaded {len(self.positions)} positions from FIFO data")
    
    def load_positions_from_staged_transactions(self, staged_df: pd.DataFrame) -> None:
        """Load positions from staged transactions (alternative to FIFO)"""
        if staged_df.empty:
            logger.warning("No staged transactions to load")
            return
        
        # Aggregate positions by symbol
        position_data = {}
        
        for _, row in staged_df.iterrows():
            symbol = row.get('token_name', row.get('asset', '')).upper()
            if not symbol:
                continue
            
            side = row.get('side', '').lower()
            quantity = Decimal(str(row.get('token_amount', 0)))
            eth_value = Decimal(str(row.get('token_value_eth', 0)))
            wallet = row.get('wallet_id', '')
            
            if symbol not in position_data:
                position_data[symbol] = {
                    'total_quantity': Decimal('0'),
                    'total_eth_value': Decimal('0'),
                    'wallets': set()
                }
            
            if side == 'buy':
                position_data[symbol]['total_quantity'] += quantity
                position_data[symbol]['total_eth_value'] += eth_value
            elif side == 'sell':
                position_data[symbol]['total_quantity'] -= quantity
                position_data[symbol]['total_eth_value'] -= eth_value
            
            if wallet:
                position_data[symbol]['wallets'].add(wallet)
        
        # Create positions
        self.positions.clear()
        for symbol, data in position_data.items():
            if data['total_quantity'] > 0:  # Only positive positions
                avg_cost_eth = (
                    data['total_eth_value'] / data['total_quantity'] 
                    if data['total_quantity'] > 0 else Decimal('0')
                )
                
                position = PortfolioPosition(
                    symbol=symbol,
                    quantity=data['total_quantity'],
                    avg_cost_eth=avg_cost_eth,
                    wallet_address=', '.join(list(data['wallets'])[:2])  # Show first 2 wallets
                )
                self.positions[symbol] = position
        
        logger.info(f"Loaded {len(self.positions)} positions from staged transactions")
    
    def update_market_values(self) -> None:
        """Update all positions with current market prices"""
        if not self.positions:
            logger.warning("No positions to update")
            return
        
        symbols = list(self.positions.keys())
        logger.info(f"Updating market values for {len(symbols)} positions")
        
        try:
            # Get current prices for all positions
            price_data = self.price_service.get_current_prices(symbols, ['usd', 'eth'])
            
            # Get ETH/USD price for cost basis conversion
            eth_price_data = self.price_service.get_current_prices(['ETH'], ['usd'])
            eth_usd_price = eth_price_data.get('ETH', {}).get('usd', Decimal('3200'))  # Fallback
            
            # Update each position
            for symbol, position in self.positions.items():
                if symbol in price_data:
                    position.update_market_data(price_data[symbol])
                    
                    # Convert ETH cost basis to USD
                    if position.avg_cost_eth > 0:
                        position.avg_cost_usd = position.avg_cost_eth * eth_usd_price
                else:
                    logger.warning(f"No price data available for {symbol}")
            
            # Calculate portfolio metrics
            self._calculate_portfolio_metrics()
            
            logger.info("Market values updated successfully")
            
        except Exception as e:
            logger.error(f"Error updating market values: {e}")
    
    def _calculate_portfolio_metrics(self) -> None:
        """Calculate portfolio-level metrics"""
        self.metrics = PortfolioMetrics()
        
        if not self.positions:
            return
        
        total_value_usd = Decimal('0')
        total_value_eth = Decimal('0')
        total_cost_basis_usd = Decimal('0')
        total_cost_basis_eth = Decimal('0')
        total_unrealized_pnl_usd = Decimal('0')
        daily_change_usd = Decimal('0')
        
        largest_position_value = Decimal('0')
        largest_position_symbol = ''
        
        for symbol, position in self.positions.items():
            total_value_usd += position.market_value_usd
            total_value_eth += position.market_value_eth
            total_cost_basis_usd += position.quantity * position.avg_cost_usd
            total_cost_basis_eth += position.quantity * position.avg_cost_eth
            total_unrealized_pnl_usd += position.unrealized_pnl_usd
            
            # Daily change calculation (approximate)
            if position.change_24h_pct != 0:
                yesterday_value = position.market_value_usd / (1 + position.change_24h_pct / 100)
                daily_change_usd += position.market_value_usd - yesterday_value
            
            # Track largest position
            if position.market_value_usd > largest_position_value:
                largest_position_value = position.market_value_usd
                largest_position_symbol = symbol
        
        # Update metrics
        self.metrics.total_value_usd = total_value_usd
        self.metrics.total_value_eth = total_value_eth
        self.metrics.total_cost_basis_usd = total_cost_basis_usd
        self.metrics.total_cost_basis_eth = total_cost_basis_eth
        self.metrics.total_unrealized_pnl_usd = total_unrealized_pnl_usd
        self.metrics.daily_change_usd = daily_change_usd
        self.metrics.token_count = len(self.positions)
        self.metrics.largest_position_symbol = largest_position_symbol
        
        # Calculate percentages
        if total_cost_basis_usd > 0:
            self.metrics.total_unrealized_pnl_pct = (total_unrealized_pnl_usd / total_cost_basis_usd * 100)
        
        if total_value_usd > 0:
            self.metrics.daily_change_pct = (daily_change_usd / (total_value_usd - daily_change_usd) * 100)
            self.metrics.largest_position_pct = (largest_position_value / total_value_usd * 100)
        
        self.metrics.last_updated = datetime.now()
    
    def get_position_summary(self) -> pd.DataFrame:
        """Get portfolio positions as DataFrame"""
        if not self.positions:
            return pd.DataFrame()
        
        data = []
        for symbol, position in self.positions.items():
            data.append(position.to_dict())
        
        df = pd.DataFrame(data)
        
        # Sort by market value (descending)
        if 'market_value_usd' in df.columns:
            df = df.sort_values('market_value_usd', ascending=False)
        
        return df
    
    def get_top_positions(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get top positions by value"""
        df = self.get_position_summary()
        if df.empty:
            return []
        
        return df.head(limit).to_dict('records')
    
    def get_allocation_data(self) -> List[Dict[str, Any]]:
        """Get portfolio allocation for pie charts"""
        if not self.positions or self.metrics.total_value_usd <= 0:
            return []
        
        allocation = []
        for symbol, position in self.positions.items():
            if position.market_value_usd > 0:
                allocation_pct = (position.market_value_usd / self.metrics.total_value_usd * 100)
                allocation.append({
                    'symbol': symbol,
                    'value': float(position.market_value_usd),
                    'percentage': float(allocation_pct),
                    'color': self._get_token_color(symbol)
                })
        
        # Sort by value
        allocation.sort(key=lambda x: x['value'], reverse=True)
        return allocation
    
    def _get_token_color(self, symbol: str) -> str:
        """Get consistent color for token"""
        # Simple color mapping for common tokens
        color_map = {
            'ETH': '#627EEA',
            'BTC': '#F7931A', 
            'USDC': '#2775CA',
            'USDT': '#26A17B',
            'DAI': '#F4B731',
            'LINK': '#375BD2',
            'UNI': '#FF007A',
            'AAVE': '#B6509E',
            'COMP': '#00D395',
            'MKR': '#1AAB9B'
        }
        
        return color_map.get(symbol.upper(), '#6C757D')  # Default gray
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get portfolio performance summary"""
        return {
            'metrics': self.metrics.to_dict(),
            'position_count': len(self.positions),
            'top_performer': self._get_top_performer(),
            'worst_performer': self._get_worst_performer(),
            'allocation': self.get_allocation_data()[:5],  # Top 5 allocations
            'last_updated': datetime.now().isoformat()
        }
    
    def _get_top_performer(self) -> Dict[str, Any]:
        """Get best performing position"""
        best_position = None
        best_performance = Decimal('-999999')
        
        for position in self.positions.values():
            if position.unrealized_pnl_pct > best_performance:
                best_performance = position.unrealized_pnl_pct
                best_position = position
        
        if best_position:
            return {
                'symbol': best_position.symbol,
                'pnl_pct': float(best_position.unrealized_pnl_pct),
                'pnl_usd': float(best_position.unrealized_pnl_usd)
            }
        
        return {'symbol': 'N/A', 'pnl_pct': 0, 'pnl_usd': 0}
    
    def _get_worst_performer(self) -> Dict[str, Any]:
        """Get worst performing position"""
        worst_position = None
        worst_performance = Decimal('999999')
        
        for position in self.positions.values():
            if position.unrealized_pnl_pct < worst_performance:
                worst_performance = position.unrealized_pnl_pct
                worst_position = position
        
        if worst_position:
            return {
                'symbol': worst_position.symbol,
                'pnl_pct': float(worst_position.unrealized_pnl_pct),
                'pnl_usd': float(worst_position.unrealized_pnl_usd)
            }
        
        return {'symbol': 'N/A', 'pnl_pct': 0, 'pnl_usd': 0}
    
    def clear_positions(self) -> None:
        """Clear all positions"""
        self.positions.clear()
        self.metrics = PortfolioMetrics()
        logger.info("Portfolio positions cleared")


# Global valuation engine instance
_valuation_engine = None


def get_valuation_engine() -> PortfolioValuationEngine:
    """Get or create global valuation engine instance"""
    global _valuation_engine
    if _valuation_engine is None:
        _valuation_engine = PortfolioValuationEngine()
    return _valuation_engine


def refresh_portfolio_valuation(fifo_positions_df: pd.DataFrame = None, 
                              staged_transactions_df: pd.DataFrame = None) -> Dict[str, Any]:
    """Convenience function to refresh portfolio valuation"""
    engine = get_valuation_engine()
    
    # Load positions
    if fifo_positions_df is not None and not fifo_positions_df.empty:
        engine.load_positions_from_fifo(fifo_positions_df)
    elif staged_transactions_df is not None and not staged_transactions_df.empty:
        engine.load_positions_from_staged_transactions(staged_transactions_df)
    
    # Update market values
    engine.update_market_values()
    
    # Return performance summary
    return engine.get_performance_summary()