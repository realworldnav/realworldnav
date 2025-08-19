# -*- coding: utf-8 -*-
"""
Performance Metrics Module

Advanced portfolio performance analytics including time-weighted returns,
risk metrics, and statistical analysis for cryptocurrency portfolios.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from decimal import Decimal, ROUND_HALF_EVEN
import logging
from scipy import stats
import math

from .price_service import get_price_service

# Set up logging
logger = logging.getLogger(__name__)

# Constants for calculations
TRADING_DAYS_PER_YEAR = 365
RISK_FREE_RATE = Decimal('0.05')  # 5% risk-free rate


class PerformanceCalculator:
    """Calculate advanced performance metrics for cryptocurrency portfolios"""
    
    def __init__(self):
        self.price_service = get_price_service()
    
    def calculate_time_weighted_return(self, portfolio_values: List[Tuple[datetime, Decimal]], 
                                     cash_flows: List[Tuple[datetime, Decimal]] = None) -> Decimal:
        """
        Calculate Time-Weighted Return (TWR)
        
        Args:
            portfolio_values: List of (datetime, portfolio_value) tuples
            cash_flows: List of (datetime, cash_flow) tuples (positive for inflows)
        
        Returns:
            Annualized TWR as decimal (0.15 = 15%)
        """
        if len(portfolio_values) < 2:
            return Decimal('0')
        
        cash_flows = cash_flows or []
        cash_flow_dict = {date: amount for date, amount in cash_flows}
        
        # Calculate period returns
        period_returns = []
        
        for i in range(1, len(portfolio_values)):
            prev_date, prev_value = portfolio_values[i-1]
            curr_date, curr_value = portfolio_values[i]
            
            # Adjust for cash flows during the period
            cash_flow = cash_flow_dict.get(curr_date, Decimal('0'))
            adjusted_prev_value = prev_value + cash_flow
            
            if adjusted_prev_value > 0:
                period_return = (curr_value - adjusted_prev_value) / adjusted_prev_value
                period_returns.append(period_return)
        
        if not period_returns:
            return Decimal('0')
        
        # Calculate compound return
        compound_return = Decimal('1')
        for return_rate in period_returns:
            compound_return *= (Decimal('1') + return_rate)
        
        # Annualize based on time period
        start_date = portfolio_values[0][0]
        end_date = portfolio_values[-1][0]
        days = (end_date - start_date).days
        
        if days == 0:
            return Decimal('0')
        
        years = Decimal(str(days)) / Decimal(str(TRADING_DAYS_PER_YEAR))
        
        if years > 0:
            annualized_return = compound_return ** (Decimal('1') / years) - Decimal('1')
        else:
            annualized_return = compound_return - Decimal('1')
        
        return annualized_return
    
    def calculate_money_weighted_return(self, cash_flows: List[Tuple[datetime, Decimal]], 
                                       final_value: Decimal) -> Decimal:
        """
        Calculate Money-Weighted Return (Internal Rate of Return)
        
        Args:
            cash_flows: List of (datetime, cash_flow) tuples (negative for outflows)
            final_value: Final portfolio value
        
        Returns:
            Annualized IRR as decimal
        """
        if not cash_flows:
            return Decimal('0')
        
        # Convert to numpy arrays for calculation
        dates = [cf[0] for cf in cash_flows]
        flows = [float(cf[1]) for cf in cash_flows]
        
        # Add final value as positive cash flow at the end
        flows.append(float(final_value))
        
        # Calculate time periods in years
        start_date = min(dates)
        periods = [(date - start_date).days / TRADING_DAYS_PER_YEAR for date in dates]
        periods.append((datetime.now() - start_date).days / TRADING_DAYS_PER_YEAR)
        
        # Use Newton-Raphson method to find IRR
        def npv(rate):
            return sum(cf / ((1 + rate) ** period) for cf, period in zip(flows, periods))
        
        def npv_derivative(rate):
            return sum(-period * cf / ((1 + rate) ** (period + 1)) for cf, period in zip(flows, periods))
        
        # Initial guess
        rate = 0.1
        
        # Newton-Raphson iterations
        for _ in range(100):  # Maximum iterations
            try:
                rate_new = rate - npv(rate) / npv_derivative(rate)
                if abs(rate_new - rate) < 1e-6:  # Convergence threshold
                    return Decimal(str(rate_new))
                rate = rate_new
            except (ZeroDivisionError, OverflowError):
                break
        
        return Decimal('0')  # Return 0 if unable to converge
    
    def calculate_sharpe_ratio(self, returns: List[Decimal], risk_free_rate: Decimal = None) -> Decimal:
        """
        Calculate Sharpe Ratio
        
        Args:
            returns: List of period returns
            risk_free_rate: Risk-free rate (annualized)
        
        Returns:
            Sharpe ratio
        """
        if not returns or len(returns) < 2:
            return Decimal('0')
        
        risk_free_rate = risk_free_rate or RISK_FREE_RATE
        
        # Convert to numpy for calculation
        returns_array = np.array([float(r) for r in returns])
        
        # Calculate excess returns
        excess_returns = returns_array - float(risk_free_rate) / TRADING_DAYS_PER_YEAR
        
        # Calculate mean and standard deviation
        mean_excess_return = np.mean(excess_returns)
        std_excess_return = np.std(excess_returns, ddof=1)
        
        if std_excess_return == 0:
            return Decimal('0')
        
        # Annualize
        sharpe = (mean_excess_return / std_excess_return) * math.sqrt(TRADING_DAYS_PER_YEAR)
        
        return Decimal(str(sharpe))
    
    def calculate_maximum_drawdown(self, portfolio_values: List[Decimal]) -> Dict[str, Decimal]:
        """
        Calculate Maximum Drawdown
        
        Args:
            portfolio_values: List of portfolio values over time
        
        Returns:
            Dict with max_drawdown, peak_value, trough_value, recovery_date
        """
        if len(portfolio_values) < 2:
            return {'max_drawdown': Decimal('0'), 'peak_value': Decimal('0'), 'trough_value': Decimal('0')}
        
        values = [float(v) for v in portfolio_values]
        
        # Track running maximum (peak)
        running_max = values[0]
        max_drawdown = 0
        peak_value = values[0]
        trough_value = values[0]
        
        for value in values[1:]:
            if value > running_max:
                running_max = value
            
            # Calculate drawdown from current peak
            drawdown = (running_max - value) / running_max if running_max > 0 else 0
            
            if drawdown > max_drawdown:
                max_drawdown = drawdown
                peak_value = running_max
                trough_value = value
        
        return {
            'max_drawdown': Decimal(str(max_drawdown)),
            'peak_value': Decimal(str(peak_value)),
            'trough_value': Decimal(str(trough_value))
        }
    
    def calculate_volatility(self, returns: List[Decimal]) -> Decimal:
        """Calculate annualized volatility"""
        if len(returns) < 2:
            return Decimal('0')
        
        returns_array = np.array([float(r) for r in returns])
        daily_vol = np.std(returns_array, ddof=1)
        annualized_vol = daily_vol * math.sqrt(TRADING_DAYS_PER_YEAR)
        
        return Decimal(str(annualized_vol))
    
    def calculate_beta(self, portfolio_returns: List[Decimal], benchmark_returns: List[Decimal]) -> Decimal:
        """
        Calculate Beta against benchmark (e.g., ETH or BTC)
        
        Args:
            portfolio_returns: Portfolio period returns
            benchmark_returns: Benchmark period returns
        
        Returns:
            Beta coefficient
        """
        if len(portfolio_returns) != len(benchmark_returns) or len(portfolio_returns) < 2:
            return Decimal('0')
        
        port_returns = np.array([float(r) for r in portfolio_returns])
        bench_returns = np.array([float(r) for r in benchmark_returns])
        
        # Calculate covariance and variance
        covariance = np.cov(port_returns, bench_returns)[0, 1]
        benchmark_variance = np.var(bench_returns, ddof=1)
        
        if benchmark_variance == 0:
            return Decimal('0')
        
        beta = covariance / benchmark_variance
        return Decimal(str(beta))
    
    def calculate_information_ratio(self, portfolio_returns: List[Decimal], 
                                  benchmark_returns: List[Decimal]) -> Decimal:
        """Calculate Information Ratio (excess return / tracking error)"""
        if len(portfolio_returns) != len(benchmark_returns) or len(portfolio_returns) < 2:
            return Decimal('0')
        
        port_returns = np.array([float(r) for r in portfolio_returns])
        bench_returns = np.array([float(r) for r in benchmark_returns])
        
        # Calculate excess returns
        excess_returns = port_returns - bench_returns
        
        # Calculate mean excess return and tracking error
        mean_excess_return = np.mean(excess_returns)
        tracking_error = np.std(excess_returns, ddof=1)
        
        if tracking_error == 0:
            return Decimal('0')
        
        # Annualize
        info_ratio = (mean_excess_return / tracking_error) * math.sqrt(TRADING_DAYS_PER_YEAR)
        
        return Decimal(str(info_ratio))
    
    def get_benchmark_returns(self, symbol: str, days: int = 30) -> List[Decimal]:
        """Get benchmark returns for comparison"""
        try:
            # Get historical prices for benchmark
            price_history = self.price_service.get_historical_prices(symbol, days)
            
            if price_history.empty:
                return []
            
            # Calculate daily returns
            returns = []
            prev_price = None
            
            for _, row in price_history.iterrows():
                current_price = Decimal(str(row['price_usd']))
                if prev_price is not None and prev_price > 0:
                    daily_return = (current_price - prev_price) / prev_price
                    returns.append(daily_return)
                prev_price = current_price
            
            return returns
            
        except Exception as e:
            logger.error(f"Error getting benchmark returns for {symbol}: {e}")
            return []


class PortfolioRiskAnalyzer:
    """Advanced risk analysis for cryptocurrency portfolios"""
    
    def __init__(self):
        self.performance_calc = PerformanceCalculator()
    
    def calculate_value_at_risk(self, returns: List[Decimal], confidence_level: float = 0.95) -> Decimal:
        """
        Calculate Value at Risk (VaR) using historical simulation
        
        Args:
            returns: Historical returns
            confidence_level: Confidence level (0.95 = 95%)
        
        Returns:
            VaR as percentage loss
        """
        if len(returns) < 10:  # Need sufficient data
            return Decimal('0')
        
        returns_array = np.array([float(r) for r in returns])
        
        # Calculate percentile
        var_percentile = (1 - confidence_level) * 100
        var = np.percentile(returns_array, var_percentile)
        
        return Decimal(str(abs(var)))  # Return as positive value
    
    def calculate_conditional_var(self, returns: List[Decimal], confidence_level: float = 0.95) -> Decimal:
        """Calculate Conditional VaR (Expected Shortfall)"""
        if len(returns) < 10:
            return Decimal('0')
        
        returns_array = np.array([float(r) for r in returns])
        
        # Calculate VaR threshold
        var_percentile = (1 - confidence_level) * 100
        var_threshold = np.percentile(returns_array, var_percentile)
        
        # Calculate mean of returns below VaR threshold
        tail_returns = returns_array[returns_array <= var_threshold]
        
        if len(tail_returns) == 0:
            return Decimal('0')
        
        cvar = abs(np.mean(tail_returns))
        return Decimal(str(cvar))
    
    def analyze_portfolio_concentration(self, positions: Dict[str, Decimal]) -> Dict[str, Any]:
        """Analyze portfolio concentration risk"""
        if not positions:
            return {'concentration_risk': 'Low', 'hhi_index': 0, 'top_3_concentration': 0}
        
        total_value = sum(positions.values())
        if total_value == 0:
            return {'concentration_risk': 'Low', 'hhi_index': 0, 'top_3_concentration': 0}
        
        # Calculate weights
        weights = [float(value / total_value) for value in positions.values()]
        
        # Calculate Herfindahl-Hirschman Index (HHI)
        hhi = sum(w ** 2 for w in weights)
        
        # Calculate top 3 concentration
        sorted_weights = sorted(weights, reverse=True)
        top_3_concentration = sum(sorted_weights[:3])
        
        # Determine concentration risk level
        if hhi > 0.25:
            risk_level = 'High'
        elif hhi > 0.15:
            risk_level = 'Medium'
        else:
            risk_level = 'Low'
        
        return {
            'concentration_risk': risk_level,
            'hhi_index': hhi,
            'top_3_concentration': top_3_concentration,
            'largest_position_weight': max(weights) if weights else 0
        }
    
    def calculate_portfolio_correlations(self, symbols: List[str], days: int = 30) -> pd.DataFrame:
        """Calculate correlation matrix between portfolio assets"""
        if len(symbols) < 2:
            return pd.DataFrame()
        
        # Get returns for all symbols
        returns_data = {}
        
        for symbol in symbols:
            returns = self.performance_calc.get_benchmark_returns(symbol, days)
            if returns:
                returns_data[symbol] = [float(r) for r in returns]
        
        if len(returns_data) < 2:
            return pd.DataFrame()
        
        # Create DataFrame and calculate correlations
        min_length = min(len(returns) for returns in returns_data.values())
        
        # Truncate all return series to the same length
        for symbol in returns_data:
            returns_data[symbol] = returns_data[symbol][:min_length]
        
        returns_df = pd.DataFrame(returns_data)
        correlation_matrix = returns_df.corr()
        
        return correlation_matrix


class PerformanceReporter:
    """Generate comprehensive performance reports"""
    
    def __init__(self):
        self.calc = PerformanceCalculator()
        self.risk_analyzer = PortfolioRiskAnalyzer()
    
    def generate_performance_summary(self, portfolio_data: Dict[str, Any], 
                                   benchmark_symbol: str = 'ETH') -> Dict[str, Any]:
        """
        Generate comprehensive performance summary
        
        Args:
            portfolio_data: Portfolio positions and historical values
            benchmark_symbol: Benchmark for comparison
        
        Returns:
            Complete performance metrics
        """
        try:
            # Extract data from portfolio
            positions = portfolio_data.get('positions', {})
            historical_values = portfolio_data.get('historical_values', [])
            cash_flows = portfolio_data.get('cash_flows', [])
            
            # Calculate basic metrics
            current_value = historical_values[-1][1] if historical_values else Decimal('0')
            
            # Performance metrics
            twr = self.calc.calculate_time_weighted_return(historical_values, cash_flows)
            mwr = self.calc.calculate_money_weighted_return(cash_flows, current_value)
            
            # Risk metrics
            if len(historical_values) > 1:
                portfolio_returns = []
                for i in range(1, len(historical_values)):
                    prev_val = historical_values[i-1][1]
                    curr_val = historical_values[i][1]
                    if prev_val > 0:
                        ret = (curr_val - prev_val) / prev_val
                        portfolio_returns.append(ret)
                
                volatility = self.calc.calculate_volatility(portfolio_returns)
                sharpe = self.calc.calculate_sharpe_ratio(portfolio_returns)
                max_dd = self.calc.calculate_maximum_drawdown([v[1] for v in historical_values])
                var_95 = self.risk_analyzer.calculate_value_at_risk(portfolio_returns)
                
                # Benchmark comparison
                benchmark_returns = self.calc.get_benchmark_returns(benchmark_symbol)
                beta = Decimal('0')
                info_ratio = Decimal('0')
                
                if benchmark_returns and len(benchmark_returns) == len(portfolio_returns):
                    beta = self.calc.calculate_beta(portfolio_returns, benchmark_returns)
                    info_ratio = self.calc.calculate_information_ratio(portfolio_returns, benchmark_returns)
            else:
                volatility = Decimal('0')
                sharpe = Decimal('0')
                max_dd = {'max_drawdown': Decimal('0')}
                var_95 = Decimal('0')
                beta = Decimal('0')
                info_ratio = Decimal('0')
            
            # Concentration analysis
            position_values = {k: v for k, v in positions.items() if isinstance(v, (int, float, Decimal))}
            concentration = self.risk_analyzer.analyze_portfolio_concentration(position_values)
            
            return {
                'performance': {
                    'time_weighted_return': float(twr),
                    'money_weighted_return': float(mwr),
                    'current_value': float(current_value),
                    'total_return_pct': float(twr * 100) if twr else 0
                },
                'risk': {
                    'volatility': float(volatility),
                    'sharpe_ratio': float(sharpe),
                    'maximum_drawdown': float(max_dd['max_drawdown']),
                    'value_at_risk_95': float(var_95),
                    'beta': float(beta)
                },
                'comparison': {
                    'benchmark_symbol': benchmark_symbol,
                    'information_ratio': float(info_ratio),
                    'excess_return': float(twr - self.calc.get_benchmark_returns(benchmark_symbol, 1)[0]) if twr else 0
                },
                'concentration': concentration,
                'generated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error generating performance summary: {e}")
            return {
                'performance': {},
                'risk': {},
                'comparison': {},
                'concentration': {},
                'error': str(e),
                'generated_at': datetime.now().isoformat()
            }


# Global instances
_performance_calculator = None
_risk_analyzer = None
_performance_reporter = None


def get_performance_calculator() -> PerformanceCalculator:
    """Get global performance calculator instance"""
    global _performance_calculator
    if _performance_calculator is None:
        _performance_calculator = PerformanceCalculator()
    return _performance_calculator


def get_risk_analyzer() -> PortfolioRiskAnalyzer:
    """Get global risk analyzer instance"""
    global _risk_analyzer
    if _risk_analyzer is None:
        _risk_analyzer = PortfolioRiskAnalyzer()
    return _risk_analyzer


def get_performance_reporter() -> PerformanceReporter:
    """Get global performance reporter instance"""
    global _performance_reporter
    if _performance_reporter is None:
        _performance_reporter = PerformanceReporter()
    return _performance_reporter