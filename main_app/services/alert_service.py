# -*- coding: utf-8 -*-
"""
Alert Service Module

Comprehensive alert and notification system for cryptocurrency portfolio monitoring.
Supports price alerts, portfolio thresholds, risk notifications, and performance alerts.
"""

import pandas as pd
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable, Any, Union
from dataclasses import dataclass, asdict
from enum import Enum
import logging
import json
from decimal import Decimal

from .price_service import get_price_service
from .portfolio_valuation import get_valuation_engine
from .cache_manager import get_cache_manager

# Set up logging
logger = logging.getLogger(__name__)


class AlertType(Enum):
    """Types of alerts"""
    PRICE_ABOVE = "price_above"
    PRICE_BELOW = "price_below"
    PRICE_CHANGE_PCT = "price_change_pct"
    PORTFOLIO_VALUE = "portfolio_value"
    POSITION_SIZE = "position_size"
    VOLUME_SPIKE = "volume_spike"
    LARGE_TRANSACTION = "large_transaction"
    RISK_THRESHOLD = "risk_threshold"
    PERFORMANCE_MILESTONE = "performance_milestone"


class AlertStatus(Enum):
    """Alert status states"""
    ACTIVE = "active"
    TRIGGERED = "triggered"
    PAUSED = "paused"
    EXPIRED = "expired"
    DELETED = "deleted"


class AlertPriority(Enum):
    """Alert priority levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class AlertCondition:
    """Individual alert condition definition"""
    alert_id: str
    name: str
    alert_type: AlertType
    symbol: Optional[str]  # None for portfolio-wide alerts
    condition_value: Decimal
    comparison: str  # 'above', 'below', 'equals'
    priority: AlertPriority
    status: AlertStatus
    created_at: datetime
    triggered_at: Optional[datetime] = None
    last_checked: Optional[datetime] = None
    trigger_count: int = 0
    cooldown_minutes: int = 60  # Minimum time between triggers
    expires_at: Optional[datetime] = None
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
    
    def is_expired(self) -> bool:
        """Check if alert has expired"""
        if self.expires_at is None:
            return False
        return datetime.now() > self.expires_at
    
    def is_in_cooldown(self) -> bool:
        """Check if alert is in cooldown period"""
        if self.triggered_at is None:
            return False
        return datetime.now() < self.triggered_at + timedelta(minutes=self.cooldown_minutes)
    
    def can_trigger(self) -> bool:
        """Check if alert can be triggered"""
        return (
            self.status == AlertStatus.ACTIVE and
            not self.is_expired() and
            not self.is_in_cooldown()
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'alert_id': self.alert_id,
            'name': self.name,
            'alert_type': self.alert_type.value,
            'symbol': self.symbol,
            'condition_value': float(self.condition_value),
            'comparison': self.comparison,
            'priority': self.priority.value,
            'status': self.status.value,
            'created_at': self.created_at.isoformat(),
            'triggered_at': self.triggered_at.isoformat() if self.triggered_at else None,
            'last_checked': self.last_checked.isoformat() if self.last_checked else None,
            'trigger_count': self.trigger_count,
            'cooldown_minutes': self.cooldown_minutes,
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'metadata': self.metadata
        }


@dataclass
class AlertNotification:
    """Triggered alert notification"""
    alert_id: str
    alert_name: str
    alert_type: AlertType
    symbol: Optional[str]
    current_value: Decimal
    condition_value: Decimal
    priority: AlertPriority
    message: str
    triggered_at: datetime
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'alert_id': self.alert_id,
            'alert_name': self.alert_name,
            'alert_type': self.alert_type.value,
            'symbol': self.symbol,
            'current_value': float(self.current_value),
            'condition_value': float(self.condition_value),
            'priority': self.priority.value,
            'message': self.message,
            'triggered_at': self.triggered_at.isoformat(),
            'metadata': self.metadata
        }


class AlertEngine:
    """Core alert processing engine"""
    
    def __init__(self):
        self.alerts: Dict[str, AlertCondition] = {}
        self.notifications: List[AlertNotification] = []
        self.price_service = get_price_service()
        self.valuation_engine = get_valuation_engine()
        self.cache_manager = get_cache_manager()
        
        # Monitoring thread
        self._monitoring_thread = None
        self._stop_monitoring = False
        self._check_interval = 60  # Check every minute
        
        logger.info("AlertEngine initialized")
    
    def create_alert(self, name: str, alert_type: AlertType, condition_value: Decimal,
                    symbol: str = None, comparison: str = "above", 
                    priority: AlertPriority = AlertPriority.MEDIUM,
                    cooldown_minutes: int = 60, expires_in_hours: int = None) -> str:
        """
        Create a new alert condition
        
        Returns:
            alert_id of the created alert
        """
        alert_id = f"alert_{int(time.time())}_{hash(name) % 10000}"
        
        expires_at = None
        if expires_in_hours:
            expires_at = datetime.now() + timedelta(hours=expires_in_hours)
        
        alert = AlertCondition(
            alert_id=alert_id,
            name=name,
            alert_type=alert_type,
            symbol=symbol.upper() if symbol else None,
            condition_value=Decimal(str(condition_value)),
            comparison=comparison.lower(),
            priority=priority,
            status=AlertStatus.ACTIVE,
            created_at=datetime.now(),
            cooldown_minutes=cooldown_minutes,
            expires_at=expires_at
        )
        
        self.alerts[alert_id] = alert
        
        logger.info(f"Created alert '{name}' ({alert_id}): {alert_type.value} {comparison} {condition_value}")
        
        return alert_id
    
    def delete_alert(self, alert_id: str) -> bool:
        """Delete an alert"""
        if alert_id in self.alerts:
            self.alerts[alert_id].status = AlertStatus.DELETED
            logger.info(f"Deleted alert {alert_id}")
            return True
        return False
    
    def pause_alert(self, alert_id: str) -> bool:
        """Pause an alert"""
        if alert_id in self.alerts:
            self.alerts[alert_id].status = AlertStatus.PAUSED
            logger.info(f"Paused alert {alert_id}")
            return True
        return False
    
    def resume_alert(self, alert_id: str) -> bool:
        """Resume a paused alert"""
        if alert_id in self.alerts:
            alert = self.alerts[alert_id]
            if alert.status == AlertStatus.PAUSED:
                alert.status = AlertStatus.ACTIVE
                logger.info(f"Resumed alert {alert_id}")
                return True
        return False
    
    def get_alerts(self, include_deleted: bool = False) -> List[AlertCondition]:
        """Get all alerts"""
        alerts = list(self.alerts.values())
        
        if not include_deleted:
            alerts = [a for a in alerts if a.status != AlertStatus.DELETED]
        
        return sorted(alerts, key=lambda x: x.created_at, reverse=True)
    
    def get_alert(self, alert_id: str) -> Optional[AlertCondition]:
        """Get specific alert"""
        return self.alerts.get(alert_id)
    
    def get_notifications(self, limit: int = 50) -> List[AlertNotification]:
        """Get recent notifications"""
        return sorted(self.notifications, key=lambda x: x.triggered_at, reverse=True)[:limit]
    
    def clear_notifications(self) -> None:
        """Clear all notifications"""
        self.notifications.clear()
        logger.info("Cleared all notifications")
    
    def check_alerts(self) -> List[AlertNotification]:
        """Check all active alerts and trigger notifications"""
        new_notifications = []
        
        # Get current market data
        try:
            # Update portfolio if needed
            self._update_portfolio_data()
            
            for alert in self.alerts.values():
                if not alert.can_trigger():
                    continue
                
                alert.last_checked = datetime.now()
                
                # Check specific alert type
                triggered, current_value, message = self._evaluate_alert_condition(alert)
                
                if triggered:
                    # Create notification
                    notification = AlertNotification(
                        alert_id=alert.alert_id,
                        alert_name=alert.name,
                        alert_type=alert.alert_type,
                        symbol=alert.symbol,
                        current_value=current_value,
                        condition_value=alert.condition_value,
                        priority=alert.priority,
                        message=message,
                        triggered_at=datetime.now(),
                        metadata=alert.metadata.copy()
                    )
                    
                    # Update alert status
                    alert.status = AlertStatus.TRIGGERED
                    alert.triggered_at = datetime.now()
                    alert.trigger_count += 1
                    
                    # Store notification
                    self.notifications.append(notification)
                    new_notifications.append(notification)
                    
                    logger.info(f"Alert triggered: {alert.name} - {message}")
        
        except Exception as e:
            logger.error(f"Error checking alerts: {e}")
        
        return new_notifications
    
    def _update_portfolio_data(self):
        """Update portfolio data for alert evaluation"""
        try:
            # This would typically load and refresh portfolio data
            # For now, we'll use staged transactions if available
            from ..modules.general_ledger.crypto_token_fetch import get_staged_transactions_global
            staged_df = get_staged_transactions_global()
            
            if not staged_df.empty:
                self.valuation_engine.load_positions_from_staged_transactions(staged_df)
                self.valuation_engine.update_market_values()
                
        except Exception as e:
            logger.debug(f"Could not update portfolio data for alerts: {e}")
    
    def _evaluate_alert_condition(self, alert: AlertCondition) -> tuple[bool, Decimal, str]:
        """
        Evaluate a single alert condition
        
        Returns:
            (triggered, current_value, message)
        """
        try:
            if alert.alert_type == AlertType.PRICE_ABOVE:
                return self._check_price_alert(alert, "above")
            
            elif alert.alert_type == AlertType.PRICE_BELOW:
                return self._check_price_alert(alert, "below")
            
            elif alert.alert_type == AlertType.PRICE_CHANGE_PCT:
                return self._check_price_change_alert(alert)
            
            elif alert.alert_type == AlertType.PORTFOLIO_VALUE:
                return self._check_portfolio_value_alert(alert)
            
            elif alert.alert_type == AlertType.POSITION_SIZE:
                return self._check_position_size_alert(alert)
            
            else:
                logger.warning(f"Unknown alert type: {alert.alert_type}")
                return False, Decimal('0'), "Unknown alert type"
                
        except Exception as e:
            logger.error(f"Error evaluating alert {alert.alert_id}: {e}")
            return False, Decimal('0'), f"Evaluation error: {e}"
    
    def _check_price_alert(self, alert: AlertCondition, direction: str) -> tuple[bool, Decimal, str]:
        """Check price threshold alerts"""
        if not alert.symbol:
            return False, Decimal('0'), "No symbol specified for price alert"
        
        current_price = self.price_service.get_current_price(alert.symbol, 'usd')
        if current_price is None:
            return False, Decimal('0'), f"Could not get price for {alert.symbol}"
        
        triggered = False
        if direction == "above" and current_price > alert.condition_value:
            triggered = True
        elif direction == "below" and current_price < alert.condition_value:
            triggered = True
        
        if triggered:
            message = f"{alert.symbol} price ${current_price:,.2f} is {direction} threshold ${alert.condition_value:,.2f}"
        else:
            message = f"{alert.symbol} price ${current_price:,.2f} (threshold: ${alert.condition_value:,.2f})"
        
        return triggered, current_price, message
    
    def _check_price_change_alert(self, alert: AlertCondition) -> tuple[bool, Decimal, str]:
        """Check price change percentage alerts"""
        if not alert.symbol:
            return False, Decimal('0'), "No symbol specified for price change alert"
        
        # Get current price data with 24h change
        price_data = self.price_service.get_current_prices([alert.symbol], ['usd'])
        symbol_data = price_data.get(alert.symbol.upper(), {})
        
        if not symbol_data:
            return False, Decimal('0'), f"Could not get price data for {alert.symbol}"
        
        change_24h = symbol_data.get('change_24h', Decimal('0'))
        current_price = symbol_data.get('usd', Decimal('0'))
        
        triggered = abs(change_24h) >= alert.condition_value
        
        if triggered:
            direction = "increased" if change_24h > 0 else "decreased"
            message = f"{alert.symbol} {direction} {abs(change_24h):.2f}% in 24h (threshold: {alert.condition_value:.2f}%)"
        else:
            message = f"{alert.symbol} changed {change_24h:+.2f}% in 24h"
        
        return triggered, abs(change_24h), message
    
    def _check_portfolio_value_alert(self, alert: AlertCondition) -> tuple[bool, Decimal, str]:
        """Check portfolio value threshold alerts"""
        portfolio_value = self.valuation_engine.metrics.total_value_usd
        
        triggered = False
        if alert.comparison == "above" and portfolio_value > alert.condition_value:
            triggered = True
        elif alert.comparison == "below" and portfolio_value < alert.condition_value:
            triggered = True
        
        if triggered:
            message = f"Portfolio value ${portfolio_value:,.2f} is {alert.comparison} threshold ${alert.condition_value:,.2f}"
        else:
            message = f"Portfolio value ${portfolio_value:,.2f} (threshold: ${alert.condition_value:,.2f})"
        
        return triggered, portfolio_value, message
    
    def _check_position_size_alert(self, alert: AlertCondition) -> tuple[bool, Decimal, str]:
        """Check individual position size alerts"""
        if not alert.symbol:
            return False, Decimal('0'), "No symbol specified for position alert"
        
        # Get position for specific symbol
        position = self.valuation_engine.positions.get(alert.symbol.upper())
        if not position:
            return False, Decimal('0'), f"No position found for {alert.symbol}"
        
        position_value = position.market_value_usd
        
        triggered = False
        if alert.comparison == "above" and position_value > alert.condition_value:
            triggered = True
        elif alert.comparison == "below" and position_value < alert.condition_value:
            triggered = True
        
        if triggered:
            message = f"{alert.symbol} position ${position_value:,.2f} is {alert.comparison} threshold ${alert.condition_value:,.2f}"
        else:
            message = f"{alert.symbol} position ${position_value:,.2f}"
        
        return triggered, position_value, message
    
    def start_monitoring(self) -> None:
        """Start background alert monitoring"""
        if self._monitoring_thread and self._monitoring_thread.is_alive():
            logger.warning("Alert monitoring already running")
            return
        
        self._stop_monitoring = False
        self._monitoring_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self._monitoring_thread.start()
        
        logger.info(f"Started alert monitoring (check interval: {self._check_interval}s)")
    
    def stop_monitoring(self) -> None:
        """Stop background alert monitoring"""
        self._stop_monitoring = True
        if self._monitoring_thread:
            self._monitoring_thread.join(timeout=5)
        
        logger.info("Stopped alert monitoring")
    
    def _monitoring_loop(self) -> None:
        """Background monitoring loop"""
        while not self._stop_monitoring:
            try:
                new_notifications = self.check_alerts()
                if new_notifications:
                    logger.info(f"Triggered {len(new_notifications)} alerts")
                
                # Cleanup expired alerts
                self._cleanup_expired_alerts()
                
            except Exception as e:
                logger.error(f"Alert monitoring error: {e}")
            
            # Wait for next check
            time.sleep(self._check_interval)
    
    def _cleanup_expired_alerts(self) -> None:
        """Remove expired alerts"""
        expired_count = 0
        for alert in list(self.alerts.values()):
            if alert.is_expired():
                alert.status = AlertStatus.EXPIRED
                expired_count += 1
        
        if expired_count > 0:
            logger.debug(f"Marked {expired_count} alerts as expired")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get alert engine statistics"""
        alerts = list(self.alerts.values())
        
        return {
            'total_alerts': len(alerts),
            'active_alerts': len([a for a in alerts if a.status == AlertStatus.ACTIVE]),
            'triggered_alerts': len([a for a in alerts if a.status == AlertStatus.TRIGGERED]),
            'paused_alerts': len([a for a in alerts if a.status == AlertStatus.PAUSED]),
            'expired_alerts': len([a for a in alerts if a.status == AlertStatus.EXPIRED]),
            'total_notifications': len(self.notifications),
            'monitoring_active': self._monitoring_thread is not None and self._monitoring_thread.is_alive(),
            'check_interval_seconds': self._check_interval
        }


# Global alert engine instance
_alert_engine = None


def get_alert_engine() -> AlertEngine:
    """Get global alert engine instance"""
    global _alert_engine
    if _alert_engine is None:
        _alert_engine = AlertEngine()
    return _alert_engine


# Convenience functions for common alert types
def create_price_alert(symbol: str, price: Union[float, Decimal], above: bool = True,
                      name: str = None, priority: AlertPriority = AlertPriority.MEDIUM) -> str:
    """Create a price threshold alert"""
    engine = get_alert_engine()
    
    if name is None:
        direction = "above" if above else "below"
        name = f"{symbol} price {direction} ${price}"
    
    alert_type = AlertType.PRICE_ABOVE if above else AlertType.PRICE_BELOW
    comparison = "above" if above else "below"
    
    return engine.create_alert(
        name=name,
        alert_type=alert_type,
        condition_value=Decimal(str(price)),
        symbol=symbol,
        comparison=comparison,
        priority=priority
    )


def create_portfolio_alert(value: Union[float, Decimal], above: bool = True,
                          name: str = None, priority: AlertPriority = AlertPriority.MEDIUM) -> str:
    """Create a portfolio value threshold alert"""
    engine = get_alert_engine()
    
    if name is None:
        direction = "above" if above else "below"
        name = f"Portfolio value {direction} ${value:,.0f}"
    
    comparison = "above" if above else "below"
    
    return engine.create_alert(
        name=name,
        alert_type=AlertType.PORTFOLIO_VALUE,
        condition_value=Decimal(str(value)),
        comparison=comparison,
        priority=priority
    )


def create_price_change_alert(symbol: str, change_percent: Union[float, Decimal],
                             name: str = None, priority: AlertPriority = AlertPriority.HIGH) -> str:
    """Create a price change percentage alert"""
    engine = get_alert_engine()
    
    if name is None:
        name = f"{symbol} price change ±{change_percent}%"
    
    return engine.create_alert(
        name=name,
        alert_type=AlertType.PRICE_CHANGE_PCT,
        condition_value=Decimal(str(change_percent)),
        symbol=symbol,
        priority=priority
    )