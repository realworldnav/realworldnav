# -*- coding: utf-8 -*-
"""
Cache Manager Module

Advanced caching layer with TTL support, background refresh, and S3 persistence
for cryptocurrency portfolio data and price information.
"""

import time
import threading
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict
import logging
import pickle
import os
from functools import wraps

# Set up logging
logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    """Individual cache entry with metadata"""
    key: str
    value: Any
    timestamp: float
    ttl: int  # Time to live in seconds
    access_count: int = 0
    last_access: float = 0
    size_bytes: int = 0
    
    def __post_init__(self):
        if self.last_access == 0:
            self.last_access = self.timestamp
        
        # Estimate size if not provided
        if self.size_bytes == 0:
            try:
                self.size_bytes = len(pickle.dumps(self.value))
            except:
                self.size_bytes = 1024  # Default estimate
    
    def is_expired(self) -> bool:
        """Check if cache entry has expired"""
        return time.time() - self.timestamp > self.ttl
    
    def refresh_access(self):
        """Update access statistics"""
        self.access_count += 1
        self.last_access = time.time()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'key': self.key,
            'timestamp': self.timestamp,
            'ttl': self.ttl,
            'access_count': self.access_count,
            'last_access': self.last_access,
            'size_bytes': self.size_bytes
        }


class CacheManager:
    """Advanced cache manager with TTL and statistics"""
    
    def __init__(self, max_size_mb: int = 100, cleanup_interval: int = 300):
        """
        Initialize cache manager
        
        Args:
            max_size_mb: Maximum cache size in megabytes
            cleanup_interval: Cleanup interval in seconds
        """
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = threading.RLock()
        self.max_size_bytes = max_size_mb * 1024 * 1024
        self.cleanup_interval = cleanup_interval
        
        # Statistics
        self.stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'cleanups': 0,
            'size_bytes': 0,
            'entry_count': 0
        }
        
        # Background cleanup
        self._cleanup_timer = None
        self._start_cleanup_timer()
        
        logger.info(f"CacheManager initialized: max_size={max_size_mb}MB, cleanup_interval={cleanup_interval}s")
    
    def _start_cleanup_timer(self):
        """Start background cleanup timer"""
        if self._cleanup_timer:
            self._cleanup_timer.cancel()
        
        self._cleanup_timer = threading.Timer(self.cleanup_interval, self._background_cleanup)
        self._cleanup_timer.daemon = True
        self._cleanup_timer.start()
    
    def _background_cleanup(self):
        """Background cleanup of expired entries"""
        try:
            self.cleanup_expired()
            self._start_cleanup_timer()  # Restart timer
        except Exception as e:
            logger.error(f"Background cleanup error: {e}")
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get value from cache"""
        with self._lock:
            entry = self._cache.get(key)
            
            if entry is None:
                self.stats['misses'] += 1
                return default
            
            if entry.is_expired():
                # Remove expired entry
                del self._cache[key]
                self._update_stats()
                self.stats['misses'] += 1
                return default
            
            # Update access statistics
            entry.refresh_access()
            self.stats['hits'] += 1
            
            return entry.value
    
    def set(self, key: str, value: Any, ttl: int = 3600) -> None:
        """Set value in cache with TTL"""
        with self._lock:
            # Create new entry
            entry = CacheEntry(
                key=key,
                value=value,
                timestamp=time.time(),
                ttl=ttl
            )
            
            # Check if we need to make space
            if self._would_exceed_capacity(entry):
                self._evict_lru_entries(entry.size_bytes)
            
            # Store entry
            old_entry = self._cache.get(key)
            self._cache[key] = entry
            
            # Update statistics
            if old_entry:
                self.stats['size_bytes'] -= old_entry.size_bytes
            else:
                self.stats['entry_count'] += 1
            
            self.stats['size_bytes'] += entry.size_bytes
            
            logger.debug(f"Cached {key}: {entry.size_bytes} bytes, TTL={ttl}s")
    
    def delete(self, key: str) -> bool:
        """Delete specific key from cache"""
        with self._lock:
            entry = self._cache.pop(key, None)
            if entry:
                self.stats['size_bytes'] -= entry.size_bytes
                self.stats['entry_count'] -= 1
                logger.debug(f"Deleted cache key: {key}")
                return True
            return False
    
    def clear(self) -> None:
        """Clear all cache entries"""
        with self._lock:
            self._cache.clear()
            self.stats['size_bytes'] = 0
            self.stats['entry_count'] = 0
            logger.info("Cache cleared")
    
    def cleanup_expired(self) -> int:
        """Remove all expired entries"""
        removed_count = 0
        
        with self._lock:
            current_time = time.time()
            expired_keys = [
                key for key, entry in self._cache.items()
                if current_time - entry.timestamp > entry.ttl
            ]
            
            for key in expired_keys:
                entry = self._cache.pop(key)
                self.stats['size_bytes'] -= entry.size_bytes
                self.stats['entry_count'] -= 1
                removed_count += 1
            
            self.stats['cleanups'] += 1
        
        if removed_count > 0:
            logger.debug(f"Cleaned up {removed_count} expired cache entries")
        
        return removed_count
    
    def _would_exceed_capacity(self, new_entry: CacheEntry) -> bool:
        """Check if adding entry would exceed capacity"""
        return self.stats['size_bytes'] + new_entry.size_bytes > self.max_size_bytes
    
    def _evict_lru_entries(self, space_needed: int) -> None:
        """Evict least recently used entries to make space"""
        if not self._cache:
            return
        
        # Sort by last access time (oldest first)
        sorted_entries = sorted(
            self._cache.items(),
            key=lambda x: x[1].last_access
        )
        
        space_freed = 0
        evicted_count = 0
        
        for key, entry in sorted_entries:
            if space_freed >= space_needed:
                break
            
            space_freed += entry.size_bytes
            del self._cache[key]
            self.stats['size_bytes'] -= entry.size_bytes
            self.stats['entry_count'] -= 1
            evicted_count += 1
        
        self.stats['evictions'] += evicted_count
        logger.debug(f"Evicted {evicted_count} LRU entries, freed {space_freed} bytes")
    
    def _update_stats(self) -> None:
        """Update cache statistics"""
        # This method is called when cache state changes
        pass
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        with self._lock:
            total_requests = self.stats['hits'] + self.stats['misses']
            hit_rate = self.stats['hits'] / total_requests if total_requests > 0 else 0
            
            return {
                'hits': self.stats['hits'],
                'misses': self.stats['misses'],
                'hit_rate': hit_rate,
                'evictions': self.stats['evictions'],
                'cleanups': self.stats['cleanups'],
                'entry_count': self.stats['entry_count'],
                'size_bytes': self.stats['size_bytes'],
                'size_mb': self.stats['size_bytes'] / (1024 * 1024),
                'max_size_mb': self.max_size_bytes / (1024 * 1024)
            }
    
    def get_keys(self, pattern: str = None) -> List[str]:
        """Get all cache keys, optionally filtered by pattern"""
        with self._lock:
            keys = list(self._cache.keys())
            
            if pattern:
                keys = [k for k in keys if pattern in k]
            
            return keys
    
    def get_entry_info(self, key: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a cache entry"""
        with self._lock:
            entry = self._cache.get(key)
            if entry:
                return entry.to_dict()
            return None
    
    def bulk_delete(self, pattern: str) -> int:
        """Delete multiple keys matching pattern"""
        with self._lock:
            matching_keys = [k for k in self._cache.keys() if pattern in k]
            deleted_count = 0
            
            for key in matching_keys:
                if self.delete(key):
                    deleted_count += 1
            
            return deleted_count
    
    def shutdown(self):
        """Shutdown cache manager and cleanup resources"""
        if self._cleanup_timer:
            self._cleanup_timer.cancel()
        
        logger.info("CacheManager shutdown")


class PersistentCacheManager(CacheManager):
    """Cache manager with S3/local file persistence"""
    
    def __init__(self, max_size_mb: int = 100, cleanup_interval: int = 300, 
                 persistence_dir: str = "cache_data"):
        super().__init__(max_size_mb, cleanup_interval)
        self.persistence_dir = persistence_dir
        
        # Create persistence directory
        os.makedirs(persistence_dir, exist_ok=True)
        
        # Load persisted cache on startup
        self._load_persistent_data()
    
    def _get_persistence_path(self, key: str) -> str:
        """Get file path for persistent cache key"""
        # Replace invalid filename characters
        safe_key = key.replace('/', '_').replace('\\', '_').replace(':', '_')
        return os.path.join(self.persistence_dir, f"{safe_key}.cache")
    
    def set_persistent(self, key: str, value: Any, ttl: int = 3600) -> None:
        """Set value with persistence to disk"""
        # Set in memory cache
        self.set(key, value, ttl)
        
        # Persist to disk for important data
        if ttl > 3600:  # Only persist long-lived data
            try:
                persistence_data = {
                    'value': value,
                    'timestamp': time.time(),
                    'ttl': ttl
                }
                
                file_path = self._get_persistence_path(key)
                with open(file_path, 'wb') as f:
                    pickle.dump(persistence_data, f)
                
                logger.debug(f"Persisted cache key: {key}")
                
            except Exception as e:
                logger.error(f"Failed to persist cache key {key}: {e}")
    
    def _load_persistent_data(self) -> None:
        """Load persisted cache data on startup"""
        if not os.path.exists(self.persistence_dir):
            return
        
        loaded_count = 0
        
        for filename in os.listdir(self.persistence_dir):
            if filename.endswith('.cache'):
                try:
                    file_path = os.path.join(self.persistence_dir, filename)
                    with open(file_path, 'rb') as f:
                        data = pickle.load(f)
                    
                    # Check if data is still valid
                    if time.time() - data['timestamp'] < data['ttl']:
                        key = filename[:-6]  # Remove .cache extension
                        self.set(key, data['value'], data['ttl'])
                        loaded_count += 1
                    else:
                        # Remove expired persistent data
                        os.remove(file_path)
                        
                except Exception as e:
                    logger.warning(f"Failed to load persistent cache {filename}: {e}")
        
        if loaded_count > 0:
            logger.info(f"Loaded {loaded_count} persistent cache entries")


def cached(ttl: int = 3600, cache_manager: CacheManager = None):
    """
    Decorator for caching function results
    
    Args:
        ttl: Time to live in seconds
        cache_manager: Optional cache manager instance
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Use global cache manager if none provided
            nonlocal cache_manager
            if cache_manager is None:
                cache_manager = get_cache_manager()
            
            # Generate cache key from function name and arguments
            cache_key = f"{func.__name__}_{hash(str(args) + str(sorted(kwargs.items())))}"
            
            # Try to get from cache
            result = cache_manager.get(cache_key)
            if result is not None:
                return result
            
            # Execute function and cache result
            result = func(*args, **kwargs)
            cache_manager.set(cache_key, result, ttl)
            
            return result
        
        # Add cache control methods to function
        wrapper.cache_clear = lambda: cache_manager.bulk_delete(func.__name__)
        wrapper.cache_info = lambda: cache_manager.get_stats()
        
        return wrapper
    
    return decorator


# Global cache manager instances
_cache_manager = None
_persistent_cache_manager = None


def get_cache_manager() -> CacheManager:
    """Get global cache manager instance"""
    global _cache_manager
    if _cache_manager is None:
        _cache_manager = CacheManager(max_size_mb=50)  # 50MB default
    return _cache_manager


def get_persistent_cache_manager() -> PersistentCacheManager:
    """Get global persistent cache manager instance"""
    global _persistent_cache_manager
    if _persistent_cache_manager is None:
        _persistent_cache_manager = PersistentCacheManager(
            max_size_mb=100,
            persistence_dir="crypto_cache"
        )
    return _persistent_cache_manager


# Convenience functions for common cache operations
def cache_price_data(symbol: str, price_data: Dict[str, Any], ttl: int = 300) -> None:
    """Cache price data for a token"""
    cache_manager = get_cache_manager()
    cache_key = f"price_{symbol.upper()}"
    cache_manager.set(cache_key, price_data, ttl)


def get_cached_price_data(symbol: str) -> Optional[Dict[str, Any]]:
    """Get cached price data for a token"""
    cache_manager = get_cache_manager()
    cache_key = f"price_{symbol.upper()}"
    return cache_manager.get(cache_key)


def cache_portfolio_data(portfolio_id: str, portfolio_data: Dict[str, Any], ttl: int = 60) -> None:
    """Cache portfolio valuation data"""
    cache_manager = get_cache_manager()
    cache_key = f"portfolio_{portfolio_id}"
    cache_manager.set(cache_key, portfolio_data, ttl)


def get_cached_portfolio_data(portfolio_id: str) -> Optional[Dict[str, Any]]:
    """Get cached portfolio data"""
    cache_manager = get_cache_manager()
    cache_key = f"portfolio_{portfolio_id}"
    return cache_manager.get(cache_key)


def clear_all_caches() -> None:
    """Clear all cache managers"""
    if _cache_manager:
        _cache_manager.clear()
    if _persistent_cache_manager:
        _persistent_cache_manager.clear()
    logger.info("All caches cleared")