import json
import os
import time
import hashlib
import pickle
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import threading
from collections import OrderedDict

logger = logging.getLogger(__name__)

class CacheManager:
    """Advanced caching system for query results and embeddings"""
    
    def __init__(self, cache_dir: str = "cache", max_memory_items: int = 1000, 
                 cache_ttl: int = 3600):
        self.cache_dir = cache_dir
        self.max_memory_items = max_memory_items
        self.cache_ttl = cache_ttl  # Time to live in seconds
        
        # Create cache directory
        os.makedirs(cache_dir, exist_ok=True)
        
        # In-memory cache (LRU)
        self.memory_cache = OrderedDict()
        self.cache_lock = threading.RLock()
        
        # Cache statistics
        self.stats = {
            "hits": 0,
            "misses": 0,
            "evictions": 0,
            "total_queries": 0
        }
        
        logger.info(f"CacheManager initialized with TTL={cache_ttl}s, max_items={max_memory_items}")
    
    def _generate_cache_key(self, query: str, params: Dict = None) -> str:
        """Generate cache key for query"""
        # Create a deterministic key from query and parameters
        cache_data = {
            "query": query.lower().strip(),
            "params": params or {}
        }
        
        cache_str = json.dumps(cache_data, sort_keys=True)
        return hashlib.md5(cache_str.encode()).hexdigest()
    
    def get(self, query: str, params: Dict = None) -> Optional[Dict[str, Any]]:
        """Get cached result for query"""
        cache_key = self._generate_cache_key(query, params)
        
        with self.cache_lock:
            self.stats["total_queries"] += 1
            
            # Check memory cache first
            if cache_key in self.memory_cache:
                cached_item = self.memory_cache[cache_key]
                
                # Check if cache is still valid
                if self._is_cache_valid(cached_item):
                    # Move to end (LRU)
                    self.memory_cache.move_to_end(cache_key)
                    self.stats["hits"] += 1
                    logger.debug(f"Cache hit (memory) for query: {query[:50]}...")
                    return cached_item["data"]
                else:
                    # Remove expired item
                    del self.memory_cache[cache_key]
                    self.stats["evictions"] += 1
            
            # Check disk cache
            disk_cache_path = os.path.join(self.cache_dir, f"{cache_key}.cache")
            if os.path.exists(disk_cache_path):
                try:
                    with open(disk_cache_path, 'rb') as f:
                        cached_item = pickle.load(f)
                    
                    if self._is_cache_valid(cached_item):
                        # Load into memory cache
                        self.memory_cache[cache_key] = cached_item
                        self.memory_cache.move_to_end(cache_key)
                        
                        # Evict from memory if needed
                        self._evict_if_needed()
                        
                        self.stats["hits"] += 1
                        logger.debug(f"Cache hit (disk) for query: {query[:50]}...")
                        return cached_item["data"]
                    else:
                        # Remove expired file
                        os.remove(disk_cache_path)
                        self.stats["evictions"] += 1
                        
                except Exception as e:
                    logger.warning(f"Error reading disk cache: {e}")
            
            self.stats["misses"] += 1
            logger.debug(f"Cache miss for query: {query[:50]}...")
            return None
    
    def set(self, query: str, data: Dict[str, Any], params: Dict = None, 
            persist_to_disk: bool = True):
        """Cache result for query"""
        cache_key = self._generate_cache_key(query, params)
        
        cached_item = {
            "data": data,
            "timestamp": time.time(),
            "query": query,
            "params": params or {},
            "cache_key": cache_key
        }
        
        with self.cache_lock:
            # Store in memory cache
            self.memory_cache[cache_key] = cached_item
            self.memory_cache.move_to_end(cache_key)
            
            # Evict if needed
            self._evict_if_needed()
            
            # Persist to disk if requested
            if persist_to_disk:
                self._persist_to_disk(cache_key, cached_item)
        
        logger.debug(f"Cached result for query: {query[:50]}...")
    
    def _is_cache_valid(self, cached_item: Dict[str, Any]) -> bool:
        """Check if cached item is still valid"""
        timestamp = cached_item.get("timestamp", 0)
        return (time.time() - timestamp) < self.cache_ttl
    
    def _evict_if_needed(self):
        """Evict oldest items if memory cache is full"""
        while len(self.memory_cache) > self.max_memory_items:
            oldest_key = next(iter(self.memory_cache))
            del self.memory_cache[oldest_key]
            self.stats["evictions"] += 1
            logger.debug(f"Evicted cache item: {oldest_key}")
    
    def _persist_to_disk(self, cache_key: str, cached_item: Dict[str, Any]):
        """Persist cache item to disk"""
        try:
            cache_path = os.path.join(self.cache_dir, f"{cache_key}.cache")
            with open(cache_path, 'wb') as f:
                pickle.dump(cached_item, f)
        except Exception as e:
            logger.warning(f"Error persisting cache to disk: {e}")
    
    def invalidate(self, query: str = None, pattern: str = None):
        """Invalidate cache entries"""
        with self.cache_lock:
            if query:
                # Invalidate specific query
                cache_key = self._generate_cache_key(query)
                if cache_key in self.memory_cache:
                    del self.memory_cache[cache_key]
                
                cache_path = os.path.join(self.cache_dir, f"{cache_key}.cache")
                if os.path.exists(cache_path):
                    os.remove(cache_path)
                    
            elif pattern:
                # Invalidate matching queries
                keys_to_remove = []
                for cache_key, cached_item in self.memory_cache.items():
                    if pattern.lower() in cached_item["query"].lower():
                        keys_to_remove.append(cache_key)
                
                for key in keys_to_remove:
                    del self.memory_cache[key]
                    cache_path = os.path.join(self.cache_dir, f"{key}.cache")
                    if os.path.exists(cache_path):
                        os.remove(cache_path)
            
            else:
                # Clear all cache
                self.memory_cache.clear()
                
                # Clear disk cache
                for filename in os.listdir(self.cache_dir):
                    if filename.endswith('.cache'):
                        os.remove(os.path.join(self.cache_dir, filename))
        
        logger.info(f"Cache invalidated: query={query}, pattern={pattern}")
    
    def cleanup_expired(self):
        """Clean up expired cache entries"""
        current_time = time.time()
        expired_keys = []
        
        with self.cache_lock:
            # Check memory cache
            for cache_key, cached_item in self.memory_cache.items():
                if not self._is_cache_valid(cached_item):
                    expired_keys.append(cache_key)
            
            for key in expired_keys:
                del self.memory_cache[key]
                self.stats["evictions"] += 1
            
            # Check disk cache
            for filename in os.listdir(self.cache_dir):
                if filename.endswith('.cache'):
                    cache_path = os.path.join(self.cache_dir, filename)
                    try:
                        with open(cache_path, 'rb') as f:
                            cached_item = pickle.load(f)
                        
                        if not self._is_cache_valid(cached_item):
                            os.remove(cache_path)
                            self.stats["evictions"] += 1
                    except:
                        # Remove corrupted cache files
                        os.remove(cache_path)
        
        if expired_keys:
            logger.info(f"Cleaned up {len(expired_keys)} expired cache entries")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        with self.cache_lock:
            hit_rate = (self.stats["hits"] / max(self.stats["total_queries"], 1)) * 100
            
            return {
                "memory_cache_size": len(self.memory_cache),
                "max_memory_items": self.max_memory_items,
                "cache_ttl_seconds": self.cache_ttl,
                "total_queries": self.stats["total_queries"],
                "cache_hits": self.stats["hits"],
                "cache_misses": self.stats["misses"],
                "cache_evictions": self.stats["evictions"],
                "hit_rate_percent": round(hit_rate, 2),
                "disk_cache_files": len([f for f in os.listdir(self.cache_dir) if f.endswith('.cache')])
            }
    
    def export_cache_info(self) -> Dict[str, Any]:
        """Export detailed cache information for analysis"""
        with self.cache_lock:
            cache_info = {
                "export_timestamp": datetime.now().isoformat(),
                "stats": self.get_stats(),
                "memory_cache_entries": [],
                "disk_cache_entries": []
            }
            
            # Memory cache entries
            for cache_key, cached_item in self.memory_cache.items():
                cache_info["memory_cache_entries"].append({
                    "cache_key": cache_key,
                    "query": cached_item["query"][:100] + "..." if len(cached_item["query"]) > 100 else cached_item["query"],
                    "timestamp": cached_item["timestamp"],
                    "age_seconds": time.time() - cached_item["timestamp"],
                    "data_size_bytes": len(str(cached_item["data"]))
                })
            
            # Disk cache entries
            for filename in os.listdir(self.cache_dir):
                if filename.endswith('.cache'):
                    cache_path = os.path.join(self.cache_dir, filename)
                    try:
                        stat = os.stat(cache_path)
                        cache_info["disk_cache_entries"].append({
                            "filename": filename,
                            "size_bytes": stat.st_size,
                            "modified_timestamp": datetime.fromtimestamp(stat.st_mtime).isoformat()
                        })
                    except:
                        pass
        
        return cache_info
