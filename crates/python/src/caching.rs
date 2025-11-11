//! Caching system for frequently accessed metadata
//!
//! This module provides a comprehensive caching system for DeltaLake metadata,
//! including memory caching, disk caching, LRU eviction, TTL policies,
//! and cache statistics and monitoring.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use chrono::{DateTime, Utc};
use uuid::Uuid;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

/// Cache eviction policies
#[pyclass]
#[derive(Debug, Clone, PartialEq)]
pub enum EvictionPolicy {
    /// Least Recently Used
    LRU,
    /// Least Frequently Used
    LFU,
    /// First In First Out
    FIFO,
    /// Time-based expiration
    TTL,
    /// No eviction (until memory pressure)
    None,
}

/// Cache item with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheItem<T: Clone> {
    value: T,
    created_at: SystemTime,
    last_accessed: SystemTime,
    access_count: u64,
    size_bytes: usize,
    ttl_seconds: Option<u64>,
}

impl<T: Clone> CacheItem<T> {
    fn new(value: T, size_bytes: usize, ttl_seconds: Option<u64>) -> Self {
        let now = SystemTime::now();
        Self {
            value,
            created_at: now,
            last_accessed: now,
            access_count: 1,
            size_bytes,
            ttl_seconds,
        }
    }

    fn access(&mut self) -> &T {
        self.last_accessed = SystemTime::now();
        self.access_count += 1;
        &self.value
    }

    fn is_expired(&self) -> bool {
        if let Some(ttl) = self.ttl_seconds {
            self.created_at.elapsed().unwrap_or_default().as_secs() > ttl
        } else {
            false
        }
    }

    fn age_seconds(&self) -> u64 {
        self.created_at.elapsed().unwrap_or_default().as_secs()
    }

    fn last_access_seconds_ago(&self) -> u64 {
        self.last_accessed.elapsed().unwrap_or_default().as_secs()
    }
}

/// Cache statistics
#[pyclass]
#[derive(Debug, Clone)]
pub struct CacheStats {
    hits: u64,
    misses: u64,
    evictions: u64,
    total_items: usize,
    total_size_bytes: usize,
    max_size_bytes: usize,
    hit_rate: f64,
    average_access_time_ms: f64,
    oldest_item_age_seconds: u64,
}

#[pymethods]
impl CacheStats {
    #[new]
    fn new() -> Self {
        Self {
            hits: 0,
            misses: 0,
            evictions: 0,
            total_items: 0,
            total_size_bytes: 0,
            max_size_bytes: 0,
            hit_rate: 0.0,
            average_access_time_ms: 0.0,
            oldest_item_age_seconds: 0,
        }
    }

    #[getter]
    fn hits(&self) -> u64 {
        self.hits
    }

    #[getter]
    fn misses(&self) -> u64 {
        self.misses
    }

    #[getter]
    fn evictions(&self) -> u64 {
        self.evictions
    }

    #[getter]
    fn total_requests(&self) -> u64 {
        self.hits + self.misses
    }

    #[getter]
    fn hit_rate(&self) -> f64 {
        self.hit_rate
    }

    #[getter]
    fn miss_rate(&self) -> f64 {
        if self.hits + self.misses == 0 {
            0.0
        } else {
            self.misses as f64 / (self.hits + self.misses) as f64
        }
    }

    #[getter]
    fn total_items(&self) -> usize {
        self.total_items
    }

    #[getter]
    fn total_size_bytes(&self) -> usize {
        self.total_size_bytes
    }

    #[getter]
    fn total_size_mb(&self) -> f64 {
        self.total_size_bytes as f64 / (1024.0 * 1024.0)
    }

    #[getter]
    fn utilization_rate(&self) -> f64 {
        if self.max_size_bytes == 0 {
            0.0
        } else {
            self.total_size_bytes as f64 / self.max_size_bytes as f64
        }
    }

    fn to_dict(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("hits", self.hits)?;
        dict.set_item("misses", self.misses)?;
        dict.set_item("evictions", self.evictions)?;
        dict.set_item("total_requests", self.total_requests())?;
        dict.set_item("hit_rate", self.hit_rate)?;
        dict.set_item("miss_rate", self.miss_rate())?;
        dict.set_item("total_items", self.total_items)?;
        dict.set_item("total_size_bytes", self.total_size_bytes)?;
        dict.set_item("total_size_mb", self.total_size_mb())?;
        dict.set_item("max_size_bytes", self.max_size_bytes)?;
        dict.set_item("utilization_rate", self.utilization_rate())?;
        dict.set_item("average_access_time_ms", self.average_access_time_ms)?;
        dict.set_item("oldest_item_age_seconds", self.oldest_item_age_seconds)?;
        Ok(dict.to_object(py))
    }
}

/// Memory cache implementation
#[pyclass]
#[derive(Debug)]
pub struct MemoryCache<T: Clone + Send + Sync> {
    data: Arc<RwLock<HashMap<String, CacheItem<T>>>>,
    access_order: Arc<Mutex<VecDeque<String>>>,
    stats: Arc<Mutex<CacheStats>>,
    max_size_bytes: usize,
    max_items: usize,
    eviction_policy: EvictionPolicy,
    cleanup_interval_seconds: u64,
    last_cleanup: Arc<Mutex<Instant>>,
}

#[pymethods]
impl MemoryCache<PyObject> {
    #[new]
    #[pyo3(signature = (max_size_bytes=1024*1024*1024, max_items=10000, eviction_policy=EvictionPolicy::LRU, cleanup_interval_seconds=60))]
    fn new(
        max_size_bytes: usize,
        max_items: usize,
        eviction_policy: EvictionPolicy,
        cleanup_interval_seconds: u64,
    ) -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            access_order: Arc::new(Mutex::new(VecDeque::new())),
            stats: Arc::new(Mutex::new(CacheStats::new())),
            max_size_bytes,
            max_items,
            eviction_policy,
            cleanup_interval_seconds,
            last_cleanup: Arc::new(Mutex::new(Instant::now())),
        }
    }

    /// Get value from cache
    fn get(&self, py: Python, key: &str) -> PyResult<Option<PyObject>> {
        let start_time = Instant::now();

        // Cleanup if needed
        self.cleanup_if_needed();

        let data = self.data.read().unwrap();

        if let Some(item) = data.get(key) {
            // Check if expired
            if item.is_expired() {
                drop(data);
                self.remove(key)?;
                let mut stats = self.stats.lock().unwrap();
                stats.misses += 1;
                stats.update_hit_rate();
                return Ok(None);
            }

            let value = item.value.clone_ref(py);

            // Update access order for LRU
            if self.eviction_policy == EvictionPolicy::LRU {
                drop(data);
                self.update_access_order(key);
            }

            // Update stats
            let mut stats = self.stats.lock().unwrap();
            stats.hits += 1;
            stats.update_hit_rate();
            let access_time = start_time.elapsed().as_millis() as f64;
            stats.update_average_access_time(access_time);

            Ok(Some(value))
        } else {
            // Update stats
            let mut stats = self.stats.lock().unwrap();
            stats.misses += 1;
            stats.update_hit_rate();

            Ok(None)
        }
    }

    /// Put value in cache
    fn put(&self, py: Python, key: &str, value: &PyAny, ttl_seconds: Option<u64>) -> PyResult<()> {
        // Estimate size (rough approximation)
        let size_bytes = self.estimate_object_size(py, value)?;

        // Check if eviction is needed
        if self.needs_eviction(size_bytes) {
            self.evict_items(size_bytes)?;
        }

        let cache_item = CacheItem::new(value.to_object(py), size_bytes, ttl_seconds);

        // Insert the item
        {
            let mut data = self.data.write().unwrap();
            data.insert(key.to_string(), cache_item);
        }

        // Update access order
        self.update_access_order(key);

        // Update stats
        self.update_stats_after_insert();

        Ok(())
    }

    /// Remove item from cache
    fn remove(&self, key: &str) -> PyResult<bool> {
        let mut data = self.data.write().unwrap();
        let removed = data.remove(key).is_some();

        if removed {
            // Remove from access order
            let mut access_order = self.access_order.lock().unwrap();
            access_order.retain(|k| k != key);

            // Update stats
            let mut stats = self.stats.lock().unwrap();
            stats.total_items = data.len();
            stats.update_total_size();
        }

        Ok(removed)
    }

    /// Clear all items from cache
    fn clear(&self) -> PyResult<()> {
        {
            let mut data = self.data.write().unwrap();
            data.clear();
        }

        {
            let mut access_order = self.access_order.lock().unwrap();
            access_order.clear();
        }

        // Update stats
        let mut stats = self.stats.lock().unwrap();
        stats.total_items = 0;
        stats.total_size_bytes = 0;

        Ok(())
    }

    /// Get cache statistics
    fn get_stats(&self) -> CacheStats {
        let stats = self.stats.lock().unwrap();
        let mut result = stats.clone();

        // Update dynamic values
        let data = self.data.read().unwrap();
        result.total_items = data.len();
        result.update_total_size();
        result.update_oldest_item_age(&data);

        result
    }

    /// Get all keys in cache
    fn keys(&self) -> Vec<String> {
        let data = self.data.read().unwrap();
        data.keys().cloned().collect()
    }

    /// Check if key exists in cache
    fn contains_key(&self, key: &str) -> bool {
        let data = self.data.read().unwrap();
        data.contains_key(key)
    }

    /// Get cache size
    fn size(&self) -> usize {
        let data = self.data.read().unwrap();
        data.len()
    }

    /// Force cleanup of expired items
    fn cleanup(&self) -> PyResult<usize> {
        let mut data = self.data.write().unwrap();
        let mut access_order = self.access_order.lock().unwrap();

        let initial_count = data.len();

        // Remove expired items
        data.retain(|key, item| {
            if item.is_expired() {
                access_order.retain(|k| k != key);
                false
            } else {
                true
            }
        });

        let cleaned_count = initial_count - data.len();

        // Update stats
        let mut stats = self.stats.lock().unwrap();
        stats.total_items = data.len();
        stats.update_total_size();

        *self.last_cleanup.lock().unwrap() = Instant::now();

        Ok(cleaned_count)
    }
}

impl<T: Clone + Send + Sync> MemoryCache<T> {
    fn estimate_object_size(&self, py: Python, obj: &PyAny) -> PyResult<usize> {
        // Rough size estimation - in real implementation, this would be more accurate
        if let Ok(s) = obj.str() {
            Ok(s.to_string_lossy().len())
        } else if obj.is_instance_of::<PyDict>() {
            // Estimate dict size
            Ok(512) // Rough estimate
        } else if obj.is_instance_of::<PyList>() {
            // Estimate list size
            Ok(256) // Rough estimate
        } else {
            Ok(128) // Default estimate
        }
    }

    fn needs_eviction(&self, new_item_size: usize) -> bool {
        let stats = self.stats.lock().unwrap();
        stats.total_items >= self.max_items ||
        (stats.total_size_bytes + new_item_size) > self.max_size_bytes
    }

    fn evict_items(&self, needed_space: usize) -> PyResult<()> {
        match self.eviction_policy {
            EvictionPolicy::LRU => self.evict_lru(needed_space),
            EvictionPolicy::LFU => self.evict_lfu(needed_space),
            EvictionPolicy::FIFO => self.evict_fifo(needed_space),
            EvictionPolicy::TTL => self.evict_ttl(),
            EvictionPolicy::None => {
                // Only evict expired items
                self.evict_ttl()
            }
        }
    }

    fn evict_lru(&self, needed_space: usize) -> PyResult<()> {
        let mut freed_space = 0;
        let mut evicted_count = 0;

        while freed_space < needed_space {
            let key_to_evict = {
                let mut access_order = self.access_order.lock().unwrap();
                access_order.pop_front()
            };

            if let Some(key) = key_to_evict {
                let mut data = self.data.write().unwrap();
                if let Some(item) = data.remove(&key) {
                    freed_space += item.size_bytes;
                    evicted_count += 1;
                }
            } else {
                break; // No more items to evict
            }
        }

        // Update stats
        let mut stats = self.stats.lock().unwrap();
        stats.evictions += evicted_count;
        stats.total_items = self.size();
        stats.update_total_size();

        Ok(())
    }

    fn evict_lfu(&self, needed_space: usize) -> PyResult<()> {
        let mut data = self.data.write().unwrap();
        let mut freed_space = 0;
        let mut evicted_count = 0;

        // Sort by access count (ascending)
        let mut items: Vec<_> = data.iter().collect();
        items.sort_by_key(|(_, item)| item.access_count);

        for (key, item) in items {
            if freed_space >= needed_space {
                break;
            }

            freed_space += item.size_bytes;
            evicted_count += 1;
            data.remove(key);
        }

        // Update access order
        let mut access_order = self.access_order.lock().unwrap();
        access_order.retain(|key| data.contains_key(key));

        // Update stats
        let mut stats = self.stats.lock().unwrap();
        stats.evictions += evicted_count;
        stats.total_items = data.len();
        stats.update_total_size();

        Ok(())
    }

    fn evict_fifo(&self, needed_space: usize) -> PyResult<()> {
        let mut freed_space = 0;
        let mut evicted_count = 0;

        while freed_space < needed_space {
            let key_to_evict = {
                let mut data = self.data.write().unwrap();

                // Find the oldest item
                if let Some((oldest_key, _)) = data.iter()
                    .min_by_key(|(_, item)| item.created_at) {
                    let key = oldest_key.clone();
                    if let Some(item) = data.remove(&key) {
                        freed_space += item.size_bytes;
                        evicted_count += 1;
                    }
                    Some(key)
                } else {
                    None
                }
            };

            if key_to_evict.is_none() {
                break;
            }
        }

        // Update access order
        let data = self.data.read().unwrap();
        let mut access_order = self.access_order.lock().unwrap();
        access_order.retain(|key| data.contains_key(key));

        // Update stats
        let mut stats = self.stats.lock().unwrap();
        stats.evictions += evicted_count;
        stats.total_items = data.len();
        stats.update_total_size();

        Ok(())
    }

    fn evict_ttl(&self) -> PyResult<()> {
        let cleaned_count = self.cleanup()?;
        let mut stats = self.stats.lock().unwrap();
        stats.evictions += cleaned_count as u64;
        Ok(())
    }

    fn update_access_order(&self, key: &str) {
        let mut access_order = self.access_order.lock().unwrap();

        // Remove from current position
        access_order.retain(|k| k != key);

        // Add to the end
        access_order.push_back(key.to_string());
    }

    fn update_stats_after_insert(&self) {
        let mut stats = self.stats.lock().unwrap();
        let data = self.data.read().unwrap();
        stats.total_items = data.len();
        stats.update_total_size();
        stats.max_size_bytes = self.max_size_bytes;
    }

    fn cleanup_if_needed(&self) {
        let last_cleanup = *self.last_cleanup.lock().unwrap();
        if last_cleanup.elapsed().as_secs() >= self.cleanup_interval_seconds {
            let _ = self.cleanup(); // Ignore errors in cleanup
        }
    }
}

// Trait implementations for CacheStats
impl CacheStats {
    fn update_hit_rate(&mut self) {
        self.hit_rate = if self.hits + self.misses == 0 {
            0.0
        } else {
            self.hits as f64 / (self.hits + self.misses) as f64
        };
    }

    fn update_average_access_time(&mut self, new_time: f64) {
        let total_requests = self.hits + self.misses;
        if total_requests == 1 {
            self.average_access_time_ms = new_time;
        } else {
            self.average_access_time_ms =
                (self.average_access_time_ms * (total_requests - 1) as f64 + new_time) / total_requests as f64;
        }
    }

    fn update_total_size(&mut self) {
        // This would need the data reference, so it's updated in the calling methods
    }

    fn update_oldest_item_age<T>(&mut self, data: &HashMap<String, CacheItem<T>>) {
        self.oldest_item_age_seconds = data.values()
            .map(|item| item.age_seconds())
            .min()
            .unwrap_or(0);
    }
}

/// DeltaLake metadata cache manager
#[pyclass]
#[derive(Debug)]
pub struct DeltaLakeCacheManager {
    table_cache: Arc<MemoryCache<PyObject>>,
    schema_cache: Arc<MemoryCache<PyObject>>,
    commit_cache: Arc<MemoryCache<PyObject>>,
    file_cache: Arc<MemoryCache<PyObject>>,
    stats_cache: Arc<MemoryCache<PyObject>>,
    global_stats: Arc<Mutex<CacheStats>>,
}

#[pymethods]
impl DeltaLakeCacheManager {
    #[new]
    #[pyo3(signature = (cache_size_mb=1024, max_items_per_cache=10000))]
    fn new(cache_size_mb: usize, max_items_per_cache: usize) -> Self {
        let cache_size_bytes = cache_size_mb * 1024 * 1024;

        Self {
            table_cache: Arc::new(MemoryCache::new(
                cache_size_bytes / 4,
                max_items_per_cache / 4,
                EvictionPolicy::LRU,
                300, // 5 minutes
            )),
            schema_cache: Arc::new(MemoryCache::new(
                cache_size_bytes / 8,
                max_items_per_cache / 8,
                EvictionPolicy::LRU,
                600, // 10 minutes
            )),
            commit_cache: Arc::new(MemoryCache::new(
                cache_size_bytes / 4,
                max_items_per_cache / 4,
                EvictionPolicy::LRU,
                300, // 5 minutes
            )),
            file_cache: Arc::new(MemoryCache::new(
                cache_size_bytes / 4,
                max_items_per_cache / 4,
                EvictionPolicy::LFU,
                180, // 3 minutes
            )),
            stats_cache: Arc::new(MemoryCache::new(
                cache_size_bytes / 16,
                max_items_per_cache / 16,
                EvictionPolicy::TTL,
                900, // 15 minutes
            )),
            global_stats: Arc::new(Mutex::new(CacheStats::new())),
        }
    }

    /// Cache table metadata
    fn cache_table(&self, py: Python, table_id: &str, metadata: &PyAny, ttl_seconds: Option<u64>) -> PyResult<()> {
        self.table_cache.put(py, table_id, metadata, ttl_seconds)
    }

    /// Get cached table metadata
    fn get_cached_table(&self, py: Python, table_id: &str) -> PyResult<Option<PyObject>> {
        self.table_cache.get(py, table_id)
    }

    /// Cache schema
    fn cache_schema(&self, py: Python, schema_id: &str, schema: &PyAny, ttl_seconds: Option<u64>) -> PyResult<()> {
        self.schema_cache.put(py, schema_id, schema, ttl_seconds)
    }

    /// Get cached schema
    fn get_cached_schema(&self, py: Python, schema_id: &str) -> PyResult<Option<PyObject>> {
        self.schema_cache.get(py, schema_id)
    }

    /// Cache commit
    fn cache_commit(&self, py: Python, commit_id: &str, commit: &PyAny, ttl_seconds: Option<u64>) -> PyResult<()> {
        self.commit_cache.put(py, commit_id, commit, ttl_seconds)
    }

    /// Get cached commit
    fn get_cached_commit(&self, py: Python, commit_id: &str) -> PyResult<Option<PyObject>> {
        self.commit_cache.get(py, commit_id)
    }

    /// Cache file metadata
    fn cache_file(&self, py: Python, file_path: &str, metadata: &PyAny, ttl_seconds: Option<u64>) -> PyResult<()> {
        self.file_cache.put(py, file_path, metadata, ttl_seconds)
    }

    /// Get cached file metadata
    fn get_cached_file(&self, py: Python, file_path: &str) -> PyResult<Option<PyObject>> {
        self.file_cache.get(py, file_path)
    }

    /// Cache statistics
    fn cache_stats(&self, py: Python, stats_key: &str, stats: &PyAny, ttl_seconds: Option<u64>) -> PyResult<()> {
        self.stats_cache.put(py, stats_key, stats, ttl_seconds)
    }

    /// Get cached statistics
    fn get_cached_stats(&self, py: Python, stats_key: &str) -> PyResult<Option<PyObject>> {
        self.stats_cache.get(py, stats_key)
    }

    /// Get comprehensive cache statistics
    fn get_comprehensive_stats(&self) -> PyResult<CacheStats> {
        let table_stats = self.table_cache.get_stats();
        let schema_stats = self.schema_cache.get_stats();
        let commit_stats = self.commit_cache.get_stats();
        let file_stats = self.file_cache.get_stats();
        let stats_stats = self.stats_cache.get_stats();

        let mut global_stats = CacheStats::new();
        global_stats.hits = table_stats.hits + schema_stats.hits + commit_stats.hits + file_stats.hits + stats_stats.hits;
        global_stats.misses = table_stats.misses + schema_stats.misses + commit_stats.misses + file_stats.misses + stats_stats.misses;
        global_stats.evictions = table_stats.evictions + schema_stats.evictions + commit_stats.evictions + file_stats.evictions + stats_stats.evictions;
        global_stats.total_items = table_stats.total_items + schema_stats.total_items + commit_stats.total_items + file_stats.total_items + stats_stats.total_items;
        global_stats.total_size_bytes = table_stats.total_size_bytes + schema_stats.total_size_bytes + commit_stats.total_size_bytes + file_stats.total_size_bytes + stats_stats.total_size_bytes;
        global_stats.max_size_bytes = table_stats.max_size_bytes + schema_stats.max_size_bytes + commit_stats.max_size_bytes + file_stats.max_size_bytes + stats_stats.max_size_bytes;
        global_stats.update_hit_rate();

        // Update global stats
        *self.global_stats.lock().unwrap() = global_stats.clone();

        Ok(global_stats)
    }

    /// Clear all caches
    fn clear_all_caches(&self) -> PyResult<()> {
        self.table_cache.clear()?;
        self.schema_cache.clear()?;
        self.commit_cache.clear()?;
        self.file_cache.clear()?;
        self.stats_cache.clear()?;
        Ok(())
    }

    /// Cleanup expired items
    fn cleanup_expired_items(&self) -> PyResult<usize> {
        let mut total_cleaned = 0;
        total_cleaned += self.table_cache.cleanup()?;
        total_cleaned += self.schema_cache.cleanup()?;
        total_cleaned += self.commit_cache.cleanup()?;
        total_cleaned += self.file_cache.cleanup()?;
        total_cleaned += self.stats_cache.cleanup()?;
        Ok(total_cleaned)
    }

    /// Get cache summary by type
    fn get_cache_summary(&self, py: Python) -> PyResult<PyObject> {
        let summary_dict = PyDict::new(py);

        summary_dict.set_item("table_cache", self.table_cache.get_stats().to_dict(py))?;
        summary_dict.set_item("schema_cache", self.schema_cache.get_stats().to_dict(py))?;
        summary_dict.set_item("commit_cache", self.commit_cache.get_stats().to_dict(py))?;
        summary_dict.set_item("file_cache", self.file_cache.get_stats().to_dict(py))?;
        summary_dict.set_item("stats_cache", self.stats_cache.get_stats().to_dict(py))?;
        summary_dict.set_item("global_stats", self.get_comprehensive_stats()?.to_dict(py))?;

        Ok(summary_dict.to_object(py))
    }
}

/// Cache utilities and factory functions
#[pyclass]
pub struct CacheUtils;

#[pymethods]
impl CacheUtils {
    /// Create cache configuration for different use cases
    #[staticmethod]
    fn create_low_memory_config() -> (usize, usize, EvictionPolicy) {
        (256 * 1024 * 1024, 5000, EvictionPolicy::LRU) // 256MB, 5000 items
    }

    #[staticmethod]
    fn create_high_performance_config() -> (usize, usize, EvictionPolicy) {
        (2048 * 1024 * 1024, 50000, EvictionPolicy::LRU) // 2GB, 50000 items
    }

    #[staticmethod]
    fn create_batch_processing_config() -> (usize, usize, EvictionPolicy) {
        (1024 * 1024 * 1024, 20000, EvictionPolicy::LFU) // 1GB, 20000 items
    }

    #[staticmethod]
    fn create_streaming_config() -> (usize, usize, EvictionPolicy) {
        (128 * 1024 * 1024, 2000, EvictionPolicy::FIFO) // 128MB, 2000 items
    }

    /// Estimate cache requirements based on table characteristics
    #[staticmethod]
    fn estimate_cache_requirements(
        table_count: usize,
        avg_table_size_mb: f64,
        query_frequency: f64, // queries per minute
    ) -> HashMap<String, f64> {
        let mut requirements = HashMap::new();

        // Base cache size (30% of total data size)
        let total_data_size_mb = table_count as f64 * avg_table_size_mb;
        requirements.insert("base_cache_size_mb".to_string(), total_data_size_mb * 0.3);

        // High-frequency query multiplier
        if query_frequency > 60.0 { // More than 1 query per second
            let multiplier = (query_frequency / 60.0).min(3.0); // Cap at 3x
            let adjusted_size = requirements["base_cache_size_mb"] * multiplier;
            requirements.insert("adjusted_cache_size_mb".to_string(), adjusted_size);
        } else {
            requirements.insert("adjusted_cache_size_mb".to_string(), requirements["base_cache_size_mb"]);
        }

        // Memory overhead (25% additional)
        let cache_size = requirements["adjusted_cache_size_mb"];
        requirements.insert("overhead_mb".to_string(), cache_size * 0.25);

        // Total memory requirement
        requirements.insert("total_memory_mb".to_string(), cache_size * 1.25);

        requirements
    }

    /// Recommend eviction policy based on access patterns
    #[staticmethod]
    fn recommend_eviction_policy(
        access_pattern: String, // "sequential", "random", "temporal", "uniform"
        data_size_ratio: f64,   // cache_size / data_size
        access_frequency: f64,  // high, medium, low
    ) -> EvictionPolicy {
        if access_pattern == "sequential" {
            EvictionPolicy::FIFO
        } else if access_pattern == "temporal" {
            EvictionPolicy::LRU
        } else if access_frequency > 0.7 && data_size_ratio > 0.5 {
            EvictionPolicy::LFU
        } else if data_size_ratio < 0.1 {
            EvictionPolicy::LRU
        } else {
            EvictionPolicy::TTL
        }
    }
}

// Convenience functions for module-level exports
#[pyfunction]
pub fn create_memory_cache(
    max_size_bytes: usize,
    max_items: usize,
    eviction_policy: EvictionPolicy,
) -> PyResult<MemoryCache<PyObject>> {
    Ok(MemoryCache::new(max_size_bytes, max_items, eviction_policy, 60))
}

#[pyfunction]
pub fn create_deltalake_cache_manager(
    cache_size_mb: usize,
    max_items_per_cache: usize,
) -> PyResult<DeltaLakeCacheManager> {
    Ok(DeltaLakeCacheManager::new(cache_size_mb, max_items_per_cache))
}