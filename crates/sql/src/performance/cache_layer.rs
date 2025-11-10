//! Multi-level caching system for metadata access
//!
//! This module provides intelligent caching at multiple levels to reduce database load
//! and improve response times for Delta Lake metadata queries.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use async_trait::async_trait;
use tokio::sync::{Mutex, RwLock};
use serde::{Serialize, Deserialize};

/// Cache level
#[derive(Debug, Clone, PartialEq)]
pub enum CacheLevel {
    /// In-memory cache (fastest)
    Memory,
    /// Query result cache
    QueryResult,
    /// Object cache (parsed objects)
    Object,
    /// Metadata cache (table metadata)
    Metadata,
}

/// Cache entry
#[derive(Debug, Clone)]
pub struct CacheEntry<T> {
    /// Cache key
    pub key: String,
    /// Cached value
    pub value: T,
    /// When this entry was created
    pub created_at: Instant,
    /// When this entry expires
    pub expires_at: Option<Instant>,
    /// Number of times this entry was accessed
    pub access_count: u64,
    /// Last access time
    pub last_accessed: Instant,
    /// Size of the entry in bytes
    pub size_bytes: usize,
    /// Cache level
    pub level: CacheLevel,
}

impl<T> CacheEntry<T> {
    /// Create a new cache entry
    pub fn new(key: String, value: T, level: CacheLevel) -> Self {
        let now = Instant::now();
        Self {
            key,
            value,
            created_at: now,
            expires_at: None,
            access_count: 0,
            last_accessed: now,
            size_bytes: 0, // Would be calculated based on serialized size
            level,
        }
    }

    /// Set expiration time
    pub fn with_expiration(mut self, ttl: Duration) -> Self {
        self.expires_at = Some(self.created_at + ttl);
        self
    }

    /// Check if entry is expired
    pub fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at {
            Instant::now() > expires_at
        } else {
            false
        }
    }

    /// Mark entry as accessed
    pub fn access(&mut self) {
        self.access_count += 1;
        self.last_accessed = Instant::now();
    }

    /// Get entry age
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Get time since last access
    pub fn time_since_last_access(&self) -> Duration {
        self.last_accessed.elapsed()
    }
}

/// Cache configuration
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Maximum cache size in bytes
    pub max_size_bytes: usize,
    /// Maximum number of entries
    pub max_entries: usize,
    /// Default TTL for entries
    pub default_ttl: Duration,
    /// Cleanup interval
    pub cleanup_interval: Duration,
    /// Whether to enable LRU eviction
    pub enable_lru: bool,
    /// Cache level priorities (higher = more important)
    pub level_priorities: HashMap<CacheLevel, u8>,
}

impl Default for CacheConfig {
    fn default() -> Self {
        let mut level_priorities = HashMap::new();
        level_priorities.insert(CacheLevel::Memory, 4);
        level_priorities.insert(CacheLevel::QueryResult, 3);
        level_priorities.insert(CacheLevel::Object, 2);
        level_priorities.insert(CacheLevel::Metadata, 1);

        Self {
            max_size_bytes: 100 * 1024 * 1024, // 100MB
            max_entries: 10000,
            default_ttl: Duration::from_secs(300), // 5 minutes
            cleanup_interval: Duration::from_secs(60), // 1 minute
            enable_lru: true,
            level_priorities,
        }
    }
}

/// Cache statistics
#[derive(Debug, Clone, Default)]
pub struct CacheMetrics {
    /// Total number of entries
    pub total_entries: usize,
    /// Total cache size in bytes
    pub total_size_bytes: usize,
    /// Number of cache hits
    pub hits: u64,
    /// Number of cache misses
    pub misses: u64,
    /// Cache hit rate
    pub hit_rate: f64,
    /// Number of evictions
    pub evictions: u64,
    /// Number of expired entries removed
    pub expired_removed: u64,
    /// Average access time (microseconds)
    pub avg_access_time_us: f64,
    /// Cache levels statistics
    pub level_stats: HashMap<CacheLevel, LevelStats>,
}

/// Statistics for a specific cache level
#[derive(Debug, Clone, Default)]
pub struct LevelStats {
    /// Number of entries at this level
    pub entries: usize,
    /// Size in bytes at this level
    pub size_bytes: usize,
    /// Hit rate for this level
    pub hit_rate: f64,
    /// Evictions at this level
    pub evictions: u64,
}

/// Cache invalidation strategy
#[derive(Debug, Clone, PartialEq)]
pub enum InvalidationStrategy {
    /// Time-based expiration
    TimeBased,
    /// Manual invalidation
    Manual,
    /// Tag-based invalidation
    TagBased,
    /// Dependency-based invalidation
    DependencyBased,
}

/// Cache invalidation rule
#[derive(Debug, Clone)]
pub struct InvalidationRule {
    /// Rule name
    pub name: String,
    /// Pattern to match cache keys
    pub pattern: String,
    /// Invalidation strategy
    pub strategy: InvalidationStrategy,
    /// Tags for this rule
    pub tags: Vec<String>,
    /// Dependencies for this rule
    pub dependencies: Vec<String>,
}

/// Cache manager
pub struct CacheManager {
    /// Cache configuration
    config: CacheConfig,
    /// In-memory cache entries
    memory_cache: Arc<RwLock<HashMap<String, CacheEntry<serde_json::Value>>>>,
    /// Query result cache
    query_cache: Arc<RwLock<HashMap<String, CacheEntry<serde_json::Value>>>>,
    /// Object cache
    object_cache: Arc<RwLock<HashMap<String, CacheEntry<serde_json::Value>>>>,
    /// Metadata cache
    metadata_cache: Arc<RwLock<HashMap<String, CacheEntry<serde_json::Value>>>>,
    /// Cache statistics
    metrics: Arc<RwLock<CacheMetrics>>,
    /// Invalidation rules
    invalidation_rules: Arc<Mutex<Vec<InvalidationRule>>>,
    /// Cleanup task handle
    cleanup_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl CacheManager {
    /// Create a new cache manager
    pub fn new(config: CacheConfig) -> Self {
        Self {
            config,
            memory_cache: Arc::new(RwLock::new(HashMap::new())),
            query_cache: Arc::new(RwLock::new(HashMap::new())),
            object_cache: Arc::new(RwLock::new(HashMap::new())),
            metadata_cache: Arc::new(RwLock::new(HashMap::new())),
            metrics: Arc::new(RwLock::new(CacheMetrics::default())),
            invalidation_rules: Arc::new(Mutex::new(Vec::new())),
            cleanup_task: Arc::new(Mutex::new(None)),
        }
    }

    /// Start the cache manager
    pub async fn start(&self) -> Result<(), String> {
        // Start cleanup task
        let memory_cache = self.memory_cache.clone();
        let query_cache = self.query_cache.clone();
        let object_cache = self.object_cache.clone();
        let metadata_cache = self.metadata_cache.clone();
        let metrics = self.metrics.clone();
        let config = self.config.clone();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.cleanup_interval);
            interval.tick().await; // Skip first tick

            loop {
                interval.tick().await;

                // Clean up expired entries from all cache levels
                Self::cleanup_cache_level(&memory_cache, &metrics, CacheLevel::Memory).await;
                Self::cleanup_cache_level(&query_cache, &metrics, CacheLevel::QueryResult).await;
                Self::cleanup_cache_level(&object_cache, &metrics, CacheLevel::Object).await;
                Self::cleanup_cache_level(&metadata_cache, &metrics, CacheLevel::Metadata).await;

                // Evict entries if cache is too large
                if Self::get_total_size(&memory_cache, &query_cache, &object_cache, &metadata_cache) > config.max_size_bytes {
                    Self::evict_entries(
                        &memory_cache,
                        &query_cache,
                        &object_cache,
                        &metadata_cache,
                        &metrics,
                        &config,
                    ).await;
                }
            }
        });

        *self.cleanup_task.lock().await = Some(handle);
        Ok(())
    }

    /// Stop the cache manager
    pub async fn stop(&self) {
        if let Some(handle) = self.cleanup_task.lock().await.take() {
            handle.abort();
        }
    }

    /// Get a value from cache
    pub async fn get<T>(&self, key: &str, level: CacheLevel) -> Option<T>
    where
        T: for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        let start_time = Instant::now();

        let cache = self.get_cache_for_level(level);
        let cache_guard = cache.read().await;

        if let Some(entry) = cache_guard.get(key) {
            if !entry.is_expired() {
                // Update access metrics
                drop(cache_guard);
                self.update_hit_metrics(level, start_time.elapsed()).await;

                // Try to deserialize the value
                if let Ok(value) = serde_json::from_value::<T>(entry.value.clone()) {
                    return Some(value);
                }
            }
        }

        // Update miss metrics
        self.update_miss_metrics(level, start_time.elapsed()).await;
        None
    }

    /// Put a value into cache
    pub async fn put<T>(&self, key: &str, value: T, level: CacheLevel, ttl: Option<Duration>) -> Result<(), String>
    where
        T: Serialize + Send + Sync + 'static,
    {
        let cache = self.get_cache_for_level(level);

        // Serialize the value
        let json_value = match serde_json::to_value(&value) {
            Ok(v) => v,
            Err(e) => return Err(format!("Failed to serialize value: {}", e)),
        };

        // Calculate size (rough estimate)
        let size_bytes = json_value.to_string().len();

        // Create cache entry
        let mut entry = CacheEntry::new(key.to_string(), json_value, level);
        entry.size_bytes = size_bytes;

        if let Some(ttl) = ttl {
            entry = entry.with_expiration(ttl);
        } else {
            entry = entry.with_expiration(self.config.default_ttl);
        }

        // Store in cache
        {
            let mut cache_guard = cache.write().await;

            // Check if we need to evict entries to make space
            if cache_guard.len() >= self.config.max_entries {
                self.evict_lru_entries(&mut cache_guard, level).await;
            }

            cache_guard.insert(key.to_string(), entry);
        }

        // Update metrics
        self.update_put_metrics(level, size_bytes).await;

        Ok(())
    }

    /// Invalidate cache entries by key pattern
    pub async fn invalidate(&self, pattern: &str) -> usize {
        let mut invalidated = 0;

        // Invalidate from all cache levels
        invalidated += self.invalidate_in_cache(&self.memory_cache, pattern).await;
        invalidated += self.invalidate_in_cache(&self.query_cache, pattern).await;
        invalidated += self.invalidate_in_cache(&self.object_cache, pattern).await;
        invalidated += self.invalidate_in_cache(&self.metadata_cache, pattern).await;

        invalidated
    }

    /// Invalidate cache entries by tag
    pub async fn invalidate_by_tag(&self, tag: &str) -> usize {
        let rules = self.invalidation_rules.lock().await;
        let mut invalidated = 0;

        for rule in rules.iter() {
            if rule.tags.contains(&tag.to_string()) {
                invalidated += self.invalidate(&rule.pattern).await;
            }
        }

        invalidated
    }

    /// Warm up cache with frequently accessed data
    pub async fn warm_cache(&self, warmup_data: Vec<(String, CacheLevel, Duration)>) -> Result<usize, String> {
        let mut warmed = 0;

        for (key, level, ttl) in warmup_data {
            // In a real implementation, you would fetch the data from the database
            // For now, we'll just create placeholder entries
            let placeholder = serde_json::json!({"warmed": true});

            if let Ok(_) = self.put(&key, placeholder, level, Some(ttl)).await {
                warmed += 1;
            }
        }

        Ok(warmed)
    }

    /// Get cache statistics
    pub async fn get_metrics(&self) -> CacheMetrics {
        let metrics = self.metrics.read().await;
        metrics.clone()
    }

    /// Clear all cache entries
    pub async fn clear_all(&self) {
        self.memory_cache.write().await.clear();
        self.query_cache.write().await.clear();
        self.object_cache.write().await.clear();
        self.metadata_cache.write().await.clear();

        // Reset metrics
        let mut metrics = self.metrics.write().await;
        *metrics = CacheMetrics::default();
    }

    /// Clear cache entries for a specific level
    pub async fn clear_level(&self, level: CacheLevel) {
        let cache = self.get_cache_for_level(level);
        cache.write().await.clear();
    }

    /// Add invalidation rule
    pub async fn add_invalidation_rule(&self, rule: InvalidationRule) {
        let mut rules = self.invalidation_rules.lock().await;
        rules.push(rule);
    }

    /// Get cache for specific level
    fn get_cache_for_level(&self, level: CacheLevel) -> Arc<RwLock<HashMap<String, CacheEntry<serde_json::Value>>>> {
        match level {
            CacheLevel::Memory => self.memory_cache.clone(),
            CacheLevel::QueryResult => self.query_cache.clone(),
            CacheLevel::Object => self.object_cache.clone(),
            CacheLevel::Metadata => self.metadata_cache.clone(),
        }
    }

    /// Clean up expired entries in a cache level
    async fn cleanup_cache_level(
        cache: &Arc<RwLock<HashMap<String, CacheEntry<serde_json::Value>>>>,
        metrics: &Arc<RwLock<CacheMetrics>>,
        level: CacheLevel,
    ) {
        let mut expired_keys = Vec::new();

        {
            let cache_guard = cache.read().await;
            for (key, entry) in cache_guard.iter() {
                if entry.is_expired() {
                    expired_keys.push(key.clone());
                }
            }
        }

        if !expired_keys.is_empty() {
            let mut cache_guard = cache.write().await;
            let mut total_size = 0;
            let mut removed_count = 0;

            for key in expired_keys {
                if let Some(entry) = cache_guard.remove(&key) {
                    total_size += entry.size_bytes;
                    removed_count += 1;
                }
            }

            // Update metrics
            let mut metrics_guard = metrics.write().await;
            metrics_guard.expired_removed += removed_count as u64;
            metrics_guard.total_size_bytes = metrics_guard.total_size_bytes.saturating_sub(total_size);
        }
    }

    /// Evict entries using LRU strategy
    async fn evict_entries(
        memory_cache: &Arc<RwLock<HashMap<String, CacheEntry<serde_json::Value>>>>,
        query_cache: &Arc<RwLock<HashMap<String, CacheEntry<serde_json::Value>>>>,
        object_cache: &Arc<RwLock<HashMap<String, CacheEntry<serde_json::Value>>>>,
        metadata_cache: &Arc<RwLock<HashMap<String, CacheEntry<serde_json::Value>>>>,
        metrics: &Arc<RwLock<CacheMetrics>>,
        config: &CacheConfig,
    ) {
        let mut all_entries: Vec<_> = Vec::new();

        // Collect all entries with their cache level and priority
        Self::collect_cache_entries(&memory_cache, &mut all_entries, CacheLevel::Memory, &config.level_priorities).await;
        Self::collect_cache_entries(&query_cache, &mut all_entries, CacheLevel::QueryResult, &config.level_priorities).await;
        Self::collect_cache_entries(&object_cache, &mut all_entries, CacheLevel::Object, &config.level_priorities).await;
        Self::collect_cache_entries(&metadata_cache, &mut all_entries, CacheLevel::Metadata, &config.level_priorities).await;

        // Sort by priority (higher first) and LRU (older first)
        all_entries.sort_by(|a, b| {
            match b.priority.cmp(&a.priority) {
                std::cmp::Ordering::Equal => a.last_accessed.cmp(&b.last_accessed),
                other => other,
            }
        });

        // Evict entries until we're under the size limit
        let mut total_size = Self::get_total_size(memory_cache, query_cache, object_cache, metadata_cache);
        let target_size = config.max_size_bytes * 80 / 100; // Target 80% of max size

        while total_size > target_size && !all_entries.is_empty() {
            if let Some(entry_to_evict) = all_entries.pop() {
                let cache = Self::get_cache_by_level_ref(
                    memory_cache,
                    query_cache,
                    object_cache,
                    metadata_cache,
                    entry_to_evict.level,
                );

                let mut cache_guard = cache.write().await;
                if let Some(removed_entry) = cache_guard.remove(&entry_to_evict.key) {
                    total_size = total_size.saturating_sub(removed_entry.size_bytes);

                    // Update metrics
                    let mut metrics_guard = metrics.write().await;
                    metrics_guard.evictions += 1;
                    metrics_guard.total_size_bytes = metrics_guard.total_size_bytes.saturating_sub(removed_entry.size_bytes);
                }
            } else {
                break;
            }
        }
    }

    /// Collect cache entries for eviction
    async fn collect_cache_entries(
        cache: &Arc<RwLock<HashMap<String, CacheEntry<serde_json::Value>>>>,
        all_entries: &mut Vec<CachedEntry>,
        level: CacheLevel,
        level_priorities: &HashMap<CacheLevel, u8>,
    ) {
        let cache_guard = cache.read().await;
        let priority = level_priorities.get(&level).unwrap_or(&0);

        for (key, entry) in cache_guard.iter() {
            all_entries.push(CachedEntry {
                key: key.clone(),
                last_accessed: entry.last_accessed,
                size_bytes: entry.size_bytes,
                level,
                priority: *priority,
            });
        }
    }

    /// Get cache reference by level
    fn get_cache_by_level_ref<'a>(
        memory_cache: &'a Arc<RwLock<HashMap<String, CacheEntry<serde_json::Value>>>>,
        query_cache: &'a Arc<RwLock<HashMap<String, CacheEntry<serde_json::Value>>>>,
        object_cache: &'a Arc<RwLock<HashMap<String, CacheEntry<serde_json::Value>>>>,
        metadata_cache: &'a Arc<RwLock<HashMap<String, CacheEntry<serde_json::Value>>>>,
        level: CacheLevel,
    ) -> &'a Arc<RwLock<HashMap<String, CacheEntry<serde_json::Value>>>> {
        match level {
            CacheLevel::Memory => memory_cache,
            CacheLevel::QueryResult => query_cache,
            CacheLevel::Object => object_cache,
            CacheLevel::Metadata => metadata_cache,
        }
    }

    /// Get total size of all caches
    fn get_total_size(
        memory_cache: &Arc<RwLock<HashMap<String, CacheEntry<serde_json::Value>>>>,
        query_cache: &Arc<RwLock<HashMap<String, CacheEntry<serde_json::Value>>>>,
        object_cache: &Arc<RwLock<HashMap<String, CacheEntry<serde_json::Value>>>>,
        metadata_cache: &Arc<RwLock<HashMap<String, CacheEntry<serde_json::Value>>>>,
    ) -> usize {
        let mut total = 0;

        let memory_guard = memory_cache.read().await;
        total += memory_guard.values().map(|e| e.size_bytes).sum::<usize>();

        let query_guard = query_cache.read().await;
        total += query_guard.values().map(|e| e.size_bytes).sum::<usize>();

        let object_guard = object_cache.read().await;
        total += object_guard.values().map(|e| e.size_bytes).sum::<usize>();

        let metadata_guard = metadata_cache.read().await;
        total += metadata_guard.values().map(|e| e.size_bytes).sum::<usize>();

        total
    }

    /// Evict LRU entries from a specific cache
    async fn evict_lru_entries(
        cache: &mut HashMap<String, CacheEntry<serde_json::Value>>,
        level: CacheLevel,
    ) {
        if cache.is_empty() {
            return;
        }

        // Sort entries by last access time
        let mut entries: Vec<_> = cache.iter().collect();
        entries.sort_by_key(|(_, entry)| entry.last_accessed);

        // Remove oldest 10% of entries
        let remove_count = entries.len() / 10;
        for (key, _) in entries.iter().take(remove_count) {
            cache.remove(*key);
        }
    }

    /// Invalidate entries in a cache by pattern
    async fn invalidate_in_cache(
        cache: &Arc<RwLock<HashMap<String, CacheEntry<serde_json::Value>>>>,
        pattern: &str,
    ) -> usize {
        let mut invalidated = 0;
        let mut keys_to_remove = Vec::new();

        {
            let cache_guard = cache.read().await;
            for key in cache_guard.keys() {
                if self.matches_pattern(key, pattern) {
                    keys_to_remove.push(key.clone());
                }
            }
        }

        if !keys_to_remove.is_empty() {
            let mut cache_guard = cache.write().await;
            for key in keys_to_remove {
                if cache_guard.remove(&key).is_some() {
                    invalidated += 1;
                }
            }
        }

        invalidated
    }

    /// Check if key matches pattern
    fn matches_pattern(&self, key: &str, pattern: &str) -> bool {
        // Simple pattern matching - in real implementation use regex
        if pattern.contains('*') {
            let pattern_parts: Vec<&str> = pattern.split('*').collect();
            if pattern_parts.len() == 2 {
                return key.starts_with(pattern_parts[0]) && key.ends_with(pattern_parts[1]);
            }
        }
        key.contains(pattern)
    }

    /// Update hit metrics
    async fn update_hit_metrics(&self, level: CacheLevel, access_time: Duration) {
        let mut metrics = self.metrics.write().await;
        metrics.hits += 1;
        metrics.total_entries = self.get_total_entries_count().await;

        // Update level stats
        let level_stats = metrics.level_stats.entry(level).or_default();
        level_stats.hit_rate = (level_stats.hit_rate * (level_stats.entries as f64 - 1.0) + 1.0) / level_stats.entries as f64;

        // Update average access time
        let access_time_us = access_time.as_micros() as f64;
        metrics.avg_access_time_us = (metrics.avg_access_time_us * (metrics.hits - 1) as f64 + access_time_us) / metrics.hits as f64;
    }

    /// Update miss metrics
    async fn update_miss_metrics(&self, level: CacheLevel, access_time: Duration) {
        let mut metrics = self.metrics.write().await;
        metrics.misses += 1;
        metrics.total_entries = self.get_total_entries_count().await;

        // Update hit rate
        let total_requests = metrics.hits + metrics.misses;
        if total_requests > 0 {
            metrics.hit_rate = metrics.hits as f64 / total_requests as f64;
        }

        // Update level stats
        let level_stats = metrics.level_stats.entry(level).or_default();
        if level_stats.entries > 0 {
            level_stats.hit_rate = (level_stats.hit_rate * (level_stats.entries as f64 - 1.0)) / level_stats.entries as f64;
        }
    }

    /// Update put metrics
    async fn update_put_metrics(&self, level: CacheLevel, size_bytes: usize) {
        let mut metrics = self.metrics.write().await;
        metrics.total_entries = self.get_total_entries_count().await;
        metrics.total_size_bytes += size_bytes;

        // Update level stats
        let level_stats = metrics.level_stats.entry(level).or_default();
        level_stats.entries += 1;
        level_stats.size_bytes += size_bytes;
    }

    /// Get total number of entries across all cache levels
    async fn get_total_entries_count(&self) -> usize {
        let memory_count = self.memory_cache.read().await.len();
        let query_count = self.query_cache.read().await.len();
        let object_count = self.object_cache.read().await.len();
        let metadata_count = self.metadata_cache.read().await.len();

        memory_count + query_count + object_count + metadata_count
    }
}

/// Helper struct for cache eviction
#[derive(Debug)]
struct CachedEntry {
    key: String,
    last_accessed: Instant,
    size_bytes: usize,
    level: CacheLevel,
    priority: u8,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_cache_entry_creation() {
        let entry = CacheEntry::new(
            "test_key".to_string(),
            "test_value",
            CacheLevel::Memory,
        ).with_expiration(Duration::from_secs(60));

        assert_eq!(entry.key, "test_key");
        assert_eq!(entry.level, CacheLevel::Memory);
        assert!(!entry.is_expired());
        assert_eq!(entry.access_count, 0);
    }

    #[tokio::test]
    async fn test_cache_entry_expiration() {
        let entry = CacheEntry::new(
            "test_key".to_string(),
            "test_value",
            CacheLevel::Memory,
        ).with_expiration(Duration::from_millis(10));

        assert!(!entry.is_expired());
        sleep(Duration::from_millis(20)).await;
        assert!(entry.is_expired());
    }

    #[tokio::test]
    async fn test_cache_entry_access() {
        let mut entry = CacheEntry::new(
            "test_key".to_string(),
            "test_value",
            CacheLevel::Memory,
        );

        assert_eq!(entry.access_count, 0);
        entry.access();
        assert_eq!(entry.access_count, 1);
    }

    #[tokio::test]
    async fn test_cache_manager_creation() {
        let config = CacheConfig::default();
        let manager = CacheManager::new(config);

        // Test putting and getting values
        let result = manager.put("test_key", "test_value", CacheLevel::Memory, None).await;
        assert!(result.is_ok());

        let value: Option<String> = manager.get("test_key", CacheLevel::Memory).await;
        assert!(value.is_some());
        assert_eq!(value.unwrap(), "test_value");
    }

    #[tokio::test]
    async fn test_cache_invalidation() {
        let config = CacheConfig::default();
        let manager = CacheManager::new(config);

        // Put some values
        manager.put("test_key1", "value1", CacheLevel::Memory, None).await.unwrap();
        manager.put("test_key2", "value2", CacheLevel::Memory, None).await.unwrap();
        manager.put("other_key", "value3", CacheLevel::Memory, None).await.unwrap();

        // Invalidate by pattern
        let invalidated = manager.invalidate("test_key").await;
        assert_eq!(invalidated, 2);

        // Check remaining value
        let value: Option<String> = manager.get("other_key", CacheLevel::Memory).await;
        assert!(value.is_some());
        assert_eq!(value.unwrap(), "value3");

        // Check invalidated values
        let value1: Option<String> = manager.get("test_key1", CacheLevel::Memory).await;
        let value2: Option<String> = manager.get("test_key2", CacheLevel::Memory).await;
        assert!(value1.is_none());
        assert!(value2.is_none());
    }

    #[tokio::test]
    async fn test_cache_metrics() {
        let config = CacheConfig::default();
        let manager = CacheManager::new(config);

        // Initially empty
        let metrics = manager.get_metrics().await;
        assert_eq!(metrics.hits, 0);
        assert_eq!(metrics.misses, 0);
        assert_eq!(metrics.hit_rate, 0.0);

        // Put and get some values
        manager.put("test_key", "test_value", CacheLevel::Memory, None).await.unwrap();

        let _: Option<String> = manager.get("test_key", CacheLevel::Memory).await; // Hit
        let _: Option<String> = manager.get("missing_key", CacheLevel::Memory).await; // Miss

        let metrics = manager.get_metrics().await;
        assert_eq!(metrics.hits, 1);
        assert_eq!(metrics.misses, 1);
        assert_eq!(metrics.hit_rate, 0.5);
    }

    #[tokio::test]
    async fn test_cache_cleanup() {
        let mut config = CacheConfig::default();
        config.cleanup_interval = Duration::from_millis(50);
        let manager = CacheManager::new(config);

        manager.start().await.unwrap();

        // Put a value with short TTL
        manager.put("test_key", "test_value", CacheLevel::Memory, Some(Duration::from_millis(25))).await.unwrap();

        // Value should be there initially
        let value: Option<String> = manager.get("test_key", CacheLevel::Memory).await;
        assert!(value.is_some());

        // Wait for cleanup
        sleep(Duration::from_millis(100)).await;

        // Value should be expired and removed
        let value: Option<String> = manager.get("test_key", CacheLevel::Memory).await;
        assert!(value.is_none());

        manager.stop().await;
    }
}