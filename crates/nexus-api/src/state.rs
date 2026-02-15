use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use nexus_core::config::Settings;
use nexus_core::embeddings::OpenAiEmbeddingsClient;
use nexus_db::{CatalogStore, Db, EmbeddingsStore, JobStore, LineageStore, PipelineStore};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct QueryEmbeddingCache {
    inner: Arc<Mutex<HashMap<String, QueryEmbeddingCacheEntry>>>,
    ttl: Duration,
    max_entries: usize,
}

#[derive(Clone)]
struct QueryEmbeddingCacheEntry {
    vector: Vec<f32>,
    inserted_at: Instant,
}

impl QueryEmbeddingCache {
    pub fn new(ttl: Duration, max_entries: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            ttl,
            max_entries: max_entries.max(1),
        }
    }

    pub async fn get(&self, key: &str) -> Option<Vec<f32>> {
        let mut guard = self.inner.lock().await;
        let now = Instant::now();
        guard.retain(|_, entry| now.duration_since(entry.inserted_at) <= self.ttl);
        guard.get(key).map(|entry| entry.vector.clone())
    }

    pub async fn insert(&self, key: String, vector: Vec<f32>) {
        let mut guard = self.inner.lock().await;
        let now = Instant::now();
        guard.retain(|_, entry| now.duration_since(entry.inserted_at) <= self.ttl);
        if guard.len() >= self.max_entries {
            let mut oldest_key: Option<String> = None;
            let mut oldest_ts = now;
            for (entry_key, entry) in guard.iter() {
                if entry.inserted_at <= oldest_ts {
                    oldest_ts = entry.inserted_at;
                    oldest_key = Some(entry_key.clone());
                }
            }
            if let Some(oldest_key) = oldest_key {
                guard.remove(&oldest_key);
            }
        }
        guard.insert(
            key,
            QueryEmbeddingCacheEntry {
                vector,
                inserted_at: now,
            },
        );
    }
}

#[derive(Clone)]
pub struct ApiState {
    pub settings: Settings,
    pub db: Db,
    pub jobs: JobStore,
    pub catalog: CatalogStore,
    pub lineage: LineageStore,
    pub pipeline: PipelineStore,
    pub embeddings: EmbeddingsStore,
    pub embedding_client: Option<OpenAiEmbeddingsClient>,
    pub query_embedding_cache: QueryEmbeddingCache,
}

impl ApiState {
    pub fn new(settings: Settings, db: Db) -> Self {
        let jobs = JobStore::new(db.pool().clone());
        let catalog = CatalogStore::new(db.pool().clone());
        let lineage = LineageStore::new(db.pool().clone());
        let pipeline = PipelineStore::new(db.pool().clone());
        let embeddings = EmbeddingsStore::new(db.pool().clone());
        let embedding_client = OpenAiEmbeddingsClient::from_settings(&settings);
        let query_embedding_cache = QueryEmbeddingCache::new(
            Duration::from_secs(settings.embeddings.query_cache_ttl_secs),
            settings.embeddings.query_cache_max_entries,
        );
        Self {
            settings,
            db,
            jobs,
            catalog,
            lineage,
            pipeline,
            embeddings,
            embedding_client,
            query_embedding_cache,
        }
    }
}
