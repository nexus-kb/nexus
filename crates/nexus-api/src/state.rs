use std::time::Duration;

use moka::future::Cache;
use nexus_core::config::Settings;
use nexus_core::embeddings::OpenAiEmbeddingsClient;
use nexus_db::{CatalogStore, Db, EmbeddingsStore, JobStore, LineageStore, PipelineStore};

#[derive(Clone)]
pub struct QueryEmbeddingCache {
    inner: Cache<String, Vec<f32>>,
}

impl QueryEmbeddingCache {
    pub fn new(ttl: Duration, max_entries: usize) -> Self {
        Self {
            inner: Cache::builder()
                .time_to_live(ttl)
                .max_capacity(max_entries.max(1) as u64)
                .build(),
        }
    }

    pub async fn get(&self, key: &str) -> Option<Vec<f32>> {
        self.inner.get(key).await
    }

    pub async fn insert(&self, key: String, vector: Vec<f32>) {
        self.inner.insert(key, vector).await;
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
    pub embedding_client: OpenAiEmbeddingsClient,
    pub http_client: reqwest::Client,
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
        let http_client = reqwest::Client::new();
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
            http_client,
            query_embedding_cache,
        }
    }
}
