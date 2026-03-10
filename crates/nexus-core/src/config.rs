use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub url: String,
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
}

fn default_max_connections() -> u32 {
    20
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct AppConfig {
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_build_sha")]
    pub build_sha: String,
    #[serde(default = "default_build_time")]
    pub build_time: String,
    #[serde(default = "default_schema_version")]
    pub schema_version: String,
}

fn default_port() -> u16 {
    3000
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

fn default_build_sha() -> String {
    "dev".to_string()
}

fn default_build_time() -> String {
    chrono::Utc::now().to_rfc3339()
}

fn default_schema_version() -> String {
    "phase0".to_string()
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct AdminConfig {
    #[serde(default = "default_admin_token")]
    pub token: String,
}

fn default_admin_token() -> String {
    "nexus-dev-admin".to_string()
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct MailConfig {
    #[serde(default = "default_mirror_root")]
    pub mirror_root: String,
    #[serde(default = "default_commit_batch_size")]
    pub commit_batch_size: usize,
}

fn default_mirror_root() -> String {
    "/opt/nexus/mailing-lists".to_string()
}

fn default_commit_batch_size() -> usize {
    250
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct MainlineConfig {
    #[serde(default = "default_mainline_repo_path")]
    pub repo_path: String,
    #[serde(default = "default_mainline_commit_window_size")]
    pub commit_window_size: usize,
    #[serde(default = "default_mainline_scan_parallelism")]
    pub scan_parallelism: usize,
}

fn default_mainline_repo_path() -> String {
    "/opt/nexus/mainline".to_string()
}

fn default_mainline_commit_window_size() -> usize {
    1_000
}

fn default_mainline_scan_parallelism() -> usize {
    4
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct MeiliConfig {
    #[serde(default = "default_meili_url")]
    pub url: String,
    #[serde(default = "default_meili_master_key")]
    pub master_key: String,
    #[serde(default = "default_meili_upsert_batch_size")]
    pub upsert_batch_size: usize,
}

fn default_meili_url() -> String {
    "http://127.0.0.1:7700".to_string()
}

fn default_meili_master_key() -> String {
    "nexus-dev-key".to_string()
}

fn default_meili_upsert_batch_size() -> usize {
    1000
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct EmbeddingsConfig {
    #[serde(default = "default_embeddings_base_url")]
    pub base_url: String,
    #[serde(default = "default_embeddings_api_key")]
    pub api_key: String,
    #[serde(default = "default_embeddings_model")]
    pub model: String,
    #[serde(default = "default_embeddings_dimensions")]
    pub dimensions: usize,
    #[serde(default = "default_embeddings_embedder_name")]
    pub embedder_name: String,
    #[serde(default = "default_embeddings_query_cache_ttl_secs")]
    pub query_cache_ttl_secs: u64,
    #[serde(default = "default_embeddings_query_cache_max_entries")]
    pub query_cache_max_entries: usize,
    #[serde(default = "default_embeddings_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_embeddings_enqueue_batch_size")]
    pub enqueue_batch_size: usize,
    #[serde(default)]
    pub openrouter_referer: Option<String>,
    #[serde(default)]
    pub openrouter_title: Option<String>,
}

fn default_embeddings_base_url() -> String {
    "https://openrouter.ai/api/v1".to_string()
}

fn default_embeddings_api_key() -> String {
    String::new()
}

fn default_embeddings_model() -> String {
    "qwen/qwen3-embedding-4b".to_string()
}

fn default_embeddings_dimensions() -> usize {
    768
}

fn default_embeddings_embedder_name() -> String {
    "qwen3".to_string()
}

fn default_embeddings_query_cache_ttl_secs() -> u64 {
    120
}

fn default_embeddings_query_cache_max_entries() -> usize {
    10_000
}

fn default_embeddings_batch_size() -> usize {
    32
}

fn default_embeddings_enqueue_batch_size() -> usize {
    100
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct WorkerConfig {
    #[serde(default = "default_worker_poll_ms")]
    pub poll_ms: u64,
    #[serde(default = "default_worker_claim_batch")]
    pub claim_batch: i64,
    #[serde(default = "default_worker_lease_ms")]
    pub lease_ms: i64,
    #[serde(default = "default_worker_heartbeat_ms")]
    pub heartbeat_ms: u64,
    #[serde(default = "default_worker_sweep_ms")]
    pub sweep_ms: u64,
    #[serde(default = "default_worker_base_backoff_ms")]
    pub base_backoff_ms: u64,
    #[serde(default = "default_worker_max_backoff_ms")]
    pub max_backoff_ms: u64,
    #[serde(default = "default_worker_ingest_parse_concurrency")]
    pub ingest_parse_concurrency: usize,
    #[serde(default = "default_worker_max_inflight_jobs")]
    pub max_inflight_jobs: usize,
    #[serde(default = "default_worker_progress_checkpoint_interval")]
    pub progress_checkpoint_interval: usize,
}

fn default_worker_poll_ms() -> u64 {
    1500
}

fn default_worker_claim_batch() -> i64 {
    16
}

fn default_worker_lease_ms() -> i64 {
    45_000
}

fn default_worker_heartbeat_ms() -> u64 {
    15_000
}

fn default_worker_sweep_ms() -> u64 {
    10_000
}

fn default_worker_base_backoff_ms() -> u64 {
    15_000
}

fn default_worker_max_backoff_ms() -> u64 {
    3_600_000
}

fn default_worker_ingest_parse_concurrency() -> usize {
    8
}

fn default_worker_max_inflight_jobs() -> usize {
    1
}

fn default_worker_progress_checkpoint_interval() -> usize {
    10_000
}

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub database: DatabaseConfig,
    #[serde(default)]
    pub app: AppConfig,
    #[serde(default)]
    pub admin: AdminConfig,
    #[serde(default)]
    pub mail: MailConfig,
    #[serde(default)]
    pub mainline: MainlineConfig,
    #[serde(default)]
    pub meili: MeiliConfig,
    #[serde(default)]
    pub embeddings: EmbeddingsConfig,
    #[serde(default)]
    pub worker: WorkerConfig,
}

pub fn load() -> Result<Settings, crate::Error> {
    let cfg = config::Config::builder()
        .add_source(config::Environment::with_prefix("NEXUS").separator("__"))
        .build()
        .map_err(|e| crate::Error::Config(e.to_string()))?;

    let mut settings: Settings = cfg
        .try_deserialize()
        .map_err(|e| crate::Error::Config(e.to_string()))?;

    if settings.database.max_connections == 0 {
        settings.database.max_connections = default_max_connections();
    }
    if settings.app.host.trim().is_empty() {
        settings.app.host = default_host();
    }
    if settings.app.port == 0 {
        settings.app.port = default_port();
    }
    if settings.admin.token.trim().is_empty() {
        settings.admin.token = default_admin_token();
    }
    if settings.mail.mirror_root.trim().is_empty() {
        settings.mail.mirror_root = default_mirror_root();
    }
    if settings.mail.commit_batch_size == 0 {
        settings.mail.commit_batch_size = default_commit_batch_size();
    }
    if settings.mainline.repo_path.trim().is_empty() {
        settings.mainline.repo_path = default_mainline_repo_path();
    }
    if settings.mainline.commit_window_size == 0 {
        settings.mainline.commit_window_size = default_mainline_commit_window_size();
    }
    if settings.mainline.scan_parallelism == 0 {
        settings.mainline.scan_parallelism = default_mainline_scan_parallelism();
    }
    if settings.meili.url.trim().is_empty() {
        settings.meili.url = default_meili_url();
    }
    if settings.meili.master_key.trim().is_empty() {
        settings.meili.master_key = default_meili_master_key();
    }
    if settings.meili.upsert_batch_size == 0 {
        settings.meili.upsert_batch_size = default_meili_upsert_batch_size();
    }
    if settings.embeddings.base_url.trim().is_empty() {
        settings.embeddings.base_url = default_embeddings_base_url();
    }
    if settings.embeddings.model.trim().is_empty() {
        settings.embeddings.model = default_embeddings_model();
    }
    if settings.embeddings.dimensions == 0 {
        settings.embeddings.dimensions = default_embeddings_dimensions();
    }
    if settings.embeddings.embedder_name.trim().is_empty() {
        settings.embeddings.embedder_name = default_embeddings_embedder_name();
    }
    if settings.embeddings.query_cache_ttl_secs == 0 {
        settings.embeddings.query_cache_ttl_secs = default_embeddings_query_cache_ttl_secs();
    }
    if settings.embeddings.query_cache_max_entries == 0 {
        settings.embeddings.query_cache_max_entries = default_embeddings_query_cache_max_entries();
    }
    if settings.embeddings.batch_size == 0 {
        settings.embeddings.batch_size = default_embeddings_batch_size();
    }
    if settings.embeddings.enqueue_batch_size == 0 {
        settings.embeddings.enqueue_batch_size = default_embeddings_enqueue_batch_size();
    }
    if settings.embeddings.api_key.trim().is_empty() {
        return Err(crate::Error::Config(
            "embeddings requires NEXUS__EMBEDDINGS__API_KEY".to_string(),
        ));
    }
    if settings.embeddings.base_url.trim().is_empty() {
        return Err(crate::Error::Config(
            "embeddings requires NEXUS__EMBEDDINGS__BASE_URL".to_string(),
        ));
    }
    if settings.embeddings.model.trim().is_empty() {
        return Err(crate::Error::Config(
            "embeddings requires NEXUS__EMBEDDINGS__MODEL".to_string(),
        ));
    }
    if settings.worker.poll_ms == 0 {
        settings.worker.poll_ms = default_worker_poll_ms();
    }
    if settings.worker.claim_batch <= 0 {
        settings.worker.claim_batch = default_worker_claim_batch();
    }
    if settings.worker.lease_ms <= 0 {
        settings.worker.lease_ms = default_worker_lease_ms();
    }
    if settings.worker.heartbeat_ms == 0 {
        settings.worker.heartbeat_ms = default_worker_heartbeat_ms();
    }
    if settings.worker.sweep_ms == 0 {
        settings.worker.sweep_ms = default_worker_sweep_ms();
    }
    if settings.worker.base_backoff_ms == 0 {
        settings.worker.base_backoff_ms = default_worker_base_backoff_ms();
    }
    if settings.worker.max_backoff_ms == 0 {
        settings.worker.max_backoff_ms = default_worker_max_backoff_ms();
    }
    if settings.worker.ingest_parse_concurrency == 0 {
        settings.worker.ingest_parse_concurrency = default_worker_ingest_parse_concurrency();
    }
    if settings.worker.max_inflight_jobs == 0 {
        settings.worker.max_inflight_jobs = default_worker_max_inflight_jobs();
    }
    if settings.worker.progress_checkpoint_interval == 0 {
        settings.worker.progress_checkpoint_interval =
            default_worker_progress_checkpoint_interval();
    }

    Ok(settings)
}
