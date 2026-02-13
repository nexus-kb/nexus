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
pub struct MeiliConfig {
    #[serde(default = "default_meili_url")]
    pub url: String,
    #[serde(default = "default_meili_master_key")]
    pub master_key: String,
}

fn default_meili_url() -> String {
    "http://127.0.0.1:7700".to_string()
}

fn default_meili_master_key() -> String {
    "nexus-dev-key".to_string()
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
    pub meili: MeiliConfig,
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
    if settings.meili.url.trim().is_empty() {
        settings.meili.url = default_meili_url();
    }
    if settings.meili.master_key.trim().is_empty() {
        settings.meili.master_key = default_meili_master_key();
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

    Ok(settings)
}
