use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub url: String,
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
}

fn default_max_connections() -> u32 {
    10
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct AppConfig {
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_host")]
    pub host: String,
}

fn default_port() -> u16 {
    3000
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct MailConfig {
    #[serde(default = "default_mirror_root")]
    pub mirror_root: String,
    #[serde(default)]
    pub webhook_secret: Option<String>,
}

fn default_mirror_root() -> String {
    "/Users/tansanrao/work/nexus/mirrors".to_string()
}

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub database: DatabaseConfig,
    #[serde(default)]
    pub app: AppConfig,
    #[serde(default)]
    pub mail: MailConfig,
}

pub fn load() -> Result<Settings, crate::Error> {
    let builder = config::Config::builder()
        .add_source(config::Environment::with_prefix("NEXUS").separator("__"));
    let cfg = builder
        .build()
        .map_err(|e| crate::Error::Config(e.to_string()))?;
    let mut settings: Settings = cfg
        .try_deserialize()
        .map_err(|e| crate::Error::Config(e.to_string()))?;

    // Inject defaults if empty/zero provided
    if settings.app.port == 0 {
        settings.app.port = default_port();
    }
    if settings.app.host.trim().is_empty() {
        settings.app.host = default_host();
    }
    if settings.database.max_connections == 0 {
        settings.database.max_connections = default_max_connections();
    }
    if settings.mail.mirror_root.trim().is_empty() {
        settings.mail.mirror_root = default_mirror_root();
    }

    Ok(settings)
}
