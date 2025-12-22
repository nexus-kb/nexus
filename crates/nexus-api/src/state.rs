use nexus_db::Db;

/// Shared application state for handlers.
#[derive(Clone)]
pub struct ApiState {
    pub db: Db,
    pub webhook_secret: Option<String>,
}
