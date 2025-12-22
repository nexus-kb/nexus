//! Environment-backed configuration helpers.

use std::env;

/// Resolve the database URL from standard Nexus environment variables.
pub fn resolve_db_url() -> Result<String, Box<dyn std::error::Error>> {
    if let Ok(url) = env::var("NEXUS_DATABASE_URL") {
        return Ok(url);
    }
    if let Ok(url) = env::var("NEXUS__DATABASE__URL") {
        return Ok(url);
    }
    Err("NEXUS_DATABASE_URL or NEXUS__DATABASE__URL must be set".into())
}
