use std::time::Duration;

use nexus_core::config::DatabaseConfig;
use sqlx::PgPool;
use sqlx::postgres::{PgListener, PgPoolOptions};

use crate::Result;

/// Database handle with connection pool and migration helpers.
#[derive(Clone)]
pub struct Db {
    pool: PgPool,
}

impl Db {
    /// Connect to Postgres using the provided configuration.
    pub async fn connect(cfg: &DatabaseConfig) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(cfg.max_connections)
            .acquire_timeout(Duration::from_secs(10))
            .connect(&cfg.url)
            .await?;
        Ok(Self { pool })
    }

    /// Borrow the underlying connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Run SQL migrations embedded by sqlx.
    pub async fn migrate(&self) -> Result<()> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }

    /// Open a Postgres LISTEN/NOTIFY listener on the same connection pool.
    pub async fn listener(&self) -> Result<PgListener> {
        PgListener::connect_with(&self.pool).await
    }
}
