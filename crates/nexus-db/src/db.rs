use std::time::Duration;

use nexus_core::config::DatabaseConfig;
use sqlx::PgPool;
use sqlx::postgres::{PgListener, PgPoolOptions};

use crate::Result;

#[derive(Clone)]
pub struct Db {
    pool: PgPool,
}

impl Db {
    pub async fn connect(cfg: &DatabaseConfig) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(cfg.max_connections)
            .acquire_timeout(Duration::from_secs(15))
            .connect(&cfg.url)
            .await?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn migrate(&self) -> std::result::Result<(), sqlx::migrate::MigrateError> {
        sqlx::migrate!("./migrations").run(&self.pool).await
    }

    pub async fn listener(&self) -> Result<PgListener> {
        PgListener::connect_with(&self.pool).await
    }
}
