//! Database reset command.

use std::io::{self, Write};

use nexus_core::config::DatabaseConfig;
use nexus_db::Db;
use tracing::{info, warn};

use crate::config::resolve_db_url;

/// Execute the reset-db command.
pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let db_url = resolve_db_url()?;
    warn!(
        db = %db_url,
        "WARNING: this will DROP ALL DATA in the database and re-run migrations"
    );

    print!("Type RESET to continue: ");
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    if input.trim() != "RESET" {
        println!("Aborted.");
        return Ok(());
    }

    let db = Db::connect(&DatabaseConfig {
        url: db_url,
        max_connections: 5,
    })
    .await?;
    let pool = db.pool().clone();

    sqlx::query("DROP SCHEMA public CASCADE")
        .execute(&pool)
        .await?;
    sqlx::query("CREATE SCHEMA public").execute(&pool).await?;
    sqlx::query("GRANT ALL ON SCHEMA public TO public")
        .execute(&pool)
        .await?;
    sqlx::query("GRANT ALL ON SCHEMA public TO CURRENT_USER")
        .execute(&pool)
        .await?;

    db.migrate().await?;
    info!("database reset complete");

    Ok(())
}
