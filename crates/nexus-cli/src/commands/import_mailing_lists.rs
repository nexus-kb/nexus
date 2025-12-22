//! Import mailing list metadata from a public-inbox manifest.

use chrono::{DateTime, TimeZone, Utc};
use nexus_core::config::DatabaseConfig;
use nexus_db::Db;
use sqlx::{PgPool, Row};
use std::env;
use std::path::PathBuf;
use tracing::{info, warn};

use crate::config::resolve_db_url;
use crate::manifest::fetch_manifest;

const DEFAULT_MANIFEST_URL: &str = "https://lore.kernel.org/manifest.js.gz";
const DEFAULT_MIRROR_ROOT: &str = "/Users/tansanrao/work/nexus/mirrors";

/// Execute the import-mailing-lists command.
pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_url =
        env::var("MANIFEST_URL").unwrap_or_else(|_| DEFAULT_MANIFEST_URL.to_string());
    let mirror_root = env::var("MIRROR_ROOT").unwrap_or_else(|_| DEFAULT_MIRROR_ROOT.to_string());
    let db_url = resolve_db_url()?;

    let manifest = fetch_manifest(&manifest_url).await?;
    info!(count = manifest.len(), "manifest loaded");

    let db = Db::connect(&DatabaseConfig {
        url: db_url,
        max_connections: 5,
    })
    .await?;
    db.migrate().await?;

    let pool = db.pool().clone();
    let mirror_root = PathBuf::from(mirror_root);

    let mut inserted = 0usize;
    let mut skipped_missing = 0usize;

    for (path, entry) in manifest {
        let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
        if parts.len() < 3 {
            warn!(path, "unexpected path format, skipping");
            continue;
        }
        let slug = parts[0].to_string();
        let epoch_part = parts[2];
        let epoch = epoch_part
            .trim_end_matches(".git")
            .parse::<i16>()
            .unwrap_or(0);
        let repo_relpath = format!("{}/{}", parts[1], epoch_part);

        let repo_path = mirror_root.join(&slug).join(&repo_relpath);
        if !repo_path.exists() {
            skipped_missing += 1;
            warn!(
                slug,
                epoch,
                repo = %repo_path.display(),
                "mirror repo missing locally; keeping metadata only"
            );
        }

        let desc = entry.description.clone();
        let modified_at = entry
            .modified
            .and_then(|ts| Utc.timestamp_opt(ts, 0).single());
        let reference_epoch = entry
            .reference
            .as_deref()
            .and_then(|r| r.split('/').last())
            .and_then(|s| s.trim_end_matches(".git").parse::<i16>().ok());

        let mailing_list_id = upsert_mailing_list(&pool, &slug, desc.as_deref()).await?;
        upsert_epoch(
            &pool,
            mailing_list_id,
            epoch,
            &repo_relpath,
            entry.fingerprint.as_deref(),
            modified_at,
            reference_epoch,
        )
        .await?;

        inserted += 1;
    }

    info!(inserted, skipped_missing, "manifest ingest complete");
    Ok(())
}

async fn upsert_mailing_list(
    pool: &PgPool,
    slug: &str,
    description: Option<&str>,
) -> Result<i32, sqlx::Error> {
    let rec = sqlx::query(
        r#"INSERT INTO mailing_lists (name, slug, description, enabled)
            VALUES ($1, $2, $3, FALSE)
            ON CONFLICT (slug) DO UPDATE SET description = EXCLUDED.description
            RETURNING id"#,
    )
    .bind(slug)
    .bind(slug)
    .bind(description)
    .fetch_one(pool)
    .await?;
    Ok(rec.get::<i32, _>("id"))
}

async fn upsert_epoch(
    pool: &PgPool,
    mailing_list_id: i32,
    epoch: i16,
    repo_relpath: &str,
    fingerprint: Option<&str>,
    modified_at: Option<DateTime<Utc>>,
    reference_epoch: Option<i16>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"INSERT INTO mailing_list_epochs (mailing_list_id, epoch, repo_relpath, fingerprint, modified_at, reference_epoch)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (mailing_list_id, epoch)
            DO UPDATE SET
                repo_relpath = EXCLUDED.repo_relpath,
                fingerprint = EXCLUDED.fingerprint,
                modified_at = EXCLUDED.modified_at,
                reference_epoch = EXCLUDED.reference_epoch"#,
    )
    .bind(mailing_list_id)
    .bind(epoch)
    .bind(repo_relpath)
    .bind(fingerprint)
    .bind(modified_at)
    .bind(reference_epoch)
    .execute(pool)
    .await?;
    Ok(())
}
