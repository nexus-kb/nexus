//! Shared helper routines for explorer handlers.

use nexus_db::MailingListStore;
use sqlx::PgPool;

use crate::http::{internal_error, ApiError};

/// Resolve a mailing list slug into a database id.
pub async fn resolve_mailing_list_id(pool: &PgPool, slug: &str) -> Result<i32, ApiError> {
    let store = MailingListStore::new(pool.clone());
    let list = store.find_by_slug(slug).await.map_err(internal_error)?;
    match list {
        Some(list) => Ok(list.id),
        None => Err(ApiError::not_found(&format!("mailing list '{}'", slug))),
    }
}
