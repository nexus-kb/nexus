//! Database access layer for Nexus.

mod db;
mod epochs;
mod jobs;
mod mail_ingest;
mod mailing_lists;

pub use crate::db::Db;
pub use crate::epochs::{MailingListEpoch, MailingListEpochStore};
pub use crate::jobs::{Job, JobStats, JobStore, StatusCount, notify_queue, worker_id};
pub use crate::mail_ingest::{EmailInput, EmailRecipient, MailIngestStore};
pub use crate::mailing_lists::{MailingList, MailingListStore};

/// Shared result type for database operations.
pub type Result<T> = std::result::Result<T, sqlx::Error>;
