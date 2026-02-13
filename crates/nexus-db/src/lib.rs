//! Nexus KB Phase 0 database layer.

mod catalog;
mod db;
mod ingest;
mod jobs;
mod models;

pub use crate::catalog::{CatalogStore, MailingList, MailingListRepo};
pub use crate::db::Db;
pub use crate::ingest::{IngestStore, ParsedBodyInput, ParsedMessageInput, WriteOutcome};
pub use crate::jobs::{
    EnqueueJobParams, JobAttempt, JobStore, JobStoreMetrics, ListJobsParams, RetryDecision,
};
pub use crate::models::{Job, JobState};

pub type Result<T> = std::result::Result<T, sqlx::Error>;
