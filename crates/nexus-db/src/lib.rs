//! Nexus KB Phase 0 database layer.

mod catalog;
mod db;
mod ingest;
mod jobs;
mod lineage;
mod models;
mod threading;

pub use crate::catalog::{CatalogStore, MailingList, MailingListRepo};
pub use crate::db::Db;
pub use crate::ingest::{IngestStore, ParsedBodyInput, ParsedMessageInput, WriteOutcome};
pub use crate::jobs::{
    EnqueueJobParams, JobAttempt, JobStore, JobStoreMetrics, ListJobsParams, RetryDecision,
};
pub use crate::lineage::{
    AssembledItemRecord, LineageSourceMessage, LineageStore, MessageBodyRecord, PatchItemDetailRecord,
    PatchItemDiffRecord, PatchItemFileDiffSliceSource, PatchItemFileRecord, PatchItemRecord,
    PatchLogicalRecord, PatchSeriesRecord, PatchSeriesVersionRecord, SeriesExportMessageRecord,
    SeriesVersionPatchRef, ThreadMessageRecord, ThreadSummaryRecord, UpsertPatchItemFileInput,
    UpsertPatchItemInput, UpsertPatchSeriesInput, UpsertPatchSeriesVersionInput,
};
pub use crate::models::{Job, JobState};
pub use crate::threading::{
    ThreadComponentWrite, ThreadMessageWrite, ThreadNodeWrite, ThreadSourceMessage,
    ThreadSummaryWrite, ThreadingApplyStats, ThreadingStore,
};

pub type Result<T> = std::result::Result<T, sqlx::Error>;
