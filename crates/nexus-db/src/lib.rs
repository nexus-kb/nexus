//! Nexus KB Phase 0 database layer.

mod catalog;
mod db;
mod embeddings;
mod ingest;
mod jobs;
mod lineage;
mod models;
mod pipeline;
mod search;
mod threading;

pub use crate::catalog::{
    CatalogStore, DbListStorageRecord, DbStorageTotalsRecord, ListActivityByDayRecord,
    ListCatalogItemRecord, ListDetailRecord, ListStatsTotalsRecord, ListTopAuthorRecord,
    MailingList, MailingListRepo,
};
pub use crate::db::Db;
pub use crate::embeddings::{
    EmbeddingInputRow, EmbeddingVectorUpsert, EmbeddingsStore, ListMeiliBootstrapRunsParams,
};
pub use crate::ingest::{
    BatchWriteOutcome, IngestCommitRow, IngestStore, ParsedBodyInput, ParsedMessageInput,
    WriteOutcome,
};
pub use crate::jobs::{
    EnqueueJobParams, JobAttempt, JobStateCount, JobStore, JobStoreMetrics, JobTypeStateCount,
    ListJobsParams, RetryDecision, RunningJobSnapshot,
};
pub use crate::lineage::{
    AssembledItemRecord, LineageSourceMessage, LineageStore, ListThreadsParams, MessageBodyRecord,
    MessageDetailRecord, PatchItemDetailRecord, PatchItemDiffRecord, PatchItemFileAggregateRecord,
    PatchItemFileBatchInput, PatchItemFileDiffSliceSource, PatchItemFileRecord, PatchItemRecord,
    PatchLogicalRecord, PatchSeriesRecord, PatchSeriesVersionRecord, SeriesListItemRecord,
    SeriesLogicalCompareRow, SeriesVersionPatchItemRecord, SeriesVersionPatchRef,
    SeriesVersionSummaryRecord, ThreadListItemRecord, ThreadMessageRecord, ThreadParticipantRecord,
    ThreadRefRecord, ThreadSummaryRecord, UpsertPatchItemFileInput, UpsertPatchItemInput,
    UpsertPatchSeriesInput, UpsertPatchSeriesVersionInput,
};
pub use crate::models::{EmbeddingBackfillRun, Job, JobState, MeiliBootstrapRun, PipelineRun};
pub use crate::pipeline::{ListPipelineRunsParams, PipelineStore};
pub use crate::search::SearchStore;
pub use crate::threading::{
    ThreadComponentWrite, ThreadMessageWrite, ThreadNodeWrite, ThreadSourceMessage,
    ThreadSummaryWrite, ThreadingApplyStats, ThreadingRunContext, ThreadingStore,
};

pub type Result<T> = std::result::Result<T, sqlx::Error>;
