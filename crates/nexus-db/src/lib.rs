//! Nexus KB Phase 0 database layer.

mod catalog;
mod db;
mod embeddings;
mod ingest;
mod jobs;
mod lineage;
mod mainline;
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
    BackfillProgressUpdate, EmbeddingInputRow, EmbeddingVectorUpsert, EmbeddingsStore,
    ListMeiliBootstrapRunsParams,
};
pub use crate::ingest::{
    BatchWriteOutcome, IngestCommitRow, IngestStore, ParsedBodyInput, ParsedMessageInput,
    ParsedPatchFactsInput, ParsedPatchFileFactInput, WriteOutcome,
};
pub use crate::jobs::{
    EnqueueJobParams, JobAttempt, JobStateCount, JobStore, JobStoreMetrics, JobTypeStateCount,
    ListJobsParams, RetryDecision, RetryJobResult, RunningJobSnapshot,
    is_running_attempt_unique_violation,
};
pub use crate::lineage::{
    AssembledItemRecord, LineageSourceMessage, LineageStore, ListThreadsParams, MessageBodyRecord,
    MessageDetailRecord, MessageThreadMatchRecord, PatchFactHydrationOutcome,
    PatchItemDetailRecord, PatchItemDiffRecord, PatchItemFileAggregateRecord,
    PatchItemFileBatchInput, PatchItemFileDiffSliceSource, PatchItemFileRecord, PatchItemRecord,
    PatchLogicalRecord, PatchSeriesRecord, PatchSeriesVersionRecord, SeriesListItemRecord,
    SeriesLogicalCompareRow, SeriesVersionPatchItemRecord, SeriesVersionPatchRef,
    SeriesVersionSummaryRecord, SeriesVersionThreadRefBackfillCandidate, ThreadListItemRecord,
    ThreadMessageRecord, ThreadParticipantRecord, ThreadRefRecord, ThreadSummaryRecord,
    UpsertPatchItemFileInput, UpsertPatchItemInput, UpsertPatchSeriesInput,
    UpsertPatchSeriesVersionInput,
};
pub use crate::mainline::{
    CanonicalPatchMatchInput, CreateMainlineScanRunParams, ListMainlineScanRunsParams,
    MainlinePatchCandidate, MainlineStore, SeriesMergeSummary, UpsertMainlineCommitInput,
    VersionMergeSummary,
};
pub use crate::models::{
    EmbeddingBackfillRun, Job, JobState, MainlineScanRun, MainlineScanState, MeiliBootstrapRun,
    PipelineRun,
};
pub use crate::pipeline::{ListPipelineRunsParams, PipelineStore};
pub use crate::search::SearchStore;
pub use crate::threading::{
    ThreadComponentWrite, ThreadMessageWrite, ThreadNodeWrite, ThreadSourceMessage,
    ThreadSummaryWrite, ThreadingApplyStats, ThreadingRunContext, ThreadingStore,
};

pub type Result<T> = std::result::Result<T, sqlx::Error>;
