use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use axum::Json;
use axum::extract::{Path as AxumPath, Query, State};
use chrono::{DateTime, Utc};
use nexus_core::search::MeiliIndexKind;
use nexus_db::{
    DbListStorageRecord, DbStorageTotalsRecord, EnqueueJobParams, Job, JobState, JobStateCount,
    JobTypeStateCount, ListJobsParams, ListMeiliBootstrapRunsParams, ListPipelineRunsParams,
    RunningJobSnapshot,
};
use nexus_jobs::payloads::{
    EmbeddingBackfillRunPayload, EmbeddingScope, LineageRebuildListPayload,
    LineageThreadRefsBackfillListPayload, MeiliBootstrapRunPayload, MeiliBootstrapScope,
    PipelineIngestPayload, ThreadingRebuildListPayload,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;

use crate::error::ApiError;
use crate::state::ApiState;

pub(crate) mod diagnostics;
pub(crate) mod embeddings;
mod helpers_and_tests;
pub(crate) mod ingest;
pub(crate) mod jobs;
pub(crate) mod mainline;
pub(crate) mod meili_bootstrap;
pub(crate) mod openapi;
pub(crate) mod pipeline;
pub(crate) mod rebuild;

pub use diagnostics::{diagnostics_queue, diagnostics_storage};
pub use embeddings::{get_search_embeddings_backfill, search_embeddings_backfill};
pub use ingest::{ingest_grokmirror, ingest_sync, reset_watermark};
pub use jobs::{cancel_job, enqueue_job, get_job, list_job_attempts, list_jobs, retry_job};
pub use mainline::{
    cancel_mainline_scan_run, get_mainline_scan_run, list_mainline_scan_runs, start_mainline_scan,
};
pub use meili_bootstrap::{
    cancel_meili_bootstrap_run, get_meili_bootstrap_run, list_meili_bootstrap_runs,
    start_meili_bootstrap,
};
pub use openapi::{openapi_docs, openapi_json};
pub use pipeline::{get_pipeline_run, list_pipeline_runs};
pub use rebuild::{lineage_rebuild, lineage_thread_refs_backfill, threading_rebuild};

use diagnostics::{
    QueueStateCountsResponse, StorageDbListCountsResponse, StorageDbListRepoCountsResponse,
    StorageDbListResponse, StorageDbTotalsResponse, StorageDriftResponse,
    StorageMeiliIndexResponse, StorageMeiliIndexesResponse, StorageMeiliListResponse,
    StorageMeiliResponse, StorageMeiliTotalsResponse,
};
use embeddings::MeiliStoragePayload;
use helpers_and_tests::*;
use ingest::{IngestQueueError, IngestSyncResponse, MirrorListCandidate};
use jobs::{ActionResponse, CursorPageInfoResponse, EnqueueResponse, IdCursorToken, default_limit};

type HandlerResult<T> = Result<T, ApiError>;
