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
    MeiliBootstrapRunPayload, MeiliBootstrapScope, PipelineIngestPayload,
    ThreadingRebuildListPayload,
};
use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::state::ApiState;

mod diagnostics;
mod embeddings;
mod helpers_and_tests;
mod ingest;
mod jobs;
mod meili_bootstrap;
mod pipeline;
mod rebuild;

pub use diagnostics::{diagnostics_queue, diagnostics_storage};
pub use embeddings::{get_search_embeddings_backfill, search_embeddings_backfill};
pub use ingest::{ingest_grokmirror, ingest_sync, reset_watermark};
pub use jobs::{cancel_job, enqueue_job, get_job, list_job_attempts, list_jobs, retry_job};
pub use meili_bootstrap::{
    cancel_meili_bootstrap_run, get_meili_bootstrap_run, list_meili_bootstrap_runs,
    start_meili_bootstrap,
};
pub use pipeline::{get_pipeline_run, list_pipeline_runs};
pub use rebuild::{lineage_rebuild, threading_rebuild};

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
