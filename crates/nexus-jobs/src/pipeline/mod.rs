use std::collections::{BTreeSet, HashSet};
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use chrono::Utc;
use gix::hash::ObjectId;
use nexus_core::config::Settings;
use nexus_core::embeddings::{EmbeddingsClientError, OpenAiEmbeddingsClient};
use nexus_core::search::MeiliIndexKind;
use nexus_db::{
    BackfillProgressUpdate, CatalogStore, EmbeddingVectorUpsert, EmbeddingsStore, EnqueueJobParams,
    IngestCommitRow, IngestStore, Job, JobStore, JobStoreMetrics, LineageStore, MailingListRepo,
    MainlineStore, ParsedBodyInput, ParsedMessageInput, ParsedPatchFactsInput,
    ParsedPatchFileFactInput, PipelineStore, SearchStore, ThreadComponentWrite, ThreadMessageWrite,
    ThreadNodeWrite, ThreadSummaryWrite, ThreadingApplyStats, ThreadingRunContext, ThreadingStore,
};
use once_cell::sync::Lazy;
use regex::Regex;
use sha2::{Digest, Sha256};
use tokio::task::JoinSet;
use tokio::time::{Duration, sleep};
use tracing::{info, warn};

use crate::diff_metadata::parse_diff_metadata;
use crate::lineage::{process_patch_enrichment_batch, process_patch_extract_threads};
use crate::mail::{ParseEmailError, parse_email};
use crate::meili::{MeiliClient, MeiliClientError, settings_differ};
use crate::patch_detect::extract_diff_text;
use crate::patch_id::compute_patch_id_stable;
use crate::payloads::{
    EmbeddingBackfillRunPayload, EmbeddingGenerateBatchPayload, EmbeddingScope,
    LineageRebuildListPayload, MainlineScanRunPayload, MeiliBootstrapRunPayload,
    PipelineEmbeddingPayload, PipelineIngestPayload, PipelineLexicalPayload,
    PipelineLineagePayload, PipelineThreadingPayload, ThreadingRebuildListPayload,
};
use crate::scanner::stream_new_commit_oid_chunks;
use crate::threading::{ThreadingInputMessage, build_threads};
use crate::{ExecutionContext, JobExecutionOutcome};

#[derive(Clone)]
pub struct Phase0JobHandler {
    settings: Settings,
    catalog: CatalogStore,
    ingest: IngestStore,
    threading: ThreadingStore,
    lineage: LineageStore,
    pipeline: PipelineStore,
    mainline: MainlineStore,
    search: SearchStore,
    embeddings: EmbeddingsStore,
    jobs: JobStore,
    meili: MeiliClient,
    embedding_client: OpenAiEmbeddingsClient,
}

const STAGE_INGEST: &str = "ingest";
const STAGE_THREADING: &str = "threading";
const STAGE_LINEAGE: &str = "lineage";
const STAGE_LEXICAL: &str = "lexical";
const STAGE_EMBEDDING: &str = "embedding";

const PRIORITY_INGEST: i32 = 20;
const PRIORITY_THREADING: i32 = 16;
const PRIORITY_LINEAGE: i32 = 14;
const PRIORITY_LEXICAL: i32 = 12;
const PRIORITY_PIPELINE_EMBEDDING: i32 = 3;
const PRIORITY_EMBEDDING_BATCH: i32 = 2;

static CHANGE_ID_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?im)^change-id:\s*(\S+)").expect("valid change-id regex"));
static BASE_COMMIT_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?im)^base-commit:\s*(\S+)").expect("valid base-commit regex"));

impl Phase0JobHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        settings: Settings,
        catalog: CatalogStore,
        ingest: IngestStore,
        threading: ThreadingStore,
        lineage: LineageStore,
        pipeline: PipelineStore,
        mainline: MainlineStore,
        search: SearchStore,
        embeddings: EmbeddingsStore,
        jobs: JobStore,
    ) -> Self {
        let meili = MeiliClient::from_settings(&settings);
        let embedding_client = OpenAiEmbeddingsClient::from_settings(&settings);
        Self {
            settings,
            catalog,
            ingest,
            threading,
            lineage,
            pipeline,
            mainline,
            search,
            embeddings,
            jobs,
            meili,
            embedding_client,
        }
    }

    pub async fn handle(&self, job: Job, ctx: ExecutionContext) -> JobExecutionOutcome {
        match job.job_type.as_str() {
            "pipeline_ingest" => self.handle_pipeline_ingest(job, ctx).await,
            "pipeline_threading" => self.handle_pipeline_threading(job, ctx).await,
            "pipeline_lineage" => self.handle_pipeline_lineage(job, ctx).await,
            "pipeline_lexical" => self.handle_pipeline_lexical(job, ctx).await,
            "pipeline_embedding" => self.handle_pipeline_embedding(job, ctx).await,
            "threading_rebuild_list" => self.handle_threading_rebuild_list(job, ctx).await,
            "lineage_rebuild_list" => self.handle_lineage_rebuild_list(job, ctx).await,
            "embedding_backfill_run" => self.handle_embedding_backfill_run(job, ctx).await,
            "embedding_generate_batch" => self.handle_embedding_generate_batch(job, ctx).await,
            "meili_bootstrap_run" => self.handle_meili_bootstrap_run(job, ctx).await,
            "mainline_scan_run" => self.handle_mainline_scan_run(job, ctx).await,
            other => JobExecutionOutcome::Terminal {
                reason: format!("unknown job type: {other}"),
                kind: "invalid_job_type".to_string(),
                metrics: empty_metrics(0),
            },
        }
    }
}

mod embedding_backfill;
mod embedding_generate;
mod helpers;
mod ingest;
mod lexical;
mod lineage;
mod mainline;
mod meili_bootstrap;
mod pipeline_flow;
mod rebuild;
mod threading;

use helpers::*;
