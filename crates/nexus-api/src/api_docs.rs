use std::iter;

use utoipa::OpenApi;
use utoipa::openapi::path::{HttpMethod, Operation, OperationBuilder, Paths};
use utoipa::openapi::request_body::{RequestBody, RequestBodyBuilder};
use utoipa::openapi::response::{Response, ResponseBuilder, Responses};
use utoipa::openapi::security::{ApiKey, ApiKeyValue, SecurityRequirement, SecurityScheme};
use utoipa::openapi::{Content, OpenApi as UtoipaOpenApi, Ref, Required, Tag};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Nexus KB Public API",
        version = "v1",
        description = "Read-only public API for lists, threads, messages, series, diffs, and search."
    ),
    components(schemas(
        crate::error::InvalidParam,
        crate::error::ProblemDetails,
        crate::handlers::health::HealthResponse,
        crate::handlers::health::ReadyDeps,
        crate::handlers::health::ReadyResponse,
        crate::handlers::health::VersionResponse,
        crate::handlers::public::types::PageInfoResponse,
        crate::handlers::public::types::ListSummaryResponse,
        crate::handlers::public::types::ListCatalogResponse,
        crate::handlers::public::types::ListMirrorStateResponse,
        crate::handlers::public::types::ListCountsResponse,
        crate::handlers::public::types::ListFacetsHintResponse,
        crate::handlers::public::types::ListDetailResponse,
        crate::handlers::public::types::ListTopAuthorResponse,
        crate::handlers::public::types::ListActivityByDayResponse,
        crate::handlers::public::types::ListStatsResponse,
        crate::handlers::public::types::ThreadMessageParticipant,
        crate::handlers::public::types::ThreadMessageEntry,
        crate::handlers::public::types::ThreadDetailResponse,
        crate::handlers::public::types::ThreadListItemResponse,
        crate::handlers::public::types::ThreadListResponse,
        crate::handlers::public::types::ThreadMessagesResponse,
        crate::handlers::public::types::MessageBodyQuery,
        crate::handlers::public::types::MessageBodyResponse,
        crate::handlers::public::types::MessageResponse,
        crate::handlers::public::types::PatchItemResponse,
        crate::handlers::public::types::PatchItemFileResponse,
        crate::handlers::public::types::PatchItemFilesResponse,
        crate::handlers::public::types::PatchItemDiffResponse,
        crate::handlers::public::types::PatchItemFileDiffResponse,
        crate::handlers::public::types::SeriesListItemResponse,
        crate::handlers::public::types::SeriesListResponse,
        crate::handlers::public::types::SeriesAuthorResponse,
        crate::handlers::public::types::SeriesThreadRefResponse,
        crate::handlers::public::types::SeriesVersionSummaryResponse,
        crate::handlers::public::types::SeriesDetailResponse,
        crate::handlers::public::types::SeriesVersionPatchItemResponse,
        crate::handlers::public::types::SeriesVersionResponse,
        crate::handlers::public::types::SeriesCompareSummary,
        crate::handlers::public::types::SeriesComparePatchRow,
        crate::handlers::public::types::SeriesCompareFileRow,
        crate::handlers::public::types::SeriesCompareResponse,
        crate::handlers::public::types::SearchItemResponse,
        crate::handlers::public::types::SearchResponse
    ))
)]
struct PublicApiSchemas;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Nexus KB Admin API",
        version = "v1",
        description = "Control-plane API for jobs, ingest orchestration, diagnostics, rebuilds, and indexing maintenance."
    ),
    components(schemas(
        crate::error::InvalidParam,
        crate::error::ProblemDetails,
        crate::handlers::admin::jobs::EnqueueRequest,
        crate::handlers::admin::jobs::EnqueueResponse,
        crate::handlers::admin::jobs::CursorPageInfoResponse,
        crate::handlers::admin::jobs::ListJobsResponse,
        crate::handlers::admin::jobs::JobAttemptsResponse,
        crate::handlers::admin::jobs::ActionResponse,
        crate::handlers::admin::ingest::IngestSyncRequest,
        crate::handlers::admin::ingest::IngestSyncResponse,
        crate::handlers::admin::ingest::IngestGrokmirrorResponse,
        crate::handlers::admin::ingest::IngestGrokmirrorListResult,
        crate::handlers::admin::ingest::ResetWatermarkRequest,
        crate::handlers::admin::rebuild::ThreadingRebuildRequest,
        crate::handlers::admin::rebuild::LineageRebuildRequest,
        crate::handlers::admin::embeddings::SearchEmbeddingsBackfillRequest,
        crate::handlers::admin::embeddings::SearchEmbeddingsBackfillResponse,
        crate::handlers::admin::meili_bootstrap::StartMeiliBootstrapRequest,
        crate::handlers::admin::meili_bootstrap::MeiliBootstrapEstimatesResponse,
        crate::handlers::admin::meili_bootstrap::StartMeiliBootstrapResponse,
        crate::handlers::admin::meili_bootstrap::ListMeiliBootstrapRunsResponse,
        crate::handlers::admin::pipeline::ListPipelineRunsResponse,
        crate::handlers::admin::diagnostics::QueueStateCountsResponse,
        crate::handlers::admin::diagnostics::QueueDiagnosticsResponse,
        crate::handlers::admin::diagnostics::StorageDbTotalsResponse,
        crate::handlers::admin::diagnostics::StorageDbListRepoCountsResponse,
        crate::handlers::admin::diagnostics::StorageDbListCountsResponse,
        crate::handlers::admin::diagnostics::StorageDbListResponse,
        crate::handlers::admin::diagnostics::StorageDbResponse,
        crate::handlers::admin::diagnostics::StorageMeiliIndexResponse,
        crate::handlers::admin::diagnostics::StorageMeiliIndexesResponse,
        crate::handlers::admin::diagnostics::StorageMeiliTotalsResponse,
        crate::handlers::admin::diagnostics::StorageMeiliListResponse,
        crate::handlers::admin::diagnostics::StorageMeiliResponse,
        crate::handlers::admin::diagnostics::StorageDriftResponse,
        crate::handlers::admin::diagnostics::StorageDiagnosticsResponse,
        nexus_db::JobState,
        nexus_db::Job,
        nexus_db::JobAttempt,
        nexus_db::JobTypeStateCount,
        nexus_db::RunningJobSnapshot,
        nexus_db::PipelineRun,
        nexus_db::EmbeddingBackfillRun,
        nexus_db::MeiliBootstrapRun
    ))
)]
struct AdminApiSchemas;

#[derive(Clone, Copy)]
struct OperationDef {
    path: &'static str,
    method: &'static str,
    summary: &'static str,
    tag: &'static str,
    success_status: &'static str,
    success_schema: Option<&'static str>,
    request_schema: Option<&'static str>,
    admin_security: bool,
    extra_problem_statuses: &'static [&'static str],
}

const PUBLIC_OPERATIONS: &[OperationDef] = &[
    op(
        "/api/v1/healthz",
        "get",
        "Health probe",
        "Core",
        "200",
        Some("HealthResponse"),
    ),
    op(
        "/api/v1/readyz",
        "get",
        "Readiness probe",
        "Core",
        "200",
        Some("ReadyResponse"),
    ),
    op(
        "/api/v1/version",
        "get",
        "Build metadata",
        "Core",
        "200",
        Some("VersionResponse"),
    ),
    op(
        "/api/v1/openapi.json",
        "get",
        "OpenAPI document",
        "Core",
        "200",
        None,
    ),
    op("/api/v1/docs", "get", "RapiDoc UI", "Core", "200", None),
    op(
        "/api/v1/lists",
        "get",
        "List catalog",
        "Lists",
        "200",
        Some("ListCatalogResponse"),
    ),
    op(
        "/api/v1/lists/{list_key}",
        "get",
        "List detail",
        "Lists",
        "200",
        Some("ListDetailResponse"),
    ),
    op(
        "/api/v1/lists/{list_key}/stats",
        "get",
        "List statistics window",
        "Lists",
        "200",
        Some("ListStatsResponse"),
    ),
    op(
        "/api/v1/lists/{list_key}/threads",
        "get",
        "Threads for list",
        "Threads",
        "200",
        Some("ThreadListResponse"),
    ),
    op(
        "/api/v1/lists/{list_key}/threads/{thread_id}",
        "get",
        "Thread detail",
        "Threads",
        "200",
        Some("ThreadDetailResponse"),
    ),
    op(
        "/api/v1/lists/{list_key}/threads/{thread_id}/messages",
        "get",
        "Thread messages page",
        "Threads",
        "200",
        Some("ThreadMessagesResponse"),
    ),
    op(
        "/api/v1/messages/{message_id}",
        "get",
        "Message metadata",
        "Messages",
        "200",
        Some("MessageResponse"),
    ),
    op(
        "/api/v1/messages/{message_id}/body",
        "get",
        "Message body payload",
        "Messages",
        "200",
        Some("MessageBodyResponse"),
    ),
    op(
        "/api/v1/r/{msgid}",
        "get",
        "Message-ID redirect",
        "Messages",
        "302",
        None,
    ),
    op(
        "/api/v1/series",
        "get",
        "Patch series list",
        "Series",
        "200",
        Some("SeriesListResponse"),
    ),
    op(
        "/api/v1/series/{series_id}",
        "get",
        "Patch series detail",
        "Series",
        "200",
        Some("SeriesDetailResponse"),
    ),
    op(
        "/api/v1/series/{series_id}/versions/{series_version_id}",
        "get",
        "Patch series version detail",
        "Series",
        "200",
        Some("SeriesVersionResponse"),
    ),
    op(
        "/api/v1/series/{series_id}/compare",
        "get",
        "Compare two patch series versions",
        "Series",
        "200",
        Some("SeriesCompareResponse"),
    ),
    op(
        "/api/v1/patch-items/{patch_item_id}",
        "get",
        "Patch item detail",
        "Diff",
        "200",
        Some("PatchItemResponse"),
    ),
    op(
        "/api/v1/patch-items/{patch_item_id}/files",
        "get",
        "Patch item file metadata",
        "Diff",
        "200",
        Some("PatchItemFilesResponse"),
    ),
    op(
        "/api/v1/patch-items/{patch_item_id}/files/{path}/diff",
        "get",
        "Patch file diff slice",
        "Diff",
        "200",
        Some("PatchItemFileDiffResponse"),
    ),
    op(
        "/api/v1/patch-items/{patch_item_id}/diff",
        "get",
        "Full patch diff",
        "Diff",
        "200",
        Some("PatchItemDiffResponse"),
    ),
    op(
        "/api/v1/search",
        "get",
        "Search threads and series",
        "Search",
        "200",
        Some("SearchResponse"),
    ),
];

const ADMIN_OPERATIONS: &[OperationDef] = &[
    op_admin(
        "/admin/v1/openapi.json",
        "get",
        "Admin OpenAPI document",
        "Core",
        "200",
        None,
        None,
    ),
    op_admin(
        "/admin/v1/docs",
        "get",
        "Admin RapiDoc UI",
        "Core",
        "200",
        None,
        None,
    ),
    op_admin(
        "/admin/v1/jobs/enqueue",
        "post",
        "Enqueue a job",
        "Jobs",
        "200",
        Some("EnqueueResponse"),
        Some("EnqueueRequest"),
    ),
    op_admin(
        "/admin/v1/jobs",
        "get",
        "List jobs",
        "Jobs",
        "200",
        Some("ListJobsResponse"),
        None,
    ),
    op_admin(
        "/admin/v1/jobs/{job_id}",
        "get",
        "Get job",
        "Jobs",
        "200",
        Some("Job"),
        None,
    ),
    op_admin(
        "/admin/v1/jobs/{job_id}/attempts",
        "get",
        "List job attempts",
        "Jobs",
        "200",
        Some("JobAttemptsResponse"),
        None,
    ),
    op_admin(
        "/admin/v1/jobs/{job_id}/cancel",
        "post",
        "Cancel job",
        "Jobs",
        "200",
        Some("ActionResponse"),
        None,
    ),
    op_admin_with_errors(
        "/admin/v1/jobs/{job_id}/retry",
        "post",
        "Retry job",
        "Jobs",
        "200",
        Some("ActionResponse"),
        None,
        &["409"],
    ),
    op_admin(
        "/admin/v1/diagnostics/queue",
        "get",
        "Queue diagnostics",
        "Diagnostics",
        "200",
        Some("QueueDiagnosticsResponse"),
        None,
    ),
    op_admin(
        "/admin/v1/diagnostics/storage",
        "get",
        "Storage diagnostics",
        "Diagnostics",
        "200",
        Some("StorageDiagnosticsResponse"),
        None,
    ),
    op_admin(
        "/admin/v1/diagnostics/meili/bootstrap",
        "post",
        "Start Meili bootstrap run",
        "Diagnostics",
        "200",
        Some("StartMeiliBootstrapResponse"),
        Some("StartMeiliBootstrapRequest"),
    ),
    op_admin(
        "/admin/v1/diagnostics/meili/bootstrap/runs",
        "get",
        "List Meili bootstrap runs",
        "Diagnostics",
        "200",
        Some("ListMeiliBootstrapRunsResponse"),
        None,
    ),
    op_admin(
        "/admin/v1/diagnostics/meili/bootstrap/{run_id}",
        "get",
        "Get Meili bootstrap run",
        "Diagnostics",
        "200",
        Some("MeiliBootstrapRun"),
        None,
    ),
    op_admin(
        "/admin/v1/diagnostics/meili/bootstrap/{run_id}/cancel",
        "post",
        "Cancel Meili bootstrap run",
        "Diagnostics",
        "200",
        Some("ActionResponse"),
        None,
    ),
    op_admin(
        "/admin/v1/ingest/sync",
        "post",
        "Sync ingest for one list",
        "Ingest",
        "200",
        Some("IngestSyncResponse"),
        Some("IngestSyncRequest"),
    ),
    op_admin(
        "/admin/v1/ingest/grokmirror",
        "post",
        "Sync ingest for all mirror lists",
        "Ingest",
        "200",
        None,
        None,
    ),
    op_admin(
        "/admin/v1/ingest/reset-watermark",
        "post",
        "Reset ingest watermark",
        "Ingest",
        "200",
        Some("ActionResponse"),
        Some("ResetWatermarkRequest"),
    ),
    op_admin(
        "/admin/v1/pipeline/runs",
        "get",
        "List pipeline runs",
        "Pipeline",
        "200",
        Some("ListPipelineRunsResponse"),
        None,
    ),
    op_admin(
        "/admin/v1/pipeline/runs/{run_id}",
        "get",
        "Get pipeline run",
        "Pipeline",
        "200",
        Some("PipelineRun"),
        None,
    ),
    op_admin(
        "/admin/v1/threading/rebuild",
        "post",
        "Enqueue threading rebuild",
        "Rebuild",
        "200",
        Some("EnqueueResponse"),
        Some("ThreadingRebuildRequest"),
    ),
    op_admin(
        "/admin/v1/lineage/rebuild",
        "post",
        "Enqueue lineage rebuild",
        "Rebuild",
        "200",
        Some("EnqueueResponse"),
        Some("LineageRebuildRequest"),
    ),
    op_admin(
        "/admin/v1/search/embeddings/backfill",
        "post",
        "Start embeddings backfill",
        "Embeddings",
        "200",
        Some("SearchEmbeddingsBackfillResponse"),
        Some("SearchEmbeddingsBackfillRequest"),
    ),
    op_admin(
        "/admin/v1/search/embeddings/backfill/{run_id}",
        "get",
        "Get embeddings backfill run",
        "Embeddings",
        "200",
        Some("EmbeddingBackfillRun"),
        None,
    ),
];

const fn op(
    path: &'static str,
    method: &'static str,
    summary: &'static str,
    tag: &'static str,
    success_status: &'static str,
    success_schema: Option<&'static str>,
) -> OperationDef {
    OperationDef {
        path,
        method,
        summary,
        tag,
        success_status,
        success_schema,
        request_schema: None,
        admin_security: false,
        extra_problem_statuses: &[],
    }
}

const fn op_admin(
    path: &'static str,
    method: &'static str,
    summary: &'static str,
    tag: &'static str,
    success_status: &'static str,
    success_schema: Option<&'static str>,
    request_schema: Option<&'static str>,
) -> OperationDef {
    OperationDef {
        path,
        method,
        summary,
        tag,
        success_status,
        success_schema,
        request_schema,
        admin_security: true,
        extra_problem_statuses: &[],
    }
}

const fn op_admin_with_errors(
    path: &'static str,
    method: &'static str,
    summary: &'static str,
    tag: &'static str,
    success_status: &'static str,
    success_schema: Option<&'static str>,
    request_schema: Option<&'static str>,
    extra_problem_statuses: &'static [&'static str],
) -> OperationDef {
    OperationDef {
        path,
        method,
        summary,
        tag,
        success_status,
        success_schema,
        request_schema,
        admin_security: true,
        extra_problem_statuses,
    }
}

pub fn public_openapi() -> UtoipaOpenApi {
    let mut doc = PublicApiSchemas::openapi();
    doc.paths = build_paths(PUBLIC_OPERATIONS);
    doc.tags = Some(build_tags(&[
        "Core", "Lists", "Threads", "Messages", "Series", "Diff", "Search",
    ]));
    doc
}

pub fn admin_openapi() -> UtoipaOpenApi {
    let mut doc = AdminApiSchemas::openapi();
    doc.paths = build_paths(ADMIN_OPERATIONS);
    doc.tags = Some(build_tags(&[
        "Core",
        "Jobs",
        "Diagnostics",
        "Ingest",
        "Pipeline",
        "Rebuild",
        "Embeddings",
    ]));

    let components = doc
        .components
        .get_or_insert_with(utoipa::openapi::Components::new);
    components.add_security_scheme(
        "admin_token",
        SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::with_description(
            "x-nexus-admin-token",
            "Pre-shared admin token required by /admin/v1 endpoints.",
        ))),
    );

    doc
}

fn build_paths(ops: &[OperationDef]) -> Paths {
    let mut paths = Paths::new();
    for op in ops {
        paths.add_path_operation(op.path, vec![http_method(op.method)], operation(op));
    }
    paths
}

fn operation(op: &OperationDef) -> Operation {
    let mut builder = OperationBuilder::new()
        .summary(Some(op.summary))
        .tag(op.tag)
        .responses(operation_responses(op));

    if let Some(schema_name) = op.request_schema {
        builder = builder.request_body(Some(operation_request_body(schema_name)));
    }
    if op.admin_security {
        builder = builder.security(SecurityRequirement::new(
            "admin_token",
            iter::empty::<&str>(),
        ));
    }

    builder.build()
}

fn operation_request_body(schema_name: &str) -> RequestBody {
    RequestBodyBuilder::new()
        .required(Some(Required::True))
        .content(
            "application/json",
            Content::new(Some(Ref::from_schema_name(schema_name))),
        )
        .build()
}

fn operation_responses(op: &OperationDef) -> Responses {
    let mut responses = vec![(
        op.success_status.to_string(),
        operation_success_response(op.success_schema),
    )];
    responses.extend(
        op.extra_problem_statuses
            .iter()
            .map(|status| (status.to_string(), operation_default_problem_response())),
    );
    responses.push(("default".to_string(), operation_default_problem_response()));
    Responses::from_iter(responses)
}

fn operation_success_response(schema_name: Option<&str>) -> Response {
    let mut builder = ResponseBuilder::new().description("Success");
    if let Some(schema_name) = schema_name {
        builder = builder.content(
            "application/json",
            Content::new(Some(Ref::from_schema_name(schema_name))),
        );
    }
    builder.build()
}

fn operation_default_problem_response() -> Response {
    ResponseBuilder::new()
        .description("Error")
        .content(
            "application/problem+json",
            Content::new(Some(Ref::from_schema_name("ProblemDetails"))),
        )
        .build()
}

fn http_method(method: &str) -> HttpMethod {
    match method {
        "get" => HttpMethod::Get,
        "post" => HttpMethod::Post,
        "put" => HttpMethod::Put,
        "patch" => HttpMethod::Patch,
        "delete" => HttpMethod::Delete,
        "options" => HttpMethod::Options,
        "head" => HttpMethod::Head,
        "trace" => HttpMethod::Trace,
        _ => panic!("unsupported method in OpenAPI operation definition: {method}"),
    }
}

fn build_tags(tags: &[&str]) -> Vec<Tag> {
    tags.iter().map(Tag::new).collect()
}

#[cfg(test)]
mod tests {
    use utoipa::openapi::path::HttpMethod;

    use super::{admin_openapi, public_openapi};

    #[test]
    fn public_openapi_contains_paths() {
        let doc = public_openapi();
        assert!(!doc.paths.paths.is_empty());
        assert!(
            doc.paths
                .get_path_operation("/api/v1/search", HttpMethod::Get)
                .is_some()
        );
    }

    #[test]
    fn admin_openapi_contains_security() {
        let doc = admin_openapi();
        let components = doc.components.as_ref().expect("components must exist");
        assert!(components.security_schemes.contains_key("admin_token"));
        let operation = doc
            .paths
            .get_path_operation("/admin/v1/jobs", HttpMethod::Get)
            .expect("admin jobs path must exist");
        assert!(operation.security.is_some());
    }

    #[test]
    fn admin_retry_job_documents_conflict_response() {
        let doc = admin_openapi();
        let operation = doc
            .paths
            .get_path_operation("/admin/v1/jobs/{job_id}/retry", HttpMethod::Post)
            .expect("retry job path must exist");
        assert!(operation.responses.responses.contains_key("409"));
    }
}
