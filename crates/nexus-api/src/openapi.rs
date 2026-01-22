//! OpenAPI document builder using okapi types.

use okapi::Map;
use okapi::openapi3::{
    Components, Info, MediaType, OpenApi, Operation, Parameter, ParameterValue, PathItem, RefOr,
    RequestBody, Response, Responses, Tag,
};
use schemars::schema::{InstanceType, Schema, SchemaObject};
use schemars::{JsonSchema, schema_for};

use crate::handlers::jobs::{EnqueueRequest, EnqueueResponse};
use crate::handlers::mailing_lists::UpdateMailingListRequest;
use crate::handlers::webhooks::WebhookEnqueueResponse;
use crate::models::{
    AuthorWithStats, EmailHierarchy, EmailWithAuthor, ListAggregateStats, MailingListStats,
    PatchMetadata, PatchRegion, PatchRegionType, Thread, ThreadDetail, ThreadWithStarter,
};
use crate::response::{ApiResponse, PaginationMeta, ResponseMeta, SortDescriptor, SortDirection};
use nexus_db::{Job, JobStats, MailingList};

type EmailListResponse = ApiResponse<Vec<EmailWithAuthor>>;
type EmailResponse = ApiResponse<EmailWithAuthor>;
type ThreadListResponse = ApiResponse<Vec<ThreadWithStarter>>;
type ThreadDetailResponse = ApiResponse<ThreadDetail>;
type AuthorListResponse = ApiResponse<Vec<AuthorWithStats>>;
type AuthorResponse = ApiResponse<AuthorWithStats>;
type AuthorEmailsResponse = ApiResponse<Vec<EmailWithAuthor>>;
type AuthorThreadsResponse = ApiResponse<Vec<ThreadWithStarter>>;
type MailingListStatsResponse = ApiResponse<MailingListStats>;
type AggregateStatsResponse = ApiResponse<ListAggregateStats>;

fn schema_ref(name: &str) -> SchemaObject {
    SchemaObject {
        reference: Some(format!("#/components/schemas/{name}")),
        ..SchemaObject::default()
    }
}

fn register_schema<T: JsonSchema>(components: &mut Components, name: &str) -> SchemaObject {
    let schema = schema_for!(T);
    for (def_name, def_schema) in schema.definitions {
        components
            .schemas
            .entry(def_name)
            .or_insert_with(|| def_schema.into_object());
    }
    components.schemas.insert(name.to_string(), schema.schema);
    schema_ref(name)
}

fn json_media(schema: SchemaObject) -> MediaType {
    MediaType {
        schema: Some(schema),
        ..MediaType::default()
    }
}

fn response_json(description: &str, schema: SchemaObject) -> Response {
    let mut content = Map::new();
    content.insert("application/json".to_string(), json_media(schema));
    Response {
        description: description.to_string(),
        content,
        ..Response::default()
    }
}

fn response_text(description: &str) -> Response {
    Response {
        description: description.to_string(),
        ..Response::default()
    }
}

fn ok_responses(schema: SchemaObject) -> Responses {
    let mut responses = Responses::default();
    responses.responses.insert(
        "200".to_string(),
        RefOr::Object(response_json("OK", schema)),
    );
    responses
}

fn ok_responses_no_body() -> Responses {
    let mut responses = Responses::default();
    responses
        .responses
        .insert("200".to_string(), RefOr::Object(response_text("OK")));
    responses
}

fn operation(
    summary: &str,
    tags: &[&str],
    parameters: Vec<Parameter>,
    request_body: Option<RequestBody>,
    responses: Responses,
) -> Operation {
    Operation {
        summary: Some(summary.to_string()),
        tags: tags.iter().map(|tag| (*tag).to_string()).collect(),
        parameters: parameters.into_iter().map(RefOr::Object).collect(),
        request_body: request_body.map(RefOr::Object),
        responses,
        ..Operation::default()
    }
}

fn path_item(
    get: Option<Operation>,
    post: Option<Operation>,
    patch: Option<Operation>,
) -> PathItem {
    PathItem {
        get,
        post,
        patch,
        ..PathItem::default()
    }
}

fn string_schema() -> SchemaObject {
    SchemaObject {
        instance_type: Some(InstanceType::String.into()),
        ..SchemaObject::default()
    }
}

fn integer_schema(format: Option<&str>) -> SchemaObject {
    let mut schema = SchemaObject {
        instance_type: Some(InstanceType::Integer.into()),
        ..SchemaObject::default()
    };
    if let Some(format) = format {
        schema.format = Some(format.to_string());
    }
    schema
}

fn boolean_schema() -> SchemaObject {
    SchemaObject {
        instance_type: Some(InstanceType::Boolean.into()),
        ..SchemaObject::default()
    }
}

fn array_schema(item: SchemaObject) -> SchemaObject {
    SchemaObject {
        instance_type: Some(InstanceType::Array.into()),
        array: Some(Box::new(schemars::schema::ArrayValidation {
            items: Some(schemars::schema::SingleOrVec::Single(Box::new(
                Schema::Object(item),
            ))),
            ..schemars::schema::ArrayValidation::default()
        })),
        ..SchemaObject::default()
    }
}

fn query_param(name: &str, schema: SchemaObject, description: &str) -> Parameter {
    Parameter {
        name: name.to_string(),
        location: "query".to_string(),
        description: Some(description.to_string()),
        required: false,
        deprecated: false,
        allow_empty_value: false,
        value: ParameterValue::Schema {
            style: None,
            explode: None,
            allow_reserved: false,
            schema,
            example: None,
            examples: None,
        },
        extensions: Map::new(),
    }
}

fn path_param(name: &str, schema: SchemaObject, description: &str) -> Parameter {
    Parameter {
        name: name.to_string(),
        location: "path".to_string(),
        description: Some(description.to_string()),
        required: true,
        deprecated: false,
        allow_empty_value: false,
        value: ParameterValue::Schema {
            style: None,
            explode: None,
            allow_reserved: false,
            schema,
            example: None,
            examples: None,
        },
        extensions: Map::new(),
    }
}

fn header_param(name: &str, schema: SchemaObject, description: &str) -> Parameter {
    Parameter {
        name: name.to_string(),
        location: "header".to_string(),
        description: Some(description.to_string()),
        required: false,
        deprecated: false,
        allow_empty_value: false,
        value: ParameterValue::Schema {
            style: None,
            explode: None,
            allow_reserved: false,
            schema,
            example: None,
            examples: None,
        },
        extensions: Map::new(),
    }
}

fn request_body_json(schema: SchemaObject, description: &str) -> RequestBody {
    let mut content = Map::new();
    content.insert("application/json".to_string(), json_media(schema));
    RequestBody {
        description: Some(description.to_string()),
        content,
        required: true,
        ..RequestBody::default()
    }
}

/// Build the OpenAPI spec for the HTTP API.
pub fn build_openapi() -> OpenApi {
    let mut api = OpenApi::default();
    api.openapi = "3.0.0".to_string();
    api.info = Info {
        title: "Nexus API".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        ..Info::default()
    };

    let mut components = Components::default();
    let mailing_list_schema = register_schema::<MailingList>(&mut components, "MailingList");
    let mailing_list_list_schema =
        register_schema::<Vec<MailingList>>(&mut components, "MailingListList");
    let job_schema = register_schema::<Job>(&mut components, "Job");
    let job_list_schema = register_schema::<Vec<Job>>(&mut components, "JobList");
    let job_stats_schema = register_schema::<JobStats>(&mut components, "JobStats");
    let enqueue_request_schema =
        register_schema::<EnqueueRequest>(&mut components, "EnqueueRequest");
    let enqueue_response_schema =
        register_schema::<EnqueueResponse>(&mut components, "EnqueueResponse");
    let update_list_schema =
        register_schema::<UpdateMailingListRequest>(&mut components, "UpdateMailingListRequest");
    let webhook_response_schema =
        register_schema::<WebhookEnqueueResponse>(&mut components, "WebhookEnqueueResponse");

    let _ = register_schema::<EmailWithAuthor>(&mut components, "EmailWithAuthor");
    let _ = register_schema::<EmailHierarchy>(&mut components, "EmailHierarchy");
    let _ = register_schema::<Thread>(&mut components, "Thread");
    let _ = register_schema::<ThreadWithStarter>(&mut components, "ThreadWithStarter");
    let _ = register_schema::<ThreadDetail>(&mut components, "ThreadDetail");
    let _ = register_schema::<AuthorWithStats>(&mut components, "AuthorWithStats");
    let _ = register_schema::<MailingListStats>(&mut components, "MailingListStats");
    let _ = register_schema::<ListAggregateStats>(&mut components, "ListAggregateStats");
    let _ = register_schema::<PaginationMeta>(&mut components, "PaginationMeta");
    let _ = register_schema::<ResponseMeta>(&mut components, "ResponseMeta");
    let _ = register_schema::<SortDescriptor>(&mut components, "SortDescriptor");
    let _ = register_schema::<SortDirection>(&mut components, "SortDirection");
    let _ = register_schema::<PatchMetadata>(&mut components, "PatchMetadata");
    let _ = register_schema::<PatchRegion>(&mut components, "PatchRegion");
    let _ = register_schema::<PatchRegionType>(&mut components, "PatchRegionType");

    let email_list_schema = register_schema::<EmailListResponse>(&mut components, "EmailList");
    let email_schema = register_schema::<EmailResponse>(&mut components, "EmailResponse");
    let thread_list_schema = register_schema::<ThreadListResponse>(&mut components, "ThreadList");
    let thread_detail_schema =
        register_schema::<ThreadDetailResponse>(&mut components, "ThreadDetailResponse");
    let author_list_schema = register_schema::<AuthorListResponse>(&mut components, "AuthorList");
    let author_schema = register_schema::<AuthorResponse>(&mut components, "AuthorResponse");
    let author_emails_schema =
        register_schema::<AuthorEmailsResponse>(&mut components, "AuthorEmailsResponse");
    let author_threads_schema =
        register_schema::<AuthorThreadsResponse>(&mut components, "AuthorThreadsResponse");
    let list_stats_schema =
        register_schema::<MailingListStatsResponse>(&mut components, "MailingListStatsResponse");
    let aggregate_stats_schema =
        register_schema::<AggregateStatsResponse>(&mut components, "AggregateStatsResponse");

    api.components = Some(components);

    let mut paths = Map::new();
    paths.insert(
        "/health".to_string(),
        path_item(
            Some(operation(
                "Health check",
                &["Health"],
                vec![],
                None,
                ok_responses_no_body(),
            )),
            None,
            None,
        ),
    );
    paths.insert(
        "/hello".to_string(),
        path_item(
            Some(operation(
                "Hello endpoint",
                &["Health"],
                vec![],
                None,
                ok_responses_no_body(),
            )),
            None,
            None,
        ),
    );
    paths.insert(
        "/jobs".to_string(),
        path_item(
            Some(operation(
                "List jobs",
                &["Jobs"],
                vec![
                    query_param(
                        "status",
                        string_schema(),
                        "Filter jobs by status (queued/running/succeeded/failed)",
                    ),
                    query_param("limit", integer_schema(Some("int64")), "Max results"),
                    query_param(
                        "offset",
                        integer_schema(Some("int64")),
                        "Offset into result set",
                    ),
                ],
                None,
                ok_responses(job_list_schema),
            )),
            Some(operation(
                "Enqueue job",
                &["Jobs"],
                vec![],
                Some(request_body_json(
                    enqueue_request_schema,
                    "Job payload and scheduling options",
                )),
                ok_responses(enqueue_response_schema),
            )),
            None,
        ),
    );
    paths.insert(
        "/jobs/{id}".to_string(),
        path_item(
            Some(operation(
                "Get job status",
                &["Jobs"],
                vec![path_param("id", integer_schema(Some("int64")), "Job id")],
                None,
                ok_responses(job_schema.clone()),
            )),
            None,
            None,
        ),
    );
    paths.insert(
        "/jobs/stats".to_string(),
        path_item(
            Some(operation(
                "Job statistics",
                &["Jobs"],
                vec![],
                None,
                ok_responses(job_stats_schema),
            )),
            None,
            None,
        ),
    );
    paths.insert(
        "/openapi.json".to_string(),
        path_item(
            Some(operation(
                "OpenAPI spec",
                &["Docs"],
                vec![],
                None,
                ok_responses_no_body(),
            )),
            None,
            None,
        ),
    );
    paths.insert(
        "/docs".to_string(),
        path_item(
            Some(operation(
                "API docs (RapiDoc)",
                &["Docs"],
                vec![],
                None,
                ok_responses_no_body(),
            )),
            None,
            None,
        ),
    );
    paths.insert(
        "/mailing-lists".to_string(),
        path_item(
            Some(operation(
                "List mailing lists",
                &["MailingLists"],
                vec![
                    query_param("enabled", boolean_schema(), "Filter enabled lists"),
                    query_param("limit", integer_schema(Some("int64")), "Max results"),
                    query_param(
                        "offset",
                        integer_schema(Some("int64")),
                        "Offset into result set",
                    ),
                ],
                None,
                ok_responses(mailing_list_list_schema.clone()),
            )),
            None,
            None,
        ),
    );
    paths.insert(
        "/mailing-lists/enabled".to_string(),
        path_item(
            Some(operation(
                "List enabled mailing lists",
                &["MailingLists"],
                vec![
                    query_param("limit", integer_schema(Some("int64")), "Max results"),
                    query_param(
                        "offset",
                        integer_schema(Some("int64")),
                        "Offset into result set",
                    ),
                ],
                None,
                ok_responses(mailing_list_list_schema),
            )),
            None,
            None,
        ),
    );
    paths.insert(
        "/mailing-lists/{slug}".to_string(),
        path_item(
            None,
            None,
            Some(operation(
                "Update mailing list",
                &["MailingLists"],
                vec![path_param("slug", string_schema(), "Mailing list slug")],
                Some(request_body_json(
                    update_list_schema,
                    "Mailing list update payload",
                )),
                ok_responses(mailing_list_schema.clone()),
            )),
        ),
    );
    paths.insert(
        "/webhooks/grokmirror".to_string(),
        path_item(
            None,
            Some(operation(
                "Grokmirror webhook",
                &["Webhooks"],
                vec![header_param(
                    "x-nexus-webhook-secret",
                    string_schema(),
                    "Shared secret for webhook authentication",
                )],
                None,
                ok_responses(webhook_response_schema),
            )),
            None,
        ),
    );
    paths.insert(
        "/lists/stats".to_string(),
        path_item(
            Some(operation(
                "Aggregate list stats",
                &["Lists"],
                vec![],
                None,
                ok_responses(aggregate_stats_schema),
            )),
            None,
            None,
        ),
    );
    paths.insert(
        "/lists/{slug}/stats".to_string(),
        path_item(
            Some(operation(
                "Mailing list stats",
                &["Lists"],
                vec![path_param("slug", string_schema(), "Mailing list slug")],
                None,
                ok_responses(list_stats_schema),
            )),
            None,
            None,
        ),
    );
    paths.insert(
        "/lists/{slug}/emails".to_string(),
        path_item(
            Some(operation(
                "List emails",
                &["Emails"],
                vec![
                    path_param("slug", string_schema(), "Mailing list slug"),
                    query_param(
                        "page",
                        integer_schema(Some("int64")),
                        "Page index (1-based)",
                    ),
                    query_param("pageSize", integer_schema(Some("int64")), "Page size"),
                    query_param(
                        "sort",
                        array_schema(string_schema()),
                        "Sort fields (e.g. date:desc)",
                    ),
                ],
                None,
                ok_responses(email_list_schema),
            )),
            None,
            None,
        ),
    );
    paths.insert(
        "/lists/{slug}/emails/{email_id}".to_string(),
        path_item(
            Some(operation(
                "Get email",
                &["Emails"],
                vec![
                    path_param("slug", string_schema(), "Mailing list slug"),
                    path_param("email_id", integer_schema(Some("int32")), "Email id"),
                ],
                None,
                ok_responses(email_schema),
            )),
            None,
            None,
        ),
    );
    paths.insert(
        "/lists/{slug}/threads".to_string(),
        path_item(
            Some(operation(
                "List threads",
                &["Threads"],
                vec![
                    path_param("slug", string_schema(), "Mailing list slug"),
                    query_param(
                        "page",
                        integer_schema(Some("int64")),
                        "Page index (1-based)",
                    ),
                    query_param("pageSize", integer_schema(Some("int64")), "Page size"),
                    query_param(
                        "sort",
                        array_schema(string_schema()),
                        "Sort fields (e.g. lastActivity:desc)",
                    ),
                ],
                None,
                ok_responses(thread_list_schema),
            )),
            None,
            None,
        ),
    );
    paths.insert(
        "/lists/{slug}/threads/{thread_id}".to_string(),
        path_item(
            Some(operation(
                "Get thread",
                &["Threads"],
                vec![
                    path_param("slug", string_schema(), "Mailing list slug"),
                    path_param("thread_id", integer_schema(Some("int32")), "Thread id"),
                ],
                None,
                ok_responses(thread_detail_schema),
            )),
            None,
            None,
        ),
    );
    paths.insert(
        "/authors".to_string(),
        path_item(
            Some(operation(
                "List authors",
                &["Authors"],
                vec![
                    query_param(
                        "page",
                        integer_schema(Some("int64")),
                        "Page index (1-based)",
                    ),
                    query_param("pageSize", integer_schema(Some("int64")), "Page size"),
                    query_param(
                        "sort",
                        array_schema(string_schema()),
                        "Sort fields (e.g. lastSeen:desc)",
                    ),
                    query_param("q", string_schema(), "Search term for name or email"),
                    query_param("listSlug", string_schema(), "Filter by mailing list slug"),
                ],
                None,
                ok_responses(author_list_schema),
            )),
            None,
            None,
        ),
    );
    paths.insert(
        "/authors/{author_id}".to_string(),
        path_item(
            Some(operation(
                "Get author",
                &["Authors"],
                vec![path_param(
                    "author_id",
                    integer_schema(Some("int32")),
                    "Author id",
                )],
                None,
                ok_responses(author_schema),
            )),
            None,
            None,
        ),
    );
    paths.insert(
        "/authors/{author_id}/lists/{slug}/emails".to_string(),
        path_item(
            Some(operation(
                "List author emails",
                &["Authors"],
                vec![
                    path_param("author_id", integer_schema(Some("int32")), "Author id"),
                    path_param("slug", string_schema(), "Mailing list slug"),
                    query_param(
                        "page",
                        integer_schema(Some("int64")),
                        "Page index (1-based)",
                    ),
                    query_param("pageSize", integer_schema(Some("int64")), "Page size"),
                ],
                None,
                ok_responses(author_emails_schema),
            )),
            None,
            None,
        ),
    );
    paths.insert(
        "/authors/{author_id}/lists/{slug}/threads-started".to_string(),
        path_item(
            Some(operation(
                "List threads started by author",
                &["Authors"],
                vec![
                    path_param("author_id", integer_schema(Some("int32")), "Author id"),
                    path_param("slug", string_schema(), "Mailing list slug"),
                    query_param(
                        "page",
                        integer_schema(Some("int64")),
                        "Page index (1-based)",
                    ),
                    query_param("pageSize", integer_schema(Some("int64")), "Page size"),
                    query_param(
                        "sort",
                        array_schema(string_schema()),
                        "Sort fields (e.g. lastActivity:desc)",
                    ),
                ],
                None,
                ok_responses(author_threads_schema.clone()),
            )),
            None,
            None,
        ),
    );
    paths.insert(
        "/authors/{author_id}/lists/{slug}/threads-participated".to_string(),
        path_item(
            Some(operation(
                "List threads participated by author",
                &["Authors"],
                vec![
                    path_param("author_id", integer_schema(Some("int32")), "Author id"),
                    path_param("slug", string_schema(), "Mailing list slug"),
                    query_param(
                        "page",
                        integer_schema(Some("int64")),
                        "Page index (1-based)",
                    ),
                    query_param("pageSize", integer_schema(Some("int64")), "Page size"),
                    query_param(
                        "sort",
                        array_schema(string_schema()),
                        "Sort fields (e.g. lastActivity:desc)",
                    ),
                ],
                None,
                ok_responses(author_threads_schema),
            )),
            None,
            None,
        ),
    );

    api.paths = paths;
    api.tags = vec![
        Tag {
            name: "Health".to_string(),
            description: Some("Health and smoke test endpoints".to_string()),
            ..Tag::default()
        },
        Tag {
            name: "Jobs".to_string(),
            description: Some("Background job queue management".to_string()),
            ..Tag::default()
        },
        Tag {
            name: "MailingLists".to_string(),
            description: Some("Mailing list management endpoints".to_string()),
            ..Tag::default()
        },
        Tag {
            name: "Webhooks".to_string(),
            description: Some("Webhook triggers for sync pipelines".to_string()),
            ..Tag::default()
        },
        Tag {
            name: "Lists".to_string(),
            description: Some("Explorer list statistics endpoints".to_string()),
            ..Tag::default()
        },
        Tag {
            name: "Emails".to_string(),
            description: Some("Explorer email endpoints".to_string()),
            ..Tag::default()
        },
        Tag {
            name: "Threads".to_string(),
            description: Some("Explorer thread endpoints".to_string()),
            ..Tag::default()
        },
        Tag {
            name: "Authors".to_string(),
            description: Some("Explorer author endpoints".to_string()),
            ..Tag::default()
        },
        Tag {
            name: "Docs".to_string(),
            description: Some("OpenAPI documentation endpoints".to_string()),
            ..Tag::default()
        },
    ];

    api
}
