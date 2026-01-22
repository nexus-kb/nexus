//! Author endpoints providing global and list-scoped views.

use axum::Json;
use axum::extract::{Path, Query, State};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sqlx::QueryBuilder;

use crate::handlers::helpers::resolve_mailing_list_id;
use crate::http::{ApiError, internal_error};
use crate::models::{AuthorWithStats, EmailWithAuthor, ThreadWithStarter};
use crate::params::{AuthorListParams, PaginationParams, ThreadListParams};
use crate::response::{ApiResponse, PaginationMeta, ResponseMeta, SortDescriptor, SortDirection};
use crate::state::ApiState;

fn parse_thread_sorts(values: &[String]) -> (Vec<String>, Vec<SortDescriptor>) {
    let mut clauses = Vec::new();
    let mut descriptors = Vec::new();

    for value in values {
        let mut parts = value.splitn(2, ':');
        let field = parts.next().unwrap_or_default().trim();
        if field.is_empty() {
            continue;
        }
        let direction = parts.next().unwrap_or("desc").trim();
        let (column, api_field) = match field {
            "startDate" => ("t.start_date", "startDate"),
            "lastActivity" => ("t.last_date", "lastActivity"),
            "messageCount" => ("t.message_count", "messageCount"),
            _ => continue,
        };

        let dir = if direction.eq_ignore_ascii_case("asc") {
            SortDirection::Asc
        } else {
            SortDirection::Desc
        };

        let sql_dir = match dir {
            SortDirection::Asc => "ASC",
            SortDirection::Desc => "DESC",
        };

        clauses.push(format!("{column} {sql_dir}"));
        descriptors.push(SortDescriptor {
            field: api_field.to_string(),
            direction: dir,
        });
    }

    if clauses.is_empty() {
        clauses.push("t.last_date DESC".to_string());
        descriptors.push(SortDescriptor {
            field: "lastActivity".to_string(),
            direction: SortDirection::Desc,
        });
    }

    (clauses, descriptors)
}

fn parse_author_sorts(values: &[String]) -> (Vec<String>, Vec<SortDescriptor>) {
    let mut clauses = Vec::new();
    let mut descriptors = Vec::new();

    for value in values {
        let mut parts = value.splitn(2, ':');
        let field = parts.next().unwrap_or_default().trim();
        if field.is_empty() {
            continue;
        }
        let direction = parts.next().unwrap_or("desc").trim();
        let (column, api_field) = match field {
            "lastSeen" => ("a.last_seen", "lastSeen"),
            "firstSeen" => ("a.first_seen", "firstSeen"),
            "email" => ("LOWER(a.email)", "email"),
            "canonicalName" => ("LOWER(a.name)", "canonicalName"),
            "activity" => ("email_count", "activity"),
            "threadCount" => ("thread_count", "threadCount"),
            _ => continue,
        };

        let dir = if direction.eq_ignore_ascii_case("asc") {
            SortDirection::Asc
        } else {
            SortDirection::Desc
        };

        let sql_dir = match dir {
            SortDirection::Asc => "ASC",
            SortDirection::Desc => "DESC",
        };

        clauses.push(format!("{column} {sql_dir}"));
        descriptors.push(SortDescriptor {
            field: api_field.to_string(),
            direction: dir,
        });
    }

    if clauses.is_empty() {
        clauses.push("a.last_seen DESC".to_string());
        descriptors.push(SortDescriptor {
            field: "lastSeen".to_string(),
            direction: SortDirection::Desc,
        });
    }

    (clauses, descriptors)
}

fn apply_author_filters<'a>(
    builder: &mut QueryBuilder<'a, sqlx::Postgres>,
    list_slug: Option<&'a str>,
    normalized_query: Option<&'a str>,
) {
    let mut has_where = false;

    if let Some(slug) = list_slug {
        builder.push(if has_where { " AND " } else { " WHERE " });
        builder.push("ml.slug = ");
        builder.push_bind(slug);
        has_where = true;
    }

    if let Some(query) = normalized_query {
        let pattern = format!("%{}%", query);
        builder.push(if has_where { " AND " } else { " WHERE " });
        builder.push("(");
        builder.push("LOWER(a.email) LIKE ");
        builder.push_bind(pattern.clone());
        builder.push(" OR LOWER(a.name) LIKE ");
        builder.push_bind(pattern);
        builder.push(")");
    }
}

/// List authors with optional filtering.
pub async fn list_authors(
    State(state): State<ApiState>,
    Query(params): Query<AuthorListParams>,
) -> Result<Json<ApiResponse<Vec<AuthorWithStats>>>, ApiError> {
    let page = params.page();
    let page_size = params.page_size();
    let offset = (page - 1) * page_size;
    let list_slug = params.list_slug();
    let normalized_query = params.normalized_query();
    let (order_clauses, sort_meta) = parse_author_sorts(&params.sort());
    let order_sql = order_clauses.join(", ");

    let mut count_builder = QueryBuilder::new("SELECT COUNT(DISTINCT a.id) FROM authors a");
    count_builder.push(" LEFT JOIN author_mailing_list_activity act ON act.author_id = a.id");
    count_builder.push(" LEFT JOIN mailing_lists ml ON ml.id = act.mailing_list_id");
    apply_author_filters(
        &mut count_builder,
        list_slug.as_deref(),
        normalized_query.as_deref(),
    );

    let total = count_builder
        .build_query_scalar::<i64>()
        .fetch_one(state.db.pool())
        .await
        .map_err(internal_error)?;

    let mut data_builder = QueryBuilder::new(
        "SELECT \
            a.id, a.email, a.name AS canonical_name, a.first_seen, a.last_seen, \
            COALESCE(SUM(act.created_count), 0) AS email_count, \
            COALESCE(SUM(act.participated_count), 0) AS thread_count, \
            MIN(act.first_email_date) AS first_email_date, \
            MAX(act.last_email_date) AS last_email_date, \
            COALESCE(ARRAY_REMOVE(ARRAY_AGG(DISTINCT ml.slug), NULL), ARRAY[]::text[]) AS mailing_lists, \
            ARRAY[]::text[] AS name_variations \
        FROM authors a \
        LEFT JOIN author_mailing_list_activity act ON act.author_id = a.id \
        LEFT JOIN mailing_lists ml ON ml.id = act.mailing_list_id",
    );

    apply_author_filters(
        &mut data_builder,
        list_slug.as_deref(),
        normalized_query.as_deref(),
    );
    data_builder.push(" GROUP BY a.id");
    data_builder.push(" ORDER BY ");
    data_builder.push(order_sql);
    data_builder.push(" LIMIT ");
    data_builder.push_bind(page_size);
    data_builder.push(" OFFSET ");
    data_builder.push_bind(offset);

    let authors: Vec<AuthorWithStats> = data_builder
        .build_query_as()
        .fetch_all(state.db.pool())
        .await
        .map_err(internal_error)?;

    let mut meta = ResponseMeta::default()
        .with_pagination(PaginationMeta::new(page, page_size, total))
        .with_sort(sort_meta);

    let mut filters = JsonMap::new();
    if let Some(slug) = list_slug {
        filters.insert("listSlug".to_string(), JsonValue::String(slug));
    }
    if let Some(raw_query) = params.raw_query() {
        filters.insert("q".to_string(), JsonValue::String(raw_query));
    }
    if !filters.is_empty() {
        meta = meta.with_filters(filters);
    }

    Ok(Json(ApiResponse::with_meta(authors, meta)))
}

/// Fetch a single author by id.
pub async fn get_author(
    State(state): State<ApiState>,
    Path(author_id): Path<i32>,
) -> Result<Json<ApiResponse<AuthorWithStats>>, ApiError> {
    let mut builder = QueryBuilder::new(
        "SELECT \
            a.id, a.email, a.name AS canonical_name, a.first_seen, a.last_seen, \
            COALESCE(SUM(act.created_count), 0) AS email_count, \
            COALESCE(SUM(act.participated_count), 0) AS thread_count, \
            MIN(act.first_email_date) AS first_email_date, \
            MAX(act.last_email_date) AS last_email_date, \
            COALESCE(ARRAY_REMOVE(ARRAY_AGG(DISTINCT ml.slug), NULL), ARRAY[]::text[]) AS mailing_lists, \
            ARRAY[]::text[] AS name_variations \
        FROM authors a \
        LEFT JOIN author_mailing_list_activity act ON act.author_id = a.id \
        LEFT JOIN mailing_lists ml ON ml.id = act.mailing_list_id",
    );
    builder.push(" WHERE a.id = ");
    builder.push_bind(author_id);
    builder.push(" GROUP BY a.id");

    let row: Option<AuthorWithStats> = builder
        .build_query_as()
        .fetch_optional(state.db.pool())
        .await
        .map_err(internal_error)?;

    match row {
        Some(row) => Ok(Json(ApiResponse::new(row))),
        None => Err(ApiError::not_found(&format!("author {}", author_id))),
    }
}

/// List emails authored by a specific author in a mailing list.
pub async fn get_author_emails(
    State(state): State<ApiState>,
    Path((author_id, slug)): Path<(i32, String)>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<ApiResponse<Vec<EmailWithAuthor>>>, ApiError> {
    let page = params.page();
    let page_size = params.page_size();
    let offset = (page - 1) * page_size;
    let mailing_list_id = resolve_mailing_list_id(state.db.pool(), &slug).await?;

    let total: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM emails WHERE mailing_list_id = $1 AND author_id = $2")
            .bind(mailing_list_id)
            .bind(author_id)
            .fetch_one(state.db.pool())
            .await
            .map_err(internal_error)?;

    let emails = sqlx::query_as::<_, EmailWithAuthor>(
        r#"
        SELECT
            e.id, e.mailing_list_id, e.message_id, e.blob_oid, e.author_id,
            e.subject, e.date, e.in_reply_to, b.body, e.created_at,
            a.name AS author_name, a.email AS author_email
        FROM emails e
        JOIN authors a ON e.author_id = a.id
        LEFT JOIN email_bodies b ON e.id = b.email_id AND e.mailing_list_id = b.mailing_list_id
        WHERE e.mailing_list_id = $1 AND e.author_id = $2
        ORDER BY e.date DESC
        LIMIT $3 OFFSET $4
        "#,
    )
    .bind(mailing_list_id)
    .bind(author_id)
    .bind(page_size)
    .bind(offset)
    .fetch_all(state.db.pool())
    .await
    .map_err(internal_error)?;

    let meta = ResponseMeta::default()
        .with_list_id(slug)
        .with_pagination(PaginationMeta::new(page, page_size, total.0));

    Ok(Json(ApiResponse::with_meta(emails, meta)))
}

/// List threads started by an author in a mailing list.
pub async fn get_author_threads_started(
    State(state): State<ApiState>,
    Path((author_id, slug)): Path<(i32, String)>,
    Query(params): Query<ThreadListParams>,
) -> Result<Json<ApiResponse<Vec<ThreadWithStarter>>>, ApiError> {
    let page = params.page();
    let page_size = params.page_size();
    let offset = (page - 1) * page_size;
    let mailing_list_id = resolve_mailing_list_id(state.db.pool(), &slug).await?;
    let sort_values = params.sort();
    let (order_clauses, sort_meta) = parse_thread_sorts(&sort_values);
    let order_sql = order_clauses.join(", ");

    let total: (i64,) = sqlx::query_as(
        r#"
        SELECT COUNT(*)
        FROM threads t
        JOIN emails e ON t.root_message_id = e.message_id AND t.mailing_list_id = e.mailing_list_id
        WHERE t.mailing_list_id = $1 AND e.author_id = $2
        "#,
    )
    .bind(mailing_list_id)
    .bind(author_id)
    .fetch_one(state.db.pool())
    .await
    .map_err(internal_error)?;

    let query = format!(
        r#"
        SELECT t.id, t.mailing_list_id, t.root_message_id, t.subject, t.start_date, t.last_date,
               CAST(t.message_count AS INTEGER) AS message_count,
               e.author_id AS starter_id,
               a.name AS starter_name,
               a.email AS starter_email
        FROM threads t
        JOIN emails e ON t.root_message_id = e.message_id AND t.mailing_list_id = e.mailing_list_id
        JOIN authors a ON e.author_id = a.id
        WHERE t.mailing_list_id = $1 AND e.author_id = $2
        ORDER BY {order_sql}
        LIMIT $3 OFFSET $4
        "#
    );

    let threads = sqlx::query_as::<_, ThreadWithStarter>(&query)
        .bind(mailing_list_id)
        .bind(author_id)
        .bind(page_size)
        .bind(offset)
        .fetch_all(state.db.pool())
        .await
        .map_err(internal_error)?;

    let meta = ResponseMeta::default()
        .with_list_id(slug)
        .with_sort(sort_meta)
        .with_pagination(PaginationMeta::new(page, page_size, total.0));

    Ok(Json(ApiResponse::with_meta(threads, meta)))
}

/// List threads an author participated in within a mailing list.
pub async fn get_author_threads_participated(
    State(state): State<ApiState>,
    Path((author_id, slug)): Path<(i32, String)>,
    Query(params): Query<ThreadListParams>,
) -> Result<Json<ApiResponse<Vec<ThreadWithStarter>>>, ApiError> {
    let page = params.page();
    let page_size = params.page_size();
    let offset = (page - 1) * page_size;
    let mailing_list_id = resolve_mailing_list_id(state.db.pool(), &slug).await?;
    let sort_values = params.sort();
    let (order_clauses, sort_meta) = parse_thread_sorts(&sort_values);
    let order_sql = order_clauses.join(", ");

    let total: (i64,) = sqlx::query_as(
        r#"
        SELECT COUNT(DISTINCT t.id)
        FROM thread_memberships tm
        JOIN emails e ON e.id = tm.email_id AND e.mailing_list_id = tm.mailing_list_id
        JOIN threads t ON t.id = tm.thread_id AND t.mailing_list_id = tm.mailing_list_id
        WHERE tm.mailing_list_id = $1 AND e.author_id = $2
        "#,
    )
    .bind(mailing_list_id)
    .bind(author_id)
    .fetch_one(state.db.pool())
    .await
    .map_err(internal_error)?;

    let query = format!(
        r#"
        SELECT DISTINCT t.id, t.mailing_list_id, t.root_message_id, t.subject, t.start_date, t.last_date,
               CAST(t.message_count AS INTEGER) AS message_count,
               starter.author_id AS starter_id,
               sa.name AS starter_name,
               sa.email AS starter_email
        FROM thread_memberships tm
        JOIN emails e ON e.id = tm.email_id AND e.mailing_list_id = tm.mailing_list_id
        JOIN threads t ON t.id = tm.thread_id AND t.mailing_list_id = tm.mailing_list_id
        JOIN emails starter ON starter.message_id = t.root_message_id AND starter.mailing_list_id = t.mailing_list_id
        JOIN authors sa ON sa.id = starter.author_id
        WHERE tm.mailing_list_id = $1 AND e.author_id = $2
        ORDER BY {order_sql}
        LIMIT $3 OFFSET $4
        "#
    );

    let threads = sqlx::query_as::<_, ThreadWithStarter>(&query)
        .bind(mailing_list_id)
        .bind(author_id)
        .bind(page_size)
        .bind(offset)
        .fetch_all(state.db.pool())
        .await
        .map_err(internal_error)?;

    let meta = ResponseMeta::default()
        .with_list_id(slug)
        .with_sort(sort_meta)
        .with_pagination(PaginationMeta::new(page, page_size, total.0));

    Ok(Json(ApiResponse::with_meta(threads, meta)))
}
