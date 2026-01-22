//! Email endpoints scoped to mailing lists.

use crate::handlers::helpers::resolve_mailing_list_id;
use crate::http::{ApiError, internal_error};
use crate::models::EmailWithAuthor;
use crate::params::EmailListParams;
use crate::response::{ApiResponse, PaginationMeta, ResponseMeta, SortDescriptor, SortDirection};
use crate::state::ApiState;
use axum::Json;
use axum::extract::{Path, Query, State};

fn parse_email_sorts(values: &[String]) -> (Vec<String>, Vec<SortDescriptor>) {
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
            "date" => ("e.date", "date"),
            "subject" => ("e.subject", "subject"),
            "createdAt" => ("e.created_at", "createdAt"),
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
        clauses.push("e.date DESC".to_string());
        descriptors.push(SortDescriptor {
            field: "date".to_string(),
            direction: SortDirection::Desc,
        });
    }

    (clauses, descriptors)
}

/// List emails for a mailing list.
pub async fn list_emails(
    State(state): State<ApiState>,
    Path(slug): Path<String>,
    Query(params): Query<EmailListParams>,
) -> Result<Json<ApiResponse<Vec<EmailWithAuthor>>>, ApiError> {
    let mailing_list_id = resolve_mailing_list_id(state.db.pool(), &slug).await?;
    let page = params.page();
    let page_size = params.page_size();
    let offset = (page - 1) * page_size;
    let sort_values = params.normalized_sort();
    let (order_clauses, sort_meta) = parse_email_sorts(&sort_values);
    let order_sql = order_clauses.join(", ");

    let total: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM emails WHERE mailing_list_id = $1")
        .bind(mailing_list_id)
        .fetch_one(state.db.pool())
        .await
        .map_err(internal_error)?;

    let query = format!(
        r#"
        SELECT
            e.id, e.mailing_list_id, e.message_id, e.blob_oid, e.author_id,
            e.subject, e.date, e.in_reply_to, b.body, e.created_at,
            a.name AS author_name, a.email AS author_email, e.patch_metadata
        FROM emails e
        JOIN authors a ON e.author_id = a.id
        LEFT JOIN email_bodies b ON e.id = b.email_id AND e.mailing_list_id = b.mailing_list_id
        WHERE e.mailing_list_id = $1
        ORDER BY {order_sql}
        LIMIT $2 OFFSET $3
        "#
    );

    let emails = sqlx::query_as::<_, EmailWithAuthor>(&query)
        .bind(mailing_list_id)
        .bind(page_size)
        .bind(offset)
        .fetch_all(state.db.pool())
        .await
        .map_err(internal_error)?;

    let meta = ResponseMeta::default()
        .with_list_id(slug)
        .with_sort(sort_meta)
        .with_pagination(PaginationMeta::new(page, page_size, total.0));

    Ok(Json(ApiResponse::with_meta(emails, meta)))
}

/// Fetch a single email by id.
pub async fn get_email(
    State(state): State<ApiState>,
    Path((slug, email_id)): Path<(String, i32)>,
) -> Result<Json<ApiResponse<EmailWithAuthor>>, ApiError> {
    let mailing_list_id = resolve_mailing_list_id(state.db.pool(), &slug).await?;

    let email = sqlx::query_as::<_, EmailWithAuthor>(
        r#"
        SELECT
            e.id, e.mailing_list_id, e.message_id, e.blob_oid, e.author_id,
            e.subject, e.date, e.in_reply_to, b.body, e.created_at,
            a.name AS author_name, a.email AS author_email, e.patch_metadata
        FROM emails e
        JOIN authors a ON e.author_id = a.id
        LEFT JOIN email_bodies b ON e.id = b.email_id AND e.mailing_list_id = b.mailing_list_id
        WHERE e.mailing_list_id = $1 AND e.id = $2
        "#,
    )
    .bind(mailing_list_id)
    .bind(email_id)
    .fetch_optional(state.db.pool())
    .await
    .map_err(internal_error)?;

    match email {
        Some(email) => {
            let meta = ResponseMeta::default().with_list_id(slug);
            Ok(Json(ApiResponse::with_meta(email, meta)))
        }
        None => Err(ApiError::not_found(&format!("email {}", email_id))),
    }
}
