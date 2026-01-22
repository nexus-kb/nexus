//! Thread endpoints scoped to mailing lists.

use std::collections::HashMap;

use axum::Json;
use axum::extract::{Path, Query, State};

use crate::handlers::helpers::resolve_mailing_list_id;
use crate::http::{ApiError, internal_error};
use crate::models::{EmailHierarchy, Thread, ThreadDetail, ThreadWithStarter};
use crate::params::ThreadListParams;
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

/// List threads for a mailing list.
pub async fn list_threads(
    State(state): State<ApiState>,
    Path(slug): Path<String>,
    Query(params): Query<ThreadListParams>,
) -> Result<Json<ApiResponse<Vec<ThreadWithStarter>>>, ApiError> {
    let mailing_list_id = resolve_mailing_list_id(state.db.pool(), &slug).await?;
    let page = params.page();
    let page_size = params.page_size();
    let offset = (page - 1) * page_size;
    let sort_values = params.sort();
    let (order_clauses, sort_meta) = parse_thread_sorts(&sort_values);
    let order_sql = order_clauses.join(", ");

    let total: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM threads WHERE mailing_list_id = $1")
        .bind(mailing_list_id)
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
        WHERE t.mailing_list_id = $1
        ORDER BY {order_sql}
        LIMIT $2 OFFSET $3
        "#
    );

    let threads = sqlx::query_as::<_, ThreadWithStarter>(&query)
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

    Ok(Json(ApiResponse::with_meta(threads, meta)))
}

/// Fetch a single thread and its emails.
pub async fn get_thread(
    State(state): State<ApiState>,
    Path((slug, thread_id)): Path<(String, i32)>,
) -> Result<Json<ApiResponse<ThreadDetail>>, ApiError> {
    let mailing_list_id = resolve_mailing_list_id(state.db.pool(), &slug).await?;

    let thread = sqlx::query_as::<_, Thread>(
        r#"
        SELECT id, mailing_list_id, root_message_id, subject, start_date, last_date,
               CAST(message_count AS INTEGER) AS message_count
        FROM threads
        WHERE mailing_list_id = $1 AND id = $2
        "#,
    )
    .bind(mailing_list_id)
    .bind(thread_id)
    .fetch_optional(state.db.pool())
    .await
    .map_err(internal_error)?;

    let thread = match thread {
        Some(thread) => thread,
        None => {
            return Err(ApiError::not_found(&format!("thread {}", thread_id)));
        }
    };

    let mut emails = sqlx::query_as::<_, EmailHierarchy>(
        r#"
        SELECT
            e.id, e.mailing_list_id, e.message_id, e.blob_oid, e.author_id,
            e.subject, e.date, e.in_reply_to, b.body, e.created_at,
            a.name AS author_name, a.email AS author_email, e.patch_metadata,
            CAST(COALESCE(tm.depth, 0) AS INTEGER) AS depth
        FROM emails e
        JOIN authors a ON e.author_id = a.id
        JOIN thread_memberships tm ON e.id = tm.email_id AND tm.mailing_list_id = $1
        LEFT JOIN email_bodies b ON e.id = b.email_id AND e.mailing_list_id = b.mailing_list_id
        WHERE tm.thread_id = $2 AND tm.mailing_list_id = $1
        "#,
    )
    .bind(mailing_list_id)
    .bind(thread_id)
    .fetch_all(state.db.pool())
    .await
    .map_err(internal_error)?;

    emails = sort_emails_by_thread_order(emails);

    let detail = ThreadDetail { thread, emails };
    let meta = ResponseMeta::default().with_list_id(slug);
    Ok(Json(ApiResponse::with_meta(detail, meta)))
}

fn sort_emails_by_thread_order(emails: Vec<EmailHierarchy>) -> Vec<EmailHierarchy> {
    let email_map: HashMap<String, &EmailHierarchy> =
        emails.iter().map(|e| (e.message_id.clone(), e)).collect();

    let mut children_map: HashMap<Option<String>, Vec<&EmailHierarchy>> = HashMap::new();
    for email in &emails {
        children_map
            .entry(email.in_reply_to.clone())
            .or_default()
            .push(email);
    }

    for children in children_map.values_mut() {
        children.sort_by(|a, b| a.date.cmp(&b.date));
    }

    let mut result = Vec::new();

    fn add_email_and_children(
        email: &EmailHierarchy,
        children_map: &HashMap<Option<String>, Vec<&EmailHierarchy>>,
        result: &mut Vec<EmailHierarchy>,
    ) {
        result.push(email.clone());

        if let Some(children) = children_map.get(&Some(email.message_id.clone())) {
            for child in children {
                add_email_and_children(child, children_map, result);
            }
        }
    }

    if let Some(roots) = children_map.get(&None) {
        for root in roots {
            add_email_and_children(root, &children_map, &mut result);
        }
    }

    for email in &emails {
        if let Some(ref parent_msg_id) = email.in_reply_to {
            if !email_map.contains_key(parent_msg_id) && !result.iter().any(|e| e.id == email.id) {
                add_email_and_children(email, &children_map, &mut result);
            }
        }
    }

    result
}
