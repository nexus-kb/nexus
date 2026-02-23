use super::*;

pub async fn message_body(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(message_id): Path<i64>,
    Query(query): Query<MessageBodyQuery>,
) -> Result<Response, StatusCode> {
    let include_diff = query.include_diff.unwrap_or(false);
    let strip_quotes = query.strip_quotes.unwrap_or(false);

    let Some(record) = state
        .lineage
        .get_message_body(message_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };

    let body_text = record
        .body_text
        .as_deref()
        .map(|body| strip_quoted_lines(body, strip_quotes));

    json_response_with_cache(
        &headers,
        &MessageBodyResponse {
            message_id: record.message_pk,
            subject: record.subject_raw,
            body_text,
            diff_text: if include_diff { record.diff_text } else { None },
            has_diff: record.has_diff,
            has_attachments: record.has_attachments,
        },
        CACHE_LONG,
        None,
    )
}

pub async fn message_detail(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(message_id): Path<i64>,
) -> Result<Response, StatusCode> {
    let Some(record) = state
        .lineage
        .get_message_detail(message_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };

    json_response_with_cache(
        &headers,
        &MessageResponse {
            message_id: record.message_pk,
            message_id_primary: record.message_id_primary,
            subject: record.subject_raw,
            subject_norm: record.subject_norm,
            from: ThreadMessageParticipant {
                name: record.from_name,
                email: record.from_email,
            },
            date_utc: record.date_utc,
            to_raw: record.to_raw,
            cc_raw: record.cc_raw,
            references: record.references_ids,
            in_reply_to: record.in_reply_to_ids,
            has_diff: record.has_diff,
            has_attachments: record.has_attachments,
        },
        CACHE_LONG,
        None,
    )
}

pub async fn message_id_redirect(
    State(state): State<ApiState>,
    Path(path): Path<MessageIdPath>,
) -> Result<Response, StatusCode> {
    let mut candidates = vec![path.msgid.clone()];
    if !path.msgid.starts_with('<') && !path.msgid.ends_with('>') {
        candidates.push(format!("<{}>", path.msgid));
    }

    for candidate in candidates {
        if let Some(message_pk) = state
            .lineage
            .resolve_message_pk_by_message_id(&candidate)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        {
            return redirect_response(&format!("/api/v1/messages/{message_pk}"));
        }
    }

    Err(StatusCode::NOT_FOUND)
}
