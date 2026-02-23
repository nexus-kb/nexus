use super::*;

pub async fn patch_item(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(patch_item_id): Path<i64>,
) -> Result<Response, StatusCode> {
    let Some(item) = state
        .lineage
        .get_patch_item_detail(patch_item_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };

    json_response_with_cache(&headers, &map_patch_item(item), CACHE_LONG, None)
}

pub async fn patch_item_files(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(patch_item_id): Path<i64>,
) -> Result<Response, StatusCode> {
    let exists = state
        .lineage
        .get_patch_item_detail(patch_item_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .is_some();
    if !exists {
        return Err(StatusCode::NOT_FOUND);
    }

    let files = state
        .lineage
        .list_patch_item_files(patch_item_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .into_iter()
        .map(|file| PatchItemFileResponse {
            patch_item_id: file.patch_item_id,
            path: file.new_path,
            old_path: file.old_path,
            change_type: file.change_type,
            is_binary: file.is_binary,
            additions: file.additions,
            deletions: file.deletions,
            hunks: file.hunk_count,
            diff_start: file.diff_start,
            diff_end: file.diff_end,
        })
        .collect::<Vec<_>>();

    json_response_with_cache(&headers, &files, CACHE_LONG, None)
}

pub async fn patch_item_file_diff(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(path): Path<PatchItemFileDiffPath>,
) -> Result<Response, StatusCode> {
    let Some(source) = state
        .lineage
        .get_patch_item_file_diff_source(path.patch_item_id, &path.path)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };

    let diff_text = source.diff_text.ok_or(StatusCode::NOT_FOUND)?;
    let sliced = slice_by_offsets(&diff_text, source.diff_start, source.diff_end)
        .ok_or(StatusCode::UNPROCESSABLE_ENTITY)?;

    json_response_with_cache(
        &headers,
        &PatchItemFileDiffResponse {
            patch_item_id: path.patch_item_id,
            path: source.new_path,
            diff_text: sliced,
        },
        CACHE_LONG,
        None,
    )
}

pub async fn patch_item_diff(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(patch_item_id): Path<i64>,
) -> Result<Response, StatusCode> {
    let Some(diff_text) = state
        .lineage
        .get_patch_item_full_diff(patch_item_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };

    json_response_with_cache(
        &headers,
        &PatchItemDiffResponse {
            patch_item_id,
            diff_text,
        },
        CACHE_LONG,
        None,
    )
}
