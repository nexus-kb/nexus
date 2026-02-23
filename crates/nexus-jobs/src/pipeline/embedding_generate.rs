use super::*;

impl Phase0JobHandler {
    pub(super) async fn handle_embedding_generate_batch(
        &self,
        job: Job,
        ctx: ExecutionContext,
    ) -> JobExecutionOutcome {
        let started = Instant::now();
        let payload: EmbeddingGenerateBatchPayload =
            match serde_json::from_value(job.payload_json.clone()) {
                Ok(value) => value,
                Err(err) => {
                    return JobExecutionOutcome::Terminal {
                        reason: format!("invalid embedding_generate_batch payload: {err}"),
                        kind: "payload".to_string(),
                        metrics: empty_metrics(started.elapsed().as_millis()),
                    };
                }
            };

        if payload.model_key != self.settings.embeddings.model {
            return JobExecutionOutcome::Terminal {
                reason: format!(
                    "unsupported model_key {} (configured model is {})",
                    payload.model_key, self.settings.embeddings.model
                ),
                kind: "payload".to_string(),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }

        let mut ids = payload.ids.clone();
        ids.sort_unstable();
        ids.dedup();
        ids.retain(|value| *value > 0);
        if ids.is_empty() {
            return JobExecutionOutcome::Success {
                result_json: serde_json::json!({
                    "scope": payload.scope.as_str(),
                    "ids_count": 0,
                    "source_job_id": payload.source_job_id,
                    "skipped": "empty_id_set",
                }),
                metrics: empty_metrics(started.elapsed().as_millis()),
            };
        }

        let inputs = match self
            .embeddings
            .build_embedding_inputs(payload.scope.as_str(), &ids)
            .await
        {
            Ok(value) => value,
            Err(err) => {
                let stage_name = embedding_doc_build_stage(payload.scope.as_str());
                let failing_doc_id = self
                    .find_first_failing_embedding_doc_id(payload.scope.as_str(), &ids)
                    .await;
                let failing_source = if matches!(payload.scope, EmbeddingScope::Thread) {
                    if let Some(thread_id) = failing_doc_id {
                        match self.embeddings.lookup_thread_message_body(thread_id).await {
                            Ok(value) => value.map(|(message_pk, body_id)| {
                                format!("message_pk={message_pk} body_id={body_id}")
                            }),
                            Err(_) => None,
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };
                return retryable_error(
                    format!(
                        "failed to build embedding inputs for scope {}: stage={} ids_count={} failing_doc_id={:?} failing_source={:?} error={err}",
                        payload.scope.as_str(),
                        stage_name,
                        ids.len(),
                        failing_doc_id,
                        failing_source
                    ),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };
        if inputs.is_empty() {
            return JobExecutionOutcome::Success {
                result_json: serde_json::json!({
                    "scope": payload.scope.as_str(),
                    "ids_count": ids.len(),
                    "source_job_id": payload.source_job_id,
                    "skipped": "no_inputs_for_ids",
                }),
                metrics: JobStoreMetrics {
                    duration_ms: started.elapsed().as_millis(),
                    rows_written: 0,
                    bytes_read: 0,
                    commit_count: ids.len() as u64,
                    parse_errors: 0,
                },
            };
        }

        let batch_size = self.settings.embeddings.batch_size.max(1);
        let mut upserts: Vec<EmbeddingVectorUpsert> = Vec::with_capacity(inputs.len());
        let mut bytes_read = 0u64;
        let mut request_count = 0u64;
        for (idx, chunk) in inputs.chunks(batch_size).enumerate() {
            if let Err(err) = ctx.heartbeat().await {
                warn!(job_id = job.id, error = %err, "heartbeat update failed");
            }
            let text_batch = chunk
                .iter()
                .map(|row| row.text.clone())
                .collect::<Vec<String>>();
            let vectors = match self.embed_text_batch_with_retry(&text_batch).await {
                Ok(value) => value,
                Err(err) => {
                    if err.is_transient() {
                        return retryable_error(
                            format!(
                                "embedding request failed for scope {} batch {}: {err}",
                                payload.scope.as_str(),
                                idx
                            ),
                            "embeddings",
                            &job,
                            started.elapsed().as_millis(),
                            &self.settings,
                        );
                    }
                    return JobExecutionOutcome::Terminal {
                        reason: format!(
                            "embedding request failed for scope {} batch {}: {err}",
                            payload.scope.as_str(),
                            idx
                        ),
                        kind: "embeddings".to_string(),
                        metrics: empty_metrics(started.elapsed().as_millis()),
                    };
                }
            };
            if vectors.len() != chunk.len() {
                return JobExecutionOutcome::Terminal {
                    reason: format!(
                        "embedding vector count mismatch for scope {}: inputs={} vectors={}",
                        payload.scope.as_str(),
                        chunk.len(),
                        vectors.len()
                    ),
                    kind: "embeddings".to_string(),
                    metrics: empty_metrics(started.elapsed().as_millis()),
                };
            }

            request_count += 1;
            for (row, vector) in chunk.iter().zip(vectors.into_iter()) {
                if vector.len() != self.settings.embeddings.dimensions {
                    return JobExecutionOutcome::Terminal {
                        reason: format!(
                            "embedding dimensions mismatch for scope {} doc_id={} expected={} got={}",
                            payload.scope.as_str(),
                            row.doc_id,
                            self.settings.embeddings.dimensions,
                            vector.len()
                        ),
                        kind: "embeddings".to_string(),
                        metrics: empty_metrics(started.elapsed().as_millis()),
                    };
                }
                bytes_read += row.text.len() as u64;
                upserts.push(EmbeddingVectorUpsert {
                    doc_id: row.doc_id,
                    vector,
                    source_hash: row.source_hash.clone(),
                });
            }
        }

        let rows_written = match self
            .embeddings
            .upsert_vectors(
                payload.scope.as_str(),
                &self.settings.embeddings.model,
                self.settings.embeddings.dimensions as i32,
                &upserts,
            )
            .await
        {
            Ok(value) => value,
            Err(err) => {
                return retryable_error(
                    format!(
                        "failed writing embeddings for scope {}: upserts={} error={err}",
                        payload.scope.as_str(),
                        upserts.len()
                    ),
                    "db",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        let index = match payload.scope {
            EmbeddingScope::Thread => MeiliIndexKind::ThreadDocs,
            EmbeddingScope::Series => MeiliIndexKind::PatchSeriesDocs,
        };
        let embedding_chunk_limit = self.settings.embeddings.enqueue_batch_size.max(1);
        let meili_docs_upserted = match self
            .upsert_meili_docs_inline(index, &ids, embedding_chunk_limit, &ctx)
            .await
        {
            Ok(n) => n,
            Err(err) => {
                return retryable_error(
                    format!("failed to upsert meili docs after embeddings: {err}"),
                    "meili",
                    &job,
                    started.elapsed().as_millis(),
                    &self.settings,
                );
            }
        };

        JobExecutionOutcome::Success {
            result_json: serde_json::json!({
                "scope": payload.scope.as_str(),
                "model_key": payload.model_key,
                "ids_count": ids.len(),
                "inputs_count": inputs.len(),
                "request_count": request_count,
                "vectors_written": rows_written,
                "source_job_id": payload.source_job_id,
                "meili_docs_upserted": meili_docs_upserted,
            }),
            metrics: JobStoreMetrics {
                duration_ms: started.elapsed().as_millis(),
                rows_written,
                bytes_read,
                commit_count: ids.len() as u64,
                parse_errors: 0,
            },
        }
    }

    pub(super) async fn ensure_index_settings(
        &self,
        index_spec: &'static nexus_core::search::MeiliIndexSpec,
        settings: &serde_json::Value,
        ctx: &ExecutionContext,
    ) -> Result<(), MeiliClientError> {
        let mut current_settings = self.meili.get_settings(index_spec.uid).await?;
        if current_settings.is_none() {
            if let Some(task_uid) = self.meili.ensure_index_exists(index_spec).await? {
                match self.wait_for_task(task_uid, ctx).await {
                    Ok(()) => {}
                    Err(MeiliClientError::Protocol(message))
                        if message.contains("code=index_already_exists") => {}
                    Err(err) => return Err(err),
                }
            }
            current_settings = self.meili.get_settings(index_spec.uid).await?;
        }

        if settings_differ(current_settings.as_ref(), settings) {
            let task_uid = self.meili.update_settings(index_spec.uid, settings).await?;
            self.wait_for_task(task_uid, ctx).await?;
        }

        Ok(())
    }

    pub(super) async fn ensure_index_exists_only(
        &self,
        index_spec: &'static nexus_core::search::MeiliIndexSpec,
        ctx: &ExecutionContext,
    ) -> Result<(), MeiliClientError> {
        if let Some(task_uid) = self.meili.ensure_index_exists(index_spec).await? {
            match self.wait_for_task(task_uid, ctx).await {
                Ok(()) => {}
                Err(MeiliClientError::Protocol(message))
                    if message.contains("code=index_already_exists") => {}
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    pub(super) async fn upsert_meili_bootstrap_chunk(
        &self,
        index: MeiliIndexKind,
        ids: &[i64],
        ctx: &ExecutionContext,
    ) -> Result<(u64, i64, i64), MeiliClientError> {
        let docs = self
            .build_docs_for_index(index, ids, VectorAttachmentMode::StoredVector)
            .await
            .map_err(|err| MeiliClientError::Protocol(format!("doc build: {err}")))?;
        if docs.is_empty() {
            return Ok((0, 0, 0));
        }

        let embedder_name = self.settings.embeddings.embedder_name.as_str();
        let mut vectors_attached = 0i64;
        let mut placeholders_written = 0i64;
        for doc in &docs {
            let Some(obj) = doc.as_object() else {
                continue;
            };
            let Some(vector_map) = obj.get("_vectors").and_then(serde_json::Value::as_object)
            else {
                continue;
            };
            let Some(entry) = vector_map.get(embedder_name) else {
                continue;
            };
            if entry.is_array() {
                vectors_attached += 1;
            } else if entry.is_null() {
                placeholders_written += 1;
            }
        }

        let task_uid = self.meili.upsert_documents(index.spec().uid, &docs).await?;
        self.wait_for_task(task_uid, ctx).await?;

        Ok((docs.len() as u64, vectors_attached, placeholders_written))
    }

    pub(super) async fn wait_for_task(
        &self,
        task_uid: i64,
        ctx: &ExecutionContext,
    ) -> Result<(), MeiliClientError> {
        loop {
            let _ = ctx.heartbeat().await;
            let task = self.meili.get_task(task_uid).await?;
            match task.status.as_str() {
                "enqueued" | "processing" => {
                    sleep(Duration::from_millis(300)).await;
                }
                "succeeded" => return Ok(()),
                "failed" | "canceled" => {
                    let code = task.error_code.unwrap_or_else(|| "unknown".to_string());
                    let message = task
                        .error_message
                        .unwrap_or_else(|| "unknown meili task failure".to_string());
                    return Err(MeiliClientError::Protocol(format!(
                        "task status={} code={} message={}",
                        task.status, code, message
                    )));
                }
                other => {
                    return Err(MeiliClientError::Protocol(format!(
                        "unexpected task status: {other}"
                    )));
                }
            }
        }
    }

    pub(super) fn meili_index_settings(&self, index: MeiliIndexKind) -> serde_json::Value {
        let mut settings = index.spec().settings_json();
        if matches!(
            index,
            MeiliIndexKind::ThreadDocs | MeiliIndexKind::PatchSeriesDocs
        ) {
            settings["embedders"] = serde_json::json!({
                self.settings.embeddings.embedder_name.clone(): {
                    "source": "userProvided",
                    "dimensions": self.settings.embeddings.dimensions
                }
            });
        }
        settings
    }

    pub(super) async fn build_docs_for_index(
        &self,
        index: MeiliIndexKind,
        ids: &[i64],
        vector_mode: VectorAttachmentMode,
    ) -> nexus_db::Result<Vec<serde_json::Value>> {
        let mut docs = match index {
            MeiliIndexKind::PatchItemDocs => self.search.build_patch_item_docs(ids).await?,
            MeiliIndexKind::PatchSeriesDocs => self.search.build_patch_series_docs(ids).await?,
            MeiliIndexKind::ThreadDocs => self.search.build_thread_docs(ids).await?,
        };

        let scope = match index {
            MeiliIndexKind::ThreadDocs => Some(EmbeddingScope::Thread),
            MeiliIndexKind::PatchSeriesDocs => Some(EmbeddingScope::Series),
            MeiliIndexKind::PatchItemDocs => None,
        };
        let Some(scope) = scope else {
            return Ok(docs);
        };

        let vectors = match vector_mode {
            VectorAttachmentMode::StoredVector => Some(
                self.embeddings
                    .get_vectors(scope.as_str(), &self.settings.embeddings.model, ids)
                    .await?,
            ),
            VectorAttachmentMode::NullPlaceholder => None,
        };
        let embedder_name = self.settings.embeddings.embedder_name.clone();

        for doc in &mut docs {
            let Some(doc_obj) = doc.as_object_mut() else {
                continue;
            };
            let vector_value = match vector_mode {
                VectorAttachmentMode::StoredVector => {
                    let Some(id) = doc_obj.get("id").and_then(|value| value.as_i64()) else {
                        continue;
                    };
                    vectors
                        .as_ref()
                        .and_then(|map| map.get(&id))
                        .map(|vector| serde_json::json!(vector))
                        .unwrap_or(serde_json::Value::Null)
                }
                VectorAttachmentMode::NullPlaceholder => serde_json::Value::Null,
            };
            doc_obj.insert(
                "_vectors".to_string(),
                serde_json::json!({
                    embedder_name.clone(): vector_value
                }),
            );
        }

        Ok(docs)
    }

    pub(super) async fn find_first_failing_embedding_doc_id(
        &self,
        scope: &str,
        ids: &[i64],
    ) -> Option<i64> {
        for doc_id in ids {
            if self
                .embeddings
                .build_embedding_inputs(scope, &[*doc_id])
                .await
                .is_err()
            {
                return Some(*doc_id);
            }
        }
        None
    }

    /// Build docs and upsert them into Meilisearch inline (replaces enqueue_meili_upsert_batch).
    pub(super) async fn upsert_meili_docs_inline(
        &self,
        index: MeiliIndexKind,
        ids: &[i64],
        chunk_limit: usize,
        ctx: &ExecutionContext,
    ) -> Result<u64, MeiliClientError> {
        let mut ids = ids.to_vec();
        ids.sort_unstable();
        ids.dedup();
        ids.retain(|v| *v > 0);
        if ids.is_empty() {
            return Ok(0);
        }

        let index_spec = index.spec();
        let chunk_limit = chunk_limit.max(1);
        let mut docs_upserted = 0u64;

        for chunk in ids.chunks(chunk_limit) {
            let docs = self
                .build_docs_for_index(index, chunk, VectorAttachmentMode::StoredVector)
                .await
                .map_err(|err| MeiliClientError::Protocol(format!("doc build: {err}")))?;
            if docs.is_empty() {
                continue;
            }

            let task_uid = self.meili.upsert_documents(index_spec.uid, &docs).await?;
            self.wait_for_task(task_uid, ctx).await?;
            docs_upserted += docs.len() as u64;
        }

        Ok(docs_upserted)
    }

    pub(super) async fn enqueue_embedding_generate_batch(
        &self,
        list_key: &str,
        scope: EmbeddingScope,
        ids: &[i64],
        source_job_id: Option<i64>,
        priority: i32,
    ) -> Result<bool, sqlx::Error> {
        let mut ids = ids.to_vec();
        ids.sort_unstable();
        ids.dedup();
        ids.retain(|value| *value > 0);
        if ids.is_empty() {
            return Ok(false);
        }

        let source = source_job_id.unwrap_or_default();
        let dedupe_key = format!(
            "{}:{}:{}:{}",
            scope.as_str(),
            self.settings.embeddings.model,
            source,
            hash_i64_list(&ids)
        );
        let payload = EmbeddingGenerateBatchPayload {
            scope,
            list_key: Some(list_key.to_string()),
            ids,
            model_key: self.settings.embeddings.model.clone(),
            source_job_id,
        };

        self.jobs
            .enqueue(EnqueueJobParams {
                job_type: "embedding_generate_batch".to_string(),
                payload_json: serde_json::to_value(payload)
                    .unwrap_or_else(|_| serde_json::json!({})),
                priority,
                dedupe_scope: Some(format!("list:{list_key}:embeddings")),
                dedupe_key: Some(dedupe_key),
                run_after: None,
                max_attempts: Some(8),
            })
            .await?;
        Ok(true)
    }

    pub(super) async fn embed_text_batch_with_retry(
        &self,
        texts: &[String],
    ) -> Result<Vec<Vec<f32>>, EmbeddingsClientError> {
        let mut backoff_ms = self.settings.worker.base_backoff_ms.max(250);
        let mut attempts = 0u8;
        loop {
            attempts = attempts.saturating_add(1);
            match self.embedding_client.embed_texts(texts).await {
                Ok(vectors) => return Ok(vectors),
                Err(err) => {
                    if !err.is_transient() || attempts >= 4 {
                        return Err(err);
                    }
                    sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms.saturating_mul(2))
                        .min(self.settings.worker.max_backoff_ms.max(backoff_ms));
                }
            }
        }
    }
}
