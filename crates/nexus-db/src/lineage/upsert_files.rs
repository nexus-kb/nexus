use super::*;

impl LineageStore {
    pub async fn replace_patch_item_files(
        &self,
        patch_item_id: i64,
        files: &[UpsertPatchItemFileInput],
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        sqlx::query("DELETE FROM patch_item_files WHERE patch_item_id = $1")
            .bind(patch_item_id)
            .execute(&mut *tx)
            .await?;

        if !files.is_empty() {
            let mut qb = QueryBuilder::new(
                "INSERT INTO patch_item_files \
                 (patch_item_id, old_path, new_path, change_type, is_binary, additions, deletions, hunk_count, diff_start, diff_end)",
            );
            qb.push_values(files.iter(), |mut b, file| {
                b.push_bind(patch_item_id)
                    .push_bind(&file.old_path)
                    .push_bind(&file.new_path)
                    .push_bind(&file.change_type)
                    .push_bind(file.is_binary)
                    .push_bind(file.additions)
                    .push_bind(file.deletions)
                    .push_bind(file.hunk_count)
                    .push_bind(file.diff_start)
                    .push_bind(file.diff_end);
            });
            qb.build().execute(&mut *tx).await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn replace_patch_item_files_batch(
        &self,
        batches: &[PatchItemFileBatchInput],
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;
        let mut deduped_file_rows: Vec<(i64, UpsertPatchItemFileInput)> = Vec::new();
        let mut row_idx_by_key: BTreeMap<(i64, String), usize> = BTreeMap::new();
        let mut stats_by_patch_item: BTreeMap<i64, (i32, i32, i32, i32)> = BTreeMap::new();

        for batch in batches {
            stats_by_patch_item
                .entry(batch.patch_item_id)
                .or_insert((0, 0, 0, 0));
            for file in &batch.files {
                let key = (batch.patch_item_id, file.new_path.clone());
                if let Some(idx) = row_idx_by_key.get(&key).copied() {
                    merge_patch_item_file_row(&mut deduped_file_rows[idx].1, file);
                } else {
                    let idx = deduped_file_rows.len();
                    deduped_file_rows.push((batch.patch_item_id, file.clone()));
                    row_idx_by_key.insert(key, idx);
                }
            }
        }

        for (patch_item_id, file) in &deduped_file_rows {
            let stats = stats_by_patch_item
                .entry(*patch_item_id)
                .or_insert((0, 0, 0, 0));
            stats.0 = stats.0.saturating_add(1);
            stats.1 = stats.1.saturating_add(file.additions);
            stats.2 = stats.2.saturating_add(file.deletions);
            stats.3 = stats.3.saturating_add(file.hunk_count);
        }

        let patch_item_ids: Vec<i64> = stats_by_patch_item.keys().copied().collect();

        sqlx::query("DELETE FROM patch_item_files WHERE patch_item_id = ANY($1)")
            .bind(&patch_item_ids)
            .execute(&mut *tx)
            .await?;

        if !deduped_file_rows.is_empty() {
            let rows_per_insert =
                (MAX_QUERY_BIND_PARAMS / PATCH_ITEM_FILE_INSERT_BINDS_PER_ROW).max(1);
            for chunk in deduped_file_rows.chunks(rows_per_insert) {
                let mut qb: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
                    "INSERT INTO patch_item_files \
                    (patch_item_id, old_path, new_path, change_type, is_binary, additions, deletions, hunk_count, diff_start, diff_end) ",
                );
                qb.push_values(chunk.iter(), |mut b, (patch_item_id, file)| {
                    b.push_bind(patch_item_id)
                        .push_bind(&file.old_path)
                        .push_bind(&file.new_path)
                        .push_bind(&file.change_type)
                        .push_bind(file.is_binary)
                        .push_bind(file.additions)
                        .push_bind(file.deletions)
                        .push_bind(file.hunk_count)
                        .push_bind(file.diff_start)
                        .push_bind(file.diff_end);
                });
                qb.push(
                    " ON CONFLICT (patch_item_id, new_path) DO UPDATE SET \
                      old_path = EXCLUDED.old_path, \
                      change_type = EXCLUDED.change_type, \
                      is_binary = EXCLUDED.is_binary, \
                      additions = EXCLUDED.additions, \
                      deletions = EXCLUDED.deletions, \
                      hunk_count = EXCLUDED.hunk_count, \
                      diff_start = EXCLUDED.diff_start, \
                      diff_end = EXCLUDED.diff_end",
                );
                qb.build().execute(&mut *tx).await?;
            }
        }

        let stats_rows: Vec<(i64, i32, i32, i32, i32)> = stats_by_patch_item
            .into_iter()
            .map(
                |(patch_item_id, (file_count, additions, deletions, hunk_count))| {
                    (patch_item_id, file_count, additions, deletions, hunk_count)
                },
            )
            .collect();
        let stats_rows_per_chunk =
            (MAX_QUERY_BIND_PARAMS / PATCH_ITEM_FILE_STATS_BINDS_PER_ROW).max(1);
        for chunk in stats_rows.chunks(stats_rows_per_chunk) {
            let mut stats_qb: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
                "UPDATE patch_items pi \
                 SET file_count = stats.file_count, \
                     additions = stats.additions, \
                     deletions = stats.deletions, \
                     hunk_count = stats.hunk_count \
                 FROM (",
            );
            stats_qb.push_values(chunk.iter(), |mut b, row| {
                b.push_bind(row.0)
                    .push_bind(row.1)
                    .push_bind(row.2)
                    .push_bind(row.3)
                    .push_bind(row.4);
            });
            stats_qb.push(
                ") AS stats(patch_item_id, file_count, additions, deletions, hunk_count) \
                 WHERE pi.id = stats.patch_item_id",
            );
            stats_qb.build().execute(&mut *tx).await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn hydrate_patch_items_from_message_facts(
        &self,
        patch_item_ids: &[i64],
    ) -> Result<PatchFactHydrationOutcome> {
        let mut ids = patch_item_ids
            .iter()
            .copied()
            .filter(|patch_item_id| *patch_item_id > 0)
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids.dedup();

        if ids.is_empty() {
            return Ok(PatchFactHydrationOutcome::default());
        }

        #[derive(sqlx::FromRow)]
        struct PatchFactRow {
            patch_item_id: i64,
            has_fact: bool,
            has_diff: bool,
            patch_id_stable: Option<String>,
        }

        let fact_rows = sqlx::query_as::<_, PatchFactRow>(
            r#"SELECT
                pi.id AS patch_item_id,
                (mpf.message_pk IS NOT NULL) AS has_fact,
                COALESCE(mpf.has_diff, false) AS has_diff,
                mpf.patch_id_stable
            FROM patch_items pi
            LEFT JOIN message_patch_facts mpf
              ON mpf.message_pk = pi.message_pk
            WHERE pi.id = ANY($1)
            ORDER BY pi.id ASC"#,
        )
        .bind(&ids)
        .fetch_all(&self.pool)
        .await?;

        let mut seen_patch_item_ids = BTreeSet::new();
        let mut hydrated_patch_item_ids = Vec::new();
        let mut missing_patch_item_ids = Vec::new();
        let mut patch_id_updates = Vec::new();
        let mut has_diff_updates = Vec::new();

        for row in fact_rows {
            seen_patch_item_ids.insert(row.patch_item_id);
            if row.has_fact {
                hydrated_patch_item_ids.push(row.patch_item_id);
                patch_id_updates.push((row.patch_item_id, row.patch_id_stable));
                has_diff_updates.push((row.patch_item_id, row.has_diff));
            } else {
                missing_patch_item_ids.push(row.patch_item_id);
            }
        }

        for patch_item_id in ids {
            if !seen_patch_item_ids.contains(&patch_item_id) {
                missing_patch_item_ids.push(patch_item_id);
            }
        }

        if hydrated_patch_item_ids.is_empty() {
            missing_patch_item_ids.sort_unstable();
            missing_patch_item_ids.dedup();
            return Ok(PatchFactHydrationOutcome {
                hydrated_patch_items: 0,
                patch_item_files_written: 0,
                missing_patch_item_ids,
            });
        }

        #[derive(sqlx::FromRow)]
        struct PatchFileRow {
            patch_item_id: i64,
            old_path: Option<String>,
            new_path: String,
            change_type: String,
            is_binary: bool,
            additions: i32,
            deletions: i32,
            hunk_count: i32,
            diff_start: i32,
            diff_end: i32,
        }

        let file_rows = sqlx::query_as::<_, PatchFileRow>(
            r#"SELECT
                pi.id AS patch_item_id,
                mpff.old_path,
                mpff.new_path,
                mpff.change_type,
                mpff.is_binary,
                mpff.additions,
                mpff.deletions,
                mpff.hunk_count,
                mpff.diff_start,
                mpff.diff_end
            FROM patch_items pi
            JOIN message_patch_file_facts mpff
              ON mpff.message_pk = pi.message_pk
            WHERE pi.id = ANY($1)
            ORDER BY pi.id ASC, mpff.new_path ASC"#,
        )
        .bind(&hydrated_patch_item_ids)
        .fetch_all(&self.pool)
        .await?;

        let mut files_by_patch_item = BTreeMap::<i64, Vec<UpsertPatchItemFileInput>>::new();
        for patch_item_id in &hydrated_patch_item_ids {
            files_by_patch_item.insert(*patch_item_id, Vec::new());
        }

        for row in file_rows {
            files_by_patch_item
                .entry(row.patch_item_id)
                .or_default()
                .push(UpsertPatchItemFileInput {
                    old_path: row.old_path,
                    new_path: row.new_path,
                    change_type: row.change_type,
                    is_binary: row.is_binary,
                    additions: row.additions,
                    deletions: row.deletions,
                    hunk_count: row.hunk_count,
                    diff_start: row.diff_start,
                    diff_end: row.diff_end,
                });
        }

        let mut patch_item_files_written = 0u64;
        let mut file_batches = Vec::with_capacity(hydrated_patch_item_ids.len());
        for patch_item_id in &hydrated_patch_item_ids {
            let files = files_by_patch_item
                .remove(patch_item_id)
                .unwrap_or_default();
            patch_item_files_written += files.len() as u64;

            let mut additions = 0i32;
            let mut deletions = 0i32;
            let mut hunk_count = 0i32;
            for file in &files {
                additions = additions.saturating_add(file.additions);
                deletions = deletions.saturating_add(file.deletions);
                hunk_count = hunk_count.saturating_add(file.hunk_count);
            }

            file_batches.push(PatchItemFileBatchInput {
                patch_item_id: *patch_item_id,
                file_count: files.len() as i32,
                additions,
                deletions,
                hunk_count,
                files,
            });
        }

        self.replace_patch_item_files_batch(&file_batches).await?;
        self.set_patch_item_patch_ids_batch(&patch_id_updates)
            .await?;

        const HAS_DIFF_BINDS_PER_ROW: usize = 2;
        let rows_per_chunk = (MAX_QUERY_BIND_PARAMS / HAS_DIFF_BINDS_PER_ROW).max(1);
        for chunk in has_diff_updates.chunks(rows_per_chunk) {
            let mut qb: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
                "UPDATE patch_items pi \
                 SET has_diff = vals.has_diff \
                 FROM (",
            );
            qb.push_values(chunk.iter(), |mut b, (patch_item_id, has_diff)| {
                b.push_bind(*patch_item_id).push_bind(*has_diff);
            });
            qb.push(") AS vals(patch_item_id, has_diff) WHERE pi.id = vals.patch_item_id");
            qb.build().execute(&self.pool).await?;
        }

        missing_patch_item_ids.sort_unstable();
        missing_patch_item_ids.dedup();

        Ok(PatchFactHydrationOutcome {
            hydrated_patch_items: hydrated_patch_item_ids.len() as u64,
            patch_item_files_written,
            missing_patch_item_ids,
        })
    }

    pub async fn update_patch_item_diff_stats(
        &self,
        patch_item_id: i64,
        file_count: i32,
        additions: i32,
        deletions: i32,
        hunk_count: i32,
    ) -> Result<()> {
        sqlx::query(
            r#"UPDATE patch_items
            SET file_count = $2,
                additions = $3,
                deletions = $4,
                hunk_count = $5
            WHERE id = $1"#,
        )
        .bind(patch_item_id)
        .bind(file_count)
        .bind(additions)
        .bind(deletions)
        .bind(hunk_count)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
