use std::path::Path;

use chrono::Utc;
use gix::hash::ObjectId;
use nexus_db::{EmailInput, MailIngestStore, MailingListEpoch, MailingListEpochStore};
use tracing::{info, warn};

use super::parse::parse_email;
use super::{MailJobError, MailJobResult};

/// Maximum emails per ingest batch.
const BATCH_SIZE: usize = 10_000;

/// Message payload exchanged between git parsing and database ingest.
struct BatchMessage {
    emails: Vec<EmailInput>,
    last_commit: Option<String>,
}

/// Parse a public-inbox git epoch and ingest new emails into the database.
pub async fn sync_epoch(
    repo_path: &Path,
    ingest_store: &MailIngestStore,
    epoch_store: &MailingListEpochStore,
    epoch: MailingListEpoch,
    mailing_list_id: i32,
    list_slug: &str,
    total_emails: &mut usize,
) -> MailJobResult<(usize, usize)> {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<BatchMessage>(2);
    let repo_path = repo_path.to_path_buf();
    let last_indexed = epoch.last_indexed_commit.clone();
    let epoch_id = epoch.id;
    let epoch_value = epoch.epoch;

    let producer = tokio::task::spawn_blocking(move || {
        let mut repo =
            gix::open(&repo_path).map_err(|err| MailJobError::Gitoxide(err.to_string()))?;
        repo.object_cache_size_if_unset(64 * 1024 * 1024);
        let commits = collect_commits(&repo, last_indexed.as_deref())?;
        let mut commits_processed = 0usize;
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        let mut last_processed_commit: Option<String> = None;

        for commit_id in commits.iter() {
            commits_processed += 1;
            let commit_hex = commit_id.to_hex().to_string();
            last_processed_commit = Some(commit_hex.clone());

            let commit = repo
                .find_object(*commit_id)
                .map_err(|err| MailJobError::Gitoxide(err.to_string()))?
                .try_into_commit()
                .map_err(|err| MailJobError::Gitoxide(err.to_string()))?;

            let tree = commit
                .tree()
                .map_err(|err| MailJobError::Gitoxide(err.to_string()))?;

            let mut blob_oid: Option<ObjectId> = None;
            for entry in tree
                .iter()
                .map(|entry| entry.map_err(|err| MailJobError::Gitoxide(err.to_string())))
            {
                let entry = entry?;
                if entry.filename() == "m" {
                    blob_oid = Some(entry.id().detach());
                    break;
                }
            }

            match blob_oid {
                Some(oid) => {
                    let blob = repo
                        .find_object(oid)
                        .map_err(|err| MailJobError::Gitoxide(err.to_string()))?
                        .try_into_blob()
                        .map_err(|err| MailJobError::Gitoxide(err.to_string()))?;

                    let blob_hex = oid.to_hex().to_string();
                    let parsed = match parse_email(&blob.data) {
                        Ok(parsed) => parsed,
                        Err(err) => {
                            warn!(
                                repo = %repo_path.display(),
                                commit = %commit_hex,
                                error = %err,
                                "mail parse failed"
                            );
                            continue;
                        }
                    };

                    // Serialize patch_metadata to JSON for storage
                    let patch_metadata_json = parsed
                        .patch_metadata
                        .as_ref()
                        .and_then(|pm| serde_json::to_value(pm).ok());

                    batch.push(EmailInput {
                        mailing_list_id,
                        message_id: parsed.message_id,
                        blob_oid: blob_hex,
                        epoch: epoch_value,
                        author_email: parsed.from.email,
                        author_name: parsed.from.name,
                        subject: parsed.subject,
                        normalized_subject: parsed.normalized_subject,
                        date: parsed.date,
                        in_reply_to: parsed.in_reply_to,
                        body: parsed.body,
                        patch_metadata: patch_metadata_json,
                        references: parsed.references,
                        to: parsed.to,
                        cc: parsed.cc,
                    });

                    if batch.len() >= BATCH_SIZE {
                        tx.blocking_send(BatchMessage {
                            emails: std::mem::take(&mut batch),
                            last_commit: last_processed_commit.clone(),
                        })
                        .map_err(|_| MailJobError::Gitoxide("batch channel closed".to_string()))?;
                    }
                }
                None => {
                    warn!(repo = %repo_path.display(), commit = %commit_hex, "commit missing mail blob");
                }
            }
        }

        if !batch.is_empty() || last_processed_commit.is_some() {
            tx.blocking_send(BatchMessage {
                emails: batch,
                last_commit: last_processed_commit,
            })
            .map_err(|_| MailJobError::Gitoxide("batch channel closed".to_string()))?;
        }

        Ok::<usize, MailJobError>(commits_processed)
    });

    let mut emails_ingested = 0usize;
    let mut last_commit_to_update: Option<String> = None;

    while let Some(message) = rx.recv().await {
        if !message.emails.is_empty() {
            let batch_size = ingest_store
                .ingest_batch(mailing_list_id, &message.emails)
                .await?;
            emails_ingested += batch_size;
            *total_emails += batch_size;
            info!(
                list = %list_slug,
                epoch = epoch.epoch,
                batch_size,
                total_emails = *total_emails,
                "mailing list batch imported"
            );
        }

        if let Some(commit) = message.last_commit {
            last_commit_to_update = Some(commit);
        }

        if let Some(commit_hex) = last_commit_to_update.as_deref() {
            epoch_store
                .update_last_indexed_commit(epoch_id, commit_hex, Utc::now())
                .await?;
            last_commit_to_update = None;
        }
    }

    let commits_processed = producer
        .await
        .map_err(|err| MailJobError::Gitoxide(err.to_string()))??;

    Ok((commits_processed, emails_ingested))
}

fn collect_commits(
    repo: &gix::Repository,
    last_commit: Option<&str>,
) -> MailJobResult<Vec<ObjectId>> {
    let head = repo
        .head_id()
        .map_err(|err| MailJobError::Gitoxide(err.to_string()))?
        .detach();

    let walk = repo.rev_walk([head]);
    let walk = if let Some(last_commit) = last_commit {
        match ObjectId::from_hex(last_commit.as_bytes()) {
            Ok(last_id) => walk
                .selected(move |id| id != last_id)
                .map_err(|err| MailJobError::Gitoxide(err.to_string()))?,
            Err(err) => {
                warn!(error = %err, "invalid last_indexed_commit; full scan");
                walk.all()
                    .map_err(|err| MailJobError::Gitoxide(err.to_string()))?
            }
        }
    } else {
        walk.all()
            .map_err(|err| MailJobError::Gitoxide(err.to_string()))?
    };

    let mut commits = Vec::new();
    for info in walk {
        let info = info.map_err(|err| MailJobError::Gitoxide(err.to_string()))?;
        commits.push(info.id);
    }
    commits.reverse();
    Ok(commits)
}
