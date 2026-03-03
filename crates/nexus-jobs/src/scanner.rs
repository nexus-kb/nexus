use std::path::Path;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::{Child, ChildStdout, Command};

pub struct CommitOidChunkStream {
    child: Child,
    stdout: BufReader<ChildStdout>,
    chunk_size: usize,
    finished: bool,
}

impl CommitOidChunkStream {
    pub async fn next_chunk(&mut self) -> anyhow::Result<Option<Vec<String>>> {
        if self.finished {
            return Ok(None);
        }

        let mut chunk = Vec::with_capacity(self.chunk_size);
        while chunk.len() < self.chunk_size {
            let mut line = String::new();
            let read = self.stdout.read_line(&mut line).await?;
            if read == 0 {
                break;
            }

            let oid = line.trim();
            if !oid.is_empty() {
                chunk.push(oid.to_string());
            }
        }

        if !chunk.is_empty() {
            return Ok(Some(chunk));
        }

        self.finished = true;
        let status = self.child.wait().await?;
        if status.success() {
            return Ok(None);
        }

        let mut stderr = String::new();
        if let Some(mut stderr_pipe) = self.child.stderr.take() {
            let mut bytes = Vec::new();
            if stderr_pipe.read_to_end(&mut bytes).await.is_ok() {
                stderr = String::from_utf8_lossy(&bytes).to_string();
            }
        }

        anyhow::bail!("git rev-list failed: {}", stderr.trim());
    }
}

impl Drop for CommitOidChunkStream {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.child.start_kill();
        }
    }
}

pub async fn stream_new_commit_oid_chunks(
    repo_path: &Path,
    since_commit_oid: Option<&str>,
    chunk_size: usize,
) -> anyhow::Result<CommitOidChunkStream> {
    if chunk_size == 0 {
        anyhow::bail!("chunk size must be > 0");
    }

    let incremental_since = match since_commit_oid {
        Some(oid) if commit_exists(repo_path, oid).await? => Some(oid),
        _ => None,
    };

    let mut cmd = Command::new("git");
    cmd.kill_on_drop(true);
    cmd.arg("-C")
        .arg(repo_path)
        .arg("rev-list")
        .arg("--reverse")
        // Track commit progress against branch tips only; `--all` includes metadata refs
        // (for example `refs/meta/*`) and can repeatedly resurface historical commits.
        .arg("--branches");

    if let Some(since) = incremental_since {
        // Exclude the prior watermark and everything reachable from it to avoid a full scan.
        cmd.arg(format!("^{since}"));
    }

    cmd.stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let mut child = cmd.spawn()?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("missing git stdout pipe"))?;

    Ok(CommitOidChunkStream {
        child,
        stdout: BufReader::new(stdout),
        chunk_size,
        finished: false,
    })
}

#[cfg(test)]
pub fn chunk_commit_oids(commits: &[String], chunk_size: usize) -> Vec<Vec<String>> {
    if chunk_size == 0 {
        return Vec::new();
    }

    let mut chunks = Vec::new();
    let mut cursor = 0usize;

    while cursor < commits.len() {
        let next = (cursor + chunk_size).min(commits.len());
        chunks.push(commits[cursor..next].to_vec());
        cursor = next;
    }

    chunks
}

async fn commit_exists(repo_path: &Path, commit_oid: &str) -> anyhow::Result<bool> {
    let status = Command::new("git")
        .arg("-C")
        .arg(repo_path)
        .arg("cat-file")
        .arg("-e")
        .arg(format!("{commit_oid}^{{commit}}"))
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await?;
    Ok(status.success())
}

#[cfg(test)]
mod tests {
    use super::chunk_commit_oids;

    #[test]
    fn chunking_is_stable_and_ordered() {
        let commits: Vec<String> = (1..=7).map(|n| format!("c{n}")).collect();
        let chunks = chunk_commit_oids(&commits, 3);

        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0], vec!["c1", "c2", "c3"]);
        assert_eq!(chunks[1], vec!["c4", "c5", "c6"]);
        assert_eq!(chunks[2], vec!["c7"]);
    }
}
