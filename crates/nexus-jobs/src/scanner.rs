use std::path::Path;
use std::process::Command;

pub fn collect_new_commit_oids(
    repo_path: &Path,
    since_commit_oid: Option<&str>,
) -> anyhow::Result<Vec<String>> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo_path)
        .arg("rev-list")
        .arg("--reverse")
        .arg("--all")
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("git rev-list failed: {stderr}");
    }

    let mut commits: Vec<String> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToString::to_string)
        .collect();

    if let Some(since) = since_commit_oid
        && let Some(pos) = commits.iter().position(|oid| oid == since)
    {
        commits = commits.into_iter().skip(pos + 1).collect();
    }

    Ok(commits)
}

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
