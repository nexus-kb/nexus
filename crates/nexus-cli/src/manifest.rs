//! Manifest download and parsing for public-inbox mirrors.

use flate2::read::GzDecoder;
use serde::Deserialize;
use std::collections::HashMap;
use std::io::Read;

/// Single entry in the public-inbox manifest.
#[derive(Deserialize, Debug)]
pub struct ManifestEntry {
    pub description: Option<String>,
    pub fingerprint: Option<String>,
    pub modified: Option<i64>,
    pub reference: Option<String>,
}

/// Manifest mapping of mirror paths to entries.
pub type Manifest = HashMap<String, ManifestEntry>;

/// Fetch and parse the gzip-compressed JSON manifest.
pub async fn fetch_manifest(url: &str) -> Result<Manifest, Box<dyn std::error::Error>> {
    let resp = reqwest::get(url).await?.error_for_status()?;
    let bytes = resp.bytes().await?;
    let mut decoder = GzDecoder::new(bytes.as_ref());
    let mut buf = String::new();
    decoder.read_to_string(&mut buf)?;
    let manifest: Manifest = serde_json::from_str(&buf)?;
    Ok(manifest)
}
