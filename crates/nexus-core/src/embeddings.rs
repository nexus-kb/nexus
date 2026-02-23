use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::config::{EmbeddingsConfig, Settings};

#[derive(Debug, Clone)]
pub struct OpenAiEmbeddingsClient {
    http: reqwest::Client,
    base_url: String,
    api_key: String,
    model: String,
    dimensions: usize,
    openrouter_referer: Option<String>,
    openrouter_title: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum EmbeddingsClientError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("unexpected response status {status}: {body}")]
    Status { status: u16, body: String },
    #[error("protocol error: {0}")]
    Protocol(String),
}

impl EmbeddingsClientError {
    pub fn is_transient(&self) -> bool {
        match self {
            EmbeddingsClientError::Http(_) => true,
            EmbeddingsClientError::Status { status, .. } => *status == 429 || *status >= 500,
            EmbeddingsClientError::Protocol(_) => false,
        }
    }
}

impl OpenAiEmbeddingsClient {
    pub fn from_settings(settings: &Settings) -> Self {
        Self::from_config(&settings.embeddings)
    }

    pub fn from_config(cfg: &EmbeddingsConfig) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url: cfg.base_url.trim_end_matches('/').to_string(),
            api_key: cfg.api_key.clone(),
            model: cfg.model.clone(),
            dimensions: cfg.dimensions,
            openrouter_referer: cfg.openrouter_referer.clone(),
            openrouter_title: cfg.openrouter_title.clone(),
        }
    }

    pub fn model(&self) -> &str {
        &self.model
    }

    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    pub async fn embed_texts(
        &self,
        inputs: &[String],
    ) -> Result<Vec<Vec<f32>>, EmbeddingsClientError> {
        if inputs.is_empty() {
            return Ok(Vec::new());
        }

        let mut request = self
            .http
            .post(format!("{}/embeddings", self.base_url))
            .bearer_auth(&self.api_key)
            .json(&json!({
                "model": self.model,
                "input": inputs,
                "dimensions": self.dimensions,
            }));

        if let Some(referer) = self.openrouter_referer.as_deref() {
            request = request.header("HTTP-Referer", referer);
        }
        if let Some(title) = self.openrouter_title.as_deref() {
            request = request.header("X-Title", title);
        }

        let response = request.send().await?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            return Err(EmbeddingsClientError::Status { status, body });
        }

        let payload: Value = response.json().await?;
        let data = payload
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                EmbeddingsClientError::Protocol("response missing data[]".to_string())
            })?;

        let mut vectors = vec![Vec::<f32>::new(); inputs.len()];
        for entry in data {
            let Some(index) = entry.get("index").and_then(Value::as_u64) else {
                return Err(EmbeddingsClientError::Protocol(
                    "embedding entry missing index".to_string(),
                ));
            };
            let idx = index as usize;
            if idx >= inputs.len() {
                return Err(EmbeddingsClientError::Protocol(format!(
                    "embedding index {idx} out of bounds for {} inputs",
                    inputs.len()
                )));
            }
            let Some(raw_embedding) = entry.get("embedding").and_then(Value::as_array) else {
                return Err(EmbeddingsClientError::Protocol(
                    "embedding entry missing embedding[]".to_string(),
                ));
            };
            let mut embedding = Vec::with_capacity(raw_embedding.len());
            for value in raw_embedding {
                let Some(number) = value.as_f64() else {
                    return Err(EmbeddingsClientError::Protocol(
                        "embedding value is not numeric".to_string(),
                    ));
                };
                embedding.push(number as f32);
            }
            vectors[idx] = embedding;
        }

        if vectors.iter().any(|v| v.is_empty()) {
            return Err(EmbeddingsClientError::Protocol(
                "response missing one or more embeddings".to_string(),
            ));
        }

        Ok(vectors)
    }

    pub async fn embed_query(&self, query: &str) -> Result<Vec<f32>, EmbeddingsClientError> {
        let vectors = self.embed_texts(&[query.to_string()]).await?;
        vectors.into_iter().next().ok_or_else(|| {
            EmbeddingsClientError::Protocol("response missing query embedding".to_string())
        })
    }
}

pub fn status_is_retryable(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}
