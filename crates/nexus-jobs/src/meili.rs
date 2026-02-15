use nexus_core::config::Settings;
use nexus_core::search::MeiliIndexSpec;
use reqwest::StatusCode;
use serde_json::{Value, json};

#[derive(Debug, Clone)]
pub struct MeiliClient {
    http: reqwest::Client,
    base_url: String,
    master_key: String,
}

#[derive(Debug, Clone)]
pub struct MeiliTaskStatus {
    pub status: String,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum MeiliClientError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("unexpected response status {status}: {body}")]
    Status { status: u16, body: String },
    #[error("protocol error: {0}")]
    Protocol(String),
}

impl MeiliClientError {
    pub fn is_transient(&self) -> bool {
        match self {
            MeiliClientError::Http(_) => true,
            MeiliClientError::Status { status, .. } => *status == 429 || *status >= 500,
            MeiliClientError::Protocol(_) => false,
        }
    }
}

impl MeiliClient {
    pub fn from_settings(settings: &Settings) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url: settings.meili.url.trim_end_matches('/').to_string(),
            master_key: settings.meili.master_key.clone(),
        }
    }

    pub async fn ensure_index_exists(
        &self,
        spec: &MeiliIndexSpec,
    ) -> Result<Option<i64>, MeiliClientError> {
        let response = self
            .http
            .post(format!("{}/indexes", self.base_url))
            .bearer_auth(&self.master_key)
            .json(&json!({
                "uid": spec.uid,
                "primaryKey": spec.primary_key
            }))
            .send()
            .await?;

        if response.status().is_success() {
            let value: Value = response.json().await?;
            let task_uid = parse_task_uid(&value)?;
            return Ok(Some(task_uid));
        }

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if status == StatusCode::BAD_REQUEST || status == StatusCode::CONFLICT {
            let code = parse_error_code(&body);
            if matches!(code.as_deref(), Some("index_already_exists")) {
                return Ok(None);
            }
        }

        Err(MeiliClientError::Status {
            status: status.as_u16(),
            body,
        })
    }

    pub async fn get_settings(&self, index_uid: &str) -> Result<Option<Value>, MeiliClientError> {
        let response = self
            .http
            .get(format!("{}/indexes/{index_uid}/settings", self.base_url))
            .bearer_auth(&self.master_key)
            .send()
            .await?;

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(MeiliClientError::Status {
                status: status.as_u16(),
                body,
            });
        }

        let value: Value = response.json().await?;
        Ok(Some(value))
    }

    pub async fn update_settings(
        &self,
        index_uid: &str,
        settings: &Value,
    ) -> Result<i64, MeiliClientError> {
        let response = self
            .http
            .patch(format!("{}/indexes/{index_uid}/settings", self.base_url))
            .bearer_auth(&self.master_key)
            .json(settings)
            .send()
            .await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(MeiliClientError::Status {
                status: status.as_u16(),
                body,
            });
        }
        let value: Value = response.json().await?;
        parse_task_uid(&value)
    }

    pub async fn upsert_documents(
        &self,
        index_uid: &str,
        docs: &[Value],
    ) -> Result<i64, MeiliClientError> {
        let response = self
            .http
            .post(format!("{}/indexes/{index_uid}/documents", self.base_url))
            .bearer_auth(&self.master_key)
            .json(docs)
            .send()
            .await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(MeiliClientError::Status {
                status: status.as_u16(),
                body,
            });
        }
        let value: Value = response.json().await?;
        parse_task_uid(&value)
    }

    pub async fn get_task(&self, task_uid: i64) -> Result<MeiliTaskStatus, MeiliClientError> {
        let response = self
            .http
            .get(format!("{}/tasks/{task_uid}", self.base_url))
            .bearer_auth(&self.master_key)
            .send()
            .await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(MeiliClientError::Status {
                status: status.as_u16(),
                body,
            });
        }
        let value: Value = response.json().await?;
        let status = value
            .get("status")
            .and_then(Value::as_str)
            .ok_or_else(|| MeiliClientError::Protocol("task response missing status".to_string()))?
            .to_string();
        let error_code = value
            .get("error")
            .and_then(|err| err.get("code"))
            .and_then(Value::as_str)
            .map(ToString::to_string);
        let error_message = value
            .get("error")
            .and_then(|err| err.get("message"))
            .and_then(Value::as_str)
            .map(ToString::to_string);

        Ok(MeiliTaskStatus {
            status,
            error_code,
            error_message,
        })
    }
}

pub fn settings_differ(current: Option<&Value>, target: &Value) -> bool {
    let Some(current) = current else {
        return true;
    };
    for key in [
        "searchableAttributes",
        "filterableAttributes",
        "sortableAttributes",
        "displayedAttributes",
        "rankingRules",
        "embedders",
    ] {
        if current.get(key) != target.get(key) {
            return true;
        }
    }
    false
}

fn parse_task_uid(value: &Value) -> Result<i64, MeiliClientError> {
    if let Some(task_uid) = value.get("taskUid").and_then(Value::as_i64) {
        return Ok(task_uid);
    }
    if let Some(task_uid) = value.get("uid").and_then(Value::as_i64) {
        return Ok(task_uid);
    }
    Err(MeiliClientError::Protocol(format!(
        "missing task uid in response: {value}"
    )))
}

fn parse_error_code(body: &str) -> Option<String> {
    serde_json::from_str::<Value>(body).ok().and_then(|v| {
        v.get("code")
            .and_then(Value::as_str)
            .map(ToString::to_string)
    })
}
