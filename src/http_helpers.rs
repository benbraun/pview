use anyhow::Context;
use std::sync::OnceLock;
use std::time::Duration;
use thiserror::Error;

#[derive(Error, Debug)]
#[error("Hub is Locked for maintenance. Response: {body}")]
pub struct LockedError {
    pub body: String,
}

/// Shared client for hub REST calls, so connections are pooled.
/// The connect timeout is short so that an unreachable hub surfaces as a
/// connect error quickly (serve-mqtt's unresponsive-hub detection keys off
/// `is_connect()`) instead of stalling callers for the full request timeout.
fn http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .build()
            .expect("failed to construct reqwest client")
    })
}

pub async fn json_body<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
) -> anyhow::Result<T> {
    let data = response.bytes().await.context("ready response body")?;
    serde_json::from_slice(&data).with_context(|| {
        format!(
            "parsing response as json: {}",
            String::from_utf8_lossy(&data)
        )
    })
}

pub async fn get_request_with_json_response<T: reqwest::IntoUrl, R: serde::de::DeserializeOwned>(
    url: T,
) -> anyhow::Result<R> {
    let response = http_client()
        .request(reqwest::Method::GET, url)
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        let url = response.url().clone();
        let body_bytes = response.bytes().await.with_context(|| {
            format!(
                "request status {}: {}, and failed to read response body",
                status.as_u16(),
                status.canonical_reason().unwrap_or("")
            )
        })?;

        if status.as_u16() == 423 {
            let body = String::from_utf8_lossy(&body_bytes).to_string();
            return Err(LockedError { body }).with_context(move || format!("GET {url}"));
        }

        anyhow::bail!(
            "request status {}: {}. Response body: {}",
            status.as_u16(),
            status.canonical_reason().unwrap_or(""),
            String::from_utf8_lossy(&body_bytes)
        );
    }
    json_body(response).await.with_context(|| {
        format!(
            "request status {}: {}",
            status.as_u16(),
            status.canonical_reason().unwrap_or("")
        )
    })
}

pub async fn request_with_json_response<
    T: reqwest::IntoUrl,
    B: serde::Serialize,
    R: serde::de::DeserializeOwned,
>(
    method: reqwest::Method,
    url: T,
    body: &B,
) -> anyhow::Result<R> {
    let response = http_client().request(method, url).json(body).send().await?;

    let status = response.status();
    if !status.is_success() {
        let body_bytes = response.bytes().await.with_context(|| {
            format!(
                "request status {}: {}, and failed to read response body",
                status.as_u16(),
                status.canonical_reason().unwrap_or("")
            )
        })?;
        anyhow::bail!(
            "request status {}: {}. Response body: {}",
            status.as_u16(),
            status.canonical_reason().unwrap_or(""),
            String::from_utf8_lossy(&body_bytes)
        );
    }
    json_body(response).await.with_context(|| {
        format!(
            "request status {}: {}",
            status.as_u16(),
            status.canonical_reason().unwrap_or("")
        )
    })
}
