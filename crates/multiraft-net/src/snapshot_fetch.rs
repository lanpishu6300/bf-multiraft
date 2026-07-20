//! Chunked HTTP Range snapshot download with resume via temp files.

use std::fs;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use sha2::Digest;
use sha2::Sha256;

/// Snapshot bytes plus meta headers from a fetch server.
#[derive(Clone, Debug)]
pub struct FetchedSnapshot {
    pub last_index: u64,
    pub last_term: u64,
    pub snapshot_id: Option<String>,
    pub sha256_hex: String,
    pub data: Vec<u8>,
}

/// Pull snapshot bytes using HTTP Range requests (resume-capable temp file).
///
/// Temp path: `{temp_dir}/multiraft-snap-{url_hash}.partial`. On success the
/// temp file is deleted. On failure the partial file is left for resume.
pub async fn pull_snapshot_chunked(
    fetch_url: &str,
    chunk_bytes: usize,
    temp_dir: &Path,
) -> Result<FetchedSnapshot, anyhow::Error> {
    let chunk_bytes = chunk_bytes.max(1);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()?;

    let probe = client.head(fetch_url).send().await;
    let (headers, total_hint) = match probe {
        Ok(resp) if resp.status().is_success() || resp.status().as_u16() == 206 => {
            let headers = resp.headers().clone();
            let total = content_length(&headers).or_else(|| content_range_total(&headers));
            (headers, total)
        }
        _ => {
            // HEAD unsupported: probe with a tiny Range GET.
            let resp = client
                .get(fetch_url)
                .header(reqwest::header::RANGE, "bytes=0-0")
                .send()
                .await?;
            if !(resp.status().is_success() || resp.status().as_u16() == 206) {
                anyhow::bail!("fetch {fetch_url}: HTTP {}", resp.status());
            }
            let headers = resp.headers().clone();
            let total = content_length(&headers)
                .or_else(|| content_range_total(&headers))
                .or(Some(1));
            let _ = resp.bytes().await;
            (headers, total)
        }
    };

    let last_index = header_u64(&headers, "x-snapshot-index")
        .ok_or_else(|| anyhow::anyhow!("missing X-Snapshot-Index header"))?;
    let last_term = header_u64(&headers, "x-snapshot-term")
        .ok_or_else(|| anyhow::anyhow!("missing X-Snapshot-Term header"))?;
    let expected_sha = header_str(&headers, "x-snapshot-sha256")
        .ok_or_else(|| anyhow::anyhow!("missing X-Snapshot-Sha256 header"))?;
    let snapshot_id = header_str(&headers, "x-snapshot-id");

    let total = match total_hint.or_else(|| content_length(&headers)) {
        Some(t) => t,
        None => {
            // Server may omit length on HEAD; fall back to full GET.
            return pull_snapshot_full(&client, fetch_url, last_index, last_term, snapshot_id, &expected_sha)
                .await;
        }
    };

    fs::create_dir_all(temp_dir)?;
    let temp_path = temp_path_for_url(temp_dir, fetch_url, &expected_sha);
    let mut offset = if temp_path.exists() {
        fs::metadata(&temp_path)?.len()
    } else {
        0
    };
    if offset > total {
        let _ = fs::remove_file(&temp_path);
        offset = 0;
    }

    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(&temp_path)?;

    while offset < total {
        let end = (offset + chunk_bytes as u64 - 1).min(total - 1);
        let range = format!("bytes={offset}-{end}");
        let resp = client
            .get(fetch_url)
            .header(reqwest::header::RANGE, &range)
            .send()
            .await?;
        let status = resp.status().as_u16();
        if status != 206 && !(status == 200 && offset == 0) {
            anyhow::bail!("fetch {fetch_url} range {range}: HTTP {status}");
        }
        if status == 200 {
            // Server ignored Range — take full body once.
            let body = resp.bytes().await?.to_vec();
            verify_sha(&body, &expected_sha)?;
            let _ = fs::remove_file(&temp_path);
            return Ok(FetchedSnapshot {
                last_index,
                last_term,
                snapshot_id,
                sha256_hex: expected_sha,
                data: body,
            });
        }
        let chunk = resp.bytes().await?;
        if chunk.is_empty() {
            anyhow::bail!("empty range body for {range}");
        }
        file.write_all(&chunk)?;
        file.sync_all()?;
        offset += chunk.len() as u64;
    }

    let data = fs::read(&temp_path)?;
    verify_sha(&data, &expected_sha)?;
    let _ = fs::remove_file(&temp_path);
    Ok(FetchedSnapshot {
        last_index,
        last_term,
        snapshot_id,
        sha256_hex: expected_sha,
        data,
    })
}

async fn pull_snapshot_full(
    client: &reqwest::Client,
    fetch_url: &str,
    last_index: u64,
    last_term: u64,
    snapshot_id: Option<String>,
    expected_sha: &str,
) -> Result<FetchedSnapshot, anyhow::Error> {
    let resp = client.get(fetch_url).send().await?;
    if !resp.status().is_success() {
        anyhow::bail!("fetch {fetch_url}: HTTP {}", resp.status());
    }
    let data = resp.bytes().await?.to_vec();
    verify_sha(&data, expected_sha)?;
    Ok(FetchedSnapshot {
        last_index,
        last_term,
        snapshot_id,
        sha256_hex: expected_sha.to_string(),
        data,
    })
}

fn temp_path_for_url(temp_dir: &Path, fetch_url: &str, sha: &str) -> PathBuf {
    let mut h = Sha256::new();
    h.update(fetch_url.as_bytes());
    h.update(sha.as_bytes());
    let digest = h.finalize();
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    temp_dir.join(format!("multiraft-snap-{}.partial", &hex[..16]))
}

fn verify_sha(data: &[u8], expected: &str) -> Result<(), anyhow::Error> {
    let actual = hex_sha256(data);
    if actual != expected {
        anyhow::bail!("snapshot sha256 mismatch: expected {expected}, got {actual}");
    }
    Ok(())
}

fn hex_sha256(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

fn header_str(headers: &reqwest::header::HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

fn header_u64(headers: &reqwest::header::HeaderMap, name: &str) -> Option<u64> {
    header_str(headers, name)?.parse().ok()
}

fn content_length(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    headers
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok())
}

/// Parse `Content-Range: bytes start-end/total`.
fn content_range_total(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    let v = headers.get(reqwest::header::CONTENT_RANGE)?.to_str().ok()?;
    let total = v.split('/').nth(1)?;
    if total == "*" {
        return None;
    }
    total.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temp_path_stable_for_same_url() {
        let dir = std::env::temp_dir();
        let a = temp_path_for_url(&dir, "http://x/s", "abc");
        let b = temp_path_for_url(&dir, "http://x/s", "abc");
        assert_eq!(a, b);
    }
}
