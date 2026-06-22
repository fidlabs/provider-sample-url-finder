use std::str::FromStr;
use std::{net::IpAddr, str};

use color_eyre::{
    Result,
    eyre::{Context, eyre},
};
use futures::StreamExt;
use reqwest::Url;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::types::BigDecimal;

const MAX_MANIFEST_BYTES: usize = 10 * 1024 * 1024;
const MAX_NUMERIC_DIGITS: usize = 78;
const MAX_MANIFEST_PIECES: usize = 100_000;
const MAX_PIECE_CID_LENGTH: usize = 256;
const MAX_MANIFEST_FIELD_LENGTH: usize = 1024;

#[derive(Debug, Clone)]
pub struct FetchedManifestSnapshot {
    pub manifest_hash: String,
    pub manifest_location: String,
    pub raw_content: String,
    pub parsed_content: Value,
    pub content_byte_length: i64,
    pub computed_hash: String,
    pub pieces: Vec<DerivedManifestPiece>,
}

#[derive(Debug, Clone)]
pub struct DerivedManifestPiece {
    pub piece_index: i32,
    pub piece_cid: String,
    pub piece_size_bytes: Option<BigDecimal>,
    pub file_size_bytes: Option<BigDecimal>,
    pub root_cid: Option<String>,
    pub storage_path: Option<String>,
    pub piece_type: Option<String>,
}

pub async fn fetch_manifest_snapshot(
    client: &reqwest::Client,
    manifest_location: &str,
    expected_hash: &str,
) -> Result<FetchedManifestSnapshot> {
    let manifest_url = validate_manifest_location(manifest_location)?;
    let response = client
        .get(manifest_url)
        .send()
        .await
        .wrap_err_with(|| format!("failed to fetch manifest from {manifest_location}"))?;

    let status = response.status();
    if !status.is_success() {
        return Err(eyre!(
            "manifest fetch from {manifest_location} returned HTTP {status}"
        ));
    }

    let bytes = read_manifest_body(response, manifest_location).await?;
    let computed_hash = compute_manifest_hash(&bytes);
    if !manifest_hash_matches(expected_hash, &bytes) {
        return Err(eyre!(
            "manifest hash mismatch for {manifest_location}: expected {expected_hash}, computed {computed_hash}"
        ));
    }

    let raw_content = str::from_utf8(&bytes)
        .wrap_err("manifest content must be UTF-8 JSON for snapshot storage")?
        .to_string();
    let parsed_content = parse_manifest(&raw_content)?;
    let pieces = derive_manifest_pieces(&parsed_content)?;
    if pieces.is_empty() {
        return Err(eyre!("manifest must contain at least one piece"));
    }

    Ok(FetchedManifestSnapshot {
        manifest_hash: normalize_manifest_hash(expected_hash),
        manifest_location: manifest_location.to_string(),
        raw_content,
        parsed_content,
        content_byte_length: i64::try_from(bytes.len())
            .wrap_err("manifest content length exceeded i64::MAX")?,
        computed_hash,
        pieces,
    })
}

fn validate_manifest_location(manifest_location: &str) -> Result<Url> {
    let url = Url::parse(manifest_location).wrap_err("manifest_location must be a valid URL")?;
    match url.scheme() {
        "http" | "https" => {}
        scheme => return Err(eyre!("manifest_location scheme {scheme} is not supported")),
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(eyre!("manifest_location must not include credentials"));
    }
    if url.fragment().is_some() {
        return Err(eyre!("manifest_location must not include a fragment"));
    }
    if let Some(host) = url.host_str()
        && let Ok(ip) = host.parse::<IpAddr>()
        && !is_public_ip(ip)
        && !cfg!(debug_assertions)
    {
        return Err(eyre!("manifest_location host must be public"));
    }
    Ok(url)
}

fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            !(ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_broadcast()
                || ip.is_documentation()
                || ip.is_unspecified())
        }
        IpAddr::V6(ip) => {
            !(ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_unique_local()
                || ip.is_unicast_link_local())
        }
    }
}

async fn read_manifest_body(
    response: reqwest::Response,
    manifest_location: &str,
) -> Result<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_MANIFEST_BYTES as u64)
    {
        return Err(eyre!(
            "manifest from {manifest_location} exceeds maximum size of {MAX_MANIFEST_BYTES} bytes"
        ));
    }

    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk
            .wrap_err_with(|| format!("failed to read manifest body from {manifest_location}"))?;
        if bytes.len() + chunk.len() > MAX_MANIFEST_BYTES {
            return Err(eyre!(
                "manifest from {manifest_location} exceeds maximum size of {MAX_MANIFEST_BYTES} bytes"
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

pub fn compute_manifest_hash(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

pub fn manifest_hash_matches(expected_hash: &str, bytes: &[u8]) -> bool {
    normalize_manifest_hash(expected_hash) == compute_manifest_hash(bytes)
}

pub fn parse_manifest(raw_content: &str) -> Result<Value> {
    let parsed: Value =
        serde_json::from_str(raw_content).wrap_err("manifest must be valid JSON")?;
    if !parsed.is_array() {
        return Err(eyre!("manifest top-level value must be an array"));
    }
    Ok(parsed)
}

pub fn derive_manifest_pieces(parsed: &Value) -> Result<Vec<DerivedManifestPiece>> {
    let attachments = parsed
        .as_array()
        .ok_or_else(|| eyre!("manifest top-level value must be an array"))?;
    let mut pieces = Vec::new();

    for attachment in attachments {
        let Some(attachment_pieces) = attachment.get("pieces").and_then(Value::as_array) else {
            continue;
        };

        for piece in attachment_pieces {
            if pieces.len() >= MAX_MANIFEST_PIECES {
                return Err(eyre!(
                    "manifest piece count exceeds maximum of {MAX_MANIFEST_PIECES}"
                ));
            }
            let piece_index =
                i32::try_from(pieces.len()).wrap_err("manifest piece count exceeded i32::MAX")?;
            let piece_cid = required_string(piece, "pieceCid")?;
            validate_piece_cid(&piece_cid)?;
            pieces.push(DerivedManifestPiece {
                piece_index,
                piece_cid,
                piece_size_bytes: Some(required_decimal(piece, "pieceSize")?),
                file_size_bytes: Some(required_decimal(piece, "fileSize")?),
                root_cid: optional_string(piece, "rootCid")?,
                storage_path: optional_string(piece, "storagePath")?,
                piece_type: optional_string(piece, "pieceType")?,
            });
        }
    }

    Ok(pieces)
}

fn normalize_manifest_hash(value: &str) -> String {
    value
        .trim()
        .strip_prefix("0x")
        .unwrap_or_else(|| value.trim())
        .to_ascii_lowercase()
}

fn required_string(value: &Value, field: &str) -> Result<String> {
    optional_string(value, field)?
        .filter(|value| !value.is_empty())
        .ok_or_else(|| eyre!("manifest piece {field} is required"))
}

fn optional_string(value: &Value, field: &str) -> Result<Option<String>> {
    let Some(value) = value.get(field).and_then(Value::as_str).map(str::trim) else {
        return Ok(None);
    };
    if value.is_empty() {
        return Ok(None);
    }
    if value.len() > MAX_MANIFEST_FIELD_LENGTH {
        return Err(eyre!("manifest piece {field} exceeds maximum length"));
    }
    Ok(Some(value.to_string()))
}

fn validate_piece_cid(piece_cid: &str) -> Result<()> {
    if piece_cid.len() > MAX_PIECE_CID_LENGTH
        || !piece_cid.chars().all(|c| c.is_ascii_alphanumeric())
    {
        return Err(eyre!("manifest piece pieceCid must be an ASCII CID string"));
    }
    Ok(())
}

fn optional_decimal(value: &Value, field: &str) -> Result<Option<BigDecimal>> {
    let Some(raw_value) = value.get(field) else {
        return Ok(None);
    };

    let decimal_string = match raw_value {
        Value::Number(number) => number.to_string(),
        Value::String(value) => value.clone(),
        _ => return Err(eyre!("manifest piece {field} must be a base-10 integer")),
    };

    if decimal_string.is_empty()
        || decimal_string.len() > MAX_NUMERIC_DIGITS
        || !decimal_string.chars().all(|c| c.is_ascii_digit())
    {
        return Err(eyre!("manifest piece {field} must be a base-10 integer"));
    }

    BigDecimal::from_str(&decimal_string)
        .map(Some)
        .wrap_err_with(|| format!("manifest piece {field} must be a base-10 integer"))
}

fn required_decimal(value: &Value, field: &str) -> Result<BigDecimal> {
    optional_decimal(value, field)?.ok_or_else(|| eyre!("manifest piece {field} is required"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_manifest_pieces_in_attachment_order() {
        let raw = r#"[{
            "pieces": [
                {
                    "pieceType": "dag",
                    "pieceCid": "baga6ea4seaqpiuajbrhcrmbodbk72kau7qssqu7ydpivldhq7azjpuls5wruafi",
                    "pieceSize": 134217728,
                    "fileSize": 81667561,
                    "rootCid": "bafybeihi2gd73rodpxxkwaglwwv2mwdpkzjrfheseszyjx2evt2iykpbcm",
                    "storagePath": "baga6ea4seaqpiuajbrhcrmbodbk72kau7qssqu7ydpivldhq7azjpuls5wruafi.car"
                }
            ]
        }]"#;

        let parsed = parse_manifest(raw).unwrap();
        let pieces = derive_manifest_pieces(&parsed).unwrap();

        assert_eq!(pieces.len(), 1);
        assert_eq!(pieces[0].piece_index, 0);
        assert_eq!(
            pieces[0].piece_size_bytes.as_ref().unwrap().to_string(),
            "134217728"
        );
        assert_eq!(
            pieces[0].file_size_bytes.as_ref().unwrap().to_string(),
            "81667561"
        );
        assert_eq!(
            pieces[0].root_cid.as_deref(),
            Some("bafybeihi2gd73rodpxxkwaglwwv2mwdpkzjrfheseszyjx2evt2iykpbcm")
        );
    }

    #[test]
    fn verifies_sha256_manifest_hash_with_optional_0x_prefix() {
        let raw = br#"[{"pieces":[]}]"#;
        let expected = compute_manifest_hash(raw);

        assert!(manifest_hash_matches(&expected, raw));
        assert!(manifest_hash_matches(&format!("0x{expected}"), raw));
        assert!(!manifest_hash_matches("00", raw));
    }
}
