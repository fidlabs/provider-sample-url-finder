//! CAR (Content Addressable aRchive) header parsing.
//!
//! Parses CAR v1/v2 headers to extract root CID for verification against deal Labels.

use ciborium::Value;
use tracing::trace;

/// CARv2 pragma: fixed 11 bytes identifying CARv2 format
const CAR_V2_PRAGMA: &[u8] = &[
    0x0a, 0xa1, 0x67, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x02,
];

/// Result of parsing CAR header
#[derive(Debug, Clone, Default)]
pub struct CarHeaderParseResult {
    pub is_valid: bool,
    pub version: Option<u8>,
    pub root_cid: Option<String>,
    pub header_size: Option<usize>,
}

impl CarHeaderParseResult {
    fn invalid() -> Self {
        Self::default()
    }
}

/// Parse CAR header from response bytes, extract root CID if valid.
pub fn parse_car_header(bytes: &[u8]) -> CarHeaderParseResult {
    if bytes.is_empty() {
        return CarHeaderParseResult::invalid();
    }

    // Check for CARv2 pragma
    if bytes.len() >= CAR_V2_PRAGMA.len() && bytes.starts_with(CAR_V2_PRAGMA) {
        return parse_car_v2_header(bytes);
    }

    parse_car_v1_header(bytes)
}

fn parse_car_v1_header(bytes: &[u8]) -> CarHeaderParseResult {
    // 1. Read varint length prefix (LEB128 unsigned)
    let (header_len, varint_size) = match read_varint(bytes) {
        Some(v) => v,
        None => return CarHeaderParseResult::invalid(),
    };

    // Sanity check: header shouldn't be too large
    if header_len > 10_000 {
        trace!("CAR header too large: {header_len}");
        return CarHeaderParseResult::invalid();
    }

    // 2. Bounds check
    let header_end = varint_size + header_len;
    if bytes.len() < header_end {
        trace!(
            "CAR header truncated: need {header_end}, have {}",
            bytes.len()
        );
        return CarHeaderParseResult::invalid();
    }

    // 3. Parse DAG-CBOR header
    let header_bytes = &bytes[varint_size..header_end];
    let cbor: Value = match ciborium::from_reader(header_bytes) {
        Ok(v) => v,
        Err(e) => {
            trace!("CAR CBOR parse failed: {e}");
            return CarHeaderParseResult::invalid();
        }
    };

    // 4. Extract version and roots from CBOR map
    let map = match cbor.as_map() {
        Some(m) => m,
        None => {
            trace!("CAR header not a CBOR map");
            return CarHeaderParseResult::invalid();
        }
    };

    let version = extract_version(map);
    if version != Some(1) {
        trace!("CAR version not 1: {version:?}");
        return CarHeaderParseResult::invalid();
    }

    let root_cid = extract_first_root_cid(map);

    CarHeaderParseResult {
        is_valid: true,
        version: Some(1),
        root_cid,
        header_size: Some(header_end),
    }
}

fn parse_car_v2_header(bytes: &[u8]) -> CarHeaderParseResult {
    // CARv2 structure:
    // [11-byte pragma][40-byte header][CARv1 payload at data_offset]
    //
    // 40-byte header:
    // - 16 bytes: characteristics (bitfield)
    // - 8 bytes: data_offset (u64 little-endian)
    // - 8 bytes: data_size (u64 little-endian)
    // - 8 bytes: index_offset (u64 little-endian)

    const HEADER_START: usize = 11;
    const HEADER_SIZE: usize = 40;
    const DATA_OFFSET_POS: usize = HEADER_START + 16;

    if bytes.len() < HEADER_START + HEADER_SIZE {
        trace!("CARv2 header truncated");
        return CarHeaderParseResult::invalid();
    }

    // Read data_offset (little-endian u64)
    let data_offset_bytes: [u8; 8] = bytes[DATA_OFFSET_POS..DATA_OFFSET_POS + 8]
        .try_into()
        .unwrap();
    let data_offset_u64 = u64::from_le_bytes(data_offset_bytes);
    let data_offset = match usize::try_from(data_offset_u64) {
        Ok(offset) => offset,
        Err(_) => {
            trace!("CARv2 data_offset too large for platform: {data_offset_u64}");
            return CarHeaderParseResult::invalid();
        }
    };

    // Parse inner CARv1 at data_offset
    if bytes.len() <= data_offset {
        trace!("CARv2 data_offset beyond bytes");
        return CarHeaderParseResult::invalid();
    }

    let mut inner_result = parse_car_v1_header(&bytes[data_offset..]);
    if inner_result.is_valid {
        inner_result.version = Some(2);
        if let Some(size) = inner_result.header_size {
            inner_result.header_size = Some(data_offset + size);
        }
    }
    inner_result
}

/// Read unsigned LEB128 varint, return (value, bytes_consumed)
fn read_varint(bytes: &[u8]) -> Option<(usize, usize)> {
    let mut result: usize = 0;
    let mut shift = 0;

    for (i, &byte) in bytes.iter().enumerate() {
        if i >= 10 {
            // Varint too long (max 10 bytes for u64)
            return None;
        }

        result |= ((byte & 0x7F) as usize) << shift;

        if byte & 0x80 == 0 {
            return Some((result, i + 1));
        }

        shift += 7;
    }

    None // Incomplete varint
}

fn extract_version(map: &[(Value, Value)]) -> Option<u64> {
    map.iter()
        .find(|(k, _)| matches!(k, Value::Text(s) if s == "version"))
        .and_then(|(_, v)| match v {
            Value::Integer(i) => u64::try_from(*i).ok(),
            _ => None,
        })
}

fn extract_first_root_cid(map: &[(Value, Value)]) -> Option<String> {
    let roots = map
        .iter()
        .find(|(k, _)| matches!(k, Value::Text(s) if s == "roots"))
        .and_then(|(_, v)| v.as_array())?;

    let first_root = roots.first()?;

    // CID is stored as CBOR tag 42 with byte string
    // Tag 42 value contains: [0x00 multibase prefix][CID bytes]
    let cid_bytes = match first_root {
        Value::Tag(42, inner) => inner.as_bytes()?,
        _ => return None,
    };

    // Skip the 0x00 identity multibase prefix
    if cid_bytes.is_empty() || cid_bytes[0] != 0x00 {
        return None;
    }

    let raw_cid = &cid_bytes[1..];
    Some(encode_cid_base32(raw_cid))
}

/// Encode raw CID bytes to base32lower (multibase 'b' prefix)
fn encode_cid_base32(bytes: &[u8]) -> String {
    // RFC 4648 base32 lowercase alphabet
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";

    let mut result = String::with_capacity(1 + (bytes.len() * 8).div_ceil(5));
    result.push('b'); // multibase base32lower prefix

    let mut buffer: u64 = 0;
    let mut bits_in_buffer = 0;

    for &byte in bytes {
        buffer = (buffer << 8) | (byte as u64);
        bits_in_buffer += 8;

        while bits_in_buffer >= 5 {
            bits_in_buffer -= 5;
            let index = ((buffer >> bits_in_buffer) & 0x1F) as usize;
            result.push(ALPHABET[index] as char);
        }
    }

    // Handle remaining bits
    if bits_in_buffer > 0 {
        let index = ((buffer << (5 - bits_in_buffer)) & 0x1F) as usize;
        result.push(ALPHABET[index] as char);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_varint_single_byte() {
        let bytes = [0x39]; // 57
        assert_eq!(read_varint(&bytes), Some((57, 1)));
    }

    #[test]
    fn test_read_varint_multi_byte() {
        let bytes = [0x96, 0x01]; // 150
        assert_eq!(read_varint(&bytes), Some((150, 2)));
    }

    #[test]
    fn test_read_varint_incomplete() {
        let bytes = [0x80]; // High bit set, no continuation
        assert_eq!(read_varint(&bytes), None);
    }

    #[test]
    fn test_invalid_empty_bytes() {
        let result = parse_car_header(&[]);
        assert!(!result.is_valid);
    }

    #[test]
    fn test_invalid_garbage() {
        let garbage = b"<html>Not Found</html>";
        let result = parse_car_header(garbage);
        assert!(!result.is_valid);
    }

    #[test]
    fn test_carv2_pragma_detection() {
        // Just the pragma, not enough data
        let result = parse_car_header(CAR_V2_PRAGMA);
        assert!(!result.is_valid);
    }

    #[test]
    fn test_encode_cid_base32() {
        // CIDv1 raw bytes (simplified test)
        let bytes = [0x01, 0x55, 0x12, 0x20]; // version, codec, hash fn, length prefix
        let encoded = encode_cid_base32(&bytes);
        assert!(encoded.starts_with('b')); // multibase prefix
        assert!(
            encoded
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        );
    }
}
