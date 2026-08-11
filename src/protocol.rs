use bytes::{Bytes, BytesMut};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt};

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("incomplete frame")]
    ShortFrame,
    #[error("missing magic byte: got 0x{0:02x}")]
    MissingMagic(u8),
    #[error("short payload: {0}")]
    ShortPayload(String),
    #[error("extra {0} bytes after parsing fields")]
    ExtraBytes(usize),
    #[error("invalid UTF-8 in status string")]
    InvalidUtf8,
    #[error("invalid response: {0}")]
    InvalidResponse(String),
}

// Header is the decoded fixed part of the frame.
#[derive(Debug, Clone, Copy)]
pub struct Header {
    pub clr_id: u32,
    pub status_len: u8, // "SUCCESS" or "ERROR"
    pub field_cnt: u16, // number of fields that follow
}

pub async fn drain_frame_async(
    reader: &mut (impl AsyncRead + Unpin),
    buf: &mut BytesMut,
) -> Result<(Header, Bytes), ProtocolError> {
    // Read header (9 bytes)
    while buf.len() < 9 {
        if reader
            .read_buf(buf)
            .await
            .map_err(|_| ProtocolError::ShortFrame)?
            == 0
        {
            return Err(ProtocolError::ShortFrame);
        }
    }

    let header_bytes = &buf[..9];
    if header_bytes[0] != 0xFF {
        return Err(ProtocolError::MissingMagic(header_bytes[0]));
    }

    let clr_id = u32::from_le_bytes(header_bytes[1..5].try_into().unwrap());
    let payload_len = u32::from_le_bytes(header_bytes[5..9].try_into().unwrap()) as usize;

    // Read full payload
    while buf.len() < 9 + payload_len {
        if reader
            .read_buf(buf)
            .await
            .map_err(|_| ProtocolError::ShortFrame)?
            == 0
        {
            return Err(ProtocolError::ShortFrame);
        }
    }

    // Explicitly discard the header part — we don't need it
    _ = buf.split_to(9);

    // Take the payload
    let payload_mut = buf.split_to(payload_len);
    let payload = payload_mut.freeze();

    if payload.is_empty() {
        return Err(ProtocolError::ShortPayload("empty payload".into()));
    }

    let status_len = payload[0] as usize;
    if payload.len() < 1 + status_len + 2 {
        return Err(ProtocolError::ShortPayload("missing field count".into()));
    }

    let field_cnt = u16::from_le_bytes(
        payload[1 + status_len..1 + status_len + 2]
            .try_into()
            .unwrap(),
    );

    let hdr = Header {
        clr_id,
        status_len: payload[0],
        field_cnt,
    };

    Ok((hdr, payload))
}

pub fn prepend_header(clr_id: u32, payload: &[u8]) -> Vec<u8> {
    let total_len = payload.len() as u32;
    let mut out = Vec::with_capacity(9 + total_len as usize);
    out.push(0xFF);
    out.extend_from_slice(&clr_id.to_le_bytes());
    out.extend_from_slice(&total_len.to_le_bytes());
    out.extend_from_slice(payload);
    out
}

use crate::error::RZError;

const ROUTER_MAGIC: u8 = 0xFE;
const SHARD_MAGIC: u8 = 0xFF;

/// Split a complete router frame into (is_write, shard_frame).
/// `src` must contain one full router frame (magic … end of shardFrame).
pub fn split_router_frame(src: &[u8]) -> Result<(bool, Vec<u8>), RZError> {
    if src.len() < 7 || src[0] != ROUTER_MAGIC {
        return Err(RZError::ParseError("bad router magic or too short".into()));
    }

    let total_len = u32::from_le_bytes(src[1..5].try_into().unwrap()) as usize;
    // total_len covers everything after the 4-byte length field
    if src.len() < 5 + total_len {
        return Err(RZError::ParseError("incomplete router frame".into()));
    }

    let seg_len = src[5] as usize;
    let is_write_pos = 6 + seg_len;
    if is_write_pos >= 5 + total_len {
        return Err(RZError::ParseError("segment length out of range".into()));
    }

    let is_write = src[is_write_pos] == 0x01;
    let shard_start = is_write_pos + 1;
    let shard_end = 5 + total_len;

    if shard_start >= shard_end || src[shard_start] != SHARD_MAGIC {
        return Err(RZError::ParseError("missing or bad shard magic".into()));
    }

    Ok((is_write, src[shard_start..shard_end].to_vec()))
}

/// Try to decode one router frame from the front of `buf`.
/// Returns None if more bytes are needed.
/// On success returns (bytes_consumed, is_write, shard_frame).
pub fn try_decode_router(buf: &[u8]) -> Option<(usize, bool, Vec<u8>)> {
    if buf.len() < 5 {
        return None;
    }
    if buf[0] != ROUTER_MAGIC {
        // fatal for this connection — caller should close
        return None;
    }
    let total_len = u32::from_le_bytes(buf[1..5].try_into().ok()?) as usize;
    let frame_len = 5 + total_len;
    if buf.len() < frame_len {
        return None;
    }
    match split_router_frame(&buf[..frame_len]) {
        Ok((is_write, shard)) => Some((frame_len, is_write, shard)),
        Err(_) => None,
    }
}
