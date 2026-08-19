// // SPDX-License-Identifier: BUSL-1.1
// // Copyright (c) 2026 M. Javani
// //
// // This file is part of rzbridge.
// //
// // Use of this software is governed by the Business Source License 1.1
// // included in the LICENSE file in the root of this repository.

use crate::error::RZError;
use bytes::{Bytes, BytesMut};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt};

pub const ROUTER_MAGIC: u8 = 0xFE;
pub const SHARD_MAGIC: u8 = 0xFF;
pub const KEEPALIVE_SEGMENT: &str = "__keepalive__";

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

    // Take the full frame (header + payload) without splitting
    let full_frame = buf.split_to(9 + payload_len).freeze();

    // Payload starts at offset 9
    let payload = full_frame.slice(9..);

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

    Ok((hdr, full_frame))
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

/// Result of trying to decode one router frame.
pub enum DecodeResult {
    /// Need more bytes
    NeedMore,
    /// Successfully decoded a full frame
    Frame {
        frame_len: usize,
        segment: String,
        is_write: bool,
        /// The raw shard frame (starts with 0xFF + clrid + ...)
        shard_frame: Vec<u8>,
        /// Offset of the 4-byte clrid inside the original buffer (for error replies)
        clrid_offset: usize,
        original_clrid: u32,
    },
    /// Structural error → caller should try to resync
    Error(RZError),
}

/// Try to decode one complete router frame at the front of `buf`.
pub fn try_decode_router(buf: &[u8]) -> DecodeResult {
    if buf.len() < 5 {
        return DecodeResult::NeedMore;
    }

    if buf[0] != ROUTER_MAGIC {
        return DecodeResult::Error(RZError::ParseError("bad router magic".into()));
    }

    let total_len = match buf[1..5].try_into() {
        Ok(b) => u32::from_le_bytes(b) as usize,
        Err(_) => return DecodeResult::Error(RZError::ParseError("bad length".into())),
    };

    // Reject absurd sizes early
    if total_len > 16 * 1024 * 1024 {
        return DecodeResult::Error(RZError::ParseError("frame too large".into()));
    }

    let frame_len = 5 + total_len;
    if buf.len() < frame_len {
        return DecodeResult::NeedMore;
    }

    if total_len < 2 {
        return DecodeResult::Error(RZError::ParseError("router total_len too small".into()));
    }

    let seg_len = buf[5] as usize;
    let seg_start = 6;
    let is_write_pos = seg_start + seg_len;

    if is_write_pos >= frame_len {
        return DecodeResult::Error(RZError::ParseError("segment length out of range".into()));
    }

    let segment = match std::str::from_utf8(&buf[seg_start..seg_start + seg_len]) {
        Ok(s) => s.to_string(),
        Err(_) => return DecodeResult::Error(RZError::ParseError("segment not utf-8".into())),
    };

    let is_write = buf[is_write_pos] == 0x01;
    let shard_start = is_write_pos + 1;

    if shard_start + 5 > frame_len || buf[shard_start] != SHARD_MAGIC {
        return DecodeResult::Error(RZError::ParseError("missing shard magic".into()));
    }

    let original_clrid =
        u32::from_le_bytes(buf[shard_start + 1..shard_start + 5].try_into().unwrap());
    let clrid_offset = shard_start + 1;

    let shard_frame = buf[shard_start..frame_len].to_vec();

    DecodeResult::Frame {
        frame_len,
        segment,
        is_write,
        shard_frame,
        clrid_offset,
        original_clrid,
    }
}

/// Find next ROUTER_MAGIC for resync
pub fn find_next_router_magic(buf: &[u8], from: usize) -> Option<usize> {
    buf[from..]
        .iter()
        .position(|&b| b == ROUTER_MAGIC)
        .map(|p| from + p)
}

/// Simple error response (same format the routers understand)
pub fn build_error_response(clrid: u32, error_code: u8) -> Vec<u8> {
    // Re-use the same constant error format you already have in the router
    // (magic + clrid + payload_len + "ERROR" block + message)
    // For brevity I keep a minimal version here – you can copy the full one from the router.

    let msg = match error_code {
        1 => b"404",
        2 => b"503",
        3 => b"408",
        _ => b"500",
    };

    // Very small ERROR payload (you can replace with the full version)
    let mut payload = Vec::new();
    payload.push(5); // "ERROR"
    payload.extend_from_slice(b"ERROR");
    payload.extend_from_slice(&1u16.to_le_bytes()); // 1 field
    payload.extend_from_slice(&1u16.to_le_bytes()); // field id
    payload.push(1); // type
    payload.extend_from_slice(&4u32.to_le_bytes()); // len
    payload.extend_from_slice(&(msg.len() as u32).to_le_bytes());
    payload.extend_from_slice(msg);

    let mut buf = Vec::with_capacity(9 + payload.len());
    buf.push(0xFF);
    buf.extend_from_slice(&clrid.to_le_bytes());
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(&payload);
    buf
}
