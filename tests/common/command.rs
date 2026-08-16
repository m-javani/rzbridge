use bytes::{Bytes, BytesMut};
use rzbridge::error::RZError;
use tokio::io::{AsyncRead, AsyncReadExt};
use uuid::Uuid;

// Constants
pub const ROUTER_MAGIC: u8 = 0xFE;
pub const SHARD_MAGIC: u8 = 0xFF;

#[derive(Debug, PartialEq)]
pub enum CommandResponse {
    GetPropRoomDay(GetPropRoomDayResponse),
}

// Response struct for GETPROPROOMDAY
#[derive(Debug, PartialEq)]
pub struct GetPropRoomDayResponse {
    pub property_id: String,
    pub date: String,
    pub availability: u8,
    pub final_price: u32,
    pub rate_feature_mask: u32,
}

// Command builder
pub fn build_get_prop_room_day(property_id: &str, room_type: &str, date: &str) -> Vec<u8> {
    let mut buf = Vec::new();

    let cmd_name = "GETPROPROOMDAY";
    buf.push(cmd_name.len() as u8);
    buf.extend_from_slice(cmd_name.as_bytes());

    // Field count: always 3
    buf.extend_from_slice(&3u16.to_le_bytes());

    // Field 1: property_id
    buf.extend_from_slice(&0x01u16.to_le_bytes());
    buf.push(0x01);
    buf.extend_from_slice(&(property_id.len() as u32).to_le_bytes());
    buf.extend_from_slice(property_id.as_bytes());

    // Field 2: room_type
    buf.extend_from_slice(&0x02u16.to_le_bytes());
    buf.push(0x01);
    buf.extend_from_slice(&(room_type.len() as u32).to_le_bytes());
    buf.extend_from_slice(room_type.as_bytes());

    // Field 3: date
    buf.extend_from_slice(&0x03u16.to_le_bytes());
    buf.push(0x01);
    buf.extend_from_slice(&(date.len() as u32).to_le_bytes());
    buf.extend_from_slice(date.as_bytes());

    buf
}

// Router header prepender
pub fn prepend_router_header(segment: &str, is_write: bool, clrid: u32, payload: &[u8]) -> Vec<u8> {
    let shard_total_len = payload.len() as u32;
    let shard_frame_len = 9 + shard_total_len; // magic(1) + clrid(4) + totalLen(4) + payload

    let segment_len = segment.len() as u32;
    let router_header_len = 1 + segment_len + 1; // segmentLen(1) + segment(n) + isWrite(1)

    let total_len = 1 + 4 + router_header_len + shard_frame_len;

    let mut out = Vec::with_capacity(total_len as usize);

    // Router magic byte
    out.push(ROUTER_MAGIC);

    // Total length (everything after this field)
    out.extend_from_slice(&((router_header_len + shard_frame_len) as u32).to_le_bytes());

    // Segment length
    out.push(segment_len as u8);

    // Segment
    out.extend_from_slice(segment.as_bytes());

    // IsWrite flag
    out.push(if is_write { 0x01 } else { 0x00 });

    // Shard frame (magic, clrid, totalLen, payload)
    out.push(SHARD_MAGIC);
    out.extend_from_slice(&clrid.to_le_bytes());
    out.extend_from_slice(&shard_total_len.to_le_bytes());
    out.extend_from_slice(payload);

    out
}

// Frame drainer
pub async fn drain_frame_async(
    reader: &mut (impl AsyncRead + Unpin),
    buf: &mut BytesMut,
) -> Result<(u32, Bytes), RZError> {
    // Read header (9 bytes)
    while buf.len() < 9 {
        if reader
            .read_buf(buf)
            .await
            .map_err(|_| RZError::System("Short frame".into()))?
            == 0
        {
            return Err(RZError::System("Short frame".into()));
        }
    }

    let header_bytes = &buf[..9];
    if header_bytes[0] != SHARD_MAGIC {
        return Err(RZError::System(format!(
            "Missing magic: {}",
            header_bytes[0]
        )));
    }

    let clr_id = u32::from_le_bytes(header_bytes[1..5].try_into().unwrap());
    let payload_len = u32::from_le_bytes(header_bytes[5..9].try_into().unwrap()) as usize;

    // Read full payload
    while buf.len() < 9 + payload_len {
        if reader
            .read_buf(buf)
            .await
            .map_err(|_| RZError::System("Short frame".into()))?
            == 0
        {
            return Err(RZError::System("Short frame".into()));
        }
    }

    // Discard the header
    _ = buf.split_to(9);

    // Take the payload
    let payload_mut = buf.split_to(payload_len);
    let payload = payload_mut.freeze();

    if payload.is_empty() {
        return Err(RZError::System("Empty payload".into()));
    }

    Ok((clr_id, payload))
}

// Decoder for GETPROPROOMDAY response
pub fn decode_get_prop_room_day_response(
    payload: &Bytes,
) -> Result<GetPropRoomDayResponse, RZError> {
    let data = payload.as_ref();

    if data.is_empty() {
        return Err(RZError::System("Empty response".into()));
    }

    let status_len = data[0] as usize;
    let min_len = 1 + status_len + 2;
    if data.len() < min_len {
        return Err(RZError::System("Response too short".into()));
    }

    let status = &data[1..1 + status_len];
    let field_count = u16::from_le_bytes([data[1 + status_len], data[1 + status_len + 1]]);

    let mut offset = 1 + status_len + 2;

    if status != b"SUCCESS" {
        return Err(RZError::System(format!(
            "Non-success status: {}",
            String::from_utf8_lossy(status)
        )));
    }

    if field_count != 5 {
        return Err(RZError::System(format!(
            "Expected 5 fields, got {}",
            field_count
        )));
    }

    // Helper to read a field
    macro_rules! read_field {
        () => {{
            if offset + 7 > data.len() {
                return Err(RZError::System("Field header too short".into()));
            }
            let id = u16::from_le_bytes([data[offset], data[offset + 1]]);
            let typ = data[offset + 2];
            let len = u32::from_le_bytes([
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
            ]) as usize;
            offset += 7;
            if offset + len > data.len() {
                return Err(RZError::System("Field data too short".into()));
            }
            let slice = &data[offset..offset + len];
            offset += len;
            (id, typ, slice)
        }};
    }

    // Field 1: property_id (string)
    let (id1, typ1, property_id_bytes) = read_field!();
    if id1 != 1 || typ1 != 0x01 {
        return Err(RZError::System("Invalid property_id field".into()));
    }

    let property_id = bytes_to_property_id(property_id_bytes);

    // Field 2: date (string)
    let (id2, typ2, date_bytes) = read_field!();
    if id2 != 2 || typ2 != 0x01 {
        return Err(RZError::System("Invalid date field".into()));
    }
    let date = String::from_utf8(date_bytes.to_vec())
        .map_err(|_| RZError::System("Invalid UTF-8 in date".into()))?;

    // Field 3: availability (u8)
    let (id3, typ3, avail_bytes) = read_field!();
    if id3 != 3 || typ3 != 0x02 || avail_bytes.len() != 1 {
        return Err(RZError::System("Invalid availability field".into()));
    }
    let availability = avail_bytes[0];

    // Field 4: final_price (u32)
    let (id4, typ4, price_bytes) = read_field!();
    if id4 != 4 || typ4 != 0x03 || price_bytes.len() != 4 {
        return Err(RZError::System("Invalid final_price field".into()));
    }
    let final_price = u32::from_le_bytes([
        price_bytes[0],
        price_bytes[1],
        price_bytes[2],
        price_bytes[3],
    ]);

    // Field 5: rate_feature_mask (u32)
    let (id5, typ5, rate_bytes) = read_field!();
    if id5 != 5 || typ5 != 0x03 || rate_bytes.len() != 4 {
        return Err(RZError::System("Invalid rate_feature_mask field".into()));
    }
    let rate_feature_mask =
        u32::from_le_bytes([rate_bytes[0], rate_bytes[1], rate_bytes[2], rate_bytes[3]]);

    if offset != data.len() {
        return Err(RZError::System("Trailing data after fields".into()));
    }

    Ok(GetPropRoomDayResponse {
        property_id,
        date,
        availability,
        final_price,
        rate_feature_mask,
    })
}

/// Converts raw property ID bytes into String
pub fn bytes_to_property_id(data: &[u8]) -> String {
    // Case 1: too short
    if data.len() < 7 {
        return String::new();
    }

    // Case 2: short string marker (0xF0 in byte 6)
    if data[6] == 0xF0 {
        let mut left_len = 0;
        for &b in &data[..6] {
            if b == 0 {
                break;
            }
            left_len += 1;
        }

        let mut right_len = 0;
        for &b in &data[7..] {
            if b == 0 {
                break;
            }
            right_len += 1;
        }

        let mut result = Vec::with_capacity(left_len + right_len);
        result.extend_from_slice(&data[..left_len]);
        result.extend_from_slice(&data[7..7 + right_len]);
        return String::from_utf8_lossy(&result).to_string();
    }

    // Case 3: UUID detection (valid version in high nibble of byte 6)
    let version = (data[6] & 0xF0) >> 4;
    if matches!(version, 1 | 2 | 3 | 4 | 5 | 7) {
        let mut uuid_bytes = [0u8; 16];
        let copy_len = data.len().min(16);
        uuid_bytes[..copy_len].copy_from_slice(&data[..copy_len]);

        let u = Uuid::from_bytes(uuid_bytes);
        return u.to_string();
    }

    // Fallback — should never happen with valid server data
    String::new()
}

// Main command serializer (extendable)
pub fn get_serialized_command(cmd: &str) -> Vec<u8> {
    match cmd {
        "GETPROPROOMDAY" => {
            // Hardcoded test values
            let property_id = "s1_seg1_p1";
            let room_type = "room1";
            let date = chrono::Utc::now()
                .date_naive()
                .checked_add_days(chrono::Days::new(1))
                .unwrap()
                .format("%Y-%m-%d")
                .to_string();

            let payload = build_get_prop_room_day(property_id, room_type, &date);
            prepend_router_header("segment_1", false, 0, &payload)
        }
        _ => {
            // Fallback for other commands
            format!("CMD:{}", cmd).into_bytes()
        }
    }
}

// Remove the manual frame parsing from process_response since client handles it
pub fn process_response(cmd: &str, payload: &[u8]) -> Result<CommandResponse, RZError> {
    match cmd {
        "GETPROPROOMDAY" => {
            let bytes = Bytes::from(payload.to_vec());
            let res = decode_get_prop_room_day_response(&bytes)?;
            Ok(CommandResponse::GetPropRoomDay(res))
        }
        _ => Err(RZError::System(format!("Unknown command: {}", cmd))),
    }
}
