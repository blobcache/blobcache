use crate::types::{Error, Message};
use std::io::{Read, Write};

pub const HEADER_LEN: usize = 8;
pub type MessageCode = u16;
const SECTION_SIZE: u16 = 256;

pub const MT_ENDPOINT: MessageCode = 2;
pub const MT_INSPECT: MessageCode = 3;

pub const MT_HANDLE_INSPECT: MessageCode = (1 * SECTION_SIZE) + 0;
pub const MT_HANDLE_DROP: MessageCode = (1 * SECTION_SIZE) + 1;
pub const MT_HANDLE_KEEP_ALIVE: MessageCode = (1 * SECTION_SIZE) + 2;
pub const MT_HANDLE_SHARE_OUT: MessageCode = (1 * SECTION_SIZE) + 3;
pub const MT_HANDLE_SHARE_IN: MessageCode = (1 * SECTION_SIZE) + 4;

pub const MT_VOLUME_INSPECT: MessageCode = (2 * SECTION_SIZE) + 0;
pub const MT_VOLUME_BEGIN_TX: MessageCode = (2 * SECTION_SIZE) + 1;
pub const MT_OPEN_FIAT: MessageCode = 4;
pub const MT_OPEN_FROM: MessageCode = (2 * SECTION_SIZE) + 2;
pub const MT_CREATE_VOLUME: MessageCode = (3 * SECTION_SIZE) - 1;

pub const MT_TX_INSPECT: MessageCode = (3 * SECTION_SIZE) + 0;
pub const MT_TX_ABORT: MessageCode = (3 * SECTION_SIZE) + 1;
pub const MT_TX_COMMIT: MessageCode = (3 * SECTION_SIZE) + 2;
pub const MT_TX_LOAD: MessageCode = (3 * SECTION_SIZE) + 3;
pub const MT_TX_SAVE: MessageCode = (3 * SECTION_SIZE) + 4;
pub const MT_TX_POST: MessageCode = (3 * SECTION_SIZE) + 5;
pub const MT_TX_POST_SALT: MessageCode = (3 * SECTION_SIZE) + 6;
pub const MT_TX_GET: MessageCode = (3 * SECTION_SIZE) + 7;
pub const MT_TX_GET_SALT: MessageCode = (3 * SECTION_SIZE) + 8;
pub const MT_TX_EXISTS: MessageCode = (3 * SECTION_SIZE) + 9;
pub const MT_TX_DELETE: MessageCode = (3 * SECTION_SIZE) + 10;
pub const MT_TX_COPY: MessageCode = (3 * SECTION_SIZE) + 11;
pub const MT_TX_LINK: MessageCode = (3 * SECTION_SIZE) + 12;
pub const MT_TX_UNLINK: MessageCode = (3 * SECTION_SIZE) + 13;
pub const MT_TX_VISIT: MessageCode = (3 * SECTION_SIZE) + 14;
pub const MT_TX_IS_VISITED: MessageCode = (3 * SECTION_SIZE) + 15;
pub const MT_TX_VISIT_LINKS: MessageCode = (3 * SECTION_SIZE) + 16;

pub const MT_QUEUE_INSPECT: MessageCode = (4 * SECTION_SIZE) + 0;
pub const MT_QUEUE_ENQUEUE: MessageCode = (4 * SECTION_SIZE) + 1;
pub const MT_QUEUE_DEQUEUE: MessageCode = (4 * SECTION_SIZE) + 2;
pub const MT_QUEUE_SUB_TO_VOLUME: MessageCode = (4 * SECTION_SIZE) + 3;
pub const MT_QUEUE_CREATE: MessageCode = (5 * SECTION_SIZE) - 1;

pub const MT_OK: MessageCode = (255 * SECTION_SIZE) + 0;
pub const MT_ERROR_TIMEOUT: MessageCode = (255 * SECTION_SIZE) + 1;
pub const MT_ERROR_INVALID_HANDLE: MessageCode = (255 * SECTION_SIZE) + 2;
pub const MT_ERROR_NOT_FOUND: MessageCode = (255 * SECTION_SIZE) + 3;
pub const MT_ERROR_NO_PERMISSION: MessageCode = (255 * SECTION_SIZE) + 4;
pub const MT_ERROR_NO_LINK: MessageCode = (255 * SECTION_SIZE) + 5;
pub const MT_ERROR_TOO_LARGE: MessageCode = (255 * SECTION_SIZE) + 6;
pub const MT_ERROR_UNKNOWN: MessageCode = u16::MAX;

pub fn write_message<W: Write>(mut w: W, code: MessageCode, body: &[u8]) -> Result<(), Error> {
    let mut header = [0u8; HEADER_LEN];
    header[0] = (code >> 8) as u8;
    header[1] = (code & 0xff) as u8;
    let body_len = (body.len() as u32).to_le_bytes();
    header[4..8].copy_from_slice(&body_len);
    w.write_all(&header)?;
    w.write_all(body)?;
    Ok(())
}

pub fn read_message<R: Read>(mut r: R) -> Result<(MessageCode, Vec<u8>), Error> {
    let mut header = [0u8; HEADER_LEN];
    r.read_exact(&mut header)?;
    let code = u16::from_be_bytes([header[0], header[1]]);
    let body_len = u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;
    let mut body = vec![0u8; body_len];
    r.read_exact(&mut body)?;
    Ok((code, body))
}

pub fn check_ok(code: MessageCode, body: &[u8]) -> Result<(), Error> {
    if code == MT_OK {
        return Ok(());
    }
    if code > MT_OK {
        let msg = String::from_utf8_lossy(body).to_string();
        return Err(match code {
            MT_ERROR_TIMEOUT => Error::WireError { code, message: msg },
            MT_ERROR_INVALID_HANDLE => Error::WireInvalidHandle(msg),
            MT_ERROR_NOT_FOUND => Error::WireNotFound(msg),
            MT_ERROR_NO_PERMISSION => Error::WireNoPermission(msg),
            MT_ERROR_NO_LINK => Error::WireNoLink(msg),
            MT_ERROR_TOO_LARGE => Error::WireTooLarge(msg),
            MT_ERROR_UNKNOWN => Error::WireUnknown(msg),
            _ => Error::WireError { code, message: msg },
        });
    }
    Err(Error::WireError {
        code,
        message: "unexpected non-OK response code".to_string(),
    })
}

pub fn write_uvarint(out: &mut Vec<u8>, mut x: u64) {
    while x >= 0x80 {
        out.push((x as u8) | 0x80);
        x >>= 7;
    }
    out.push(x as u8);
}

pub fn read_uvarint(data: &[u8]) -> Result<(u64, &[u8]), Error> {
    let mut x = 0u64;
    let mut s = 0u32;
    for (i, b) in data.iter().enumerate() {
        if *b < 0x80 {
            if i > 9 || (i == 9 && *b > 1) {
                return Err(Error::InvalidMessage("uvarint overflow".to_string()));
            }
            return Ok((x | ((*b as u64) << s), &data[i + 1..]));
        }
        x |= ((*b & 0x7f) as u64) << s;
        s += 7;
    }
    Err(Error::InvalidMessage("truncated uvarint".to_string()))
}

pub fn append_lp(out: &mut Vec<u8>, data: &[u8]) {
    write_uvarint(out, data.len() as u64);
    out.extend_from_slice(data);
}

pub fn read_lp(data: &[u8]) -> Result<(&[u8], &[u8]), Error> {
    let (len, rest) = read_uvarint(data)?;
    let len = len as usize;
    if rest.len() < len {
        return Err(Error::InvalidMessage("truncated lp payload".to_string()));
    }
    Ok((&rest[..len], &rest[len..]))
}

pub fn decode_bools(data: &[u8], n: usize) -> Vec<bool> {
    let mut out = Vec::with_capacity(n);
    for byte in data {
        for bit in 0..8 {
            if out.len() == n {
                return out;
            }
            out.push((byte & (1 << bit)) != 0);
        }
    }
    out.resize(n, false);
    out
}

pub fn encode_message(msg: &Message) -> Vec<u8> {
    let mut out = Vec::new();
    msg.marshal(&mut out);
    out
}
