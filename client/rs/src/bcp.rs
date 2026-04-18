use crate::types::{Error, Message};
use std::io::{Read, Write};

pub const HEADER_LEN: usize = 8;

pub const MT_ENDPOINT: u8 = 2;

pub const MT_HANDLE_INSPECT: u8 = 16;
pub const MT_HANDLE_DROP: u8 = 17;
pub const MT_HANDLE_KEEP_ALIVE: u8 = 18;
pub const MT_HANDLE_SHARE: u8 = 19;
pub const MT_HANDLE_ADOPT: u8 = 20;
pub const MT_HANDLE_INSPECT_OBJECT: u8 = 21;

pub const MT_OPEN_FIAT: u8 = 32;
pub const MT_OPEN_FROM: u8 = 33;
pub const MT_VOLUME_INSPECT: u8 = 34;
pub const MT_VOLUME_BEGIN_TX: u8 = 36;
pub const MT_VOLUME_CLONE: u8 = 37;
pub const MT_CREATE_VOLUME: u8 = 47;

pub const MT_TX_INSPECT: u8 = 48;
pub const MT_TX_COMMIT: u8 = 49;
pub const MT_TX_ABORT: u8 = 50;
pub const MT_TX_LOAD: u8 = 51;
pub const MT_TX_SAVE: u8 = 52;
pub const MT_TX_POST: u8 = 53;
pub const MT_TX_POST_SALT: u8 = 54;
pub const MT_TX_GET: u8 = 55;
pub const MT_TX_EXISTS: u8 = 56;
pub const MT_TX_DELETE: u8 = 57;
pub const MT_TX_ADD_FROM: u8 = 58;
pub const MT_TX_VISIT: u8 = 59;
pub const MT_TX_IS_VISITED: u8 = 60;
pub const MT_TX_LINK: u8 = 61;
pub const MT_TX_UNLINK: u8 = 62;
pub const MT_TX_VISIT_LINKS: u8 = 63;

pub const MT_QUEUE_INSPECT: u8 = 80;
pub const MT_QUEUE_ENQUEUE: u8 = 81;
pub const MT_QUEUE_DEQUEUE: u8 = 82;
pub const MT_QUEUE_SUB_TO_VOLUME: u8 = 83;
pub const MT_QUEUE_CREATE: u8 = 96;

pub const MT_OK: u8 = 128;
pub const MT_ERROR_INVALID_HANDLE: u8 = 130;
pub const MT_ERROR_NOT_FOUND: u8 = 131;
pub const MT_ERROR_NO_PERMISSION: u8 = 132;
pub const MT_ERROR_NO_LINK: u8 = 133;
pub const MT_ERROR_TOO_LARGE: u8 = 134;
pub const MT_ERROR_UNKNOWN: u8 = 255;

pub fn write_message<W: Write>(mut w: W, code: u8, body: &[u8]) -> Result<(), Error> {
    let mut header = [0u8; HEADER_LEN];
    header[0] = code;
    let body_len = (body.len() as u32).to_le_bytes();
    header[4..8].copy_from_slice(&body_len);
    w.write_all(&header)?;
    w.write_all(body)?;
    Ok(())
}

pub fn read_message<R: Read>(mut r: R) -> Result<(u8, Vec<u8>), Error> {
    let mut header = [0u8; HEADER_LEN];
    r.read_exact(&mut header)?;
    let body_len = u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;
    let mut body = vec![0u8; body_len];
    r.read_exact(&mut body)?;
    Ok((header[0], body))
}

pub fn check_ok(code: u8, body: &[u8]) -> Result<(), Error> {
    if code == MT_OK {
        return Ok(());
    }
    if code > MT_OK {
        let msg = String::from_utf8_lossy(body).to_string();
        return Err(match code {
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
