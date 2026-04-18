use base64::alphabet;
use base64::engine::general_purpose::{GeneralPurpose, GeneralPurposeConfig};
use base64::Engine;
use serde::de::{self, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::io;
use std::str::FromStr;

pub const OID_SIZE: usize = 16;
pub const CID_SIZE: usize = 32;
pub const HANDLE_SIZE: usize = OID_SIZE + 16;
pub const LINK_TOKEN_SIZE: usize = 16 + 8 + 24;
pub const PEER_ID_SIZE: usize = 32;

pub type ActionSet = u64;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unsupported endpoint: {0}")]
    UnsupportedEndpoint(String),
    #[error("invalid endpoint: {0}")]
    InvalidEndpoint(String),
    #[error("invalid oid: {0}")]
    InvalidOid(String),
    #[error("invalid cid: {0}")]
    InvalidCid(String),
    #[error("invalid peer id: {0}")]
    InvalidPeerId(String),
    #[error("invalid handle: {0}")]
    InvalidHandle(String),
    #[error("invalid link token: {0}")]
    InvalidLinkToken(String),
    #[error("invalid message: {0}")]
    InvalidMessage(String),
    #[error("wire error code {code}: {message}")]
    WireError { code: u8, message: String },
    #[error("wire error: invalid handle: {0}")]
    WireInvalidHandle(String),
    #[error("wire error: not found: {0}")]
    WireNotFound(String),
    #[error("wire error: no permission: {0}")]
    WireNoPermission(String),
    #[error("wire error: no link: {0}")]
    WireNoLink(String),
    #[error("wire error: too large: {0}")]
    WireTooLarge(String),
    #[error("wire error: unknown: {0}")]
    WireUnknown(String),
    #[error("request failed with status {status}: {body}")]
    HttpStatus { status: u16, body: String },
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("request error: {0}")]
    Request(#[from] reqwest::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("hex decode error: {0}")]
    Hex(#[from] hex::FromHexError),
    #[error("base64 decode error: {0}")]
    Base64(#[from] base64::DecodeError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OID(pub [u8; OID_SIZE]);

impl OID {
    pub fn from_bytes(data: &[u8]) -> Result<Self, Error> {
        if data.len() != OID_SIZE {
            return Err(Error::InvalidOid(format!("wrong length {}", data.len())));
        }
        let mut out = [0u8; OID_SIZE];
        out.copy_from_slice(data);
        Ok(Self(out))
    }

    pub fn as_bytes(&self) -> &[u8; OID_SIZE] {
        &self.0
    }
}

impl fmt::Display for OID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = hex::encode_upper(self.0);
        s.insert(8, '_');
        s.insert(25, '_');
        write!(f, "{s}")
    }
}

impl FromStr for OID {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let compact = s.replace('_', "");
        if compact.len() != OID_SIZE * 2 {
            return Err(Error::InvalidOid(s.to_string()));
        }
        let bytes = hex::decode(compact)?;
        Self::from_bytes(&bytes)
    }
}

impl Serialize for OID {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for OID {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        OID::from_str(&s).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CID(pub [u8; CID_SIZE]);

impl CID {
    pub fn from_bytes(data: &[u8]) -> Result<Self, Error> {
        if data.len() != CID_SIZE {
            return Err(Error::InvalidCid(format!("wrong length {}", data.len())));
        }
        let mut out = [0u8; CID_SIZE];
        out.copy_from_slice(data);
        Ok(Self(out))
    }

    pub fn as_bytes(&self) -> &[u8; CID_SIZE] {
        &self.0
    }
}

fn cid_engine() -> GeneralPurpose {
    let alpha =
        alphabet::Alphabet::new("-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz")
            .expect("valid cid alphabet");
    let cfg = GeneralPurposeConfig::new().with_encode_padding(false);
    GeneralPurpose::new(&alpha, cfg)
}

impl fmt::Display for CID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let enc = cid_engine();
        write!(f, "{}", enc.encode(self.0))
    }
}

impl FromStr for CID {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let enc = cid_engine();
        let decoded = enc.decode(s.as_bytes())?;
        if decoded.len() != CID_SIZE {
            return Err(Error::InvalidCid(s.to_string()));
        }
        let mut out = [0u8; CID_SIZE];
        out.copy_from_slice(&decoded);
        Ok(Self(out))
    }
}

impl Serialize for CID {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for CID {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        CID::from_str(&s).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PeerID(pub [u8; PEER_ID_SIZE]);

impl fmt::Display for PeerID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let url = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        write!(f, "{}", url.encode(self.0))
    }
}

impl FromStr for PeerID {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut out = [0u8; PEER_ID_SIZE];
        let mut decoded = None;
        for engine in [
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            &base64::engine::general_purpose::STANDARD_NO_PAD,
        ] {
            if let Ok(n) = engine.decode_slice(s.as_bytes(), &mut out) {
                decoded = Some(n);
                break;
            }
        }
        match decoded {
            Some(n) if n == PEER_ID_SIZE => Ok(Self(out)),
            _ => Err(Error::InvalidPeerId(s.to_string())),
        }
    }
}

impl Serialize for PeerID {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for PeerID {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        PeerID::from_str(&s).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Handle {
    pub oid: OID,
    pub secret: [u8; 16],
}

impl Handle {
    pub fn marshal(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.oid.as_bytes());
        out.extend_from_slice(&self.secret);
    }

    pub fn unmarshal(data: &[u8]) -> Result<Self, Error> {
        if data.len() < HANDLE_SIZE {
            return Err(Error::InvalidHandle(format!("wrong length {}", data.len())));
        }
        let oid = OID::from_bytes(&data[..OID_SIZE])?;
        let mut secret = [0u8; 16];
        secret.copy_from_slice(&data[OID_SIZE..HANDLE_SIZE]);
        Ok(Self { oid, secret })
    }
}

impl fmt::Display for Handle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.oid, hex::encode(self.secret))
    }
}

impl FromStr for Handle {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (oid_part, sec_part) = s
            .split_once('.')
            .ok_or_else(|| Error::InvalidHandle(s.to_string()))?;
        let oid = OID::from_str(oid_part)?;
        let sec = hex::decode(sec_part)?;
        if sec.len() != 16 {
            return Err(Error::InvalidHandle(s.to_string()));
        }
        let mut secret = [0u8; 16];
        secret.copy_from_slice(&sec);
        Ok(Self { oid, secret })
    }
}

impl Serialize for Handle {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Handle {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Handle::from_str(&s).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandleInfo {
    pub oid: OID,
    pub rights: ActionSet,
    pub created_at: Value,
    pub expires_at: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkToken {
    pub target: OID,
    pub rights: ActionSet,
    #[serde(with = "hex_24")]
    pub secret: [u8; 24],
}

impl LinkToken {
    pub fn marshal(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.target.as_bytes());
        out.extend_from_slice(&self.rights.to_le_bytes());
        out.extend_from_slice(&self.secret);
    }

    pub fn unmarshal(data: &[u8]) -> Result<Self, Error> {
        if data.len() < LINK_TOKEN_SIZE {
            return Err(Error::InvalidLinkToken(format!(
                "wrong length {}",
                data.len()
            )));
        }
        let target = OID::from_bytes(&data[..16])?;
        let mut rights_bytes = [0u8; 8];
        rights_bytes.copy_from_slice(&data[16..24]);
        let rights = u64::from_le_bytes(rights_bytes);
        let mut secret = [0u8; 24];
        secret.copy_from_slice(&data[24..48]);
        Ok(Self {
            target,
            rights,
            secret,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Info {
    pub handle: HandleInfo,
    pub volume: Option<VolumeInfo>,
    pub tx: Option<TxInfo>,
    pub queue: Option<QueueInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Endpoint {
    pub peer: PeerID,
    pub ip_port: String,
}

impl Endpoint {
    pub fn from_bcp_bytes(data: &[u8]) -> Result<Self, Error> {
        if data.len() < PEER_ID_SIZE + 18 {
            return Err(Error::InvalidEndpoint(format!("too short {}", data.len())));
        }
        let mut peer = [0u8; PEER_ID_SIZE];
        peer.copy_from_slice(&data[..PEER_ID_SIZE]);
        let ap = &data[PEER_ID_SIZE..PEER_ID_SIZE + 18];
        let ip_port = if ap[6..].iter().all(|b| *b == 0) {
            let port = u16::from_le_bytes([ap[4], ap[5]]);
            format!("{}.{}.{}.{}:{port}", ap[0], ap[1], ap[2], ap[3])
        } else {
            let mut octets = [0u8; 16];
            octets.copy_from_slice(&ap[..16]);
            let port = u16::from_le_bytes([ap[16], ap[17]]);
            let ip = std::net::Ipv6Addr::from(octets);
            format!("[{ip}]:{port}")
        };
        Ok(Self {
            peer: PeerID(peer),
            ip_port,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxParams {
    #[serde(rename = "Modify")]
    pub modify: bool,
    #[serde(rename = "GCBlobs")]
    pub gc_blobs: bool,
    #[serde(rename = "GCLinks")]
    pub gc_links: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxInfo {
    #[serde(rename = "ID")]
    pub id: OID,
    #[serde(rename = "Volume")]
    pub volume: OID,
    #[serde(rename = "MaxSize")]
    pub max_size: i64,
    #[serde(rename = "HashAlgo")]
    pub hash_algo: HashAlgo,
    #[serde(rename = "Params")]
    pub params: TxParams,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostOpts {
    pub salt: Option<CID>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetOpts {
    pub salt: Option<CID>,
    pub skip_verify: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashAlgo(pub String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaSpec {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeConfig {
    pub schema: SchemaSpec,
    pub hash_algo: HashAlgo,
    pub max_size: i64,
    pub salted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeBackendLocal {
    pub schema: SchemaSpec,
    pub hash_algo: HashAlgo,
    pub max_size: i64,
    pub salted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeBackendRemote {
    pub endpoint: Endpoint,
    pub volume: OID,
    pub hash_algo: HashAlgo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeBackendPeer {
    pub peer: PeerID,
    pub volume: OID,
    pub hash_algo: HashAlgo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeBackendGit {
    pub url: String,
    #[serde(flatten)]
    pub config: VolumeConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeBackendVault<T> {
    pub x: T,
    #[serde(with = "hex_32")]
    pub secret: [u8; 32],
    pub hash_algo: HashAlgo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeBackendConsensus {
    pub schema: SchemaSpec,
    pub hash_algo: HashAlgo,
    pub max_size: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeBackend<T> {
    pub local: Option<VolumeBackendLocal>,
    pub remote: Option<VolumeBackendRemote>,
    pub peer: Option<VolumeBackendPeer>,
    pub git: Option<VolumeBackendGit>,
    pub vault: Option<VolumeBackendVault<T>>,
    pub consensus: Option<VolumeBackendConsensus>,
}

pub type VolumeSpec = VolumeBackend<Handle>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeInfo {
    pub id: OID,
    pub schema: SchemaSpec,
    pub hash_algo: HashAlgo,
    pub max_size: i64,
    pub salted: bool,
    pub backend: VolumeBackend<OID>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueBackendMemory {
    pub max_depth: u32,
    pub evict_oldest: bool,
    pub max_bytes_per_message: u32,
    pub max_handles_per_message: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueBackendRemote {
    pub endpoint: Endpoint,
    pub oid: OID,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueBackend<T> {
    pub memory: Option<QueueBackendMemory>,
    pub remote: Option<QueueBackendRemote>,
    #[serde(skip)]
    pub _marker: std::marker::PhantomData<T>,
}

impl<T> Default for QueueBackend<T> {
    fn default() -> Self {
        Self {
            memory: None,
            remote: None,
            _marker: std::marker::PhantomData,
        }
    }
}

pub type QueueSpec = QueueBackend<Handle>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfig {
    pub max_depth: u32,
    pub max_bytes_per_message: u32,
    pub max_handles_per_message: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueInfo {
    pub id: OID,
    pub config: QueueConfig,
    pub backend: QueueBackend<OID>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DequeueOpts {
    pub min: u32,
    pub leave_in: bool,
    pub skip: u32,
    pub max_wait: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolSubSpec {
    pub begin_tx: Option<TxParams>,
    pub send_cell: bool,
    pub send_blobs: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub handles: Vec<Handle>,
    pub bytes: Vec<u8>,
}

impl Message {
    pub fn marshal(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&(self.handles.len() as u32).to_le_bytes());
        for h in &self.handles {
            h.marshal(out);
        }
        out.extend_from_slice(&self.bytes);
    }

    pub fn unmarshal(data: &[u8]) -> Result<Self, Error> {
        if data.len() < 4 {
            return Err(Error::InvalidMessage("missing handle count".to_string()));
        }
        let mut idx = 0usize;
        let mut nbuf = [0u8; 4];
        nbuf.copy_from_slice(&data[idx..idx + 4]);
        idx += 4;
        let n = u32::from_le_bytes(nbuf) as usize;
        let mut handles = Vec::with_capacity(n);
        for _ in 0..n {
            if idx + HANDLE_SIZE > data.len() {
                return Err(Error::InvalidMessage("truncated handle list".to_string()));
            }
            handles.push(Handle::unmarshal(&data[idx..idx + HANDLE_SIZE])?);
            idx += HANDLE_SIZE;
        }
        Ok(Self {
            handles,
            bytes: data[idx..].to_vec(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertResp {
    pub success: u32,
}

mod hex_24 {
    use super::*;

    pub fn serialize<S>(bytes: &[u8; 24], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&hex::encode(bytes))
    }

    struct Hex24Visitor;

    impl<'de> Visitor<'de> for Hex24Visitor {
        type Value = [u8; 24];

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "24-byte hex string")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let b = hex::decode(v).map_err(E::custom)?;
            if b.len() != 24 {
                return Err(E::custom("expected 24 bytes"));
            }
            let mut out = [0u8; 24];
            out.copy_from_slice(&b);
            Ok(out)
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<[u8; 24], D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(Hex24Visitor)
    }
}

mod hex_32 {
    use super::*;

    pub fn serialize<S>(bytes: &[u8; 32], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&hex::encode(bytes))
    }

    struct Hex32Visitor;

    impl<'de> Visitor<'de> for Hex32Visitor {
        type Value = [u8; 32];

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "32-byte hex string")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let b = hex::decode(v).map_err(E::custom)?;
            if b.len() != 32 {
                return Err(E::custom("expected 32 bytes"));
            }
            let mut out = [0u8; 32];
            out.copy_from_slice(&b);
            Ok(out)
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<[u8; 32], D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(Hex32Visitor)
    }
}
