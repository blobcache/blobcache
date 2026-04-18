mod bcp;
mod http;
mod service;
mod types;
mod unix;

use std::env;

pub use http::HttpClient;
pub use service::Service;
pub use types::*;
pub use unix::UnixClient;

pub const ENV_BLOBCACHE_API: &str = "BLOBCACHE_API";
pub const ENV_BLOBCACHE_NS_ROOT: &str = "BLOBCACHE_NS_ROOT";

#[cfg(target_os = "linux")]
pub const DEFAULT_ENDPOINT: &str = "unix:///run/blobcache/blobcache.sock";

#[cfg(target_os = "macos")]
pub const DEFAULT_ENDPOINT: &str = "unix:///var/run/blobcache/blobcache.sock";

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub const DEFAULT_ENDPOINT: &str = "unix:///run/blobcache/blobcache.sock";

pub enum Client {
    Http(HttpClient),
    Unix(UnixClient),
}

impl Client {
    pub fn new(endpoint: impl AsRef<str>) -> Result<Self, Error> {
        let endpoint = endpoint.as_ref();
        if let Some(sock) = endpoint.strip_prefix("unix://") {
            return Ok(Self::Unix(UnixClient::new(sock)));
        }
        Ok(Self::Http(HttpClient::new(endpoint)?))
    }

    pub fn from_env() -> Result<Self, Error> {
        let endpoint = env::var(ENV_BLOBCACHE_API).unwrap_or_else(|_| DEFAULT_ENDPOINT.to_string());
        Self::new(endpoint)
    }
}

pub fn new_client(endpoint: impl AsRef<str>) -> Result<Client, Error> {
    Client::new(endpoint)
}

pub fn new_client_from_env() -> Result<Client, Error> {
    Client::from_env()
}

impl Service for Client {
    fn endpoint(&self) -> Result<Endpoint, Error> {
        match self {
            Self::Http(c) => c.endpoint(),
            Self::Unix(c) => c.endpoint(),
        }
    }

    fn inspect_handle(&self, handle: &Handle) -> Result<HandleInfo, Error> {
        match self {
            Self::Http(c) => c.inspect_handle(handle),
            Self::Unix(c) => c.inspect_handle(handle),
        }
    }

    fn drop_handle(&self, handle: &Handle) -> Result<(), Error> {
        match self {
            Self::Http(c) => c.drop_handle(handle),
            Self::Unix(c) => c.drop_handle(handle),
        }
    }

    fn keep_alive(&self, handles: &[Handle]) -> Result<(), Error> {
        match self {
            Self::Http(c) => c.keep_alive(handles),
            Self::Unix(c) => c.keep_alive(handles),
        }
    }

    fn share_out(&self, handle: &Handle, to: &NodeID, mask: ActionSet) -> Result<Handle, Error> {
        match self {
            Self::Http(c) => c.share_out(handle, to, mask),
            Self::Unix(c) => c.share_out(handle, to, mask),
        }
    }

    fn share_in(&self, host: &NodeID, handle: &Handle) -> Result<Handle, Error> {
        match self {
            Self::Http(c) => c.share_in(host, handle),
            Self::Unix(c) => c.share_in(host, handle),
        }
    }

    fn inspect(&self, handle: &Handle) -> Result<Info, Error> {
        match self {
            Self::Http(c) => c.inspect(handle),
            Self::Unix(c) => c.inspect(handle),
        }
    }

    fn open_fiat(&self, target: OID, mask: ActionSet) -> Result<Handle, Error> {
        match self {
            Self::Http(c) => c.open_fiat(target, mask),
            Self::Unix(c) => c.open_fiat(target, mask),
        }
    }

    fn open_from(
        &self,
        base: &Handle,
        token: &LinkToken,
        mask: ActionSet,
    ) -> Result<Handle, Error> {
        match self {
            Self::Http(c) => c.open_from(base, token, mask),
            Self::Unix(c) => c.open_from(base, token, mask),
        }
    }

    fn create_volume(&self, host: Option<&Endpoint>, spec: &VolumeSpec) -> Result<Handle, Error> {
        match self {
            Self::Http(c) => c.create_volume(host, spec),
            Self::Unix(c) => c.create_volume(host, spec),
        }
    }

    fn clone_volume(&self, caller: Option<&NodeID>, volume: &Handle) -> Result<Handle, Error> {
        match self {
            Self::Http(c) => c.clone_volume(caller, volume),
            Self::Unix(c) => c.clone_volume(caller, volume),
        }
    }

    fn inspect_volume(&self, volume: &Handle) -> Result<VolumeInfo, Error> {
        match self {
            Self::Http(c) => c.inspect_volume(volume),
            Self::Unix(c) => c.inspect_volume(volume),
        }
    }

    fn begin_tx(&self, volume: &Handle, params: TxParams) -> Result<Handle, Error> {
        match self {
            Self::Http(c) => c.begin_tx(volume, params),
            Self::Unix(c) => c.begin_tx(volume, params),
        }
    }

    fn inspect_tx(&self, tx: &Handle) -> Result<TxInfo, Error> {
        match self {
            Self::Http(c) => c.inspect_tx(tx),
            Self::Unix(c) => c.inspect_tx(tx),
        }
    }

    fn commit(&self, tx: &Handle) -> Result<(), Error> {
        match self {
            Self::Http(c) => c.commit(tx),
            Self::Unix(c) => c.commit(tx),
        }
    }

    fn abort(&self, tx: &Handle) -> Result<(), Error> {
        match self {
            Self::Http(c) => c.abort(tx),
            Self::Unix(c) => c.abort(tx),
        }
    }

    fn save(&self, tx: &Handle, root: &[u8]) -> Result<(), Error> {
        match self {
            Self::Http(c) => c.save(tx, root),
            Self::Unix(c) => c.save(tx, root),
        }
    }

    fn load(&self, tx: &Handle) -> Result<Vec<u8>, Error> {
        match self {
            Self::Http(c) => c.load(tx),
            Self::Unix(c) => c.load(tx),
        }
    }

    fn post(&self, tx: &Handle, data: &[u8], opts: PostOpts) -> Result<CID, Error> {
        match self {
            Self::Http(c) => c.post(tx, data, opts),
            Self::Unix(c) => c.post(tx, data, opts),
        }
    }

    fn get(&self, tx: &Handle, cid: CID, buf: &mut [u8], opts: GetOpts) -> Result<usize, Error> {
        match self {
            Self::Http(c) => c.get(tx, cid, buf, opts),
            Self::Unix(c) => c.get(tx, cid, buf, opts),
        }
    }

    fn exists(&self, tx: &Handle, cids: &[CID]) -> Result<Vec<bool>, Error> {
        match self {
            Self::Http(c) => c.exists(tx, cids),
            Self::Unix(c) => c.exists(tx, cids),
        }
    }

    fn delete(&self, tx: &Handle, cids: &[CID]) -> Result<(), Error> {
        match self {
            Self::Http(c) => c.delete(tx, cids),
            Self::Unix(c) => c.delete(tx, cids),
        }
    }

    fn copy(&self, tx: &Handle, src_txs: &[Handle], cids: &[CID]) -> Result<Vec<bool>, Error> {
        match self {
            Self::Http(c) => c.copy(tx, src_txs, cids),
            Self::Unix(c) => c.copy(tx, src_txs, cids),
        }
    }

    fn visit(&self, tx: &Handle, cids: &[CID]) -> Result<(), Error> {
        match self {
            Self::Http(c) => c.visit(tx, cids),
            Self::Unix(c) => c.visit(tx, cids),
        }
    }

    fn is_visited(&self, tx: &Handle, cids: &[CID]) -> Result<Vec<bool>, Error> {
        match self {
            Self::Http(c) => c.is_visited(tx, cids),
            Self::Unix(c) => c.is_visited(tx, cids),
        }
    }

    fn link(&self, tx: &Handle, target: &Handle, mask: ActionSet) -> Result<LinkToken, Error> {
        match self {
            Self::Http(c) => c.link(tx, target, mask),
            Self::Unix(c) => c.link(tx, target, mask),
        }
    }

    fn unlink(&self, tx: &Handle, targets: &[LinkToken]) -> Result<(), Error> {
        match self {
            Self::Http(c) => c.unlink(tx, targets),
            Self::Unix(c) => c.unlink(tx, targets),
        }
    }

    fn visit_links(&self, tx: &Handle, targets: &[LinkToken]) -> Result<(), Error> {
        match self {
            Self::Http(c) => c.visit_links(tx, targets),
            Self::Unix(c) => c.visit_links(tx, targets),
        }
    }

    fn create_queue(&self, host: Option<&Endpoint>, spec: &QueueSpec) -> Result<Handle, Error> {
        match self {
            Self::Http(c) => c.create_queue(host, spec),
            Self::Unix(c) => c.create_queue(host, spec),
        }
    }

    fn inspect_queue(&self, queue: &Handle) -> Result<QueueInfo, Error> {
        match self {
            Self::Http(c) => c.inspect_queue(queue),
            Self::Unix(c) => c.inspect_queue(queue),
        }
    }

    fn dequeue(
        &self,
        queue: &Handle,
        max: usize,
        opts: DequeueOpts,
    ) -> Result<Vec<Message>, Error> {
        match self {
            Self::Http(c) => c.dequeue(queue, max, opts),
            Self::Unix(c) => c.dequeue(queue, max, opts),
        }
    }

    fn enqueue(&self, queue: &Handle, messages: &[Message]) -> Result<InsertResp, Error> {
        match self {
            Self::Http(c) => c.enqueue(queue, messages),
            Self::Unix(c) => c.enqueue(queue, messages),
        }
    }

    fn sub_to_volume(
        &self,
        queue: &Handle,
        volume: &Handle,
        spec: &VolSubSpec,
    ) -> Result<(), Error> {
        match self {
            Self::Http(c) => c.sub_to_volume(queue, volume, spec),
            Self::Unix(c) => c.sub_to_volume(queue, volume, spec),
        }
    }
}
