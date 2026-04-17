use crate::types::*;

pub trait Service {
    fn endpoint(&self) -> Result<Endpoint, Error>;
    fn inspect_handle(&self, handle: &Handle) -> Result<HandleInfo, Error>;
    fn drop_handle(&self, handle: &Handle) -> Result<(), Error>;
    fn keep_alive(&self, handles: &[Handle]) -> Result<(), Error>;
    fn share_out(&self, handle: &Handle, to: &PeerID, mask: ActionSet) -> Result<Handle, Error>;
    fn share_in(&self, host: &PeerID, handle: &Handle) -> Result<Handle, Error>;
    fn inspect(&self, handle: &Handle) -> Result<Info, Error>;

    fn open_fiat(&self, target: OID, mask: ActionSet) -> Result<Handle, Error>;
    fn open_from(&self, base: &Handle, token: &LinkToken, mask: ActionSet)
        -> Result<Handle, Error>;
    fn create_volume(&self, host: Option<&Endpoint>, spec: &VolumeSpec) -> Result<Handle, Error>;
    fn clone_volume(&self, caller: Option<&PeerID>, volume: &Handle) -> Result<Handle, Error>;
    fn inspect_volume(&self, volume: &Handle) -> Result<VolumeInfo, Error>;

    fn begin_tx(&self, volume: &Handle, params: TxParams) -> Result<Handle, Error>;
    fn inspect_tx(&self, tx: &Handle) -> Result<TxInfo, Error>;
    fn commit(&self, tx: &Handle) -> Result<(), Error>;
    fn abort(&self, tx: &Handle) -> Result<(), Error>;
    fn save(&self, tx: &Handle, root: &[u8]) -> Result<(), Error>;
    fn load(&self, tx: &Handle) -> Result<Vec<u8>, Error>;
    fn post(&self, tx: &Handle, data: &[u8], opts: PostOpts) -> Result<CID, Error>;
    fn get(&self, tx: &Handle, cid: CID, buf: &mut [u8], opts: GetOpts) -> Result<usize, Error>;
    fn exists(&self, tx: &Handle, cids: &[CID]) -> Result<Vec<bool>, Error>;
    fn delete(&self, tx: &Handle, cids: &[CID]) -> Result<(), Error>;
    fn copy(&self, tx: &Handle, src_txs: &[Handle], cids: &[CID]) -> Result<Vec<bool>, Error>;
    fn visit(&self, tx: &Handle, cids: &[CID]) -> Result<(), Error>;
    fn is_visited(&self, tx: &Handle, cids: &[CID]) -> Result<Vec<bool>, Error>;
    fn link(&self, tx: &Handle, target: &Handle, mask: ActionSet) -> Result<LinkToken, Error>;
    fn unlink(&self, tx: &Handle, targets: &[LinkToken]) -> Result<(), Error>;
    fn visit_links(&self, tx: &Handle, targets: &[LinkToken]) -> Result<(), Error>;

    fn create_queue(&self, host: Option<&Endpoint>, spec: &QueueSpec) -> Result<Handle, Error>;
    fn inspect_queue(&self, queue: &Handle) -> Result<QueueInfo, Error>;
    fn dequeue(&self, queue: &Handle, max: usize, opts: DequeueOpts)
        -> Result<Vec<Message>, Error>;
    fn enqueue(&self, queue: &Handle, messages: &[Message]) -> Result<InsertResp, Error>;
    fn sub_to_volume(
        &self,
        queue: &Handle,
        volume: &Handle,
        spec: &VolSubSpec,
    ) -> Result<(), Error>;
}
