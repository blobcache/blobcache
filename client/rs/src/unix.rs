use crate::bcp;
use crate::service::Service;
use crate::types::*;
use std::os::unix::net::UnixStream;

pub struct UnixClient {
    socket_path: String,
}

impl UnixClient {
    pub fn new(path: impl AsRef<str>) -> Self {
        Self {
            socket_path: path.as_ref().to_string(),
        }
    }

    fn ask(&self, code: u8, body: &[u8]) -> Result<Vec<u8>, Error> {
        let mut conn = UnixStream::connect(&self.socket_path)?;
        bcp::write_message(&mut conn, code, body)?;
        let (resp_code, resp_body) = bcp::read_message(&mut conn)?;
        bcp::check_ok(resp_code, &resp_body)?;
        Ok(resp_body)
    }
}

impl Service for UnixClient {
    fn endpoint(&self) -> Result<Endpoint, Error> {
        let body = self.ask(bcp::MT_ENDPOINT, &[])?;
        Endpoint::from_bcp_bytes(&body)
    }

    fn inspect_handle(&self, handle: &Handle) -> Result<HandleInfo, Error> {
        let mut req = Vec::new();
        handle.marshal(&mut req);
        let body = self.ask(bcp::MT_HANDLE_INSPECT, &req)?;
        if body.len() < 40 {
            return Err(Error::InvalidMessage(
                "inspect handle response too short".to_string(),
            ));
        }
        let oid = OID::from_bytes(&body[..16])?;
        let rights = u64::from_le_bytes(body[16..24].try_into().expect("slice length"));
        let created_at = serde_json::Value::String(hex::encode(&body[24..32]));
        let expires_at = serde_json::Value::String(hex::encode(&body[32..40]));
        Ok(HandleInfo {
            oid,
            rights,
            created_at,
            expires_at,
        })
    }

    fn drop_handle(&self, handle: &Handle) -> Result<(), Error> {
        let mut req = Vec::new();
        handle.marshal(&mut req);
        let _ = self.ask(bcp::MT_HANDLE_DROP, &req)?;
        Ok(())
    }

    fn keep_alive(&self, handles: &[Handle]) -> Result<(), Error> {
        let mut req = Vec::new();
        for h in handles {
            h.marshal(&mut req);
        }
        let _ = self.ask(bcp::MT_HANDLE_KEEP_ALIVE, &req)?;
        Ok(())
    }

    fn share_out(&self, handle: &Handle, to: &NodeID, mask: ActionSet) -> Result<Handle, Error> {
        let mut req = Vec::new();
        handle.marshal(&mut req);
        req.extend_from_slice(&to.0);
        req.extend_from_slice(&mask.to_be_bytes());
        let body = self.ask(bcp::MT_HANDLE_SHARE, &req)?;
        Handle::unmarshal(&body)
    }

    fn share_in(&self, host: &NodeID, handle: &Handle) -> Result<Handle, Error> {
        let mut req = Vec::new();
        req.extend_from_slice(&host.0);
        handle.marshal(&mut req);
        let body = self.ask(bcp::MT_HANDLE_ADOPT, &req)?;
        Handle::unmarshal(&body)
    }

    fn inspect(&self, handle: &Handle) -> Result<Info, Error> {
        let mut req = Vec::new();
        handle.marshal(&mut req);
        let body = self.ask(bcp::MT_HANDLE_INSPECT_OBJECT, &req)?;
        Ok(serde_json::from_slice(&body)?)
    }

    fn open_fiat(&self, target: OID, mask: ActionSet) -> Result<Handle, Error> {
        let mut req = Vec::new();
        req.extend_from_slice(target.as_bytes());
        req.extend_from_slice(&mask.to_be_bytes());
        let body = self.ask(bcp::MT_OPEN_FIAT, &req)?;
        if body.len() < HANDLE_SIZE {
            return Err(Error::InvalidMessage(
                "open fiat response too short".to_string(),
            ));
        }
        Handle::unmarshal(&body[..HANDLE_SIZE])
    }

    fn open_from(
        &self,
        base: &Handle,
        token: &LinkToken,
        mask: ActionSet,
    ) -> Result<Handle, Error> {
        let mut req = Vec::new();
        base.marshal(&mut req);
        token.marshal(&mut req);
        req.extend_from_slice(&mask.to_be_bytes());
        let body = self.ask(bcp::MT_OPEN_FROM, &req)?;
        if body.len() < HANDLE_SIZE {
            return Err(Error::InvalidMessage(
                "open from response too short".to_string(),
            ));
        }
        Handle::unmarshal(&body[..HANDLE_SIZE])
    }

    fn create_volume(&self, _host: Option<&Endpoint>, spec: &VolumeSpec) -> Result<Handle, Error> {
        let req = serde_json::to_vec(spec)?;
        let body = self.ask(bcp::MT_CREATE_VOLUME, &req)?;
        if body.len() < HANDLE_SIZE {
            return Err(Error::InvalidMessage(
                "create volume response too short".to_string(),
            ));
        }
        Handle::unmarshal(&body[..HANDLE_SIZE])
    }

    fn clone_volume(&self, _caller: Option<&NodeID>, volume: &Handle) -> Result<Handle, Error> {
        let mut req = Vec::new();
        volume.marshal(&mut req);
        let body = self.ask(bcp::MT_VOLUME_CLONE, &req)?;
        Handle::unmarshal(&body)
    }

    fn inspect_volume(&self, volume: &Handle) -> Result<VolumeInfo, Error> {
        let mut req = Vec::new();
        volume.marshal(&mut req);
        let body = self.ask(bcp::MT_VOLUME_INSPECT, &req)?;
        Ok(serde_json::from_slice(&body)?)
    }

    fn begin_tx(&self, volume: &Handle, params: TxParams) -> Result<Handle, Error> {
        let mut req = Vec::new();
        volume.marshal(&mut req);
        req.extend_from_slice(&serde_json::to_vec(&params)?);
        let body = self.ask(bcp::MT_VOLUME_BEGIN_TX, &req)?;
        if body.len() < HANDLE_SIZE {
            return Err(Error::InvalidMessage(
                "begin tx response too short".to_string(),
            ));
        }
        Handle::unmarshal(&body[..HANDLE_SIZE])
    }

    fn inspect_tx(&self, tx: &Handle) -> Result<TxInfo, Error> {
        let mut req = Vec::new();
        tx.marshal(&mut req);
        let body = self.ask(bcp::MT_TX_INSPECT, &req)?;
        Ok(serde_json::from_slice(&body)?)
    }

    fn commit(&self, tx: &Handle) -> Result<(), Error> {
        let mut req = Vec::new();
        tx.marshal(&mut req);
        let _ = self.ask(bcp::MT_TX_COMMIT, &req)?;
        Ok(())
    }

    fn abort(&self, tx: &Handle) -> Result<(), Error> {
        let mut req = Vec::new();
        tx.marshal(&mut req);
        let _ = self.ask(bcp::MT_TX_ABORT, &req)?;
        Ok(())
    }

    fn save(&self, tx: &Handle, root: &[u8]) -> Result<(), Error> {
        let mut req = Vec::new();
        tx.marshal(&mut req);
        req.extend_from_slice(root);
        let _ = self.ask(bcp::MT_TX_SAVE, &req)?;
        Ok(())
    }

    fn load(&self, tx: &Handle) -> Result<Vec<u8>, Error> {
        let mut req = Vec::new();
        tx.marshal(&mut req);
        self.ask(bcp::MT_TX_LOAD, &req)
    }

    fn post(&self, tx: &Handle, data: &[u8], opts: PostOpts) -> Result<CID, Error> {
        let mut req = Vec::new();
        tx.marshal(&mut req);
        let code = if let Some(salt) = opts.salt {
            req.extend_from_slice(salt.as_bytes());
            bcp::MT_TX_POST_SALT
        } else {
            bcp::MT_TX_POST
        };
        req.extend_from_slice(data);
        let body = self.ask(code, &req)?;
        CID::from_bytes(&body)
    }

    fn get(&self, tx: &Handle, cid: CID, buf: &mut [u8], _opts: GetOpts) -> Result<usize, Error> {
        let mut req = Vec::new();
        tx.marshal(&mut req);
        req.extend_from_slice(cid.as_bytes());
        let body = self.ask(bcp::MT_TX_GET, &req)?;
        if body.len() > buf.len() {
            return Err(Error::InvalidMessage("buffer too short".to_string()));
        }
        buf[..body.len()].copy_from_slice(&body);
        Ok(body.len())
    }

    fn exists(&self, tx: &Handle, cids: &[CID]) -> Result<Vec<bool>, Error> {
        let mut req = Vec::new();
        tx.marshal(&mut req);
        for cid in cids {
            req.extend_from_slice(cid.as_bytes());
        }
        let body = self.ask(bcp::MT_TX_EXISTS, &req)?;
        Ok(bcp::decode_bools(&body, cids.len()))
    }

    fn delete(&self, tx: &Handle, cids: &[CID]) -> Result<(), Error> {
        let mut req = Vec::new();
        tx.marshal(&mut req);
        bcp::write_uvarint(&mut req, cids.len() as u64);
        for cid in cids {
            req.extend_from_slice(cid.as_bytes());
        }
        let _ = self.ask(bcp::MT_TX_DELETE, &req)?;
        Ok(())
    }

    fn copy(&self, tx: &Handle, src_txs: &[Handle], cids: &[CID]) -> Result<Vec<bool>, Error> {
        let mut req = Vec::new();
        tx.marshal(&mut req);
        bcp::write_uvarint(&mut req, cids.len() as u64);
        for cid in cids {
            req.extend_from_slice(cid.as_bytes());
        }
        bcp::write_uvarint(&mut req, src_txs.len() as u64);
        for src in src_txs {
            src.marshal(&mut req);
        }
        let body = self.ask(bcp::MT_TX_ADD_FROM, &req)?;
        Ok(bcp::decode_bools(&body, cids.len()))
    }

    fn visit(&self, tx: &Handle, cids: &[CID]) -> Result<(), Error> {
        let mut req = Vec::new();
        tx.marshal(&mut req);
        bcp::write_uvarint(&mut req, cids.len() as u64);
        for cid in cids {
            req.extend_from_slice(cid.as_bytes());
        }
        let _ = self.ask(bcp::MT_TX_VISIT, &req)?;
        Ok(())
    }

    fn is_visited(&self, tx: &Handle, cids: &[CID]) -> Result<Vec<bool>, Error> {
        let mut req = Vec::new();
        tx.marshal(&mut req);
        bcp::write_uvarint(&mut req, cids.len() as u64);
        for cid in cids {
            req.extend_from_slice(cid.as_bytes());
        }
        let body = self.ask(bcp::MT_TX_IS_VISITED, &req)?;
        Ok(bcp::decode_bools(&body, cids.len()))
    }

    fn link(&self, tx: &Handle, target: &Handle, mask: ActionSet) -> Result<LinkToken, Error> {
        let mut req = Vec::new();
        tx.marshal(&mut req);
        target.marshal(&mut req);
        req.extend_from_slice(&mask.to_be_bytes());
        let body = self.ask(bcp::MT_TX_LINK, &req)?;
        LinkToken::unmarshal(&body)
    }

    fn unlink(&self, tx: &Handle, targets: &[LinkToken]) -> Result<(), Error> {
        let mut req = Vec::new();
        tx.marshal(&mut req);
        bcp::write_uvarint(&mut req, targets.len() as u64);
        for token in targets {
            token.marshal(&mut req);
        }
        let _ = self.ask(bcp::MT_TX_UNLINK, &req)?;
        Ok(())
    }

    fn visit_links(&self, tx: &Handle, targets: &[LinkToken]) -> Result<(), Error> {
        let mut req = Vec::new();
        tx.marshal(&mut req);
        bcp::write_uvarint(&mut req, targets.len() as u64);
        for token in targets {
            token.marshal(&mut req);
        }
        let _ = self.ask(bcp::MT_TX_VISIT_LINKS, &req)?;
        Ok(())
    }

    fn create_queue(&self, _host: Option<&Endpoint>, spec: &QueueSpec) -> Result<Handle, Error> {
        let req = serde_json::to_vec(spec)?;
        let body = self.ask(bcp::MT_QUEUE_CREATE, &req)?;
        Handle::unmarshal(&body)
    }

    fn inspect_queue(&self, queue: &Handle) -> Result<QueueInfo, Error> {
        let mut req = Vec::new();
        queue.marshal(&mut req);
        let body = self.ask(bcp::MT_QUEUE_INSPECT, &req)?;
        Ok(serde_json::from_slice(&body)?)
    }

    fn dequeue(
        &self,
        queue: &Handle,
        max: usize,
        opts: DequeueOpts,
    ) -> Result<Vec<Message>, Error> {
        let mut req = Vec::new();
        queue.marshal(&mut req);
        req.extend_from_slice(&(max as u32).to_le_bytes());
        req.extend_from_slice(&serde_json::to_vec(&opts)?);
        let body = self.ask(bcp::MT_QUEUE_DEQUEUE, &req)?;
        if body.len() < 4 {
            return Err(Error::InvalidMessage(
                "dequeue response too short".to_string(),
            ));
        }
        let mut idx = 0usize;
        let num = u32::from_le_bytes(body[idx..idx + 4].try_into().expect("slice length")) as usize;
        idx += 4;
        let mut out = Vec::with_capacity(num);
        for _ in 0..num {
            let (msg_data, rest) = bcp::read_lp(&body[idx..])?;
            out.push(Message::unmarshal(msg_data)?);
            idx = body.len() - rest.len();
        }
        Ok(out)
    }

    fn enqueue(&self, queue: &Handle, messages: &[Message]) -> Result<InsertResp, Error> {
        let mut req = Vec::new();
        queue.marshal(&mut req);
        req.extend_from_slice(&(messages.len() as u32).to_le_bytes());
        for msg in messages {
            let data = bcp::encode_message(msg);
            bcp::append_lp(&mut req, &data);
        }
        let body = self.ask(bcp::MT_QUEUE_ENQUEUE, &req)?;
        if body.len() < 4 {
            return Err(Error::InvalidMessage(
                "enqueue response too short".to_string(),
            ));
        }
        let success = u32::from_le_bytes(body[..4].try_into().expect("slice length"));
        Ok(InsertResp { success })
    }

    fn sub_to_volume(
        &self,
        queue: &Handle,
        volume: &Handle,
        spec: &VolSubSpec,
    ) -> Result<(), Error> {
        let mut req = Vec::new();
        queue.marshal(&mut req);
        volume.marshal(&mut req);
        let spec_json = serde_json::to_vec(spec)?;
        bcp::append_lp(&mut req, &spec_json);
        let _ = self.ask(bcp::MT_QUEUE_SUB_TO_VOLUME, &req)?;
        Ok(())
    }
}
