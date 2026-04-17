use crate::service::Service;
use crate::types::*;
use reqwest::blocking::Client as Http;
use reqwest::Method;
use serde::de::DeserializeOwned;
use serde::Serialize;

pub struct HttpClient {
    http: Http,
    endpoint: String,
}

impl HttpClient {
    pub fn new(endpoint: &str) -> Result<Self, Error> {
        let endpoint = normalize_endpoint(endpoint)?;
        Ok(Self {
            http: Http::new(),
            endpoint,
        })
    }

    fn do_json<Req, Resp>(
        &self,
        method: Method,
        path: &str,
        secret: Option<&Handle>,
        req: &Req,
    ) -> Result<Resp, Error>
    where
        Req: Serialize + ?Sized,
        Resp: DeserializeOwned,
    {
        let mut request = self
            .http
            .request(method, format!("{}{}", self.endpoint, path))
            .header("Content-Type", "application/json")
            .json(req);
        if let Some(h) = secret {
            request = request.header("X-Secret", hex::encode(h.secret));
        }
        let response = request.send()?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().unwrap_or_default();
            return Err(Error::HttpStatus {
                status: status.as_u16(),
                body,
            });
        }
        Ok(response.json()?)
    }

    fn do_bytes(
        &self,
        method: Method,
        path: &str,
        secret: Option<&Handle>,
        salt: Option<CID>,
        body: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let mut request = self
            .http
            .request(method, format!("{}{}", self.endpoint, path))
            .body(body.to_vec());
        if let Some(h) = secret {
            request = request.header("X-Secret", hex::encode(h.secret));
        }
        if let Some(salt) = salt {
            request = request.header("X-Salt", salt.to_string());
        }
        let response = request.send()?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().unwrap_or_default();
            return Err(Error::HttpStatus {
                status: status.as_u16(),
                body,
            });
        }
        Ok(response.bytes()?.to_vec())
    }

    fn tx_path(tx: &Handle, method: &str) -> String {
        format!("/tx/{}.{}", tx.oid, method)
    }
}

fn normalize_endpoint(endpoint: &str) -> Result<String, Error> {
    if endpoint.starts_with("unix://") {
        return Err(Error::UnsupportedEndpoint(endpoint.to_string()));
    }
    let normalized = if let Some(rest) = endpoint.strip_prefix("http://") {
        rest
    } else if let Some(rest) = endpoint.strip_prefix("tcp://") {
        rest
    } else {
        endpoint
    };
    Ok(format!("http://{normalized}"))
}

impl Service for HttpClient {
    fn endpoint(&self) -> Result<Endpoint, Error> {
        #[derive(Serialize)]
        struct Req {}
        #[derive(serde::Deserialize)]
        struct Resp {
            endpoint: Endpoint,
        }
        let resp: Resp = self.do_json(Method::POST, "/Endpoint", None, &Req {})?;
        Ok(resp.endpoint)
    }

    fn inspect_handle(&self, handle: &Handle) -> Result<HandleInfo, Error> {
        #[derive(Serialize)]
        struct Req<'a> {
            handle: &'a Handle,
        }
        #[derive(serde::Deserialize)]
        struct Resp {
            info: HandleInfo,
        }
        Ok(self
            .do_json::<_, Resp>(Method::POST, "/InspectHandle", None, &Req { handle })?
            .info)
    }

    fn drop_handle(&self, handle: &Handle) -> Result<(), Error> {
        #[derive(Serialize)]
        struct Req<'a> {
            handle: &'a Handle,
        }
        let _: serde_json::Value =
            self.do_json(Method::POST, "/Drop", Some(handle), &Req { handle })?;
        Ok(())
    }

    fn keep_alive(&self, handles: &[Handle]) -> Result<(), Error> {
        #[derive(Serialize)]
        struct Req<'a> {
            handles: &'a [Handle],
        }
        let _: serde_json::Value =
            self.do_json(Method::POST, "/KeepAlive", None, &Req { handles })?;
        Ok(())
    }

    fn share_out(&self, handle: &Handle, to: &PeerID, mask: ActionSet) -> Result<Handle, Error> {
        #[derive(Serialize)]
        struct Req<'a> {
            handle: &'a Handle,
            peer: &'a PeerID,
            mask: ActionSet,
        }
        #[derive(serde::Deserialize)]
        struct Resp {
            handle: Handle,
        }
        Ok(self
            .do_json::<_, Resp>(
                Method::POST,
                "/ShareOut",
                None,
                &Req {
                    handle,
                    peer: to,
                    mask,
                },
            )?
            .handle)
    }

    fn share_in(&self, host: &PeerID, handle: &Handle) -> Result<Handle, Error> {
        #[derive(Serialize)]
        struct Req<'a> {
            host: &'a PeerID,
            handle: &'a Handle,
        }
        #[derive(serde::Deserialize)]
        struct Resp {
            handle: Handle,
        }
        Ok(self
            .do_json::<_, Resp>(Method::POST, "/ShareIn", None, &Req { host, handle })?
            .handle)
    }

    fn inspect(&self, handle: &Handle) -> Result<Info, Error> {
        #[derive(Serialize)]
        struct Req<'a> {
            handle: &'a Handle,
        }
        #[derive(serde::Deserialize)]
        struct Resp {
            info: Info,
        }
        Ok(self
            .do_json::<_, Resp>(Method::POST, "/Inspect", None, &Req { handle })?
            .info)
    }

    fn open_fiat(&self, target: OID, mask: ActionSet) -> Result<Handle, Error> {
        #[derive(Serialize)]
        struct Req {
            target: OID,
            mask: ActionSet,
        }
        #[derive(serde::Deserialize)]
        struct Resp {
            handle: Handle,
        }
        Ok(self
            .do_json::<_, Resp>(Method::POST, "/OpenFiat", None, &Req { target, mask })?
            .handle)
    }

    fn open_from(
        &self,
        base: &Handle,
        token: &LinkToken,
        mask: ActionSet,
    ) -> Result<Handle, Error> {
        #[derive(Serialize)]
        struct Req<'a> {
            base: &'a Handle,
            token: &'a LinkToken,
            mask: ActionSet,
        }
        #[derive(serde::Deserialize)]
        struct Resp {
            handle: Handle,
        }
        Ok(self
            .do_json::<_, Resp>(Method::POST, "/OpenFrom", None, &Req { base, token, mask })?
            .handle)
    }

    fn create_volume(&self, host: Option<&Endpoint>, spec: &VolumeSpec) -> Result<Handle, Error> {
        #[derive(Serialize)]
        struct Req<'a> {
            host: Option<&'a Endpoint>,
            spec: &'a VolumeSpec,
        }
        #[derive(serde::Deserialize)]
        struct Resp {
            handle: Handle,
        }
        Ok(self
            .do_json::<_, Resp>(Method::POST, "/volume/", None, &Req { host, spec })?
            .handle)
    }

    fn clone_volume(&self, _caller: Option<&PeerID>, volume: &Handle) -> Result<Handle, Error> {
        #[derive(Serialize)]
        struct Req<'a> {
            volume: &'a Handle,
        }
        #[derive(serde::Deserialize)]
        struct Resp {
            clone: Handle,
        }
        Ok(self
            .do_json::<_, Resp>(Method::POST, "/volume/Clone", None, &Req { volume })?
            .clone)
    }

    fn inspect_volume(&self, volume: &Handle) -> Result<VolumeInfo, Error> {
        let path = format!("/volume/{}.Inspect", volume.oid);
        let response = self
            .http
            .request(Method::GET, format!("{}{}", self.endpoint, path))
            .header("X-Secret", hex::encode(volume.secret))
            .send()?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().unwrap_or_default();
            return Err(Error::HttpStatus {
                status: status.as_u16(),
                body,
            });
        }
        Ok(response.json()?)
    }

    fn begin_tx(&self, volume: &Handle, params: TxParams) -> Result<Handle, Error> {
        #[derive(Serialize)]
        struct Req<'a> {
            volume: &'a Handle,
            params: TxParams,
        }
        #[derive(serde::Deserialize)]
        struct Resp {
            #[serde(rename = "handle")]
            tx: Handle,
        }
        Ok(self
            .do_json::<_, Resp>(Method::POST, "/tx/", Some(volume), &Req { volume, params })?
            .tx)
    }

    fn inspect_tx(&self, tx: &Handle) -> Result<TxInfo, Error> {
        #[derive(Serialize)]
        struct Req<'a> {
            tx: &'a Handle,
        }
        #[derive(serde::Deserialize)]
        struct Resp {
            info: TxInfo,
        }
        Ok(self
            .do_json::<_, Resp>(Method::POST, "/tx/", Some(tx), &Req { tx })?
            .info)
    }

    fn commit(&self, tx: &Handle) -> Result<(), Error> {
        let _: serde_json::Value = self.do_json(
            Method::POST,
            &Self::tx_path(tx, "Commit"),
            Some(tx),
            &serde_json::json!({}),
        )?;
        Ok(())
    }

    fn abort(&self, tx: &Handle) -> Result<(), Error> {
        let _: serde_json::Value = self.do_json(
            Method::POST,
            &Self::tx_path(tx, "Abort"),
            Some(tx),
            &serde_json::json!({}),
        )?;
        Ok(())
    }

    fn save(&self, tx: &Handle, root: &[u8]) -> Result<(), Error> {
        #[derive(Serialize)]
        struct Req {
            root: Vec<u8>,
        }
        let _: serde_json::Value = self.do_json(
            Method::POST,
            &Self::tx_path(tx, "Save"),
            Some(tx),
            &Req {
                root: root.to_vec(),
            },
        )?;
        Ok(())
    }

    fn load(&self, tx: &Handle) -> Result<Vec<u8>, Error> {
        #[derive(serde::Deserialize)]
        struct Resp {
            root: Vec<u8>,
        }
        Ok(self
            .do_json::<_, Resp>(
                Method::POST,
                &Self::tx_path(tx, "Load"),
                Some(tx),
                &serde_json::json!({}),
            )?
            .root)
    }

    fn post(&self, tx: &Handle, data: &[u8], opts: PostOpts) -> Result<CID, Error> {
        let bytes = self.do_bytes(
            Method::POST,
            &Self::tx_path(tx, "Post"),
            Some(tx),
            opts.salt,
            data,
        )?;
        CID::from_bytes(&bytes)
    }

    fn get(&self, tx: &Handle, cid: CID, buf: &mut [u8], opts: GetOpts) -> Result<usize, Error> {
        #[derive(Serialize)]
        struct Req {
            cid: CID,
            #[serde(skip_serializing_if = "Option::is_none")]
            salt: Option<CID>,
        }
        let req = Req {
            cid,
            salt: opts.salt,
        };
        let body = serde_json::to_vec(&req)?;
        let response = self
            .http
            .request(
                Method::POST,
                format!("{}{}", self.endpoint, Self::tx_path(tx, "Get")),
            )
            .header("Content-Type", "application/json")
            .header("X-Secret", hex::encode(tx.secret))
            .body(body)
            .send()?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().unwrap_or_default();
            return Err(Error::HttpStatus {
                status: status.as_u16(),
                body,
            });
        }
        let bytes = response.bytes()?;
        let n = bytes.len().min(buf.len());
        buf[..n].copy_from_slice(&bytes[..n]);
        Ok(n)
    }

    fn exists(&self, tx: &Handle, cids: &[CID]) -> Result<Vec<bool>, Error> {
        #[derive(Serialize)]
        struct Req<'a> {
            cids: &'a [CID],
        }
        #[derive(serde::Deserialize)]
        struct Resp {
            exists: Vec<bool>,
        }
        Ok(self
            .do_json::<_, Resp>(
                Method::POST,
                &Self::tx_path(tx, "Exists"),
                Some(tx),
                &Req { cids },
            )?
            .exists)
    }

    fn delete(&self, tx: &Handle, cids: &[CID]) -> Result<(), Error> {
        #[derive(Serialize)]
        struct Req<'a> {
            cids: &'a [CID],
        }
        let _: serde_json::Value = self.do_json(
            Method::POST,
            &Self::tx_path(tx, "Delete"),
            Some(tx),
            &Req { cids },
        )?;
        Ok(())
    }

    fn copy(&self, tx: &Handle, src_txs: &[Handle], cids: &[CID]) -> Result<Vec<bool>, Error> {
        #[derive(Serialize)]
        struct Req<'a> {
            cids: &'a [CID],
            srcs: &'a [Handle],
        }
        #[derive(serde::Deserialize)]
        struct Resp {
            added: Vec<bool>,
        }
        Ok(self
            .do_json::<_, Resp>(
                Method::POST,
                &Self::tx_path(tx, "AddFrom"),
                Some(tx),
                &Req {
                    cids,
                    srcs: src_txs,
                },
            )?
            .added)
    }

    fn visit(&self, tx: &Handle, cids: &[CID]) -> Result<(), Error> {
        #[derive(Serialize)]
        struct Req<'a> {
            cids: &'a [CID],
        }
        let _: serde_json::Value = self.do_json(
            Method::POST,
            &Self::tx_path(tx, "Visit"),
            Some(tx),
            &Req { cids },
        )?;
        Ok(())
    }

    fn is_visited(&self, tx: &Handle, cids: &[CID]) -> Result<Vec<bool>, Error> {
        #[derive(Serialize)]
        struct Req<'a> {
            cids: &'a [CID],
        }
        #[derive(serde::Deserialize)]
        struct Resp {
            visited: Vec<bool>,
        }
        Ok(self
            .do_json::<_, Resp>(
                Method::POST,
                &Self::tx_path(tx, "IsVisited"),
                Some(tx),
                &Req { cids },
            )?
            .visited)
    }

    fn link(&self, tx: &Handle, target: &Handle, mask: ActionSet) -> Result<LinkToken, Error> {
        #[derive(Serialize)]
        struct Req<'a> {
            target: &'a Handle,
            mask: ActionSet,
        }
        #[derive(serde::Deserialize)]
        struct Resp {
            token: LinkToken,
        }
        Ok(self
            .do_json::<_, Resp>(
                Method::POST,
                &Self::tx_path(tx, "Link"),
                Some(tx),
                &Req { target, mask },
            )?
            .token)
    }

    fn unlink(&self, tx: &Handle, targets: &[LinkToken]) -> Result<(), Error> {
        #[derive(Serialize)]
        struct Req<'a> {
            targets: &'a [LinkToken],
        }
        let _: serde_json::Value = self.do_json(
            Method::POST,
            &Self::tx_path(tx, "Unlink"),
            Some(tx),
            &Req { targets },
        )?;
        Ok(())
    }

    fn visit_links(&self, tx: &Handle, targets: &[LinkToken]) -> Result<(), Error> {
        #[derive(Serialize)]
        struct Req<'a> {
            targets: &'a [LinkToken],
        }
        let _: serde_json::Value = self.do_json(
            Method::POST,
            &Self::tx_path(tx, "VisitLinks"),
            Some(tx),
            &Req { targets },
        )?;
        Ok(())
    }

    fn create_queue(&self, host: Option<&Endpoint>, spec: &QueueSpec) -> Result<Handle, Error> {
        #[derive(Serialize)]
        struct Req<'a> {
            host: Option<&'a Endpoint>,
            spec: &'a QueueSpec,
        }
        #[derive(serde::Deserialize)]
        struct Resp {
            handle: Handle,
        }
        Ok(self
            .do_json::<_, Resp>(Method::POST, "/queue/", None, &Req { host, spec })?
            .handle)
    }

    fn inspect_queue(&self, queue: &Handle) -> Result<QueueInfo, Error> {
        #[derive(serde::Deserialize)]
        struct Resp {
            info: QueueInfo,
        }
        let path = format!("/queue/{}.Inspect", queue.oid);
        let response = self
            .http
            .request(Method::GET, format!("{}{}", self.endpoint, path))
            .header("X-Secret", hex::encode(queue.secret))
            .send()?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().unwrap_or_default();
            return Err(Error::HttpStatus {
                status: status.as_u16(),
                body,
            });
        }
        Ok(response.json::<Resp>()?.info)
    }

    fn dequeue(
        &self,
        queue: &Handle,
        max: usize,
        opts: DequeueOpts,
    ) -> Result<Vec<Message>, Error> {
        #[derive(Serialize)]
        struct Req {
            opts: DequeueOpts,
            max: usize,
        }
        #[derive(serde::Deserialize)]
        struct Resp {
            messages: Vec<Message>,
        }
        Ok(self
            .do_json::<_, Resp>(
                Method::POST,
                &format!("/queue/{}.Dequeue", queue.oid),
                Some(queue),
                &Req { opts, max },
            )?
            .messages)
    }

    fn enqueue(&self, queue: &Handle, messages: &[Message]) -> Result<InsertResp, Error> {
        #[derive(Serialize)]
        struct Req<'a> {
            messages: &'a [Message],
        }
        self.do_json(
            Method::POST,
            &format!("/queue/{}.Enqueue", queue.oid),
            Some(queue),
            &Req { messages },
        )
    }

    fn sub_to_volume(
        &self,
        queue: &Handle,
        volume: &Handle,
        spec: &VolSubSpec,
    ) -> Result<(), Error> {
        #[derive(Serialize)]
        struct Req<'a> {
            volume: &'a Handle,
            spec: &'a VolSubSpec,
        }
        let _: serde_json::Value = self.do_json(
            Method::POST,
            &format!("/queue/{}.SubToVolume", queue.oid),
            Some(queue),
            &Req { volume, spec },
        )?;
        Ok(())
    }
}
