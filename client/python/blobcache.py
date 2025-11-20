# This library provides a Python client for the Blobcache API.

import os
import json
import base64
from dataclasses import dataclass, asdict, field
from typing import List, Optional

import requests_unixsocket

class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return asdict(o)
        if isinstance(o, (Handle, OID, CID)):
            return str(o)
        if isinstance(o, bytes):
            return base64.b64encode(o).decode('utf-8')
        return super().default(o)

@dataclass
class CID:
    id: bytes

    _LEX_B64_ALPHABET = "-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz"
    _STANDARD_B64_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
    _STANDARD_TO_LEX_B64 = str.maketrans(_STANDARD_B64_ALPHABET, _LEX_B64_ALPHABET)
    _LEX_TO_STANDARD_B64 = str.maketrans(_LEX_B64_ALPHABET, _STANDARD_B64_ALPHABET)


    def __str__(self):
        return self._encode(self.id)

    @classmethod
    def from_string(cls, s: str) -> "CID":
        return cls(id=cls._decode(s))
    
    @classmethod
    def _encode(cls, data: bytes) -> str:
        """Encodes bytes using the order-preserving base64 alphabet."""
        encoded = base64.b64encode(data).decode('ascii')
        return encoded.translate(cls._STANDARD_TO_LEX_B64)

    @classmethod
    def _decode(cls, s: str) -> bytes:
        """Decodes a string using the order-preserving base64 alphabet."""
        standard_b64_str = s.translate(cls._LEX_TO_STANDARD_B64)
        return base64.b64decode(standard_b64_str)

@dataclass
class OID:
    id: bytes

    def __str__(self):
        return self.id.hex().upper()

@dataclass
class Handle:
    oid: OID
    secret: str

    def __str__(self):
        return f"{self.oid}.{self.secret}"

    @classmethod
    def from_string(cls, s: str) -> "Handle":
        oid_part, secret_part = s.split('.')
        return cls(oid=OID(bytes.fromhex(oid_part)), secret=secret_part)

@dataclass
class VolumeSpec:
    hash_algo: str

@dataclass
class VolumeInfo:
    id: OID
    spec: VolumeSpec

@dataclass
class HandleInfo:
    oid: OID
    is_volume: bool

@dataclass
class TxInfo:
    id: OID
    volume: OID
    max_size: int
    hash_algo: str

@dataclass
class Entry:
    name: str
    oid: OID

@dataclass
class Conditions:
    all_equal: List[Handle] = field(default_factory=list)

# Client is used to connect to a Blobcache service..
class Client:
    def __init__(self, endpoint: str):
        if not endpoint.startswith("http+unix://"):
            raise ValueError("endpoint must start with http+unix://")
        self.endpoint = endpoint
        self.session = requests_unixsocket.Session()

    def _do_json(self, method, path, secret=None, data=None):
        headers = {'Content-Type': 'application/json'}
        if secret:
            headers['X-Secret'] = secret
        
        url = f"{self.endpoint}{path}"
        
        encoded_data = json.dumps(data, cls=EnhancedJSONEncoder)
        
        response = self.session.request(method, url, data=encoded_data, headers=headers)
        response.raise_for_status()
        if response.content:
            return response.json()
        return None

    def _do(self, method, path, headers=None, data=None):
        url = f"{self.endpoint}{path}"
        response = self.session.request(method, url, headers=headers, data=data)
        response.raise_for_status()
        return response.content
        
    def _handle_from_response(self, resp: dict) -> Handle:
        return Handle.from_string(resp["handle"])

    def get_endpoint(self) -> str:
        resp = self._do_json("POST", "/Endpoint", data={})
        return resp.get("endpoint")

    def open(self, oid: OID) -> Handle:
        req = {"target": str(oid)}
        resp = self._do_json("POST", "/Open", data=req)
        return self._handle_from_response(resp)

    def open_at(self, ns: Handle, name: str) -> Handle:
        req = {"namespace": str(ns), "name": name}
        resp = self._do_json("POST", "/OpenAt", data=req)
        return self._handle_from_response(resp)
    
    def inspect_handle(self, h: Handle) -> HandleInfo:
        req = {"handle": str(h)}
        resp = self._do_json("POST", "/InspectHandle", data=req)
        info = resp['info']
        return HandleInfo(oid=OID(bytes.fromhex(info["oid"])), is_volume=info["is_volume"])

    def get_entry(self, ns: Handle, name: str) -> Entry:
        req = {"namespace": str(ns), "name": name}
        resp = self._do_json("POST", "/GetEntry", data=req)
        entry = resp['entry']
        return Entry(name=entry['name'], oid=OID(bytes.fromhex(entry['target'])))

    def put_entry(self, ns: Handle, name: str, target: Handle):
        req = {"namespace": str(ns), "name": name, "Target": str(target)}
        self._do_json("POST", "/PutEntry", data=req)

    def delete_entry(self, ns: Handle, name: str):
        req = {"namespace": str(ns), "name": name}
        self._do_json("POST", "/DeleteEntry", data=req)

    def list_names(self, ns: Handle) -> List[str]:
        req = {"namespace": str(ns)}
        resp = self._do_json("POST", "/ListNames", data=req)
        return resp.get("names", [])

    def create_volume_at(self, ns: Handle, name: str, spec: VolumeSpec) -> Handle:
        req = {"namespace": str(ns), "name": name, "spec": asdict(spec)}
        resp = self._do_json("POST", "/CreateVolumeAt", data=req)
        return self._handle_from_response(resp)

    def create_volume(self, spec: VolumeSpec) -> Handle:
        req = {"spec": asdict(spec)}
        resp = self._do_json("POST", "/volume/", data=req)
        return self._handle_from_response(resp)

    def inspect_volume(self, h: Handle) -> VolumeInfo:
        path = f"/volume/{h.oid}.Inspect"
        headers = {"X-Secret": h.secret}
        resp_data = self._do("GET", path, headers=headers)
        info = json.loads(resp_data)
        return VolumeInfo(
            id=OID(bytes.fromhex(info["id"])),
            spec=VolumeSpec(hash_algo=info["backend"]["local"]["hash_algo"])
        )

    def drop(self, h: Handle):
        req = {"handle": str(h)}
        self._do_json("POST", "/Drop", secret=h.secret, data=req)

    def keep_alive(self, handles: List[Handle]):
        req = {"handles": [str(h) for h in handles]}
        self._do_json("POST", "/KeepAlive", data=req)

    def begin_tx(self, vol: Handle, mutate: bool) -> Handle:
        req = {"volume": str(vol), "params": {"Mutate": mutate}}
        resp = self._do_json("POST", "/tx/", secret=vol.secret, data=req)
        return self._handle_from_response(resp)

    def inspect_tx(self, tx: Handle) -> TxInfo:
        req = {"tx": str(tx)}
        resp = self._do_json("POST", "/tx/", secret=tx.secret, data=req)
        info = resp['info']
        return TxInfo(
            id=OID(bytes.fromhex(info['id'])),
            volume=OID(bytes.fromhex(info['volume'])),
            max_size=info['max_size'],
            hash_algo=info['hash_algo']
        )

    def commit(self, tx: Handle, root: bytes):
        req = {"root": root}
        path = f"/tx/{tx.oid}.Commit"
        self._do_json("POST", path, secret=tx.secret, data=req)

    def abort(self, tx: Handle):
        path = f"/tx/{tx.oid}.Abort"
        self._do_json("POST", path, secret=tx.secret, data={})

    def load(self, tx: Handle) -> bytes:
        path = f"/tx/{tx.oid}.Load"
        resp = self._do_json("POST", path, secret=tx.secret, data={})
        return base64.b64decode(resp['root'])

    def post(self, tx: Handle, data: bytes, salt: Optional[CID] = None) -> CID:
        path = f"/tx/{tx.oid}.Post"
        headers = {"X-Secret": tx.secret}
        if salt:
            headers["X-Salt"] = str(salt)
        resp_data = self._do("POST", path, headers=headers, data=data)
        return CID(id=resp_data)

    def exists(self, tx: Handle, cid: CID) -> bool:
        path = f"/tx/{tx.oid}.Exists"
        req = {"cid": str(cid)}
        resp = self._do_json("POST", path, secret=tx.secret, data=req)
        return resp.get("exists", False)

    def delete(self, tx: Handle, cid: CID):
        path = f"/tx/{tx.oid}.Delete"
        req = {"cid": str(cid)}
        self._do_json("POST", path, secret=tx.secret, data=req)

    def get(self, tx: Handle, cid: CID, salt: Optional[CID] = None) -> bytes:
        path = f"/tx/{tx.oid}.Get"
        req = {"cid": str(cid)}
        if salt:
            req["salt"] = str(salt)
        
        req_body = json.dumps(req)
        
        headers = {
            'Content-Type': 'application/json',
            'X-Secret': tx.secret
        }

        url = f"{self.endpoint}{path}"
        response = self.session.post(url, data=req_body, headers=headers, stream=True)
        response.raise_for_status()
        return response.content

def new_client(endpoint: str) -> Client:
    return Client(endpoint)

def new_client_from_env() -> Client:
    endpoint = os.environ.get("BLOBCACHE_API", "http+unix:///tmp/blobcache.sock")
    
    if "://" not in endpoint:
        endpoint = f"http+unix://{endpoint}"
    elif not endpoint.startswith("http+unix://"):
        raise ValueError("BLOBCACHE_API must be a unix socket path or a http+unix:// URL")

    return Client(endpoint)
 
