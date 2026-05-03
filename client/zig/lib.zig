pub const bcp = @import("bcp.zig");
pub const blobcache = @import("blobcache.zig");
pub const unix_client = @import("unix_client.zig");

pub const protocol = bcp;
pub const types = blobcache;

pub const UnixClient = unix_client.UnixClient;

pub const ENV_BLOBCACHE_API = blobcache.ENV_BLOBCACHE_API;
pub const ENV_BLOBCACHE_NS_ROOT = blobcache.ENV_BLOBCACHE_NS_ROOT;
pub const DEFAULT_ENDPOINT = blobcache.DEFAULT_ENDPOINT;

pub const ActionSet = blobcache.ActionSet;
pub const OID = blobcache.OID;
pub const CID = blobcache.CID;
pub const NodeID = blobcache.NodeID;
pub const Handle = blobcache.Handle;
pub const LinkToken = blobcache.LinkToken;
pub const Endpoint = blobcache.Endpoint;
pub const TxParams = blobcache.TxParams;
pub const PostOpts = blobcache.PostOpts;
pub const GetOpts = blobcache.GetOpts;
pub const DequeueOpts = blobcache.DequeueOpts;
pub const Message = blobcache.Message;
pub const InsertResp = blobcache.InsertResp;

pub const OID_SIZE = blobcache.oid_size;
pub const CID_SIZE = blobcache.cid_size;
pub const HANDLE_SIZE = blobcache.handle_size;
pub const LINK_TOKEN_SIZE = blobcache.link_token_size;
pub const NODE_ID_SIZE = blobcache.node_id_size;

test {
    _ = @import("bcp.zig");
    _ = @import("blobcache.zig");
}
