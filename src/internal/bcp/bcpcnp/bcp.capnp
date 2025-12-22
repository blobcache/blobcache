@0xe77f58556e08090d;
using Go = import "/go.capnp";

$Go.package("bcpcnp");
$Go.import("blobcache.io/blobcache/src/internal/bcp/bcpcnp");

# OID is a blobcache object identifier
struct OID {
    w0 @0 :UInt64;
    w1 @1 :UInt64;
}

# Handle is a blobcache Handle
struct Handle {
    target @0: OID;
    secret0 @1: UInt64;
    secret1 @2: UInt64;
}

struct CID {
    w0 @0 :UInt64;
    w1 @1 :UInt64;
    w2 @2 :UInt64;
    w3 @3 :UInt64;
}

struct GetReq {
    tx @0: Handle;
    cid @1 :CID;
    maxSize @2: UInt32;
}

struct GetResp {
    data @0 :Data;
}

struct PostReq {
    volume @0: Handle;
    data @1: Data;
}

struct PostResp {
    cid @0: CID;
}
