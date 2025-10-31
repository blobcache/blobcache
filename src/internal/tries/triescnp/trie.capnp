@0x86dd963ad5de13d5;
using Go = import "/go.capnp";

$Go.package("triescnp");
$Go.import("blobcache.io/blobcache/src/internal/tries/triescnp");

struct Entry {
    key @0 :Data;
    value @1 :Data;
}

struct Node {
    entries @0 :List(Entry);
}

struct Index {
    ref @0 :Data;
    count @1 :UInt64;
    isParent @2 :Bool;
}
