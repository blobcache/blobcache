@0x86dd963ad5de13d5;
using Go = import "/go.capnp";

$Go.package("triescnp");
$Go.import("blobcache.io/blobcache/src/internal/tries/triescnp");

struct Index {
    # ref is a reference to a Node.
    ref @0 :Data;
    # count is the cumulative number of entries transitively reachable from this index.
    count @1 :UInt64;
}

# Entry contains a key and either a value, which is arbitrary data, or an index, which refers to another Node.
struct Entry {
    key @0 :Data;
    union {
        # value is arbitrary data.
        value @1 :Data;
        # index is a reference to another Node.
        index @2 :Index;
        # vnode is a nested Node.
        # all of this nodes entries share a prefix.
        vnode @3 :Node;
    }
}

struct Node {
    entries @0 :List(Entry);
}
