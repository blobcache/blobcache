# Blobcache Draft 1

## Client API
Blobcache at its simplest level is a content-addressed data store.
You give it data up to a maximum size and it gives you a hash.
This hash is called the "ID" or "Content ID" throughout.
Blobcache also allows clients to organize data into sets.
The union of these sets is used to determine what blobs to persist and which ones to eventually forget about.

Persisting data is not something that clients should have to worry about.
Depending on how blobcache is configured it may persist the data locally, or on peers.
The 

There is no notion of files, directories, or content types.
If a client needs a multi-blob data structure they will have to provide that themselves.
Merkle lists, trees, and DAGs are explicit non-goals of this project.
Blobcache does use a multi-blob data structure for some things internally (a Byte Radix Tree), but this is not exposed.

### Blobs
Data identified uniquely by a hash

```
POST / // raw data, returns base64 multihash
GET /QmA050gsd0sFfgj... // data for hash.
```

### PinSets
A set of blob ids.

```
POST /ps/ {"name": "My_New_PinSet"}
GET /ps/My_New_PinSet {"name": "My_New_PinSet", Size: 0, root: null}
POST /ps/My_New_PinSet/add/ // Adds an existing blob_id to the set
DELETE /ps/My_New_PinSet
POST /ps/My_New_Pinset // Creates a new blob in the PinSet
GET /psg/My_New_PinSet/QmAg0GSdg... alias for GET /{blob_id}
```

## Persistence
Data can be persisted either locally or on a neighbor, eventually even neighbor's neighbors.
However nodes only negotiate with one-hop neighbors for persistence.

Nodes store the blobs that they want persisted in a Trie, and advertise the root to the network.

- Nodes can keep track of all the blobs they want persisted in a trie.
Just like nodes keep track of balance, and requests made affect that balance, they can keep track of storage provisioned.
- Nodes advertise how much storage they have available.  For other nodes to ask for.
- A node issues a request to another node and asks it to persist a whole trie.
If the requester can pay for the whole manifest for some minimum amount of time, then the receiver stores the content on disk. And deducts from the nodes balance as the time goes on.  If the balance + trust goes below 0, then the blobs become eligible for cache eviction.


## Network Topology
Nodes only connect to peers in an explicit list.
There is no dynamic peering.
The network is friend-to-friend in this way.

However communication beyond one hop is required.

## Routing
Nodes assign an integer to each new connection they make to a peer.
The indexes will not be reused by another connection.
The edge index is used when specifying paths for routing.

The nodes form an overlay network which uses source routing to forward messages beyond one hop.
Nodes query their neighbors for peers, and then record the path in the overlay network to those peers.
A routing entry is a pair `(PeerID, Path)`
The entries are stored in a cache, which evicts entries for peers far away from the node's own ID in key space (Kademlia).
The Nodes constantly query the peers in the cache for more peers.

Messages are forwarded to nodes with closer IDs to their destination, unless there is a path specified.  In that case the message is forwarded along the path.

Messages are not encrypted, and can be inspected by intermediate nodes to ensure fairplay and prevent abuse of the network.

## Economics
There is one very important criterion that governs much of the design.

> Nodes should never perform actions which are not in their own self interest.

If there is ever a strategy discovered for gaming the protocol that gives nodes an advantage, it should become part of the reference implementation.
Or the protocol should be changed so that the strategy is no longer advantagous.

The protocol should gaurantee that if one of your peers tries to exploit you:
1. You will know exactly how they are trying to exploit you.
2. You never stand to loose more than the `trust` you have placed in them.

Nodes should only be connecting to peers they trust to some degree.
So any attempted exploitation will likely result in a real life confrontation in which someone will have some explaining to do.

The whole protocol can be thought of as nodes performing favors for one another in expectation that the favors will be repaid.
Favors are denominated in "credits".

Nodes keep track of the credit balance between them.

### Price Setting
This will likely become more sophisticated over time.

Nodes will never do anything for free.
The price of a request will always be `>0`.

Nodes list `Quotes` for each action.
Quotes contain the following information:
```
Quote {
    action: String
    credits: Int64
    expires_at: Timestamp,
}
```

Requests in other parts of the protocol can reference a particular quote by hash.
If a node renegs on a quote the requester should select another node.
This disinsentivises nodes from raising the cost until they find the requesters max price.
Of course if the requester has no other peers than the requestee effectively has a monopoly and can suss out the max price.

The price setting algorithm starts with the price at the minimum (1 credit by default).
If nodes are getting too many requests they should increase the price so that the number of requests falls to a managable level.


## Blob Requests
Requests for blobs require a swarm that supports `Asks`.  Requests contain the following.
```
blob_id: []byte
dst_id: []byte
path: []int
```

- Nodes index all of the blobs they are storing locally in a trie.
Other nodes request sections of the trie by prefix, nodes are expected to know where blobs close to their node ID are on the network.

- Nodes earn balance with one another by replying to requests.
If a node doesn't have the requested data it forwards the request to the neighbo
