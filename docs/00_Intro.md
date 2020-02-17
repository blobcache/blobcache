# The Blobcache Storage Network
Blobcache is a web of trust storage network.

## 1. Model
Blobcache is a content addressed data store.
You give it data, it gives you a hash.
You give it back a hash, it gives the data.

### 1.1 [Client API](11_Client_API.md)

## 2. Networking

### 2.1 Transport

Blobcache nodes peer to an explicitly defined lists of peers.
There is no dynamic peering.
Blobcache nodes typically know the IP address of their one hop peers, but not further than that.
The network is friend-to-friend in this way.

Blobcache depends on being able to send `Tell` and `Ask` messages.
- `Tell` Messages are fire and forget.
- `Ask` Messages demand a response or are considered a failure.

These messages are only to one-hop peers and should be encrypted by the transport.
Messages are not encrypted in the overlay network, and can be inspected by intermediate nodes to ensure fairplay and prevent abuse of the network.

### 2.2 [Peer Routing](22_Peer_Routing.md)
### 2.3 [Blob Routing](23_Blob_Routing.md)
### 2.4 [Blob Fetching](24_Blob_Fetching.md)

## 3. Economics
There is one very important criterion that governs much of the design.

> Nodes should never perform actions which are not in their own self interest.

If there is ever a strategy discovered for gaming the protocol that gives nodes an advantage, it should become part of the reference implementation.
Or the protocol should be changed so that the strategy is no longer advantagous.

The protocol should guarantee that if one of your peers tries to exploit you:
1. You will know exactly how they are trying to exploit you.
2. You never stand to loose more than the `trust` you have placed in them.

Nodes should only be connecting to peers they trust to some degree.
So any attempted exploitation will likely result in a real life confrontation in which someone will have some explaining to do.

The whole protocol can be thought of as nodes performing favors for one another in expectation that the favors will be repaid.
Nodes keep track of how many favors they owe, and are owed.
