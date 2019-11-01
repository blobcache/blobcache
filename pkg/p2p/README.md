# p2p
This is a simple p2p networking stack.

## Model
### Network Topology
- **Node** the top level object. The represent a single party in a peer to peer network

- **Edge** desired connection between nodes.
The include a transport, a local and remote address, and a remote peer id.
Edges are assigned a unique positive integer which will never be reused.
Edges correspond to communication mechanisms.
2 peers may be connected by more than one Edge.

- **Transport** Something that provides a communication mechanism. SSH, TLS, QUIC are all examples.

### Application
- **Swarm** Represents a collection of peers all speaking the same protocol.

- **Peer** Associated with a swarm.
Represents another party speaking the same protocol.

- **Message** A message from one peer to another.

Discovery is purposefully not included.
Including discovery by default prevents "friend-to-friend" or darknets.

Giving edges unique integer values allows efficient routing protocols to be developed on top of this stack.
Having edges map to communication mechanisms rather than peers allows those algorithms to incorporate multihoming.
