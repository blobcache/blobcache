# Peer Routing

Peer Routing is managing the information required to send a message to an arbitrary PeerID in the network, without yet knowing anything about their location.

Peering in blobcache is only to trusted nodes, so you only talk to strangers through your friends.
They aren't strangers to your friends after all.

Blobcache takes an approach similar to CJDNS.
Which is to say, we create a kademlia based DHT, where peers store the routes to peers close to them in keyspace, regardless of network distance.
If are unfamiliar with any of that, don't worry, just keep reading.

The algorithm is roughly as follows:
1. Create a cache on your node with a fixed size. Say 128 entries. Keys in the cache are PeerIDs of other nodes. Values in the cache are paths, represented as lists of integers.
When the cache is full and you want to add another entry, find the peer with PeerID farthest from yours in keyspace and remove their entry.
2. Let your "peer list" be your one-hop peers, combined with whatever peers you have paths to in the cache.
So initially just the one-hop peers.
3. Query everyone in your peer list for their peers.
They respond with their peer list, which will contain paths relative to them.
4. Go through their peer list and make the paths relative to you by prepending your path to the peer that gave you the list.
5. Take this new list of (PeerID -> Path) mappings and add them to your cache.  The cache might overflow and forget some entries, that's fine.
6. Wait a little so as not to be annoying, then go back to 3.

The magic is in the cache eviction.
It's just a simple loop that does the same thing over and over again.
But through careful forgetting, the routing information becomes more organized.

Once the routing information has been spread around, and the network has somewhat "settled" you can send a message to any target just by sending it to the peer you know of closest to the target peer in keyspace.
They will know of even closer peers, until someone knows of the target exactly and sends it to them.

## Incentives
This portion of the protocol is not incentivised, other than that knowing about other nodes is useful for fufilling your own requests.
Nodes can set their cache as small as they want and the network will be less resilient as a result, there is no way to prevent this.
Laptops and mobile devices may choose to not participate at all or very little in peer routing.

The peer route table takes a small amount of memory (<1MB), so larger machines, home servers, or cloud VMs will not be bothered by participating in peer routing.
It also gives them the oppurtunity to cache blobs (name of the game), which earns the node favor with its peers.

Further improvments to the protocol may allow nodes to assess the reliability of peers.
If nodes evict based on `floor(leading0s(XOR(localID, peerID))) then count(intersect(localRoutes, peerRoutes))`.
Nodes with fuller tables will find each other, and stitch together around unreliable nodes with smaller caches.
