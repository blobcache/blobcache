# Blob Routing

The blob routing algorithm is similar to the peer routing algorithm, except it must deal with many more entries.
With peer routing it is fine to just cap an in-memory cache at 100 or so, but for blobs, which will likely outnumber peers 1M to 1, the entries must be persisted to disk.
Further, polling for a peer's entire route table every once in a while is no longer an option.
We need an efficient syncing strategy for blob locations, so that we only pull the whole route table once, and then pull deltas after that.

A blob location is a pair (BlobID, PeerID).
They are referred to as `BlobLocs` throughout.
Many `BlobLocs` together make up a route table, provided we can query the table efficiently by BlobID.
A BlobID may be available on more than one peer, so we must have a way to store more than one in any representation, even if in most cases nodes will choose to forget multiple peers.

## Indexing Local Blobs
Blobcache uses a data structure called a Trie or Radix-Tree to store large sets of key value pairs, accessible efficiently by key.
All that you need to know right now is that it has a root BlobID, and it lets you `Get` `Put` and `Delete` key value pairs.

We can use a Trie to hold `BlocLocs`.

Let a `RouteTable` be any Trie structured as ` key = BlobID + PeerID, value = nil`.

To create a `RouteTable` for the local blobs, simply iterate through all the blobs and add them to a Trie, with our own PeerID.
This doesn't do us much good, we already know what blobs we have, and it was more efficient to just check the local database directly, but this RouteTable isn't for us.

Another property of Tries is that they recursively contain Sub-Tries with longer prefixes.
So just like we can refer to all the keys by passing around the BlobID for the root, we can refer to just the keys at prefix "aa" by passing around the BlobID for that Trie.

The `LocalIndexer` creates a `RouteTable` which is efficiently queryable by prefix.
Now when someone asks us for all the blobs with BlobID starting with "abc" we can tell them.

## Syncing Route Tables

There are 3 possible responses when querying for a section of a RouteTable.
- Parent Trie. A Trie which has so many entries that it only contains pointers to smaller Tries.
The pointers are BlobIDs and so they will always point to the same content.
- Leaf Trie.  This is just a list of key value pairs and the associated common prefix.
- Sharded.  We get this response if there is not a single Trie for this prefix.  Blobs may be coming and going so fast that the node cannot keep a single consistent Trie.

To sync someone elses route table to our own we can use the following algorithm
1. Query some prefix.
    - If we get a parent, check if we have seen the `BlobID` of the parent before.
        - If yes, we are done.
        - If no, for each of 256 child pointers, check if we have seen the `BlobID`
            - If yes, do nothing, continue.
            - If no, then ask for that prefix as if it was the root.
    
    - If we get a leaf add the routes to our table.
    - If we get a "Sharded" response, enumerate the 256 prefixes ourself and query for those.

2. Keep track of the BlobID we received at that prefix.

## Maintaining Our Route Table
We want a simple algorithm for maintaining the Blob RouteTable, just like maintaining the Peer RouteTable.
Once again the magic lies in the cache eviction strategy.

We evict entries from the Trie which are far from us in keyspace.
This is slightly more complicated than evicting from an in memory cache.
When we need to evict from Bucket _n_, enumerate all the prefixes for that bucket, and traverse down those prefixes in order until we can delete an entire leaf blob.

This will typically corresspond to hundreds of entries.

> e.g. for _locus_=0xABDC... n=_4_ the prefixes would be all the bytes that start with 0xA_.
>
> 4 means 4 bits match. Each hex digit corresponds to 4 bits.  So the first hex digit must match, the second can be anything.
> 
> There are 16 total prefixes with that description. Here they are: 0xA0, 0xA1, 0xA2, 0xA3 ... 0xA9, 0xAA, 0xAB ... 0xAF.
>
> We choose one of them, say 0xA0, but any would do. We select the Trie at that prefix.  Recall that this is efficient because of the structure of Tries.
If that is parent, then follow it further, again choosing arbitrary bytes, until we get to a child blob.
> Let's say we find one at 0xA000.  We take the BlobID referenced by 0x0A000 and delete it from the store.

Everytime we have to evict a blob we keep track of the bucket number that we evicted it from, and use that to refine our further queries.
There is no point in querying a prefix if every item we get back will just be evicted.

Now that we have covered how to implement a Kademlia cache on top of a Trie.
We can simply query our peers every so often for sections of their route table, and merge them into our own.

Because we have peers close to our key from the Peer Routing algorithm, we have peers looking for similar blobs as us, and we all benefit from finding them.

## Which Peers to Query
In order for this routing system to work.  A node needs to be aware of all blobs arbitrarily near its key.
They can get that information two ways: Either by querying every peer directly, or by querying peers in a web of peers who eventually did query them directly.
If we are mostly querying our one hop neighbors and peers close to us in keyspace, then it may take a long time for blobs close to us in keyspace, but originating on peers far from us in keyspace to make their way to us.


There are a few ways to supplement the Route Table so that we find out about near blobs from far peers more quickly.

### The Crawler Approach
Crawl peers branching out from us, near in network distance. One-hop, Two-Hop, etc. up to some _crawl radius_.
If every peer is doing this then you just have to be within one or two hops from each keyspace "neighborhood" to have all your blobs routable.

### Iterate Over Peers from the Blob Route Table
Instead of querying for `RouteTable` sections from peers in the `PeerRouter's` peer list, use the PeerIDs (potentially millions of distinct IDs) in the `BlobRouter's` table.

This has the advantage of covering a very wide portion of the network, at the expense of leaning on the peer routing system for most of the query's since we will not have paths to these peers.

## Incentives
This part of the protocol is not well incentivised, yet.
Incentivising Blob Routing, unlike Peer Routing, is important since it is potentially expensive.
There is some incentivisation to store routing info in order to cache requests.
Since nodes will often be asked for blobs close to them in keyspace, they will benefit from being able to route to blobs in that range.

There may be a mechanism once the storage service is working.
i.e. Nodes essentially pay to have their blobs indexed by their neighbors, who contract out to other neighbors closer in keyspace.
Nodes could charge more for blobs farther in keyspace, which would cause the route tables to organize.
This would work out to no net value transfer since everyone would be storing for everyone else in roughly equal measure.
The advantage would be that nodes are keeping each other honest about storing routes, so there's much more reason to believe theres's actually no net value transfer.

Since there is no net value transfer, peer routing will remained relatively unincentivised, and network operators are encouraged to deal with good people for the time being.
