# Blobcache DRAFT 1

## Persistence
Data can be persisted either locally or on a neighbor, eventually even neighbor's neighbors.
However nodes only negotiate with one-hop neighbors for persistence.

Nodes store the blobs that they want persisted in a Trie, and advertise the root to the network.

- Nodes can keep track of all the blobs they want persisted in a trie.
Just like nodes keep track of balance, and requests made affect that balance, they can keep track of storage provisioned.
- Nodes advertise how much storage they have available.  For other nodes to ask for.
- A node issues a request to another node and asks it to persist a whole trie.
If the requester can pay for the whole manifest for some minimum amount of time, then the receiver stores the content on disk. And deducts from the nodes balance as the time goes on.  If the balance + trust goes below 0, then the blobs become eligible for cache eviction.

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
