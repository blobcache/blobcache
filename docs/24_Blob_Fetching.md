# Blob Fetching

Fetching is the hot path in blobcache.
Whenever a blob is not available locally it results in a fetch over the network.

Fetching is incentivised.
Nodes only fetch Blobs for nodes which are in good standing with them.

Put explicitly:
> _requestBalance_ + _requestTrust_ > 0

Where _requestBalance_ is the number of requests the node owes it's peer. And the _requestTrust_ is the amount of requests the node is willing to get for free.

This is similar to bittorents tit-for-tat protocol.
Except rather than "optimistically unchoking" peers who have given us nothing for too long, we cut them off.

Nodes keep track of their debts to other nodes (more on that in the Economics section).
The debts can be denominated in anything, but the blob fetching service only cares about _requestBalance_.
The `Balance Server` keeps track of these balances.

## Life of a Fetch Request
A Request carries the following information
```
message GetBlobReq {
    RoutingTag: {
        Dst: PeerID
        Path: []int64
    }
    Found: Boolean
    BlobID: BlobID
}
```

And a response can be one of 2 types
```
message GetBlobRes = Data | Redirect
message Data []byte
message Redirect = GetBlobReq
```

The logic for local requests and remote requests is the same.

#### When a nodes recieves a request it does the following.
1.  Have the balance server "unsettle" 1 request unit.
    - If that fails, return an out of funds error.
    - If it succeeds there will be a timeout associated with it.  The request must complete in less than the timeout (by some margin of safety).
    After the timeout the funds become settled again.

2. Check if we have the blob.
    - If we do, great, we have just paid off a request token.
    - If we don't continue.

3. Check if we know where the blob is.  Do we have a route to it?
    - If we do, create another get request targeted at the peer who has the blob. Set `found = true`.
    - If not continue.

4. Find the peer we know of closest to the blob ID in keyspace.
Send a request for the blob, with the peer's routing information.

5. Wait for a response.

#### When a node recieves a response it does the following:
- If it is data. We did it, we got a blob, put it in the local cache and return it to the caller.
(The caller may be another peer in which case we respond to them).
If the caller is another peer claim their settled funds.
Now we owe them one less request.
Or they owe us one more, depending on how you think about it.
Their balance can go negative if we have issued them trust.

- If it is a redirect. Ensure that the new request is valid, and that we have not gone down that path before. Then send it out.

- If we got nothing, then return not found. If we were doing this for a peer release their funds.


## Byzantine Faults
If we respond to a request, but our peer doesn't get it what happens.
They think they owe us 1 less than we think they do.

This will lead to ledger drift.
Where we disagree with our peers about how much each owes each.

Unfortunately, there is no way around this without getting another peer involved.
https://en.wikipedia.org/wiki/Two_Generals%27_Problem

There are a couple possible solutions/mitigations
- Keep track of the more conservative possible value.
Eventually there will be very large disagreement about balance that impedes traffic.
Humans can get involved and split the difference or solve it through out of band means.
- In addition to returning the blob, we make it freely available for a short time period. (A minute maybe).  It is unlikely that this can be exploited since it will probably not leave requesting peer's own cache in a minute, and in the event of a failure, a retry for the blob will bring the balances in sync.
