# Client API

Blobcache at its simplest level is a content-addressed data store.
You give it data up to a maximum size and it gives you a hash.
This hash is called the "ID", "Content ID", or `BlobID` throughout.
Blobcache also allows clients to organize data into sets.
The union of these sets is used to determine what blobs to persist and which ones to eventually forget about.

Persisting data is not something that clients should have to worry about.
Depending on how blobcache is configured it may persist the data locally, or on peers.

There is no notion of files, directories, or content types.
If a client needs a multi-blob data structure they will have to provide that themselves.
Merkle lists, trees, and DAGs are explicit non-goals of this project.
Blobcache does use a multi-blob data structure for some things internally (a Byte Radix Tree), but this is not exposed.

## Blobs
Data identified uniquely by a hash

## BlobIDs
The hash of a blob.

```
POST / // raw data, returns base64 multihash
GET /QmA050gsd0sFfgj... // data for hash.
```

## PinSets
A set of blob ids.

```
POST /ps/ {"name": "My_New_PinSet"}
GET /ps/My_New_PinSet {"name": "My_New_PinSet", Size: 0, root: null}
POST /ps/My_New_PinSet/add/ // Adds an existing blob_id to the set
DELETE /ps/My_New_PinSet
POST /ps/My_New_Pinset // Creates a new blob in the PinSet
GET /psg/My_New_PinSet/QmAg0GSdg... alias for GET /{blob_id}
```

## A Quick Note About Multihash

https://github.com/multiformats/multihash

All IDs which blob cache exposes externally are multihash, however we do not store multihashes throughout the stack.
Multihash needlessly complicates many of the routing algorithms and data structures, and so we try to avoid it where possible.

It does give us the ability to change hash functions without clients noticing; that is very valuable.
We however, will notice if we have to change hash functions, and it will be a lot of effort to add indexing and storage for what is essentially a separate network.
Designing a whole system now for the case where our preferred hash function is compromised is not a good use of effort.
