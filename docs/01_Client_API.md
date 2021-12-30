# Client API

Blobcache provides an HTTP API for client applications to consume.
It listens on `localhost` on port `6025` by default.
Clients can be found underneath the `client` directory.

Blobcache organizes blobs into sets which it calls `PinSets`.
Instead of a permissions system, blobcache uses handles which contain a secret value to control access to resources.
If you have the handle to a PinSet, you can perform operations on the PinSet.
If applications want to protect a PinSet from being modified, they should be careful to keep the PinSet handles safe.
PinSets do not exists in any global sense; they are a node-local concept, and only exist to facilitate client maniuplation of blobs.
Handles on one node will be invalid on another node.

In the HTTP API, anywhere a PinSetID is used, the header `X-Handle-Secret` must be set to the base64 encoding of the 16 byte secret for the handle.

A few notes on notation.  API methods are described using `<method name> (args) -> result`, and parameters are typed with the notation `<parameter_name>: <Type>`.

# PinSet Operations

## `CreatePinSet(opts: PinSetOptions) -> PinSetHandle`
Creates a new PinSet.

```POST /s/```

## `GetPinSet(pinset: PinSetHandle) -> PinSet`
Returns information about a PinSet

```GET /s/{pinSetID}```


## `DeletePinSet(pinset: PinSetHandle)`
Deletes a PinSet

```DELETE /s/{pinSetID}```

# Content-Addressed Store Operations
## `Post(pinset: PinSetHandle, data: []byte) -> ID`
Adds data to a pinset.

```POST /s/{pinSetID}```

## `Get(pinset: PinSetHandle, blobID: ID) -> []byte`
Retrieves the data by hash ID from a PinSet

```GET /s/{pinSetID}/{blobID}```

## `Exists(pinset: PinSetHandle, id: ID) -> bool`
Returns true if the PinSet contains data with an ID and false otherwise.

The HTTP API implements this method in terms of `List`, described below.

## `Delete(pinset: PinSetHandle, blobID: ID)`
Removes data by hash ID from a PinSet

```DELETE /s/{pinSetID}/{blobID}```

## `List(pinset: PinSetHandle, first: []byte, limit: int) -> []ID`
Lists the data in a PinSet.
`first` is used as an offset, all the IDs will be >= `first`.
The number of IDs returned will be <= `limit`.

```GET /s/{pinSetID}?first={base64 bytes}&limit={int}```

## `Add(pinset: PinSetHandle, id: ID)`
Adds data to a pinset by ID.
This is an optimization to shortcut sending a whole blob to the node.
It can fail if the data doesn't exist in any other set, and the client will have to fallback to `Post`.

```PUT /s/{pinSetID}/{blobID}```
