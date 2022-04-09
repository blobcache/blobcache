# Client API

Blobcache provides a gRPC API for client applications to consume.
It listens on `localhost` port `6025` by default.
Clients can be found underneath the `client` directory.

Blobcache organizes blobs into sets which it calls `PinSets`.
It organizes `PinSets` into a directory tree like a file system.
`PinSets` can exist at any name in the directory structure. e.g. `myapp/pinset1`, `myapp/pinset2`

Instead of a permissions system, blobcache uses handles, which contain a secret value, to control access to resources.
If you have the handle to a `PinSet` or `Directory`, you can perform operations on the `PinSet` or `Directory`.
If applications want to protect a PinSet from being modified, they should be careful to keep the `PinSet` handle safe.
`PinSets` do not exist in any global sense; they are a node-local concept, and only exist to facilitate client maniuplation of blobs.
Handles on one node will be invalid on another node.

A few notes on notation.  API methods are described using `<method name> (args) -> result`, and parameters are typed with the notation `<parameter_name>: <Type>`.

# Directory Operations

## `CreateDir(h: Handle, name: string) -> Handle`
Creates a new child directory at name under the parent directory referenced by h.

## `ListEntries(h: Handle, name: string) -> []Entry`
Returns the entries under the directory referenced by h.

## `Open(h: Handle, path: []string) -> Handle`
Open resolves a path relative to the directory at h to get to another object; it returns a handle to that object.

## `DeleteEntry(h: Handle, name string)`
Removes the entry at name in the directory referenced by h from the directory.
If there is no entry DeleteEntry does not error.

# PinSet Operations

## `CreatePinSet(h: Handle, name: string, opts: PinSetOptions) -> Handle`
Creates a new PinSet.

## `GetPinSet(h: Handle, name: string, pinset: Handle) -> PinSet`
Returns information about a PinSet

# Content-Addressed Store Operations
## `Post(pinset: Handle, data: []byte) -> ID`
Adds data to a pinset.

## `Get(pinset: Handle, blobID: ID) -> []byte`
Retrieves the data by hash ID from a PinSet

## `Exists(pinset: Handle, id: ID) -> bool`
Returns true if the PinSet contains data with an ID and false otherwise.

> In the gRPC API, this is implemented in terms of List

## `Delete(pinset: Handle, blobID: ID)`
Removes data by hash ID from a PinSet

## `List(pinset: Handle, first: []byte, limit: int) -> []ID`
Lists the data in a PinSet.
`first` is used as an offset, all the IDs will be >= `first`.
The number of IDs returned will be <= `limit`.

## `Add(pinset: Handle, id: ID)`
Adds data to a pinset by ID.
This is an optimization to shortcut sending a whole blob to the node.
It can fail if the data doesn't exist in any other set, and the client will have to fallback to `Post`.
