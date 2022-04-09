# Blobcache

[![GoDoc](https://godoc.org/github.com/blobcache/blobcache?status.svg)](http://godoc.org/github.com/blobcache/blobcache)
![Matrix](https://img.shields.io/matrix/blobcache:matrix.org?label=%23blobcache%3Amatrix.org&logo=matrix)
[<img src="https://discord.com/assets/cb48d2a8d4991281d7a6a95d2f58195e.svg" width="80">](https://discord.gg/TWy6aVWJ7f)

Blobcache is a content addressed data store, designed to be a replicated data layer for applications.

For more information checkout a brief [Introduction](./docs/00_Intro.md).
And the gRPC API [docs](./docs/01_Client_API.md).

## Goals
- [x] Define a [simple API](./docs/01_Client_API.md) for content-addressed storage suitable for use by multiple applications.
- [x] Efficiently multiplex multiple logical content-addressed stores onto a single real store backed by the file system.
- [ ] Store data with other Blobcache peers.
- [ ] Store data with cloud object storage providers.

### Web Of Trust Storage Network
Blobcache is an experiment in creating a Web of Trust storage network, which is also economically aware.

The peer-to-peer components of Blobcache are still in the early development phases, but many of the components have been prototyped and are available in this repository.

Blobcache depends on [INET256](https://github.com/inet256/inet256) for connections to peers.

## Non-Goals
- Create a blockchain or cryptocurrency.
- Store data with untrusted peers.
- Altruistic data storage and retrieval like BitTorrent or IPFS.
- Merkle data structures. Blobcache only deals with blobs.

## Getting Started
You should be able to install with
```shell
$ go install ./cmd/blobcache
```

And then if `${go env GOPATH}/bin` is on your path, you should be able to run the blobcache command with
```shell
$ blobcache 
```

or to put the executable somewhere specific
```shell
$ go build -o ./put/the/binary/here/please ./cmd/blobcache 
```

### Running a Node in Memory
This is a good option if you just want to play around with the API, and don't want to persist any data, or connect to peers.

```shell
$ blobcache island
```

### Creating a Private Key
Blobcache requires a private key for each instance.
This is used to uniquely identify the instance and for communicating securely with peers using [INET256](https://github.com/inet256/inet256)

```shell
$ blobcache keygen > blobcache_pk.pem
```

### Running the daemon
The following commands create a directory for blobcache data and runs a daemon on localhost on the default port.  Change the name of the private-key to whereever your key is.
```shell
$ mkdir my-data
$ blobcache run --private-key ./blobcache_pk.pem --data-dir my-data
```

Once the daemon is running, you should be all set to connect to it and start building your application on top of content-addressed storage.

## Help, I need to store data larger than a single Blobcache blob
Blobcache is not a filesystem or object store.  It only allows storing blobs up to a fixed maximum size (currently 2MB).
It is more of a building block than a storage solution on it's own.
 
Take a look at [glfs](https://github.com/blobcache/glfs), a Git-Like FileSystem which breaks apart large files into chunks which will fit into Blobcache.

Also take a look at [GotFS](https://github.com/gotvc/got/tree/master/pkg/gotfs) which is a more complicated, but in many ways more efficient, alternative to glfs.

## Help, I need to store many small things efficiently in Blobcache
Take a look at [GotKV](https://github.com/gotvc/got/tree/master/pkg/gotkv), an immutable key-value store on top of content-addressable storage.
It's Blobcache compatible.

## Community
You can get in touch via either:
- Our dedicated Matrix room `#blobcache:matrix.org`
- The INET256 Discord, in the `#applications` channel.

## License
The Blobcache implementation is licensed under GPLv3.

All of the clients are licensed under Apache 2.0

What this means is: if you improve Blobcache you have to make the improvements available, but if you are just using a client you can do whatever you want with it.
