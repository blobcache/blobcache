# Blobcache

[![GoDoc](https://godoc.org/github.com/blobcache/blobcache?status.svg)](http://godoc.org/github.com/blobcache/blobcache)
![Matrix](https://img.shields.io/matrix/blobcache:matrix.org?label=%23blobcache%3Amatrix.org&logo=matrix)
[<img src="https://discord.com/assets/cb48d2a8d4991281d7a6a95d2f58195e.svg" width="80">](https://discord.gg/TWy6aVWJ7f)

Blobcache is a content addressed data store, designed to be a replicated data layer for applications.
If you can express your application state as a copy-on-write data structures, then Blobcache can handle syncing and replication for you.

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
$ blobcache daemon-ephemeral --serve-api unix://./blobcache.sock
```

### Running the daemon
The following command runs a daemon with state in the specified directory. 
```shell
$ blobcache daemon --state $HOME/.local/blobcache --serve-api unix:///run/blobcache/blobcache.sock
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
