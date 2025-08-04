# Blobcache

[![GoDoc](https://godoc.org/blobcache.io/blobcache?status.svg)](http://godoc.org/blobcache.io/blobcache)
[<img src="https://discord.com/assets/cb48d2a8d4991281d7a6a95d2f58195e.svg" width="80">](https://discord.gg/TWy6aVWJ7f)

Blobcache allows any device to expose storage over the network as a set of Volumes, each of which holds a hash-linked data structure.

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
$ blobcache daemon-ephemeral --serve-api unix://./blobcache.sock --listen 0.0.0.0:6025
```

### Running the daemon
The following command runs a daemon with state in the specified directory. 
```shell
$ blobcache daemon --state $HOME/.local/blobcache --serve-api unix:///run/blobcache/blobcache.sock --listen 0.0.0.0:6025
```

Once the daemon is running, you should be able to connect to it and start building your application on top of content-addressed storage.

## License
The Blobcache implementation is licensed under GPLv3.

All of the clients are licensed under MPL 2.0

What this means is: if you improve Blobcache you have to make the improvements available, but if you are just using a client you can do whatever you want with it.
