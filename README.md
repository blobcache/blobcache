# Blobcache
Blobcache is an experiment in creating a Web of Trust storage network, which is also economically aware.

Blobcache is still in the early development phases, but many of the components have been prototyped and are available in this repository.

Blobcache depends on [INET256](https://github.com/inet256/inet256) for connections to peers.

To learn more, checkout the [docs](./docs/00_Intro.md)

## Goals
- Store data.  Blobcache ensures data is replicated on peers according to the configuration.
- Create a server process that people are comfortable and confident running all the time, knowing it's not connecting to strangers or altruistically wasting bandwidth, power, or storage.

## Non-Goals
- Merkle data structures. Blobcache only deals with blobs.
- Create a blockchain.
