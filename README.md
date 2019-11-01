# blobcache
blobcache is an experiment in creating a Web of Trust storage network, which is also economically aware.

The project is still in the early design phases.
Some of the components have been prototyped, and are available in this repository.

## Goals
- WoT. Peer only with trusted parties.
Extend a certain amount of trust to each party, keep track of balance for each party.
`trust + balance >= 0` *must* always be true to serve requests from that peer.
- Only deal with data (blobs) and hashes.
Abstractions for storing data larger than a blob will not be provided.
[WebFS](https://github.com/brendoncarroll/webfs) can sit on top of this project.
- Create `bridges` to the file system and other storage networks.
- Filesystem `bridge` to help add existing content to the network from the local filesystem without duplicating it.
