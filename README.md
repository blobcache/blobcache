# blobcache
blobcache is an experiment in creating a Web of Trust storage network, which is also economically aware.

The project is still in the early design phases.
Some of the components have been prototyped, and are available in this repository.

Take a look in `./docs` for design documents.

## Goals
- Store data. Blobcache figures out with who and for how long, and how much will be owed in exchange.
- Request data. A node should be able to request any data on the network, even data which it did not post.
- Create a server process that people are comfortable and confident running all the time, knowing it's not connecting to strangers or altruistically wasting bandwidth, power, or storage.

## Non-Goals
- Merkle data structures. Blobcache only deals with blobs.
- Create a blockchain. Blobcache is more like Ripple or Hyperledger, less like Bitcoin.

## Comparison to IPFS
- Friend-To-Friend.  Blobcache peering is explicitly to known hosts.
IPFS connects to random strangers on the internet.
- Persistence is incentivised.
Blobcache nodes have an incentive to store data. Either to payoff an out-of-band exchange of value or pay off a previous storage agreement.
IPFS nodes tacitly seed content after requesting it.
- Blobcache has no opinion on data structures.  No files, no directories, no "merkle-dag" objects, no "self-describing" formats wrapping everything.
