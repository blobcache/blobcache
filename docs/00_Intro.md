# Introduction to Blobcache

Blobcache is unlike other storage networks, which use a blockchain to coordinate the buying and selling of storage.
Instead, Blobcache forms a web of trust.
The network is comprised of nodes, which store data for their respective peers.

How much data to store and for whom is managed by human operators.
You might set up a Blobcache node on your desktop, and extend an unlimited quota to your laptop.
You might extend 100GB to your friends' devices.
Maybe they extend the same quota to you.
Maybe they have quotas extended to them by their friends as well.

Forming the web of trust is easy, thanks to [INET256](https://github.com/inet256/inet256).
Adding a peer is as simple as exchanging INET256 addresses.
There are no certificates, static IP addresses, or port-forwarding required.
Communication is always secure, and addresses never need to be changed.

Blobcache is a content-addressed data store. CA-Store for short.
The fudamental operations on CA-Stores are `post(data) -> hash`, and `get(hash) -> data`.
You give it data, and it gives you a hash, you give it a hash, and it gives you back the data.
The hash is cryptographically derrived from the data, so you can be sure you got the right data back.
In addition to those fundamental operations: Blobcache provides methods for listing and deleting data as well.

Blobcache isn't just one CA-Store, but many.
Blobcache creates multiple independent CA-Stores, and data can be added, removed and listed from each store independently from the other stores.
This allows a single Blobcache node to serve multiple applications.
Each application can create as many stores as it needs.
Multiple stores makes it easier for applications to organize the data they create.
Each store can be configured with different levels of replication as well.

Blobcache calls these multiple client facing CA-Stores `PinSets`.
The word "pin" evokes a metaphor of a bulletin board.
If a note has a pin attaching it to the board, it won't fall off.
More than one pin through a note, means that you can remove a pin and the note will still be there, supported by the remaining pins.
The notes in this analogy are data blobs, and the pins represent their membership in a set.
You could imagine that each `PinSet` has different colored pins.
A data blob is considered to be contained in a PinSet if it has a pin of that set's color holding it on the board.

## Use Cases
 - I want the data from my laptop to be backed up to my desktop.
 - I have a lot of extra storage capacity, which I want my family and friends to utilize.
 - I want to trade storage with one of my friends, so that we can both have more redundant backups.

# Table of Contents

## [Client API](./01_Client_API.md)