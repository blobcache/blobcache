# Controller

The Controller is the core of blobcache, it takes in a set of blobs to store (called Sources) and a set of stores to manage (called Sinks).
Then whenever something changes in one of the Sources, the controller checks to see if anything needs to be shuffled around in the Sinks.
If something needs to be done, the Controller enqueues an operation to Add or Delete the blob.
Eventually the queued operations are flushed to the sink as a batch.
