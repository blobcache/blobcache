# Blobcache IPC

This package contains an Client and Server implementation of the Blobcache protocol over UNIX sockets.


## Client
NewClient will create a Client.
The Client maintains a connection pool, and will open and close connections as needed to serve all concurrent requests.
