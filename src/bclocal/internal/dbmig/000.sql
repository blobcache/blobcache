
CREATE TABLE blobs (
    cid BLOB PRIMARY KEY,
    salt BLOB,
    data BLOB,
    rc INTEGER NOT NULL DEFAULT 0
), WITHOUT ROWID, STRICT;

CREATE TABLE stores (
    id INTEGER PRIMARY KEY AUTOINCREMENT
), STRICT;

CREATE TABLE store_blobs (
    store_id INTEGER NOT NULL REFERENCES stores(id),
    cid BLOB NOT NULL REFERENCES blobs(cid),
    PRIMARY KEY (store_id, cid)
), WITHOUT ROWID, STRICT;

CREATE TABLE objects (
    oid BLOB PRIMARY KEY,
    created_at INTEGER NOT NULL
), WITHOUT ROWID, STRICT;

CREATE TABLE handles (
    k BLOB PRIMARY KEY,
    target BLOB NOT NULL REFERENCES objects(oid),
    created_at INTEGER NOT NULL
), WITHOUT ROWID, STRICT;

CREATE TABLE volumes (
    id BLOB REFERENCES objects(oid) PRIMARY KEY,
    root BLOB NOT NULL, 
    -- store_id is NOT NULL for local volumes
    store_id INTEGER REFERENCES stores(id)
), WITHOUT ROWID, STRICT;

-- txns are used to make changes to a volume
-- The txn volume_id will reference a local volume
CREATE TABLE txns (
    id BLOB REFERENCES objects(oid) PRIMARY KEY,
    volume_id BLOB REFERENCES local_volumes(oid),
    store_id INTEGER NOT NULL REFERENCES stores(id),
    created_at INTEGER NOT NULL
), WITHOUT ROWID, STRICT;
