
CREATE TABLE blobs (
    cid BLOB PRIMARY KEY,
    salt BLOB,
    data BLOB NOT NULL,
    rc INTEGER NOT NULL DEFAULT 0
), WITHOUT ROWID, STRICT;

CREATE TABLE local_volumes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    root BLOB NOT NULL
), STRICT;

CREATE TABLE local_volume_blobs (
    id INTEGER NOT NULL REFERENCES local_volumes(id),
    cid BLOB NOT NULL REFERENCES blobs(cid),
    txn_id INTEGER NOT NULL,
    PRIMARY KEY (vol_id, cid, txn_id),

    is_delete INTEGER NOT NULL DEFAULT FALSE
), WITHOUT ROWID, STRICT;

CREATE TABLE objects (
    id BLOB PRIMARY KEY,
    created_at INTEGER NOT NULL
), WITHOUT ROWID, STRICT;

CREATE TABLE volumes (
    id BLOB REFERENCES objects(id) PRIMARY KEY,

    max_size INTEGER NOT NULL,
    hash_algo VARCHAR(16) NOT NULL,
    sch VARCHAR(16) NOT NULL,
    backend BLOB NOT NULL,
    -- local_id is NOT NULL for local volumes
    local_id INTEGER REFERENCES local_volumes(id),
), WITHOUT ROWID, STRICT;

CREATE TABLE volumes_volumes (
    from_id BLOB NOT NULL REFERENCES volumes(id),
    to_id BLOB NOT NULL REFERENCES volumes(id),
    PRIMARY KEY (from_id, to_id)
), WITHOUT ROWID, STRICT;

CREATE INDEX idx_volumes_volumes_reverse ON volumes_volumes (to_id);

-- txns are used to make changes to a volume
-- The txn volume_id will reference a local volume
CREATE TABLE txns (
    id BLOB REFERENCES objects(id) PRIMARY KEY,
    volume_id BLOB REFERENCES volumes(id),
    store_id INTEGER NOT NULL REFERENCES stores(id),
    ver INTEGER NOT NULL,
    mutate INTEGER NOT NULL,
    created_at INTEGER NOT NULL
), WITHOUT ROWID, STRICT;

CREATE INDEX idx_txn_volume ON txns (volume_id);