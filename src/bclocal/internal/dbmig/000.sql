
CREATE TABLE blobs (
    cid BLOB PRIMARY KEY,
    salt BLOB,
    data BLOB NOT NULL,
    rc INTEGER NOT NULL DEFAULT 0
), WITHOUT ROWID, STRICT;

CREATE TABLE local_vol_blobs (
    vol_id INTEGER NOT NULL REFERENCES local_volumes(rowid),
    cid BLOB NOT NULL REFERENCES blobs(cid),
    txn_id INTEGER NOT NULL,
    is_delete INTEGER NOT NULL DEFAULT FALSE,
    PRIMARY KEY (vol_id, cid, txn_id)
), WITHOUT ROWID, STRICT;

CREATE TABLE local_vol_roots (
    vol_id INTEGER NOT NULL REFERENCES local_volumes(rowid),
    txn_id INTEGER NOT NULL,
    root BLOB NOT NULL,
    PRIMARY KEY (vol_id, txn_id)
), WITHOUT ROWID, STRICT;

-- local_txns stores active transactions on local volumes.
CREATE TABLE local_txns (
    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
    volume INTEGER NOT NULL REFERENCES local_volumes(id),
    -- mutate is true if the transaction is mutating the volume.
    mutate INTEGER NOT NULL,
    -- base is the transaction id, below which all transactions are visible.
    base INTEGER NOT NULL DEFAULT 0
), STRICT;

-- local_volumes stores state for a local volume.
CREATE TABLE local_volumes(
    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
    oid BLOB REFERENCES volumes(id),
    -- base is the transaction id, below which all transactions are visible.
    base INTEGER NOT NULL DEFAULT 0
), STRICT;

CREATE INDEX idx_local_volumes_oid ON local_volumes (oid);

CREATE TABLE objects (
    id BLOB PRIMARY KEY,
    created_at INTEGER NOT NULL
), WITHOUT ROWID, STRICT;

-- volumes stores information common to all volumes.
CREATE TABLE volumes (
    id BLOB REFERENCES objects(id) PRIMARY KEY,
    max_size INTEGER NOT NULL,
    hash_algo TEXT NOT NULL,
    salted INTEGER NOT NULL,
    sch TEXT NOT NULL,
    backend BLOB NOT NULL
), WITHOUT ROWID, STRICT;

-- volume_deps stores dependencies between volumes.
-- volume_deps is set by the volume backend implementation, not by the user.
CREATE TABLE volumes_deps (
    from_id BLOB NOT NULL REFERENCES volumes(id),
    to_id BLOB NOT NULL REFERENCES volumes(id),
    PRIMARY KEY (from_id, to_id)
), WITHOUT ROWID, STRICT;

CREATE INDEX idx_volumes_deps_reverse ON volumes_deps (to_id);

CREATE TABLE subvolumes (
    from_id BLOB NOT NULL REFERENCES volumes(id),
    to_id BLOB NOT NULL REFERENCES volumes(id),
    rights INT NOT NULL,
    PRIMARY KEY (from_id, to_id)
), WITHOUT ROWID, STRICT;

CREATE INDEX idx_subvolumes_reverse ON subvolumes (to_id);
