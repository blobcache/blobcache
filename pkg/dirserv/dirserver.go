package dirserv

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"github.com/blobcache/blobcache/pkg/bcdb"
	"github.com/brendoncarroll/go-state"
	"lukechampine.com/blake3"
)

const (
	// handlePrefix prefixes the entries mapping handles to objects
	handlePrefix = "h\x00"
	// o2hPrefix prefixes the entries mapping objects to handles
	o2hPrefix = "o2h\x00"
	// objPrefix prefixes the entries with object information
	objPrefix = "o\x00"
	// idSeq is the key used to hold the next OID in the sequence
	idSeq = "id_seq"
)

type DirServer struct {
	db         bcdb.DB
	rootHandle *Handle
}

func New(db bcdb.DB) *DirServer {
	return &DirServer{
		db: db,
		rootHandle: &Handle{
			ID:     RootOID,
			Secret: makeSecret(),
		},
	}
}

// Resolve resolves a path to Handle and returns an OID
func (s *DirServer) Resolve(ctx context.Context, x Handle, p Path) (OID, error) {
	if err := validatePath(p); err != nil {
		return NullOID, err
	}
	return bcdb.DoRet1(ctx, s.db, false, func(tx bcdb.Tx) (OID, error) {
		oid, err := s.open(tx, x)
		if err != nil {
			return 0, err
		}
		return s.resolve(tx, oid, p)
	})
}

// Open returns a handle to the object at path under x
func (s *DirServer) Open(ctx context.Context, x Handle, p Path) (*Handle, error) {
	if err := validatePath(p); err != nil {
		return nil, err
	}
	return bcdb.DoRet1(ctx, s.db, true, func(tx bcdb.Tx) (*Handle, error) {
		oid, err := s.open(tx, x)
		if err != nil {
			return nil, err
		}
		oid, err = s.resolve(tx, oid, p)
		if err != nil {
			return nil, err
		}
		return s.createHandle(tx, oid)
	})
}

// resolve turns OID + Path => OID
func (s *DirServer) resolve(tx bcdb.Tx, parent OID, p []string) (OID, error) {
	oid := parent
	for _, name := range p {
		oid2, err := s.lookup(tx, oid, name)
		if err != nil {
			return NullOID, err
		}
		oid = oid2
		if oid == NullOID {
			break
		}
	}
	return oid, nil
}

// lookupTx turns OID + string => OID
func (s *DirServer) lookup(tx bcdb.Tx, oid OID, name string) (OID, error) {
	v, err := tx.Get(makeDirEntKey(oid, name))
	if err != nil {
		return NullOID, err
	}
	if v == nil {
		return NullOID, nil
	}
	n, err := parseUint64(v)
	if err != nil {
		return NullOID, err
	}
	return OID(n), nil
}

// openTx turns Handle => OID
// open always returns a non-null OID or an error
func (s *DirServer) open(tx bcdb.Tx, x Handle) (OID, error) {
	if x == *s.rootHandle {
		return RootOID, nil
	}
	v, err := tx.Get(makeHandleKey(x))
	if err != nil {
		return NullOID, err
	}
	if v == nil {
		return NullOID, errors.New("invalid handle")
	}
	return parseOID(v)
}

// Get returns the data associated with an object id
func (s *DirServer) Get(ctx context.Context, x Handle, p Path) (OID, []byte, error) {
	if err := validatePath(p); err != nil {
		return NullOID, nil, err
	}
	return bcdb.DoRet2(ctx, s.db, false, func(tx bcdb.Tx) (OID, []byte, error) {
		oid, err := s.open(tx, x)
		if err != nil {
			return NullOID, nil, err
		}
		oid, err = s.resolve(tx, oid, p)
		if err != nil {
			return NullOID, nil, err
		}
		data, err := tx.Get(makeObjectKey(oid))
		if err != nil {
			return NullOID, nil, err
		}
		return oid, data, nil
	})
}

// Create a node in the directory tree, with optionally associated data.
// Associating data with a node prevents creating children beneath it.
func (s *DirServer) Create(ctx context.Context, x Handle, name string, data []byte) (*Handle, error) {
	return bcdb.DoRet1(ctx, s.db, true, func(tx bcdb.Tx) (*Handle, error) {
		parent, err := s.open(tx, x)
		if err != nil {
			return nil, err
		}
		oid, err := s.create(tx, parent, name, data)
		if err != nil {
			return nil, err
		}
		return s.createHandle(tx, oid)
	})
}

// createTx creates a new child at name, under parent
func (s *DirServer) create(tx bcdb.Tx, parent OID, name string, data []byte) (OID, error) {
	v, err := tx.Get(makeObjectKey(parent))
	if err != nil {
		return NullOID, err
	}
	if len(v) > 0 {
		return NullOID, fmt.Errorf("object has associated data, cannot create child. id=%v data=%q", parent, v)
	}
	childID, err := s.lookup(tx, parent, name)
	if err != nil {
		return NullOID, err
	}
	if childID != NullOID {
		return NullOID, fmt.Errorf("entry already exists")
	}
	oid, err := s.allocateID(tx)
	if err != nil {
		return NullOID, err
	}
	if err := tx.Put(makeObjectKey(oid), data); err != nil {
		return NullOID, err
	}
	if err := tx.Put(makeDirEntKey(parent, name), marshalOID(oid)); err != nil {
		return NullOID, err
	}
	return oid, nil
}

// Ensure is a Create falling back to Resolve if the object already exists.
func (s *DirServer) Ensure(ctx context.Context, x Handle, p Path, data []byte) (OID, error) {
	if err := validatePath(p); err != nil {
		return NullOID, err
	}
	return bcdb.DoRet1(ctx, s.db, true, func(tx bcdb.Tx) (OID, error) {
		oid, err := s.open(tx, x)
		if err != nil {
			return NullOID, err
		}
		return s.ensure(tx, oid, p, data)
	})
}

func (s *DirServer) ensure(tx bcdb.Tx, parent OID, p Path, data []byte) (OID, error) {
	if len(p) == 0 {
		return parent, nil
	}
	childOID, err := s.lookup(tx, parent, p[0])
	if err != nil {
		return NullOID, err
	}
	if childOID == NullOID {
		childOID, err = s.create(tx, parent, p[0], data)
	}
	return s.ensure(tx, childOID, p[1:], data)
}

func (s *DirServer) createHandle(tx bcdb.Tx, oid OID) (*Handle, error) {
	h := Handle{ID: oid, Secret: makeSecret()}
	handleKey := makeHandleKey(h)
	o2hKey := makeO2HKey(oid, h)
	if err := tx.Put(handleKey, uint64Bytes(uint64(oid))); err != nil {
		return nil, err
	}
	if err := tx.Put(o2hKey, nil); err != nil {
		return nil, err
	}
	return &h, nil
}

// Remove removes an entry from a directory, and returns the ID of the deleted object.
func (s *DirServer) Remove(ctx context.Context, h Handle, name string) (OID, error) {
	return bcdb.DoRet1(ctx, s.db, true, func(tx bcdb.Tx) (OID, error) {
		oid, err := s.open(tx, h)
		if err != nil {
			return NullOID, err
		}
		return s.remove(tx, oid, name)
	})
}

func (s *DirServer) remove(tx bcdb.Tx, oid OID, name string) (OID, error) {
	target, err := s.lookup(tx, oid, name)
	if err != nil {
		return NullOID, err
	}
	if target != NullOID {
		k := makeDirEntKey(oid, name)
		if err := tx.Delete(k); err != nil {
			return NullOID, err
		}
	}
	return target, nil
}

// List returns the children in the directory referenced by x.
func (s *DirServer) List(ctx context.Context, x Handle) ([]Entry, error) {
	return bcdb.DoRet1(ctx, s.db, false, func(tx bcdb.Tx) ([]Entry, error) {
		oid, err := s.open(tx, x)
		if err != nil {
			return nil, err
		}
		return s.listTx(tx, oid)
	})
}

func (s *DirServer) listTx(tx bcdb.Tx, x OID) ([]Entry, error) {
	var ents []Entry
	if err := tx.ForEach(makeDirEntSpan(x), func(k, v []byte) error {
		_, name, err := parseDirEntKey(k)
		if err != nil {
			return err
		}
		childID, err := parseUint64(v)
		if err != nil {
			return err
		}
		ents = append(ents, Entry{
			Name: name,
			ID:   OID(childID),
		})
		return nil
	}); err != nil {
		return nil, err
	}
	return ents, nil
}

// Root returns a handle to the root directory.
func (s *DirServer) Root() Handle {
	return *s.rootHandle
}

func (s *DirServer) allocateID(tx bcdb.Tx) (OID, error) {
	n, err := bcdb.Increment(tx, []byte(idSeq))
	if err != nil {
		return NullOID, err
	}
	return OID(n), nil
}

func makeHandleKey(x Handle) (ret []byte) {
	ret = append(ret, []byte(handlePrefix)...)
	h := blake3.New(32, nil)
	h.Write(uint64Bytes(uint64(x.ID)))
	h.Write(x.Secret[:])
	ret = append(ret, h.Sum(nil)...)
	return ret
}

func makeO2HKey(oid OID, h Handle) (ret []byte) {
	ret = append(ret, []byte(o2hPrefix)...)
	ret = append(ret, makeHandleKey(h)...)
	return ret
}

func makeObjectKey(oid OID) (ret []byte) {
	ret = append(ret, []byte(objPrefix)...)
	ret = append(ret, marshalOID(oid)...)
	return ret
}

func makeDirEntKey(oid OID, name string) (ret []byte) {
	ret = append(ret, objPrefix...)
	ret = append(ret, marshalOID(oid)...)
	ret = append(ret, 0x0)
	ret = append(ret, name...)
	return ret
}

func parseDirEntKey(x []byte) (OID, string, error) {
	x = x[len(objPrefix):]
	if len(x) < 10 {
		return 0, "", fmt.Errorf("too short to be dir ent key")
	}
	oid, err := parseOID(x[:8])
	if err != nil {
		return 0, "", err
	}
	if x[8] != 0x0 {
		return 0, "", errors.New("dir ent missing null between oid and name")
	}
	name := string(x[9:])
	return oid, name, nil
}

func makeDirEntSpan(oid OID) state.ByteSpan {
	prefix := makeDirEntKey(oid, "")
	return state.ByteSpan{
		Begin: prefix,
		End:   bcdb.PrefixEnd(prefix),
	}
}

func uint64Bytes(x uint64) []byte {
	buf := [8]byte{}
	binary.BigEndian.PutUint64(buf[:], uint64(x))
	return buf[:]
}

func parseUint64(x []byte) (uint64, error) {
	if len(x) < 8 {
		return 0, fmt.Errorf("too short to be uint64 %q", x)
	}
	return binary.BigEndian.Uint64(x), nil
}

func marshalOID(x OID) []byte {
	return uint64Bytes(uint64(x))
}

func parseOID(x []byte) (OID, error) {
	i, err := parseUint64(x)
	return OID(i), err
}

func makeSecret() (secret [16]byte) {
	if _, err := rand.Read(secret[:]); err != nil {
		panic(err)
	}
	return secret
}

func validatePath(p []string) error {
	for _, name := range p {
		if strings.Contains(name, "\x00") {
			return errors.New("path names cannot contain null")
		}
		if name == "" {
			return errors.New("names cannot be empty")
		}
	}
	return nil
}
