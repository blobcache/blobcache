package tries

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/brendoncarroll/blobcache/pkg/blobs"
	log "github.com/sirupsen/logrus"
)

var enc = base64.URLEncoding

const (
	HeaderSize    = 8
	ChildrenPer   = 256
	ChildrenSize  = blobs.IDSize * ChildrenPer
	EntryOverhead = 4
)

type Pair struct {
	Key   []byte
	Value []byte
}

type Trie interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Put(ctx context.Context, key, value []byte) error
	Delete(ctx context.Context, key []byte) error

	// parent
	IsParent() bool
	GetChild(context.Context, byte) (Trie, error)
	GetChildRef(byte) blobs.ID

	// entries
	ListEntries() []Pair
	GetEntry(key []byte) *Pair

	GetPrefix() []byte
	Marshal() []byte
	Pretty() string

	isTrie()
}

func New(store blobs.GetPostDelete) Trie {
	return newTrie(store, false, 0, 0)
}

func NewWithPrefix(store blobs.GetPostDelete, prefix []byte) Trie {
	t := newTrie(store, false, len(prefix), 0)
	t.setPrefix(prefix)
	return t
}

func Parse(store blobs.GetPostDelete, data []byte) (Trie, error) {
	return parse(store, data)
}

func Equal(a, b Trie) bool {
	idA := blobs.Hash(a.Marshal())
	idB := blobs.Hash(b.Marshal())
	return bytes.Compare(idA[:], idB[:]) == 0
}

/*
The design is motivated by the idea that we want to concatenate
a prefix of arbitrary length, a table of child refs (which is always length=256),
and and arbitrary amount of key-value pairs.
The first 8 bytes are used to store offsets that describe where those structures are located in the blob.
That being said, there aren't that many was to represent that information, so the design is relatively straight forward.

each word is 16-bits or 2 bytes
0 1     | 2 3            | 4 5           | 6 7
version | children-start | entries-start | entries-end

prefix is the bytes [8:children-start]
children is a 256 long table where each entry is 32 bytes
entries is a table of arbitrary length containing (key-start, key-stop) pairs.
the end of the last value is the end of the buffer.
*/
type trie struct {
	store blobs.GetPostDelete
	buf   []byte

	changes map[string]*Pair
}

func sizeOf(isParent bool, prefixLen, entryCount, entrySize int) int {
	childrenSize := 0
	if isParent {
		childrenSize = ChildrenSize
	}
	return HeaderSize +
		prefixLen +
		childrenSize +
		entryCount*EntryOverhead +
		entrySize
}

func newTrie(store blobs.GetPostDelete, isParent bool, prefixLen int, entryCount int) *trie {
	size := sizeOf(isParent, prefixLen, entryCount, 0)
	buf := make([]byte, size)
	if isParent {
		if entryCount > 1 {
			panic("parents cannot have more than 1 entry")
		}
		putu16(buf[2:], uint16(8+prefixLen))
		putu16(buf[4:], uint16(8+prefixLen+ChildrenSize))
		putu16(buf[6:], uint16(8+prefixLen+ChildrenSize+EntryOverhead*entryCount))
	} else {
		putu16(buf[2:], 0)
		putu16(buf[4:], uint16(8+prefixLen))
		putu16(buf[6:], uint16(8+prefixLen+EntryOverhead*entryCount))
	}
	return &trie{store: store, buf: buf}
}

func parse(store blobs.GetPostDelete, data []byte) (*trie, error) {
	if len(data) < 8 {
		return nil, errors.New("parse error, buffer too short")
	}
	t := &trie{store: store, buf: data}
	if err := t.validate(); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *trie) IsParent() bool {
	// checks for a valid children-start pointer
	x := getu16(t.buf[2:])
	return x >= 8
}

func (t *trie) Get(ctx context.Context, key []byte) ([]byte, error) {
	prefix := t.getPrefix()
	if !bytes.HasPrefix(key, prefix) {
		panic("can't get that key from this trie, wrong prefix")
	}

	if pair, exists := t.changes[string(key)]; exists {
		if pair == nil {
			return nil, nil
		}
		return pair.Value, nil
	}

	if t.IsParent() {
		c := key[len(prefix)]
		child, err := t.GetChild(ctx, c)
		if err != nil {
			return nil, err
		}
		return child.Get(ctx, key)
	}

	l := t.numEntries()
	for i := 0; i < l; i++ {
		pair := t.getEntry(i)
		if bytes.Compare(pair.Key, key) == 0 {
			return pair.Value, nil
		}
	}

	return nil, nil
}

func (t *trie) GetChild(ctx context.Context, i byte) (Trie, error) {
	return t.getChild(ctx, i)
}

func (t *trie) getChild(ctx context.Context, i byte) (*trie, error) {
	if !t.IsParent() {
		panic("called GetChild on non-parent")
	}
	ref := t.getChildRef(i)
	data, err := t.store.Get(ctx, ref)
	if err != nil {
		return nil, err
	}
	return parse(t.store, data)
}

func (t *trie) GetChildRef(i byte) blobs.ID {
	if !t.IsParent() {
		panic("called GetChildRef on non-parent")
	}
	return t.getChildRef(i)
}

func (t *trie) ListEntries() (pairs []Pair) {
	listed := map[string]bool{}
	l := t.numEntries()
	for i := 0; i < l; i++ {
		pair := t.getEntry(i)
		if pair2, exists := t.changes[string(pair.Key)]; exists {
			if pair2 != nil {
				pair = *pair2
			}
			listed[string(pair.Key)] = true
		}
		pairs = append(pairs, pair)
	}
	for _, pair := range t.changes {
		if pair == nil {
			continue
		}
		if !listed[string(pair.Key)] {
			pairs = append(pairs, *pair)
		}
	}
	sortPairs(pairs)
	return pairs
}

func (t *trie) GetEntry(key []byte) *Pair {
	if pair, exists := t.changes[string(key)]; exists {
		return pair
	}

	// TODO: binary search
	l := t.numEntries()
	for i := 0; i < l; i++ {
		pair := t.getEntry(i)
		if bytes.Compare(key, pair.Key) == 0 {
			return &pair
		}
	}

	return nil
}

func (t *trie) Put(ctx context.Context, k, v []byte) error {
	return t.put(ctx, Pair{k, v})
}

func (t *trie) put(ctx context.Context, pair Pair) error {
	prefix := t.getPrefix()
	if !bytes.HasPrefix(pair.Key, prefix) {
		panic("can't put that key in this trie, wrong prefix")
	}

	if t.IsParent() {
		if len(pair.Key) == len(prefix) {
			t.changes = map[string]*Pair{
				string(pair.Key): &pair,
			}
			return nil
		}

		c := pair.Key[len(prefix)]
		return t.replaceChild(ctx, c, func(child *trie) (*trie, error) {
			if err := child.put(ctx, pair); err != nil {
				return nil, err
			}
			return child, nil
		})
	}

	if t.changes == nil {
		t.changes = make(map[string]*Pair, 1)
	}
	t.changes[string(pair.Key)] = &pair
	if sizeOf(false, len(prefix), t.netAdditions(), t.netSizeChange()+t.entriesSize()) <= blobs.MaxSize {
		return nil
	}

	// need to split
	parent, err := t.split(ctx)
	if err != nil {
		return err
	}
	*t = *parent
	return nil
}

func (t *trie) split(ctx context.Context) (*trie, error) {
	if t.netAdditions()+t.numEntries() == 1 {
		return nil, errors.New("cannot split trie")
	}

	prefix := t.getPrefix()
	entriesSplit := [256][]Pair{}
	for _, pair := range t.ListEntries() {
		if len(pair.Key) == len(prefix) {
			continue
		}
		c := pair.Key[len(prefix)]
		entriesSplit[int(c)] = append(entriesSplit[int(c)], pair)
	}

	children := [256]blobs.ID{}
	for i := range children {
		// create child
		prefix2 := append(prefix, byte(i))
		child := newTrie(t.store, false, len(prefix2), len(entriesSplit[i]))
		child.setPrefix(prefix2)

		// add all pairs
		for _, pair := range entriesSplit[i] {
			if err := child.put(ctx, pair); err != nil {
				return nil, err
			}
		}

		// marshal and post the child
		id, err := t.store.Post(ctx, child.Marshal())
		if err != nil {
			return nil, err
		}
		children[i] = id
	}

	entryCount := 0
	if t.GetEntry(prefix) != nil {
		entryCount = 1
	}
	parent := newTrie(t.store, true, len(prefix), entryCount)
	for i := range children {
		parent.setChildRef(byte(i), children[i])
	}
	if pair := t.GetEntry(prefix); pair != nil {
		parent.appendEntry(0, *pair)
	}
	return parent, nil
}

func (t *trie) Delete(ctx context.Context, key []byte) error {
	prefix := t.getPrefix()
	if !bytes.HasPrefix(key, prefix) {
		panic("can't delete that key from this trie, wrong prefix")
	}

	if t.IsParent() {
		if len(key) == len(prefix) {
			if len(t.changes) > 0 {
				t.changes = nil
			}
			return nil
		}
		c := key[len(prefix)]
		return t.replaceChild(ctx, c, func(child *trie) (*trie, error) {
			if err := child.Delete(ctx, key); err != nil {
				return nil, err
			}
			return child, nil
		})
	}

	if t.changes == nil {
		t.changes = make(map[string]*Pair, 1)
	}

	t.changes[string(key)] = nil
	return nil
}

func (t *trie) replaceChild(ctx context.Context, c byte, fn func(*trie) (*trie, error)) error {
	x, err := t.getChild(ctx, c)
	if err != nil {
		return err
	}
	y, err := fn(x)
	if err != nil {
		return err
	}
	if y == nil {
		panic("child's replacement cannot be nil")
	}
	data := y.Marshal()
	id, err := t.store.Post(ctx, data)
	if err != nil {
		return err
	}
	oldID := t.getChildRef(c)
	t.setChildRef(c, id)
	if err := t.store.Delete(ctx, oldID); err != nil {
		log.Error("deletion failed:", err)
	}

	return nil
}

func (t *trie) GetPrefix() []byte {
	return t.getPrefix()
}

func (t *trie) Marshal() []byte {
	if len(t.changes) < 1 {
		return t.buf
	}
	if t.IsParent() {
		return t.buf
	}

	l := t.numEntries()
	entriesMap := make(map[string][]byte, l+len(t.changes))
	for i := 0; i < l; i++ {
		ent := t.getEntry(i)
		entriesMap[string(ent.Key)] = ent.Value
	}
	for _, ent := range t.changes {
		entriesMap[string(ent.Key)] = ent.Value
	}
	entries := []Pair{}
	for k, v := range entriesMap {
		entries = append(entries, Pair{
			Key:   []byte(k),
			Value: v,
		})
	}
	sortPairs(entries)

	prefix := t.getPrefix()
	t2 := newTrie(t.store, false, len(prefix), len(entries))
	t2.setPrefix(prefix)

	for i := range entries {
		t2.appendEntry(i, entries[i])
	}

	//*t = *t2
	return t2.Marshal()
}

func (t *trie) LenEntries() int {
	return t.numEntries() + t.netAdditions()
}

func (t *trie) Pretty() string {
	sb := strings.Builder{}
	if t.IsParent() {
		sb.WriteString("CHILDREN:\n")
		for i := 0; i < 256; i++ {
			id := t.GetChildRef(byte(i))
			sb.WriteString(fmt.Sprintf("\t%x: %v", byte(i), id))
		}
	}
	sb.WriteString(fmt.Sprintf("ENTRIES: (%d)\n", t.numEntries()))
	for i := 0; i < t.numEntries(); i++ {
		pair := t.getEntry(i)
		sb.WriteString(fmt.Sprintf("\t%d %v\n", i, pair))
	}
	if len(t.changes) > 0 {
		sb.WriteString(fmt.Sprintf("CHANGES:\n"))
		for key, pair := range t.changes {
			if pair != nil {
				sb.WriteString(fmt.Sprintf("\t+%x %x\n", key, pair.Value))
			} else {
				sb.WriteString(fmt.Sprintf("\t-%x", key))
			}
		}
	}

	return sb.String()
}

func (t *trie) netAdditions() int {
	na := 0
	for _, pair := range t.changes {
		if pair != nil {
			na++
		} else {
			na--
		}
	}
	return na
}

func (t *trie) netSizeChange() int {
	prefix := t.getPrefix()
	size := 0
	for key, pair := range t.changes {
		if pair != nil {
			size += len(pair.Key) - len(prefix) + len(pair.Value)
		} else {
			pair := t.GetEntry([]byte(key))
			if pair != nil {
				size -= len(pair.Key) - len(prefix) + len(pair.Value)
			}
		}
	}
	return size
}

func (t *trie) getChildRef(i byte) blobs.ID {
	if !t.IsParent() {
		panic("cannot getChild on non-parent")
	}
	childrenStart := getu16(t.buf[2:])
	off := int(childrenStart) + int(i)*32
	id := blobs.ID{}
	copy(id[:], t.buf[off:off+32])
	return id
}

func (t *trie) setChildRef(i byte, id blobs.ID) {
	if !t.IsParent() {
		panic("cannot setChildRef on non-parent")
	}
	childrenStart := getu16(t.buf[2:])
	off := int(childrenStart) + int(i)*32
	copy(t.buf[off:off+blobs.IDSize], id[:])
}

func (t *trie) entriesStart() int {
	return int(getu16(t.buf[4:]))
}

func (t *trie) entriesEnd() int {
	return int(getu16(t.buf[6:]))
}

func (t *trie) getEntry(i int) Pair {
	if i >= t.numEntries() {
		panic("entry does not exist")
	}
	entriesStart := t.entriesStart()
	entriesEnd := t.entriesEnd()

	off := entriesStart + EntryOverhead*i
	keyStart := int(getu16(t.buf[off:]))
	valueStart := int(getu16(t.buf[off+2:]))
	valueEnd := len(t.buf)
	if off+4 < entriesEnd {
		valueEnd = int(getu16(t.buf[off+4:]))
	}

	return Pair{
		Key:   t.buf[keyStart:valueStart],
		Value: t.buf[valueStart:valueEnd],
	}
}

func (t *trie) numEntries() int {
	entriesStart := getu16(t.buf[4:])
	entriesEnd := getu16(t.buf[6:])
	if entriesEnd < entriesStart {
		entriesEnd = entriesStart
	}
	return int(entriesEnd-entriesStart) / EntryOverhead
}

func (t *trie) entriesSize() int {
	return t.numEntries() * EntryOverhead
}

func (t *trie) setEntryOffsets(i, keyStart, valueStart int) {
	entriesStart := t.entriesStart()
	off := entriesStart + EntryOverhead*i
	putu16(t.buf[off:], uint16(keyStart))
	putu16(t.buf[off+2:], uint16(valueStart))
}

// appendEntry appends the entry to the buffer and sets the pointers in
// the entry table at i.
func (t *trie) appendEntry(i int, p Pair) {
	key := p.Key
	value := p.Value
	keyStart := len(t.buf)
	valueStart := keyStart + len(key)

	t.buf = append(t.buf, key...)
	t.buf = append(t.buf, value...)
	t.setEntryOffsets(i, keyStart, valueStart)
}

func (t *trie) prefixStart() int {
	return 8
}

func (t *trie) prefixEnd() int {
	childrenStart := getu16(t.buf[2:])
	entriesStart := getu16(t.buf[4:])
	prefixEnd := childrenStart
	if prefixEnd < 8 {
		prefixEnd = entriesStart
	}
	if prefixEnd < 8 {
		prefixEnd = 8
	}
	return int(prefixEnd)
}

func (t *trie) getPrefix() []byte {
	return t.buf[t.prefixStart():t.prefixEnd()]
}

func (t *trie) setPrefix(p []byte) {
	prefixStart := t.prefixStart()
	prefixEnd := t.prefixEnd()
	if prefixEnd-prefixStart != len(p) {
		panic("buffer must be setup for prefix of this size")
	}
	copy(t.buf[prefixStart:prefixEnd], p)
}

func (t *trie) validate() error {
	childrenStart := int(getu16(t.buf[2:]))
	if int(childrenStart) >= len(t.buf) {
		return &PointerError{
			Name:   "children-start",
			Ptr:    childrenStart,
			BufLen: len(t.buf),
		}
	}
	entriesStart := t.entriesStart()
	if entriesStart > len(t.buf) {
		return &PointerError{
			Name:   "entries-start",
			Ptr:    entriesStart,
			BufLen: len(t.buf),
		}
	}
	entriesEnd := t.entriesEnd()
	if entriesEnd > len(t.buf) {
		return &PointerError{
			Name:   "entriesEnd",
			Ptr:    entriesEnd,
			BufLen: len(t.buf),
		}
	}
	if entriesEnd < entriesStart {
		return fmt.Errorf("entries-end < entries-start %v %v", entriesStart, entriesEnd)
	}

	if childrenStart != 0 && entriesStart != 0 {
		if entriesStart-childrenStart != ChildrenSize {
			return fmt.Errorf("children table is wrong size: %d", entriesStart-childrenStart)
		}
	}

	// check entries
	l := t.numEntries()
	for i := 0; i < l; i++ {
		var (
			keyStart   = int(getu16(t.buf[entriesStart+i*EntryOverhead:]))
			valueStart = int(getu16(t.buf[entriesStart+i*EntryOverhead+2:]))
		)
		if keyStart > len(t.buf) {
			return &PointerError{
				Name:   fmt.Sprintf("entries[%d].keyStart", i),
				Ptr:    keyStart,
				BufLen: len(t.buf),
			}
		}
		if valueStart > len(t.buf) {
			return &PointerError{
				Name:   fmt.Sprintf("entries[%d].valueStart", i),
				Ptr:    valueStart,
				BufLen: len(t.buf),
			}
		}
	}

	return nil
}

func (t *trie) isTrie() {}

func getu16(x []byte) uint16 {
	return binary.LittleEndian.Uint16(x[:2])
}

func putu16(buf []byte, x uint16) {
	binary.LittleEndian.PutUint16(buf, x)
}

func sortPairs(pairs []Pair) {
	sort.Slice(pairs, func(i, j int) bool {
		return bytes.Compare(pairs[i].Key, pairs[j].Key) < 0
	})
}

// type Trie struct {
// 	store blobs.GetPostDelete

// 	Prefix   []byte
// 	Children *[256]blobs.ID
// 	Entries  []Pair
// }

// func New(store blobs.GetPostDelete) *Trie {
// 	return &Trie{
// 		store: store,
// 	}
// }

// func NewWithPrefix(store blobs.GetPostDelete, prefix []byte) *Trie {
// 	return &Trie{
// 		store:  store,
// 		Prefix: prefix,
// 	}
// }

// func (t *Trie) Clone() *Trie {
// 	var children *[256]blobs.ID
// 	if t.Children != nil {
// 		c := *t.Children
// 		children = &c
// 	}

// 	return &Trie{
// 		store:    t.store,
// 		Prefix:   t.Prefix,
// 		Children: children,
// 		Entries:  append([]Pair{}, t.Entries...),
// 	}
// }

// func (t *Trie) Put(ctx context.Context, key, value []byte) error {
// 	if err := t.Validate(); err != nil {
// 		return err
// 	}
// 	return t.put(ctx, Pair{Key: key, Value: value})
// }

// func (t *Trie) put(ctx context.Context, pair Pair) error {
// 	if !bytes.HasPrefix(pair.Key, t.Prefix) {
// 		return errors.New("wrong prefix for this trie")
// 	}
// 	if t.Children == nil {
// 		return t.putInThis(ctx, pair)
// 	}

// 	err := t.replaceChild(ctx, pair.Key, func(x Trie) (*Trie, error) {
// 		err := x.put(ctx, pair)
// 		return &x, err
// 	})
// 	return err
// }

// func (t *Trie) putInThis(ctx context.Context, pair Pair) error {
// 	if len(t.Entries) < maxEntries {
// 		t.Entries = append(t.Entries, pair)
// 		sort.Slice(t.Entries, func(i, j int) bool {
// 			return bytes.Compare(t.Entries[i].Key, t.Entries[j].Key) < 0
// 		})
// 		return nil
// 	}

// 	// convert to parent
// 	t.Entries = nil
// 	children := t.split()
// 	t.Children = new([256]blobs.ID)
// 	for i, child := range children {
// 		data := child.Marshal()
// 		ref, err := t.store.Post(ctx, data)
// 		if err != nil {
// 			return err
// 		}
// 		t.Children[i] = ref
// 	}

// 	// add to child
// 	return t.replaceChild(ctx, pair.Key, func(x Trie) (*Trie, error) {
// 		err := x.put(ctx, pair)
// 		return &x, err
// 	})
// }

// func (t *Trie) Get(ctx context.Context, key []byte) (*Pair, error) {
// 	if !bytes.HasPrefix(key, t.Prefix) {
// 		return nil, errors.New("wrong prefix for this trie")
// 	}

// 	if t.Children == nil {
// 		key := key[len(t.Prefix):]
// 		for _, pair := range t.Entries {
// 			if bytes.Compare(pair.Key, key) == 0 {
// 				return &pair, nil
// 			}
// 		}
// 		return nil, nil
// 	}

// 	_, subT, err := t.childFor(ctx, key)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return subT.Get(ctx, key)
// }

// func (t *Trie) GetChild(ctx context.Context, c byte) (*Trie, error) {
// 	if t.Children == nil {
// 		panic("GetChild called on child trie")
// 	}
// 	return t.getChild(ctx, int(c))
// }

// func (t *Trie) GetPrefix() []byte {
// 	return t.Prefix
// }

// func (t *Trie) IsParent() bool {
// 	return t.Children != nil
// }

// func (t *Trie) split() []Trie {
// 	subTs := make([]Trie, 256)
// 	for i := range subTs {
// 		subTs[i].store = t.store
// 		subTs[i].Prefix = append(t.Prefix, byte(i))
// 	}
// 	for _, p := range t.Entries {
// 		c := p.Key[len(t.Prefix)+1]

// 		ents := subTs[c].Entries
// 		subTs[c].Entries = append(ents, p)
// 	}
// 	return subTs
// }

// func (t *Trie) Validate() error {
// 	if len(t.Entries) > 0 && t.Children != nil {
// 		return errors.New("cannot be parent and leaf")
// 	}

// 	return nil
// }

// func (t *Trie) Delete(ctx context.Context, key []byte) error {
// 	return t.delete(ctx, key)
// }

// func (t *Trie) delete(ctx context.Context, key []byte) error {
// 	if t.Children == nil {
// 		ents := []Pair{}
// 		for _, p := range t.Entries {
// 			if bytes.Compare(p.Key, key) != 0 {
// 				ents = append(ents, p)
// 			}
// 		}
// 		t.Entries = ents
// 		return nil
// 	}
// 	err := t.replaceChild(ctx, key, func(x Trie) (*Trie, error) {
// 		err := x.delete(ctx, key)
// 		return &x, err
// 	})
// 	return err
// }

// func (t *Trie) replaceChild(ctx context.Context, key []byte, fn func(x Trie) (*Trie, error)) error {
// 	i, child, err := t.childFor(ctx, key)
// 	if err != nil {
// 		return err
// 	}
// 	child1, err := fn(*child)
// 	if err != nil {
// 		return err
// 	}
// 	data := child1.Marshal()
// 	ref, err := t.store.Post(ctx, data)
// 	if err != nil {
// 		return err
// 	}
// 	if err := t.store.Delete(ctx, t.Children[i]); err != nil {
// 		return err
// 	}
// 	t.Children[i] = ref
// 	return nil
// }

// func (t *Trie) childFor(ctx context.Context, key []byte) (int, *Trie, error) {
// 	i := int(key[len(t.Prefix)])
// 	child, err := t.getChild(ctx, i)
// 	if err != nil {
// 		return -1, nil, err
// 	}
// 	return i, child, nil
// }

// func (t *Trie) getChild(ctx context.Context, i int) (*Trie, error) {
// 	ref := t.Children[i]
// 	data, err := t.store.Get(ctx, ref)
// 	if err != nil {
// 		return nil, err
// 	}
// 	child := &Trie{}
// 	if err = t.UnmarshalText(data); err != nil {
// 		return nil, err
// 	}
// 	return child, nil
// }

// const (
// 	TypeLeaf = "leaf"
// 	TypeTree = "tree"
// )

// func (t *Trie) MarshalText() ([]byte, error) {
// 	buf := bytes.Buffer{}

// 	var ty string
// 	if t.Children == nil {
// 		ty = TypeLeaf
// 	} else {
// 		ty = TypeTree
// 	}

// 	if _, err := buf.WriteString(ty); err != nil {
// 		return nil, err
// 	}
// 	if _, err := buf.WriteString("\n"); err != nil {
// 		return nil, err
// 	}
// 	if _, err := buf.WriteString(enc.EncodeToString(t.Prefix)); err != nil {
// 		return nil, err
// 	}
// 	if _, err := buf.WriteString("\n"); err != nil {
// 		return nil, err
// 	}
// 	switch ty {
// 	case TypeTree:
// 		for _, ref := range t.Children {
// 			b64Str := enc.EncodeToString(ref[:])
// 			if _, err := buf.WriteString(b64Str); err != nil {
// 				return nil, err
// 			}
// 			if _, err := buf.WriteString("\n"); err != nil {
// 				return nil, err
// 			}
// 		}
// 	case TypeLeaf:
// 		for _, pair := range t.Entries {
// 			keyb64 := enc.EncodeToString(pair.Key)
// 			if _, err := buf.WriteString(keyb64); err != nil {
// 				return nil, err
// 			}
// 			if _, err := buf.WriteString("\t"); err != nil {
// 				return nil, err
// 			}
// 			valueb64 := enc.EncodeToString(pair.Value)
// 			if _, err := buf.WriteString(valueb64); err != nil {
// 				return nil, err
// 			}
// 			if _, err := buf.WriteString("\n"); err != nil {
// 				return nil, err
// 			}
// 		}
// 	}

// 	return buf.Bytes(), nil
// }

// func (t *Trie) Marshal() []byte {
// 	data, err := t.MarshalText()
// 	if err != nil {
// 		panic(err)
// 	}
// 	return data
// }

// func (t *Trie) UnmarshalText(data []byte) error {
// 	lines := bytes.Split(data, []byte{'\n'})
// 	if len(lines) < 2 {
// 		return errors.New("Trie.Unmarshal: too few lines")
// 	}

// 	// this is to prevent keeping a reference to the whole buffer.
// 	prefix := make([]byte, enc.DecodedLen(len(lines[1])))
// 	n, err := enc.Decode(prefix, lines[1])
// 	if err != nil {
// 		return err
// 	}
// 	prefix = prefix[:n]
// 	t.Prefix = prefix

// 	things := lines[2:]
// 	switch string(lines[0]) {
// 	case TypeLeaf:
// 		t.Entries = make([]Pair, len(things))
// 		for i := range things {
// 			if len(things[i]) < 1 {
// 				continue
// 			}
// 			parts := bytes.SplitN(things[i], []byte("\t"), 2)
// 			if len(parts) < 2 {
// 				return errors.New("Trie.Unmarshal: invalid pair")
// 			}
// 			keyb64, valueb64 := string(parts[0]), string(parts[1])
// 			key, err := enc.DecodeString(keyb64)
// 			if err != nil {
// 				return err
// 			}
// 			value, err := enc.DecodeString(valueb64)
// 			if err != nil {
// 				return err
// 			}
// 			t.Entries[i] = Pair{Key: key, Value: value}
// 		}
// 	case TypeTree:
// 		t.Children = new([256]blobs.ID)
// 		for i := range things {
// 			copy(t.Children[i][:], things[i])
// 		}
// 	default:
// 		return errors.New("invalid trie type")
// 	}

// 	return nil
// }

// func (t *Trie) Unmarshal(data []byte) error {
// 	return t.UnmarshalText(data)
// }

// func FromBytes(store blobs.GetPostDelete, data []byte) (*Trie, error) {
// 	t := &Trie{store: store}
// 	return t, t.Unmarshal(data)
// }

// func (t *Trie) ID() blobs.ID {
// 	return blobs.Hash(t.Marshal())
// }
