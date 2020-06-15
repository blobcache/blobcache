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

	"github.com/blobcache/blobcache/pkg/blobs"
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
	DeleteBranch(ctx context.Context, prefix []byte) error

	// parent
	IsParent() bool
	GetChild(context.Context, byte) (Trie, error)
	GetChildRef(byte) blobs.ID

	// entries
	ListEntries() []Pair
	GetEntry(key []byte) *Pair

	GetPrefix() []byte
	Marshal() []byte

	isTrie()
}

func New(store blobs.GetPostDelete) Trie {
	return newTrie(store, false, nil, 0)
}

func NewWithPrefix(store blobs.GetPostDelete, prefix []byte) Trie {
	t := newTrie(store, false, prefix, 0)
	return t
}

func NewParent(ctx context.Context, store blobs.GetPostDelete, children [256]Trie) (Trie, error) {
	prefix := children[0].GetPrefix()
	prefix = prefix[:len(prefix)-1]

	for _, child := range children {
		if !bytes.HasPrefix(child.GetPrefix(), prefix) {
			return nil, errors.New("tries cannot be siblings")
		}
	}
	t := newTrie(store, true, prefix, 0)

	for i, child := range children {
		id, err := store.Post(ctx, child.Marshal())
		if err != nil {
			return nil, err
		}
		if !id.Equals(blobs.ZeroID()) {
			t.setChildRef(byte(i), id)
		}
	}
	return t, nil
}

func FromBytes(store blobs.GetPostDelete, data []byte) (Trie, error) {
	return fromBytes(store, data, true)
}

func Equal(a, b Trie) bool {
	idA := blobs.Hash(a.Marshal())
	idB := blobs.Hash(b.Marshal())
	return bytes.Compare(idA[:], idB[:]) == 0
}

// HowManyUniform returns how many uniformly sized entries can fit in a child trie before it must split.
func HowManyUniform(prefixLen int, entrySize int) int {
	room := blobs.MaxSize - HeaderSize - prefixLen
	return room / (EntryOverhead + entrySize)
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

	changes   map[string]*Pair
	ourBuffer bool
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

func newTrie(store blobs.GetPostDelete, isParent bool, prefix []byte, entryCount int) *trie {
	size := sizeOf(isParent, len(prefix), entryCount, 0)
	buf := make([]byte, size)
	prefixLen := len(prefix)
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
	t := &trie{store: store, buf: buf}
	t.setPrefix(prefix)
	return t
}

func fromBytes(store blobs.GetPostDelete, data []byte, copy bool) (*trie, error) {
	if copy {
		data = append([]byte{}, data...)
	}
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
	child, err := t.getChild(ctx, i)
	if err != nil {
		return nil, err
	}
	if child == nil {
		p := append(t.getPrefix(), i)
		child = newTrie(t.store, false, p, 0)
	}
	return child, nil
}

func (t *trie) getChild(ctx context.Context, i byte) (*trie, error) {
	if !t.IsParent() {
		panic("called GetChild on non-parent")
	}
	ref := t.getChildRef(i)
	if ref.Equals(blobs.ZeroID()) {
		return nil, nil
	}
	var data []byte
	err := t.store.GetF(ctx, ref, func(b []byte) error {
		data = append(data, b...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return fromBytes(t.store, data, true)
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
			if pair2 == nil {
				listed[string(pair.Key)] = true
				continue
			} else {
				pair = *pair2
			}
		}
		pairs = append(pairs, pair)
		listed[string(pair.Key)] = true
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
			if child == nil {
				p := append(t.getPrefix(), c)
				child = newTrie(t.store, false, p, 0)
			}
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
	size := sizeOf(false, len(prefix), t.numEntries()+t.netAdditions(), t.netSizeChange()+t.entriesSize())
	if size <= blobs.MaxSize {
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
			panic("keys in parent not supported")
			continue
		}
		c := pair.Key[len(prefix)]
		entriesSplit[int(c)] = append(entriesSplit[int(c)], pair)
	}

	children := [256]blobs.ID{}
	for i := range children {
		// create child
		prefix2 := append(prefix, byte(i))
		child := newTrie(t.store, false, prefix2, 0)

		// add all pairs
		for _, pair := range entriesSplit[i] {
			if err := child.put(ctx, pair); err != nil {
				return nil, err
			}
		}

		// marshal and post the child
		if len(entriesSplit[i]) > 0 {
			id, err := t.store.Post(ctx, child.Marshal())
			if err != nil {
				return nil, err
			}
			children[i] = id
		}
	}

	entryCount := 0
	// if t.GetEntry(prefix) != nil {
	// 	entryCount = 1
	// }
	parent := newTrie(t.store, true, prefix, entryCount)
	for i := range children {
		parent.setChildRef(byte(i), children[i])
	}
	// if pair := t.GetEntry(prefix); pair != nil {
	// 	parent.appendEntry(0, *pair)
	// }
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
			if child == nil {
				return nil, ErrBranchEmpty
			}
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

func (t *trie) DeleteBranch(ctx context.Context, prefix []byte) error {
	if !bytes.HasPrefix(t.GetPrefix(), prefix) {
		panic("can't delete that key from this trie; wrong prefix")
	}

	switch {
	case len(t.GetPrefix()) == len(prefix):
		// recursed too far, cannot delete self
		return ErrCannotDeleteRoot

	case len(t.GetPrefix()) < len(prefix)-1:
		// cannot delete yet, need to recurse
		c := prefix[len(t.GetPrefix())]
		return t.replaceChild(ctx, c, func(child *trie) (*trie, error) {
			if child == nil {
				return nil, ErrBranchEmpty
			}
			err := child.DeleteBranch(ctx, prefix)
			return child, err
		})

	case len(t.GetPrefix()) == len(prefix)-1:
		if !t.IsParent() {
			return ErrPrefixNotParent
		}
		// delete the branch
		c := prefix[len(t.GetPrefix())]
		id := t.getChildRef(c)
		if err := t.store.Delete(ctx, id); err != nil {
			return err
		}
		t.ensureOurBuffer()
		t.setChildRef(c, blobs.ID{})
		return nil

	default:
		panic("trie.DeleteBranch: invalid case")
	}
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
	if CtxGetDeleteBlobs(ctx) {
		oldID := t.getChildRef(c)
		if err := t.store.Delete(ctx, oldID); err != nil {
			return err
		}
	}
	t.ensureOurBuffer()
	t.setChildRef(c, id)
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
		return append([]byte{}, t.buf...)
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
	t2 := newTrie(t.store, false, prefix, len(entries))

	for i := range entries {
		t2.appendEntry(i, entries[i])
	}

	*t = *t2
	return t2.Marshal()
}

func (t *trie) LenEntries() int {
	return t.numEntries() + t.netAdditions()
}

func (t *trie) String() string {
	parent := "CHILD"
	children := []string{}
	if t.IsParent() {
		parent = "PARENT"
		for i := 0; i < 256; i++ {
			id := t.getChildRef(byte(i))
			if !id.Equals(blobs.ZeroID()) {
				children = append(children, fmt.Sprintf("%02x->", i)+id.String()[:7]+"...")
			}
		}
	}

	return fmt.Sprintf("Trie{%s, prefix: '%x', children: %v, %d entries}", parent, t.getPrefix(), children, t.LenEntries())
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
	off := int(childrenStart) + int(i)*blobs.IDSize
	id := blobs.ID{}
	copy(id[:], t.buf[off:off+blobs.IDSize])
	return id
}

func (t *trie) setChildRef(i byte, id blobs.ID) {
	if !t.IsParent() {
		panic("cannot setChildRef on non-parent")
	}
	childrenStart := getu16(t.buf[2:])
	off := int(childrenStart) + int(i)*blobs.IDSize
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

	off := t.entriesStart() + EntryOverhead*i
	keyStart := int(getu16(t.buf[off:]))
	valueStart := int(getu16(t.buf[off+2:]))
	valueEnd := len(t.buf)
	if off+4 < t.entriesEnd() {
		valueEnd = int(getu16(t.buf[off+4:]))
	}

	prefix := append([]byte{}, t.getPrefix()...)
	return Pair{
		Key:   append(prefix, t.buf[keyStart:valueStart]...),
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
	return len(t.buf) - t.entriesEnd()
}

func (t *trie) setEntryOffsets(i, keyStart, valueStart int) {
	entriesStart := t.entriesStart()
	off := entriesStart + EntryOverhead*i
	if keyStart > blobs.MaxSize {
		panic(keyStart)
	}
	if valueStart > blobs.MaxSize {
		panic(valueStart)
	}
	putu16(t.buf[off:], uint16(keyStart))
	putu16(t.buf[off+2:], uint16(valueStart))
}

// appendEntry appends the entry to the buffer and sets the pointers in
// the entry table at i.
func (t *trie) appendEntry(i int, p Pair) {
	prefix := t.getPrefix()
	key := p.Key[len(prefix):]
	value := p.Value

	keyStart := len(t.buf)
	valueStart := keyStart + len(key)
	if keyStart >= blobs.MaxSize || valueStart >= blobs.MaxSize || valueStart+len(value) > blobs.MaxSize {
		panic("trie is full")
	}

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

func (t *trie) ensureOurBuffer() {
	if !t.ourBuffer {
		t.buf = append([]byte{}, t.buf...)
		t.ourBuffer = true
	}
}

func (t *trie) validate() error {
	childrenStart := int(getu16(t.buf[2:]))
	if int(childrenStart) >= len(t.buf) {
		return &PointerError{
			Name: "children-start",
			Ptr:  childrenStart,
			Buf:  t.buf,
		}
	}
	entriesStart := t.entriesStart()
	if entriesStart > len(t.buf) {
		return &PointerError{
			Name: "entries-start",
			Ptr:  entriesStart,
			Buf:  t.buf,
		}
	}
	entriesEnd := t.entriesEnd()
	if entriesEnd > len(t.buf) {
		return &PointerError{
			Name: "entriesEnd",
			Ptr:  entriesEnd,
			Buf:  t.buf,
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
				Name: fmt.Sprintf("entries[%d].keyStart", i),
				Ptr:  keyStart,
				Buf:  t.buf,
			}
		}
		if valueStart > len(t.buf) {
			return &PointerError{
				Name: fmt.Sprintf("entries[%d].valueStart", i),
				Ptr:  valueStart,
				Buf:  t.buf,
			}
		}
		if valueStart < keyStart {
			return fmt.Errorf("entries[%d] valueStart: %d < keyStart: %d", i, valueStart, keyStart)
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
