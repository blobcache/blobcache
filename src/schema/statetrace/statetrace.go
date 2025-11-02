// package ledger implements an append-only ledger where state transitions can be verified
package statetrace

import (
	"context"
	"fmt"

	"go.brendoncarroll.net/exp/streams"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"blobcache.io/blobcache/src/schema/merklelog"
)

type Marshaler interface {
	// Marshal appends the marshaled representation of the object to out
	// and returns the new slice (of which out is a prefix).
	Marshal(out []byte) []byte
}

// Root is contains transitive references to the entire history and current state of the system.
type Root[State Marshaler] struct {
	// History is the history of all previous states.
	History merklelog.State
	// State is the current state of the system.
	State State
}

func (r Root[State]) Marshal(out []byte) []byte {
	out = append(out, byte(len(r.History.Levels)))
	out = r.History.Marshal(out)
	out = r.State.Marshal(out)
	return out
}

// Len is the number of states in the history.
// Len will also be the Pos of this root, when it is added to a merkle log.
func (r Root[State]) Len() merklelog.Pos {
	return r.History.Len()
}

type Parser[T any] = func(data []byte) (T, error)

// Parse parses the Root from data.
func Parse[State Marshaler](data []byte, parseState Parser[State]) (Root[State], error) {
	if len(data) < 1 {
		return Root[State]{}, fmt.Errorf("ledger: data too short")
	}
	historyLen := int(data[0])
	data = data[1:]
	historySize := historyLen * blobcache.CIDSize
	if historySize > len(data) {
		return Root[State]{}, fmt.Errorf("ledger: history size %d is greater than data %d", historySize, len(data))
	}
	history, err := merklelog.Parse(data[:historySize])
	if err != nil {
		return Root[State]{}, err
	}
	data = data[historySize:]

	state, err := parseState(data)
	if err != nil {
		return Root[State]{}, err
	}
	return Root[State]{
		State:   state,
		History: history,
	}, nil
}

// Machine performs operations on a ledger.
type Machine[State Marshaler] struct {
	HashAlgo blobcache.HashAlgo
	// ParseState parses a State from a byte slice.
	ParseState Parser[State]
	// Verify verifies that prev -> next is a valid transition.
	// Verify must return nil only if the transition is valid, and never needs to be considered again.
	Verify VerifyFunc[State]
}

// VerifyFunc verifies that prev -> next is a valid transition
type VerifyFunc[State Marshaler] = func(ctx context.Context, s schema.RO, prev, next State) error

// Parse parses a Root from a byte slice.
func (m *Machine[State]) Parse(data []byte) (Root[State], error) {
	return Parse(data, m.ParseState)
}

// Initial creates a new root with the given state, and an empty history.
func (m *Machine[State]) Initial(initState State) Root[State] {
	return Root[State]{
		State:   initState,
		History: merklelog.State{},
	}
}

func (m *Machine[State]) PostRoot(ctx context.Context, s schema.WO, root Root[State]) (merklelog.CID, error) {
	return s.Post(ctx, root.Marshal(nil))
}

func (m *Machine[State]) GetRoot(ctx context.Context, s schema.RO, cid merklelog.CID) (*Root[State], error) {
	buf := make([]byte, s.MaxSize())
	n, err := s.Get(ctx, cid, buf)
	if err != nil {
		return nil, err
	}
	ret, err := m.Parse(buf[:n])
	if err != nil {
		return nil, err
	}
	return &ret, nil
}

// Slot returns the state at the given position in the history.
func (m *Machine[State]) Slot(ctx context.Context, s schema.RO, root Root[State], slot merklelog.Pos) (*Root[State], error) {
	switch {
	case root.History.Len() == slot:
		return &root, nil
	case slot > root.History.Len():
		return nil, fmt.Errorf("ledger: slot %d out of bounds (length %d)", slot, root.History.Len())
	}
	cid, err := merklelog.Get(ctx, s, root.History, slot)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, s.MaxSize())
	n, err := s.Get(ctx, cid, buf)
	if err != nil {
		return nil, err
	}
	ret, err := m.Parse(buf[:n])
	if err != nil {
		return nil, err
	}
	return &ret, nil
}

// GetPrev returns the root, immediately previous to the given root.
func (m *Machine[State]) GetPrev(ctx context.Context, s schema.RO, root Root[State]) (*Root[State], error) {
	if root.History.Len() == 0 {
		return nil, nil
	}
	return m.Slot(ctx, s, root, root.History.Len()-1)
}

// Validate verifies that prev -> next is a valid transition.
func (m *Machine[State]) Validate(ctx context.Context, s schema.RO, prev, next Root[State]) error {
	// First, the next root must include the previous root.
	// If the next root does not acknowledge all of the history that we already know is true
	// then it can't be a correct continuation.
	yesInc, err := merklelog.Includes(ctx, s, next.History, prev.History)
	if err != nil {
		return err
	}
	if !yesInc {
		return fmt.Errorf("ledger: next root does not include history of previous root")
	}
	// Next, we iterate through all of the intermediate states, and check that the transition is valid.
	it := m.NewIterator(s, prev, next)
	for {
		var root Root[State]
		if err := it.Next(ctx, &root); err != nil {
			if streams.IsEOS(err) {
				break
			}
			return err
		}
		if root.Len() == 0 {
			continue
		}
		prevRoot, err := m.GetPrev(ctx, s, root)
		if err != nil {
			return err
		}
		prevState := prevRoot.State
		nextState := root.State
		if err := m.Verify(ctx, s, prevState, nextState); err != nil {
			return err
		}
	}
	return nil
}

// AndThen creates a new root with the state.
// History is updated to reflect the previous state.
// AndThen does not modify the current root, it returns a new root.
func (m *Machine[State]) AndThen(ctx context.Context, s schema.RW, r Root[State], next State) (Root[State], error) {
	if err := m.Verify(ctx, s, r.State, next); err != nil {
		return Root[State]{}, err
	}
	prevCID, err := s.Post(ctx, r.Marshal(nil))
	if err != nil {
		return Root[State]{}, err
	}
	r2 := r
	r2.State = next
	if err := r2.History.Append(ctx, s, prevCID); err != nil {
		return Root[State]{}, err
	}
	return r2, nil
}

func (m *Machine[State]) NewIterator(s schema.RO, prev Root[State], next Root[State]) *Iterator[State] {
	return &Iterator[State]{
		m:  m,
		it: merklelog.NewIterator(next.History, s, prev.History.Len(), next.History.Len()),
		s:  s,
	}
}

// Iterator is an iterator over all the intermediate states between two roots.
type Iterator[State Marshaler] struct {
	m   *Machine[State]
	it  *merklelog.Iterator
	s   schema.RO
	buf []byte
}

func (it *Iterator[State]) Next(ctx context.Context, dst *Root[State]) error {
	var cid merklelog.CID
	if err := it.it.Next(ctx, &cid); err != nil {
		return err
	}
	if it.buf == nil {
		it.buf = make([]byte, it.s.MaxSize())
	}
	n, err := it.s.Get(ctx, cid, it.buf)
	if err != nil {
		return err
	}
	data := it.buf[:n]
	root, err := Parse(data, it.m.ParseState)
	if err != nil {
		return err
	}
	*dst = root
	return nil
}
