package groupfile

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
)

// GroupName is implemented by all types that can be the name of a group.
// GroupNames must be comparable and implement the String method
type GroupName interface {
	comparable
	String() string
}

type GroupFile[K GroupName, T any] = []Entry[K, T]

// MStmt is a statement about group membership
type MStmt[K GroupName, T any] struct {
	Group   K
	Members []Member[K, T]
}

// Entry is a single line in a GroupFile
// K is the name for a group
type Entry[K GroupName, T any] struct {
	// FileData is the full contents of the file that contain this entry
	FileData []byte
	Beg, End uint32

	// MStmt is statement about membership
	MStmt *MStmt[K, T]
	// Comment is a line that starts with #
	Comment *string
	// Include is a line that starts with !include
	// The rest of the line is interpretted as a paths to include
	Include *string
}

// Memberships returns all Memberships contained in the entry.
func (ent *Entry[K, T]) Memberships() (ret []Membership[K, T]) {
	mstmt := ent.MStmt
	if mstmt == nil {
		return nil
	}
	for _, m := range mstmt.Members {
		ret = append(ret, Membership[K, T]{
			Group:  mstmt.Group,
			Member: m,
		})
	}
	return ret
}

func (ent *Entry[K, T]) Validate() error {
	var count int
	if ent.MStmt != nil {
		count++
	}
	if ent.Comment != nil {
		count++
	}
	if ent.Include != nil {
		count++
	}

	if count > 1 {
		return fmt.Errorf("groupfile: invalid Entry")
	}
	return nil
}

func (ent *Entry[K, T]) WriteTo(w io.Writer, fmtV func(T) string) (int, error) {
	if err := ent.Validate(); err != nil {
		return 0, err
	}
	switch {
	case ent.Comment != nil:
		return fmt.Fprintf(w, "#%s\n", *ent.Comment)
	case ent.Include != nil:
		return fmt.Fprintf(w, "!include %s\n", *ent.Include)
	case ent.MStmt != nil:
		mstmt := *ent.MStmt
		var n int
		if n2, err := fmt.Fprintf(w, "%s", mstmt.Group); err != nil {
			return 0, err
		} else {
			n += n2
		}
		for _, m := range mstmt.Members {
			n2, err := fmt.Fprintf(w, " %s", m.Format(fmtV))
			if err != nil {
				return 0, err
			}
			n += n2
		}
		if n2, err := fmt.Fprintln(w); err != nil {
			return n, err
		} else {
			n += n2
		}
		return n, nil
	default:
		return fmt.Fprintln(w)
	}
}

// Membership says that a group contains a member.
type Membership[K GroupName, T any] struct {
	Group  K
	Member Member[K, T]
}

// Member is a member of a group.
// Members can either refer to another group by name, or to a single element of type T.
type Member[K GroupName, T any] struct {
	// Unit references a single element
	Unit *T
	// GroupRef references another group by name.
	GroupRef *K
	// Empty is used to create empty groups.
	Empty *struct{}
}

// Unit creates a group member that references a single element.
func Unit[K GroupName, T any](unit T) Member[K, T] {
	return Member[K, T]{Unit: &unit}
}

// GroupRef creates a group member that references a group by name.
func GroupRef[K GroupName, T any](subGroup K) Member[K, T] {
	return Member[K, T]{GroupRef: &subGroup}
}

func (m Member[K, T]) Format(format func(T) string) string {
	if m.GroupRef != nil {
		return "@" + (*m.GroupRef).String()
	}
	return format(*m.Unit)
}

// parseEntry parses a single line in
func parseEntry[K GroupName, T any](line []byte, parseK func([]byte) (K, error), parseV func([]byte) (T, error)) (*Entry[K, T], error) {
	const includePrefix = "!include "
	switch {
	case bytes.HasPrefix(line, []byte(includePrefix)):
		p := string(bytes.TrimPrefix(line, []byte(includePrefix)))
		return &Entry[K, T]{
			Include: &p,
		}, nil
	case bytes.HasPrefix(line, []byte("!")):
		return nil, fmt.Errorf("lines cannot start with !")

	case bytes.HasPrefix(line, []byte("#")):
		s := string(line[1:])
		return &Entry[K, T]{
			Comment: &s,
		}, nil
	default:
		parts := bytes.Split(line, []byte(" "))
		k, err := parseK(parts[0])
		if err != nil {
			return nil, err
		}
		var ret Entry[K, T]
		ret.MStmt = &MStmt[K, T]{
			Group: k,
		}
		mstmt := ret.MStmt
		for _, memberData := range parts[1:] {
			var m Member[K, T]
			if bytes.HasPrefix(memberData, []byte("@")) {
				subgrp, err := parseK(memberData[1:])
				if err != nil {
					return nil, err
				}
				m.GroupRef = &subgrp
			} else {
				unit, err := parseV(memberData)
				if err != nil {
					return nil, err
				}
				m.Unit = &unit
			}
			mstmt.Members = append(mstmt.Members, m)
		}
		return &ret, nil
	}
}

// Parse parses a groupfile from data, which should be the entire contents of a group file.
func Parse[K GroupName, T any](data []byte, parseK func([]byte) (K, error), parseV func([]byte) (T, error)) (ret []Entry[K, T], _ error) {
	groups := make(map[K]int)
	scn := bufio.NewScanner(bytes.NewReader(data))
	for linenum := 1; scn.Scan(); linenum++ {
		line := scn.Bytes()
		if len(line) == 0 {
			continue
		}
		ent, err := parseEntry(line, parseK, parseV)
		if err != nil {
			return nil, ErrParsing{
				FileData: data,
				End:      uint32(len(data)),
				Err:      err,
			}
		}
		if ent.MStmt != nil {
			groupName := ent.MStmt.Group
			groups[groupName] += 0
		}
		for _, mshp := range ent.Memberships() {
			elem := mshp.Member
			groupName := ent.MStmt.Group
			if elem.GroupRef != nil {
				if _, exists := groups[*elem.GroupRef]; !exists {
					return nil, fmt.Errorf("don't know what %q is, it hasn't been defined yet", *elem.GroupRef)
				}
				if *elem.GroupRef == groupName {
					return nil, fmt.Errorf("group %q cannot contain itself", groupName)
				}
			}
			groups[groupName]++
		}
		ret = append(ret, *ent)
	}

	for name, numMembers := range groups {
		if numMembers == 0 {
			ret = append(ret, Entry[K, T]{
				MStmt: &MStmt[K, T]{
					Group: name,
					Members: []Member[K, T]{
						{Empty: &struct{}{}},
					},
				},
			})
		}
	}
	return ret, nil
}

// Write writes the memberships to the writer, such that they can be parsed by Parse.
func Write[K GroupName, T any](w io.Writer, ents []Entry[K, T], format func(T) string) error {
	bw := bufio.NewWriter(w)
	for _, e := range ents {
		if _, err := e.WriteTo(bw, format); err != nil {
			return nil
		}
	}
	return bw.Flush()
}
