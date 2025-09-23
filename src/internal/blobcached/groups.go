package blobcached

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"regexp"
)

var gnre = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

type GroupName string

func ParseGroupName(x []byte) (GroupName, error) {
	if !gnre.Match(x) {
		return "", fmt.Errorf("invalid group name: %s", x)
	}
	return GroupName(string(x)), nil
}

// Membership says that a group contains a member.
type Membership[T any] struct {
	Group  GroupName
	Member Member[T]
}

// Member is a member of a group.
// Members can either refer to another group by name, or to a single element of type T.
type Member[T any] struct {
	// Unit references a single element
	Unit *T
	// SubGroup references another group by name.
	SubGroup *GroupName
}

func (m Member[T]) Format(format func(T) string) string {
	if m.SubGroup != nil {
		return "@" + string(*m.SubGroup)
	}
	return format(*m.Unit)
}

func ParseGroupsFile[T fmt.Stringer](r io.Reader, parse func([]byte) (T, error)) (ret []Membership[T], _ error) {
	groups := make(map[GroupName]struct{})
	scn := bufio.NewScanner(r)
	for linenum := 1; scn.Scan(); linenum++ {
		line := scn.Bytes()
		if len(line) == 0 {
			continue
		}
		parts := bytes.SplitN(line, []byte(" "), 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid authn line %d: %s", linenum, line)
		}
		groupName, memberData := parts[0], parts[1]

		var elem Member[T]
		if bytes.HasPrefix(memberData, []byte("@")) {
			subgrp, err := ParseGroupName(memberData[1:])
			if err != nil {
				return nil, fmt.Errorf("invalid authn line %d: %w", linenum, err)
			}
			if _, exists := groups[subgrp]; !exists {
				return nil, fmt.Errorf("don't know what %q is, it hasn't been defined yet", subgrp)
			}
			elem.SubGroup = &subgrp
		} else {
			unit, err := parse(memberData)
			if err != nil {
				return nil, fmt.Errorf("invalid authn line %d: %w", linenum, err)
			}
			elem.Unit = &unit
		}
		ret = append(ret, Membership[T]{
			Group:  GroupName(groupName),
			Member: elem,
		})
	}
	return ret, nil
}

// WriteGroupsFile writes the memberships to the writer, such that they can be parsed by ParseGroupsFile.
func WriteGroupsFile[T any](w io.Writer, membership []Membership[T], format func(T) string) error {
	bw := bufio.NewWriter(w)
	for _, m := range membership {
		_, err := fmt.Fprintf(bw, "%s %s\n", m.Group, m.Member.Format(format))
		if err != nil {
			return err
		}
	}
	return bw.Flush()
}
