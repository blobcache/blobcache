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
	// GroupRef references another group by name.
	GroupRef *GroupName
	// Empty is used to create empty groups.
	Empty *struct{}
}

// Unit creates a group member that references a single element.
func Unit[T any](unit T) Member[T] {
	return Member[T]{Unit: &unit}
}

// GroupRef creates a group member that references a group by name.
func GroupRef[T any](subGroup GroupName) Member[T] {
	return Member[T]{GroupRef: &subGroup}
}

func ParseMember[T any](data []byte, parse func([]byte) (T, error)) (Member[T], error) {
	var ret Member[T]
	if bytes.HasPrefix(data, []byte("@")) {
		subgrp, err := ParseGroupName(data[1:])
		if err != nil {
			return Member[T]{}, err
		}
		ret.GroupRef = &subgrp
	} else {
		unit, err := parse(data)
		if err != nil {
			return Member[T]{}, err
		}
		ret.Unit = &unit
	}
	return ret, nil
}

func (m Member[T]) Format(format func(T) string) string {
	if m.GroupRef != nil {
		return "@" + string(*m.GroupRef)
	}
	return format(*m.Unit)
}

func ParseGroupsFile[T any](r io.Reader, parse func([]byte) (T, error)) (ret []Membership[T], _ error) {
	groups := make(map[GroupName]int)
	scn := bufio.NewScanner(r)
	for linenum := 1; scn.Scan(); linenum++ {
		line := scn.Bytes()
		if len(line) == 0 {
			continue
		}
		parts := bytes.Split(line, []byte(" "))
		groupName, err := ParseGroupName(parts[0])
		if err != nil {
			return nil, fmt.Errorf("invalid line %d: %w", linenum, err)
		}
		groups[groupName] += 0

		for _, memberData := range parts[1:] {
			// for each remaining word on the line, try to parse it as a member
			elem, err := ParseMember(memberData, parse)
			if err != nil {
				return nil, fmt.Errorf("invalid line %d: %w", linenum, err)
			}
			if elem.GroupRef != nil {
				if _, exists := groups[*elem.GroupRef]; !exists {
					return nil, fmt.Errorf("don't know what %q is, it hasn't been defined yet", *elem.GroupRef)
				}
				if *elem.GroupRef == groupName {
					return nil, fmt.Errorf("group %q cannot contain itself", groupName)
				}
			}
			ret = append(ret, Membership[T]{
				Group:  groupName,
				Member: elem,
			})
			groups[groupName]++
		}
	}
	for name, numMembers := range groups {
		if numMembers == 0 {
			ret = append(ret, Membership[T]{
				Group:  name,
				Member: Member[T]{Empty: &struct{}{}},
			})
		}
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
