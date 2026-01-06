package blobcached

import (
	"fmt"
	"regexp"

	"blobcache.io/blobcache/src/internal/groupfile"
)

var gnre = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

type GroupName string

func (gn GroupName) String() string {
	return string(gn)
}

func ParseGroupName(x []byte) (GroupName, error) {
	if !gnre.Match(x) {
		return "", fmt.Errorf("invalid group name: %s", x)
	}
	return GroupName(string(x)), nil
}

type (
	Entry[T any]      = groupfile.Entry[GroupName, T]
	Membership[T any] = groupfile.Membership[GroupName, T]
	Member[T any]     = groupfile.Member[GroupName, T]
)
