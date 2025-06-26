package blobcached

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"regexp"
	"slices"

	"blobcache.io/blobcache/src/blobcache"
	"go.inet256.org/inet256/pkg/inet256"
)

const (
	AuthnFilename = "AUTHN"
	AuthzFilename = "AUTHZ"
)

type Membership struct {
	Group  string
	Member Identity
}

type Identity struct {
	// Name references a group.
	Name *string
	// Peer is a single peer.
	Peer *blobcache.PeerID
}

func (iden Identity) Equals(other Identity) bool {
	switch {
	case iden.Name != nil:
		return other.Name != nil && *iden.Name == *other.Name
	case iden.Peer != nil:
		return other.Peer != nil && *iden.Peer == *other.Peer
	}
	return false
}

func (iden Identity) String() string {
	if iden.Name != nil {
		return "@" + *iden.Name
	}
	return iden.Peer.String()
}

func ParseIdentity(x []byte) (Identity, error) {
	if bytes.HasPrefix(x, []byte("@")) {
		return Identity{Name: ptr(string(x[1:]))}, nil
	}
	addr, err := inet256.ParseAddrBase64(x)
	if err != nil {
		return Identity{}, err
	}
	return Identity{Peer: &addr}, nil
}

func ParseAuthnFile(r io.Reader) (ret []Membership, _ error) {
	groups := make(map[string]struct{})
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
		groupName, member := parts[0], parts[1]
		memberIden, err := ParseIdentity([]byte(member))
		if err != nil {
			return nil, fmt.Errorf("invalid authn line %d: %w", linenum, err)
		}
		if memberIden.Name != nil {
			if _, exists := groups[*memberIden.Name]; !exists {
				return nil, fmt.Errorf("don't know who %q is, it hasn't been defined yet", *memberIden.Name)
			}
		}
		groups[string(groupName)] = struct{}{}
		membership := Membership{
			Group:  string(groupName),
			Member: memberIden,
		}
		ret = append(ret, membership)
	}
	return ret, nil
}

// WriteAuthnFile writes the memberships to the writer, such that they can be parsed by ParseAuthnFile.
// It inserts an extra newline every time the group changes from the previous membership.
func WriteAuthnFile(w io.Writer, membership []Membership) error {
	bw := bufio.NewWriter(w)
	prevGroup := ""
	for i, m := range membership {
		if i > 0 && m.Group != prevGroup {
			if err := bw.WriteByte('\n'); err != nil {
				return err
			}
		}
		_, err := fmt.Fprintf(bw, "%s %s\n", m.Group, m.Member.String())
		if err != nil {
			return err
		}
		prevGroup = m.Group
	}
	return bw.Flush()
}

type Grant struct {
	Subject Identity
	Verb    Action
	Object  Object
}

func (g *Grant) Equals(other Grant) bool {
	return g.Subject.Equals(other.Subject) &&
		g.Verb == other.Verb &&
		g.Object.Equals(other.Object)
}

func parseGrant(line []byte) (*Grant, error) {
	parts := bytes.SplitN(line, []byte(" "), 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("grant must have 3 parts %v: ", line)
	}
	subject, actionStr, objectStr := parts[0], parts[1], parts[2]
	subjectIden, err := ParseIdentity([]byte(subject))
	if err != nil {
		return nil, fmt.Errorf("invalid authz line %s: %w", line, err)
	}
	action, err := ParseAction([]byte(actionStr))
	if err != nil {
		return nil, err
	}
	object, err := ParseObject([]byte(objectStr))
	if err != nil {
		return nil, err
	}
	return &Grant{
		Subject: subjectIden,
		Verb:    action,
		Object:  object,
	}, nil
}

type Action string

const (
	Action_LOOK  Action = "LOOK"
	Action_TOUCH Action = "TOUCH"
	Action_OWN   Action = "OWN"
)

func ParseAction(x []byte) (Action, error) {
	switch string(x) {
	case "LOOK":
		return Action_LOOK, nil
	case "TOUCH":
		return Action_TOUCH, nil
	}
	return "", fmt.Errorf("invalid action: %s", x)
}

// Object is something that Actions are performed on.
// It can be a specific OID, or a set of names defined by a regular expression.
type Object struct {
	ByOID   *blobcache.OID
	NameSet *regexp.Regexp
}

func (o Object) Equals(other Object) bool {
	switch {
	case o.ByOID != nil:
		return other.ByOID != nil && *o.ByOID == *other.ByOID
	case o.NameSet != nil:
		return other.NameSet != nil && o.NameSet.String() == other.NameSet.String()
	}
	return false
}

func (o Object) String() string {
	switch {
	case o.ByOID != nil:
		return o.ByOID.String()
	case o.NameSet != nil:
		return o.NameSet.String()
	default:
		return ""
	}
}

func ParseObject(x []byte) (Object, error) {
	if len(x) == hex.EncodedLen(len(blobcache.OID{})) {
		oid, err := blobcache.ParseOID(string(x))
		if err != nil {
			return Object{}, fmt.Errorf("invalid object: %s", x)
		}
		return Object{ByOID: &oid}, nil
	}
	re, err := regexp.Compile(string(x))
	if err != nil {
		return Object{}, fmt.Errorf("invalid object: %s", x)
	}
	return Object{NameSet: re}, nil
}

func ParseAuthzFile(r io.Reader) (ret []Grant, _ error) {
	scn := bufio.NewScanner(r)
	for linenum := 1; scn.Scan(); linenum++ {
		line := scn.Bytes()
		if len(line) == 0 {
			continue
		}
		grant, err := parseGrant(line)
		if err != nil {
			return nil, fmt.Errorf("invalid authz line %d: %w", linenum, err)
		}
		ret = append(ret, *grant)
	}
	return ret, nil
}

func WriteAuthzFile(w io.Writer, grants []Grant) error {
	bw := bufio.NewWriter(w)
	for _, g := range grants {
		if _, err := fmt.Fprintf(bw, "%s %s %s\n", g.Subject.String(), g.Verb, g.Object.String()); err != nil {
			return err
		}
	}
	return bw.Flush()
}

type Policy struct {
	groups map[string][]Identity
	grants []Grant
}

type grantKey struct {
	Object Object
	Verb   Action
}

func NewPolicy(membership []Membership, grants []Grant) *Policy {
	groups := make(map[string][]Identity)
	for _, m := range membership {
		groups[m.Group] = append(groups[m.Group], m.Member)
	}
	grantIdx := make(map[grantKey][]Identity)
	for _, g := range grants {
		k := grantKey{Verb: g.Verb, Object: g.Object}
		grantIdx[k] = append(grantIdx[k], g.Subject)
	}
	return &Policy{
		groups: groups,
		grants: grants,
	}
}

func (p *Policy) Supersets(a, b Identity) bool {
	switch {
	case a.Peer != nil:
		return b.Peer != nil && *a.Peer == *b.Peer
	case a.Name != nil:
		members := p.groups[*a.Name]
		for _, m := range members {
			if p.Supersets(m, b) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func (p *Policy) CanLook(subject blobcache.PeerID, oid blobcache.OID) bool {
	for _, g := range p.grants {
		if g.Verb != Action_LOOK {
			continue
		}
	}
	return false
}

// AllMemberships returns all the memberships in topological order.
// The order is such that a group can only be mentioned after all the groups it depends on have been mentioned.
func (p *Policy) AllMemberships() iter.Seq[Membership] {
	return func(yield func(Membership) bool) {
		seen := make(map[string]bool)
		var visit func(string)
		visit = func(group string) {
			if seen[group] {
				return
			}
			seen[group] = true
			for _, member := range p.groups[group] {
				if member.Name != nil {
					visit(*member.Name)
				}
			}
			for _, member := range p.groups[group] {
				if !yield(Membership{Group: group, Member: member}) {
					return
				}
			}
		}
		for group := range p.groups {
			visit(group)
		}
	}
}

func (p *Policy) AllGrants() iter.Seq[Grant] {
	return func(yield func(Grant) bool) {
		for _, g := range p.grants {
			yield(g)
		}
	}
}

func (p *Policy) AllGroups() iter.Seq[string] {
	return func(yield func(string) bool) {
		for group := range p.groups {
			yield(group)
		}
	}
}

// IsDefined returns true if the identity is a defined group, or a peer.
func (p *Policy) IsIdentityDefined(iden Identity) bool {
	switch {
	case iden.Peer != nil:
		return true
	case iden.Name != nil:
		_, exists := p.groups[*iden.Name]
		return exists
	default:
		return false
	}
}

func (p *Policy) AddMember(group string, member Identity) bool {
	if !p.IsIdentityDefined(member) {
		panic(fmt.Sprintf("identity %s is not defined", member.String()))
	}
	members := p.groups[group]
	for _, m := range members {
		if m.Equals(member) {
			return false
		}
	}
	p.groups[group] = append(members, member)
	return true
}

func (p *Policy) RemoveMember(group string, member Identity) (didChange bool) {
	members := p.groups[group]
	for i, m := range members {
		if m == member {
			members = slices.Delete(members, i, i+1)
			didChange = true
		}
	}
	p.groups[group] = members
	return didChange
}

func (p *Policy) MembersOf(group string) iter.Seq[Identity] {
	return func(yield func(Identity) bool) {
		for _, m := range p.groups[group] {
			if !yield(m) {
				return
			}
		}
	}
}

func (p *Policy) AddGrant(grant Grant) bool {
	if !p.IsIdentityDefined(grant.Subject) {
		panic(fmt.Sprintf("subject %s is not defined", grant.Subject.String()))
	}
	for _, g := range p.grants {
		if g.Equals(grant) {
			return false
		}
	}
	p.grants = append(p.grants, grant)
	return true
}

func (p *Policy) RemoveGrant(grant Grant) bool {
	for i, g := range p.grants {
		if g.Equals(grant) {
			p.grants = slices.Delete(p.grants, i, i+1)
			return true
		}
	}
	return false
}

// LoadAuthnFile loads the authn file from the filesystem.
func LoadAuthnFile(p string) ([]Membership, error) {
	f, err := os.Open(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	return ParseAuthnFile(f)
}

// LoadAuthzFile loads the authz file from the filesystem.
func LoadAuthzFile(p string) ([]Grant, error) {
	f, err := os.Open(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	return ParseAuthzFile(f)
}

// LoadPolicy loads the 2 policy files ( {stateDir}/AUTHN and {stateDir}/AUTHZ ) from the filesystem.
func LoadPolicy(stateDir string) (*Policy, error) {
	authnPath := filepath.Join(stateDir, AuthnFilename)
	authzPath := filepath.Join(stateDir, AuthzFilename)
	membership, err := LoadAuthnFile(authnPath)
	if err != nil {
		return nil, err
	}
	grants, err := LoadAuthzFile(authzPath)
	if err != nil {
		return nil, err
	}
	return NewPolicy(membership, grants), nil
}

func SavePolicy(stateDir string, policy *Policy) error {
	authnPath := filepath.Join(stateDir, AuthnFilename)
	authnFile, err := os.OpenFile(authnPath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer authnFile.Close()
	if err := WriteAuthnFile(authnFile, slices.Collect(policy.AllMemberships())); err != nil {
		return err
	}

	authzPath := filepath.Join(stateDir, AuthzFilename)
	authzFile, err := os.OpenFile(authzPath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer authzFile.Close()
	if err := WriteAuthzFile(authzFile, slices.Collect(policy.AllGrants())); err != nil {
		return err
	}
	return nil
}

func ptr[T any](x T) *T {
	return &x
}
