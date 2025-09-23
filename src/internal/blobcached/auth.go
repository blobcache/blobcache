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
	"slices"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"go.inet256.org/inet256/src/inet256"
	"golang.org/x/exp/constraints"
)

const (
	IdentitiesFilename = "IDENTITIES"
	ActionsFilename    = "ACTIONS"
	ObjectsFilename    = "OBJECTS"
	GrantsFilename     = "GRANTS"
)

type Identity struct {
	// Peer is a single peer.
	Peer *blobcache.PeerID
	// Everyone refers to all possible peers
	Everyone *struct{}
}

func (iden Identity) Equals(other Identity) bool {
	switch {
	case iden.Everyone != nil:
		return other.Everyone != nil
	case iden.Peer != nil:
		return other.Peer != nil && *iden.Peer == *other.Peer
	}
	return false
}

func (iden Identity) String() string {
	if iden.Everyone != nil {
		return "EVERYONE"
	}
	return iden.Peer.String()
}

func ParseIdentity(x []byte) (Identity, error) {
	if string(x) == "EVERYONE" {
		return Identity{Everyone: &struct{}{}}, nil
	}
	addr, err := inet256.ParseAddrBase64(x)
	if err != nil {
		return Identity{}, err
	}
	return Identity{Peer: &addr}, nil
}

// ParseIdentitiesFiles parses a Group file into a list of identity group memberships.
func ParseIdentitiesFile(r io.Reader) (ret []Membership[Identity], _ error) {
	return ParseGroupsFile(r, ParseIdentity)
}

// WriteIdentitiesFile writes the memberships to the writer, such that they can be parsed by ParseIdentitiesFile.
// It inserts an extra newline every time the group changes from the previous membership.
func WriteIdentitiesFile(w io.Writer, membership []Membership[Identity]) error {
	fmtIden := func(i Identity) string { return i.String() }
	return WriteGroupsFile(w, membership, fmtIden)
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

func ParseActionsFile(r io.Reader) (ret []Action, _ error) {
	scn := bufio.NewScanner(r)
	for linenum := 1; scn.Scan(); linenum++ {
		line := scn.Bytes()
		if len(line) == 0 {
			continue
		}
		action, err := ParseAction(line)
		if err != nil {
			return nil, fmt.Errorf("invalid action line %d: %w", linenum, err)
		}
		ret = append(ret, action)
		return ret, nil
	}
	return ret, nil
}

func WriteActionsFile(w io.Writer, actions []Membership[Action]) error {
	return WriteGroupsFile(w, actions, func(a Action) string { return string(a) })
}

// ObjectSet is something that Actions are performed on.
// It can be a specific OID, or a set of names defined by a regular expression.
type ObjectSet struct {
	ByOID *blobcache.OID
}

func (o ObjectSet) Equals(other ObjectSet) bool {
	switch {
	case o.ByOID != nil:
		return other.ByOID != nil && *o.ByOID == *other.ByOID
	}
	return false
}

func (o ObjectSet) String() string {
	switch {
	case o.ByOID != nil:
		return o.ByOID.String()
	default:
		return ""
	}
}

func ParseObject(x []byte) (ObjectSet, error) {
	if len(x) == hex.EncodedLen(len(blobcache.OID{})) {
		oid, err := blobcache.ParseOID(string(x))
		if err != nil {
			return ObjectSet{}, fmt.Errorf("invalid object: %s", x)
		}
		return ObjectSet{ByOID: &oid}, nil
	}
	return ObjectSet{}, fmt.Errorf("could not parse object set: %s", x)
}

func ParseObjectsFile(r io.Reader) (ret []Membership[ObjectSet], _ error) {
	return ParseGroupsFile(r, ParseObject)
}

func WriteObjectsFile(w io.Writer, objects []Membership[ObjectSet]) error {
	return WriteGroupsFile(w, objects, func(o ObjectSet) string { return o.String() })
}

type Grant struct {
	Subject Identity
	Verb    Action
	Object  ObjectSet
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

func ParseGrantsFile(r io.Reader) (ret []Grant, _ error) {
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

var _ bclocal.Policy = &Policy{}

type Policy struct {
	idens   map[GroupName][]Member[Identity]
	actions map[GroupName][]Action
	objects map[GroupName][]Member[ObjectSet]

	grants []Grant

	iden2Grant     map[inet256.ID][]uint16
	vol2Grant      map[blobcache.OID][]uint16
	actionsClosure map[GroupName]blobcache.ActionSet
}

func (p *Policy) CanConnect(peer blobcache.PeerID) bool {
	_, exists := p.iden2Grant[peer]
	return exists
}

func (p *Policy) Open(peer blobcache.PeerID, target blobcache.OID) blobcache.ActionSet {
	volGrants := p.vol2Grant[target]
	idenGrants := p.iden2Grant[peer]
	for grantIndex := range findCommon(idenGrants, volGrants) {
		grant := p.grants[grantIndex]
		panic(grant) // TODO
	}

	return blobcache.Action_ALL
}

func (p *Policy) CanCreate(peer blobcache.PeerID) bool {
	return false
}

func (p *Policy) actionGroupContains(name GroupName, action Action) bool {
	actions := p.actions[name]
	for _, a := range actions {
		if a == action {
			return true
		}
	}
	return false
}

// findCommon finds the common elements of two sorted slices.
func findCommon[T constraints.Ordered](a, b []T) iter.Seq[T] {
	return func(yield func(T) bool) {
		for ai, bi := 0, 0; ai < len(a) && bi < len(b); {
			switch {
			case a[ai] == b[bi]:
				if !yield(a[ai]) {
					return
				}
				ai++
				bi++
			case a[ai] < b[bi]:
				ai++
			case a[ai] > b[bi]:
				bi++
			}
		}
	}
}

type grantKey struct {
	Subject Member[Identity]
	Verb    Member[Action]
	Object  Member[ObjectSet]
}

func buildIndex[T any](membership []Membership[T]) map[GroupName][]T {
	idx := make(map[GroupName][]T)
	for _, m := range membership {
		idx[m.Group] = append(idx[m.Group], *m.Member.Unit)
	}
	return idx
}

func NewPolicy(idens []Membership[Identity], actions []Membership[Action], objects []Membership[ObjectSet], grants []Grant) *Policy {
	idenIdx := buildIndex(idens)
	actionIdx := buildIndex(actions)
	objectIdx := buildIndex(objects)
	grantIdx := make(map[grantKey][]Identity)
	for _, g := range grants {
		gk := grantKey{
			Verb:    Member[Action]{Unit: &g.Verb},
			Object:  Member[ObjectSet]{Unit: &g.Object},
			Subject: Member[Identity]{Unit: &g.Subject},
		}
		grantIdx[gk] = append(grantIdx[gk])
	}
	return &Policy{
		idens:   idenIdx,
		actions: actionIdx,
		objects: objectIdx,
		grants:  grants,
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
func (p *Policy) AllMemberships() iter.Seq[Membership[Identity]] {
	return func(yield func(Membership[Identity]) bool) {
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
				if !yield(Membership[Identity]{Group: GroupName(group), Member: Member[Identity]{Unit: &member}}) {
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
			if !yield(g) {
				return
			}
		}
	}
}

func (p *Policy) AllGroups() iter.Seq[string] {
	return func(yield func(string) bool) {
		for group := range p.groups {
			if !yield(group) {
				return
			}
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
func LoadAuthnFile(p string) ([]Membership[Identity], error) {
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
