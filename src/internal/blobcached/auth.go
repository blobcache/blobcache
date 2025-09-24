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

func (a Action) String() string { return string(a) }

const (
	Action_LOOK   Action = "LOOK"
	Action_TOUCH  Action = "TOUCH"
	Action_CREATE Action = "CREATE"
)

func ParseAction(x []byte) (Action, error) {
	switch string(x) {
	case "LOOK":
		return Action_LOOK, nil
	case "TOUCH":
		return Action_TOUCH, nil
	case "CREATE":
		return Action_CREATE, nil
	}
	return "", fmt.Errorf("invalid action: %s", x)
}

func ParseActionsFile(r io.Reader) (ret []Membership[Action], _ error) {
	return ParseGroupsFile(r, ParseAction)
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
	Subject Member[Identity]
	Verb    Member[Action]
	Object  Member[ObjectSet]
}

func (g *Grant) Equals(other Grant) bool {
	// Equality by string formatting of members
	return g.Subject.Format(func(i Identity) string { return i.String() }) == other.Subject.Format(func(i Identity) string { return i.String() }) &&
		g.Verb.Format(func(a Action) string { return string(a) }) == other.Verb.Format(func(a Action) string { return string(a) }) &&
		g.Object.Format(func(o ObjectSet) string { return o.String() }) == other.Object.Format(func(o ObjectSet) string { return o.String() })
}

func parseGrant(line []byte) (*Grant, error) {
	parts := bytes.SplitN(line, []byte(" "), 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("grant must have 3 parts %v: ", line)
	}
	subject, actionStr, objectStr := parts[0], parts[1], parts[2]
	// parse subject as Member[Identity]
	var subj Member[Identity]
	if bytes.HasPrefix(subject, []byte("@")) {
		gname, err := ParseGroupName(subject[1:])
		if err != nil {
			return nil, fmt.Errorf("invalid authz line %s: %w", line, err)
		}
		subj.GroupRef = &gname
	} else {
		subjectIden, err := ParseIdentity([]byte(subject))
		if err != nil {
			return nil, fmt.Errorf("invalid authz line %s: %w", line, err)
		}
		subj.Unit = &subjectIden
	}
	// parse action as Member[Action]
	var verb Member[Action]
	if bytes.HasPrefix(actionStr, []byte("@")) {
		gname, err := ParseGroupName(actionStr[1:])
		if err != nil {
			return nil, fmt.Errorf("invalid authz line %s: %w", line, err)
		}
		verb.GroupRef = &gname
	} else {
		action, err := ParseAction([]byte(actionStr))
		if err != nil {
			return nil, err
		}
		verb.Unit = &action
	}
	// parse object as Member[ObjectSet]
	var obj Member[ObjectSet]
	if bytes.HasPrefix(objectStr, []byte("@")) {
		gname, err := ParseGroupName(objectStr[1:])
		if err != nil {
			return nil, fmt.Errorf("invalid authz line %s: %w", line, err)
		}
		obj.GroupRef = &gname
	} else {
		object, err := ParseObject([]byte(objectStr))
		if err != nil {
			return nil, err
		}
		obj.Unit = &object
	}
	return &Grant{
		Subject: subj,
		Verb:    verb,
		Object:  obj,
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
		if _, err := fmt.Fprintf(bw, "%s %s %s\n",
			g.Subject.Format(func(i Identity) string { return i.String() }),
			g.Verb.Format(func(a Action) string { return string(a) }),
			g.Object.Format(func(o ObjectSet) string { return o.String() }),
		); err != nil {
			return err
		}
	}
	return bw.Flush()
}

var _ bclocal.Policy = &Policy{}

type Policy struct {
	idens   map[GroupName][]Member[Identity]
	actions map[GroupName][]Member[Action]
	objects map[GroupName][]Member[ObjectSet]

	grants []Grant

	// original memberships for writing back
	idenMemberships   []Membership[Identity]
	actionMemberships []Membership[Action]
	objectMemberships []Membership[ObjectSet]

	// indexes
	iden2Grant     map[inet256.ID][]uint16
	anyoneGrants   []uint16
	vol2Grant      map[blobcache.OID][]uint16
	actionsClosure map[GroupName]blobcache.ActionSet
}

func (p *Policy) CanConnect(peer blobcache.PeerID) bool {
	if len(p.anyoneGrants) > 0 {
		return true
	}
	_, exists := p.iden2Grant[peer]
	return exists
}

func (p *Policy) Open(peer blobcache.PeerID, target blobcache.OID) blobcache.ActionSet {
	volGrants := p.vol2Grant[target]
	idenGrants := append([]uint16{}, p.iden2Grant[peer]...)
	if len(p.anyoneGrants) > 0 {
		idenGrants = append(idenGrants, p.anyoneGrants...)
	}
	// ensure sorted for findCommon
	slices.Sort(idenGrants)
	slices.Sort(volGrants)
	var rights blobcache.ActionSet
	for grantIndex := range findCommon(idenGrants, volGrants) {
		grant := p.grants[grantIndex]
		rights |= p.expandActionMember(grant.Verb)
	}
	return rights
}

func (p *Policy) CanCreate(peer blobcache.PeerID) bool {
	idenGrants := append([]uint16{}, p.iden2Grant[peer]...)
	if len(p.anyoneGrants) > 0 {
		idenGrants = append(idenGrants, p.anyoneGrants...)
	}
	// check if any corresponding grant has CREATE in its action closure
	for _, gi := range idenGrants {
		grant := p.grants[gi]
		rights := p.expandActionMember(grant.Verb)
		if rights&rightsForAction(Action_CREATE) != 0 {
			return true
		}
	}
	return false
}

//

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

func buildIndex[T any](membership []Membership[T]) map[GroupName][]Member[T] {
	idx := make(map[GroupName][]Member[T])
	for _, m := range membership {
		idx[m.Group] = append(idx[m.Group], m.Member)
	}
	return idx
}

func NewPolicy(idens []Membership[Identity], actions []Membership[Action], objects []Membership[ObjectSet], grants []Grant) *Policy {
	p := &Policy{
		idens:             buildIndex(idens),
		actions:           buildIndex(actions),
		objects:           buildIndex(objects),
		grants:            grants,
		idenMemberships:   append([]Membership[Identity](nil), idens...),
		actionMemberships: append([]Membership[Action](nil), actions...),
		objectMemberships: append([]Membership[ObjectSet](nil), objects...),
		iden2Grant:        make(map[inet256.ID][]uint16),
		vol2Grant:         make(map[blobcache.OID][]uint16),
		actionsClosure:    make(map[GroupName]blobcache.ActionSet),
	}
	// build indexes
	for gi, g := range grants {
		// subjects
		everyone, peers := p.expandIdentityMember(g.Subject)
		if everyone {
			p.anyoneGrants = append(p.anyoneGrants, uint16(gi))
		}
		for _, pid := range peers {
			p.iden2Grant[pid] = append(p.iden2Grant[pid], uint16(gi))
		}
		// objects
		for _, oid := range p.expandObjectMember(g.Object) {
			p.vol2Grant[oid] = append(p.vol2Grant[oid], uint16(gi))
		}
	}
	// sort and dedup indexes
	for k := range p.iden2Grant {
		s := p.iden2Grant[k]
		slices.Sort(s)
		s = slices.Compact(s)
		p.iden2Grant[k] = s
	}
	slices.Sort(p.anyoneGrants)
	p.anyoneGrants = slices.Compact(p.anyoneGrants)
	for k := range p.vol2Grant {
		s := p.vol2Grant[k]
		slices.Sort(s)
		s = slices.Compact(s)
		p.vol2Grant[k] = s
	}
	return p
}

// expandIdentityMember returns if EVERYONE is included and the set of peers
func (p *Policy) expandIdentityMember(m Member[Identity]) (bool, []inet256.ID) {
	seen := make(map[GroupName]bool)
	var everyone bool
	peers := make(map[inet256.ID]struct{})
	var visit func(Member[Identity])
	visit = func(mx Member[Identity]) {
		if mx.GroupRef != nil {
			if seen[*mx.GroupRef] {
				return
			}
			seen[*mx.GroupRef] = true
			for _, sub := range p.idens[*mx.GroupRef] {
				visit(sub)
			}
			return
		}
		if mx.Unit == nil {
			return
		}
		id := *mx.Unit
		switch {
		case id.Everyone != nil:
			everyone = true
		case id.Peer != nil:
			peers[*id.Peer] = struct{}{}
		}
	}
	visit(m)
	out := make([]inet256.ID, 0, len(peers))
	for pid := range peers {
		out = append(out, pid)
	}
	slices.SortFunc(out, func(a, b inet256.ID) int { return bytes.Compare(a[:], b[:]) })
	return everyone, out
}

func (p *Policy) expandObjectMember(m Member[ObjectSet]) []blobcache.OID {
	seen := make(map[GroupName]bool)
	oids := make(map[blobcache.OID]struct{})
	var visit func(Member[ObjectSet])
	visit = func(mx Member[ObjectSet]) {
		if mx.GroupRef != nil {
			if seen[*mx.GroupRef] {
				return
			}
			seen[*mx.GroupRef] = true
			for _, sub := range p.objects[*mx.GroupRef] {
				visit(sub)
			}
			return
		}
		if mx.Unit == nil {
			return
		}
		obj := *mx.Unit
		if obj.ByOID != nil {
			oids[*obj.ByOID] = struct{}{}
		}
	}
	visit(m)
	out := make([]blobcache.OID, 0, len(oids))
	for oid := range oids {
		out = append(out, oid)
	}
	// keep stable order; not strictly necessary
	slices.SortFunc(out, func(a, b blobcache.OID) int { return a.Compare(b) })
	return out
}

func (p *Policy) expandActionMember(m Member[Action]) blobcache.ActionSet {
	if m.GroupRef != nil {
		if as, ok := p.actionsClosure[*m.GroupRef]; ok {
			return as
		}
		// compute closure
		seen := make(map[GroupName]bool)
		var visit func(GroupName) blobcache.ActionSet
		visit = func(gn GroupName) blobcache.ActionSet {
			if seen[gn] {
				return 0
			}
			seen[gn] = true
			var ret blobcache.ActionSet
			for _, sub := range p.actions[gn] {
				if sub.GroupRef != nil {
					ret |= visit(*sub.GroupRef)
				} else if sub.Unit != nil {
					ret |= rightsForAction(*sub.Unit)
				}
			}
			return ret
		}
		as := visit(*m.GroupRef)
		p.actionsClosure[*m.GroupRef] = as
		return as
	}
	if m.Unit == nil {
		return 0
	}
	return rightsForAction(*m.Unit)
}

func rightsForAction(a Action) blobcache.ActionSet {
	switch a {
	case Action_LOOK:
		return blobcache.Action_Tx_Inspect |
			blobcache.Action_Tx_Load |
			blobcache.Action_Tx_Get |
			blobcache.Action_Tx_Exists |
			blobcache.Action_Tx_IsVisited |
			blobcache.Action_Volume_Inspect |
			blobcache.Action_Volume_Await
	case Action_TOUCH:
		return rightsForAction(Action_LOOK) |
			blobcache.Action_Volume_BeginTx |
			blobcache.Action_Tx_Post |
			blobcache.Action_Tx_Delete |
			blobcache.Action_Tx_AddFrom |
			blobcache.Action_Tx_AllowLink |
			blobcache.Action_Tx_Visited
	case Action_CREATE:
		// used for CanCreate; no implicit Open rights
		return blobcache.ActionSet(1) << 63 // sentinel bit unlikely used; ensure non-zero for detection
	default:
		return 0
	}
}

// Management and enumeration helpers used by admin CLI

// AllMemberships returns all the memberships in topological order.
// The order is such that a group can only be mentioned after all the groups it depends on have been mentioned.
func (p *Policy) AllMemberships() iter.Seq[Membership[Identity]] {
	return func(yield func(Membership[Identity]) bool) {
		for _, m := range p.idenMemberships {
			if !yield(m) {
				return
			}
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
		for group := range p.idens {
			if !yield(string(group)) {
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
	default:
		return false
	}
}

func (p *Policy) AddMember(group string, member Identity) bool {
	if !p.IsIdentityDefined(member) {
		panic(fmt.Sprintf("identity %s is not defined", member.String()))
	}
	members := p.idens[GroupName(group)]
	// check for duplicate unit member
	for _, m := range members {
		if m.GroupRef == nil && m.Unit != nil && m.Unit.Equals(member) {
			return false
		}
	}
	unit := member
	p.idens[GroupName(group)] = append(members, Member[Identity]{Unit: &unit})
	p.idenMemberships = append(p.idenMemberships, Membership[Identity]{Group: GroupName(group), Member: Member[Identity]{Unit: &unit}})
	return true
}

func (p *Policy) RemoveMember(group string, member Identity) (didChange bool) {
	g := GroupName(group)
	members := p.idens[g]
	out := members[:0]
	for _, m := range members {
		if !(m.GroupRef == nil && m.Unit != nil && m.Unit.Equals(member)) {
			out = append(out, m)
		} else {
			didChange = true
		}
	}
	p.idens[g] = out
	// update membership slice
	ims := p.idenMemberships[:0]
	for _, m := range p.idenMemberships {
		if !(m.Group == g && m.Member.GroupRef == nil && m.Member.Unit != nil && m.Member.Unit.Equals(member)) {
			ims = append(ims, m)
		}
	}
	p.idenMemberships = ims
	return didChange
}

func (p *Policy) MembersOf(group string) iter.Seq[Identity] {
	return func(yield func(Identity) bool) {
		for _, m := range p.idens[GroupName(group)] {
			if m.Unit != nil && m.GroupRef == nil {
				if !yield(*m.Unit) {
					return
				}
			}
		}
	}
}

func (p *Policy) AddGrant(grant Grant) bool {
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

// LoadIdentitiesFile loads the identities file from the filesystem.
func LoadIdentitiesFile(p string) ([]Membership[Identity], error) {
	f, err := os.Open(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	return ParseIdentitiesFile(f)
}

// LoadActionsFile loads the actions file from the filesystem.
func LoadActionsFile(p string) ([]Membership[Action], error) {
	f, err := os.Open(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	return ParseActionsFile(f)
}

// LoadObjectsFile loads the objects file from the filesystem.
func LoadObjectsFile(p string) ([]Membership[ObjectSet], error) {
	f, err := os.Open(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	return ParseObjectsFile(f)
}

// LoadGrantsFile loads the grants file from the filesystem.
func LoadGrantsFile(p string) ([]Grant, error) {
	f, err := os.Open(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	return ParseGrantsFile(f)
}

// LoadPolicy loads the 4 policy files from the filesystem.
func LoadPolicy(stateDir string) (*Policy, error) {
	idenPath := filepath.Join(stateDir, IdentitiesFilename)
	actionPath := filepath.Join(stateDir, ActionsFilename)
	objectPath := filepath.Join(stateDir, ObjectsFilename)
	grantsPath := filepath.Join(stateDir, GrantsFilename)
	idens, err := LoadIdentitiesFile(idenPath)
	if err != nil {
		return nil, err
	}
	acts, err := LoadActionsFile(actionPath)
	if err != nil {
		return nil, err
	}
	objs, err := LoadObjectsFile(objectPath)
	if err != nil {
		return nil, err
	}
	grants, err := LoadGrantsFile(grantsPath)
	if err != nil {
		return nil, err
	}
	return NewPolicy(idens, acts, objs, grants), nil
}

func SavePolicy(stateDir string, policy *Policy) error {
	// identities
	idenPath := filepath.Join(stateDir, IdentitiesFilename)
	authnFile, err := os.OpenFile(idenPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer authnFile.Close()
	if err := WriteIdentitiesFile(authnFile, policy.idenMemberships); err != nil {
		return err
	}

	// actions
	actionPath := filepath.Join(stateDir, ActionsFilename)
	actionFile, err := os.OpenFile(actionPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer actionFile.Close()
	if err := WriteActionsFile(actionFile, policy.actionMemberships); err != nil {
		return err
	}

	// objects
	objectPath := filepath.Join(stateDir, ObjectsFilename)
	objectFile, err := os.OpenFile(objectPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer objectFile.Close()
	if err := WriteObjectsFile(objectFile, policy.objectMemberships); err != nil {
		return err
	}

	// grants
	grantsPath := filepath.Join(stateDir, GrantsFilename)
	grantsFile, err := os.OpenFile(grantsPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer grantsFile.Close()
	if err := WriteAuthzFile(grantsFile, slices.Collect(policy.AllGrants())); err != nil {
		return err
	}
	return nil
}

func ptr[T any](x T) *T {
	return &x
}
