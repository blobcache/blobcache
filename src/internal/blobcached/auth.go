package blobcached

import (
	"bufio"
	"bytes"
	"cmp"
	"encoding/hex"
	"fmt"
	"io"
	"iter"
	"os"
	"slices"
	"strings"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/groupfile"
	"go.brendoncarroll.net/exp/slices2"
	"go.inet256.org/inet256/src/inet256"
)

const (
	IdentitiesFilename = "IDENTITIES"
	ActionsFilename    = "ACTIONS"
	ObjectsFilename    = "OBJECTS"
	GrantsFilename     = "GRANTS"
)

var Everyone = inet256.Everyone()

type Identity = blobcache.PeerID

func ParseIdentity(x []byte) (Identity, error) {
	addr, err := inet256.ParseAddrBase64(x)
	if err != nil {
		return Identity{}, err
	}
	return addr, nil
}

// ParseIdentitiesFiles parses a Group file into a list of identity group memberships.
func ParseIdentitiesFile(data []byte) (ret []groupfile.Entry[GroupName, Identity], _ error) {
	return groupfile.Parse(data, ParseGroupName, ParseIdentity)
}

// WriteIdentitiesFile writes the memberships to the writer, such that they can be parsed by ParseIdentitiesFile.
// It inserts an extra newline every time the group changes from the previous membership.
func WriteIdentitiesFile(w io.Writer, ents []groupfile.Entry[GroupName, Identity]) error {
	fmtIden := func(i Identity) string { return i.String() }
	return groupfile.Write(w, ents, fmtIden)
}

func DefaultIdentitiesFile() (ret string) {
	ret += "everyone " + Everyone.String() + "\n"
	ret += "admin\n"
	return ret
}

type Action string

func (a Action) String() string { return string(a) }

const (
	Action_LOAD        Action = "LOAD"
	Action_SAVE        Action = "SAVE"
	Action_POST        Action = "POST"
	Action_GET         Action = "GET"
	Action_EXISTS      Action = "EXISTS"
	Action_DELETE      Action = "DELETE"
	Action_COPY_FROM   Action = "COPY_FROM"
	Action_COPY_TO     Action = "COPY_TO"
	Action_LINK_FROM   Action = "LINK_FROM"
	Action_LINK_TO     Action = "LINK_TO"
	Action_UNLINK_FROM Action = "UNLINK_FROM"
	Action_CLONE       Action = "CLONE"
	Action_CREATE      Action = "CREATE"
)

func (a Action) ToSet() blobcache.ActionSet {
	switch a {
	case Action_LOAD:
		return blobcache.Action_TX_LOAD
	case Action_SAVE:
		return blobcache.Action_TX_SAVE
	case Action_POST:
		return blobcache.Action_TX_POST
	case Action_GET:
		return blobcache.Action_TX_GET
	case Action_EXISTS:
		return blobcache.Action_TX_EXISTS
	case Action_DELETE:
		return blobcache.Action_TX_DELETE
	case Action_COPY_FROM:
		return blobcache.Action_TX_COPY_FROM
	case Action_COPY_TO:
		return blobcache.Action_TX_COPY_TO
	case Action_LINK_FROM:
		return blobcache.Action_TX_LINK_FROM
	case Action_LINK_TO:
		return blobcache.Action_TX_LINK_FROM
	case Action_UNLINK_FROM:
		return blobcache.Action_TX_UNLINK_FROM
	case Action_CLONE:
		return blobcache.Action_VOLUME_CLONE
	case Action_CREATE:
		return blobcache.Action_VOLUME_CREATE
	}
	panic(a)
}

func AllActions() []Action {
	return []Action{
		Action_LOAD,
		Action_SAVE,
		Action_POST,
		Action_EXISTS,
		Action_DELETE,
		Action_COPY_FROM,
		Action_COPY_TO,
		Action_LINK_FROM,
		Action_LINK_TO,
		Action_UNLINK_FROM,

		Action_CLONE,
		Action_CREATE,
	}
}

func ParseAction(x []byte) (Action, error) {
	switch string(x) {
	case "LOAD":
		return Action_LOAD, nil
	case "SAVE":
		return Action_SAVE, nil
	case "POST":
		return Action_POST, nil
	case "GET":
		return Action_GET, nil
	case "EXISTS":
		return Action_EXISTS, nil
	case "DELETE":
		return Action_DELETE, nil
	case "COPY_FROM":
		return Action_COPY_FROM, nil
	case "COPY_TO":
		return Action_COPY_TO, nil
	case "LINK_FROM":
		return Action_LINK_FROM, nil
	case "LINK_TO":
		return Action_LINK_TO, nil
	case "UNLINK_FROM":
		return Action_UNLINK_FROM, nil
	case "CLONE":
		return Action_CLONE, nil
	case "CREATE":
		return Action_CREATE, nil
	}
	return "", fmt.Errorf("invalid action: %s", x)
}

func DefaultActionsFile() (ret string) {
	appendLine := func(out string, group string, as ...Action) string {
		out += group +
			" " +
			strings.Join(slices2.Map(as, Action.String), " ") +
			"\n"
		return out
	}
	ret += appendLine(ret, "all", AllActions()...)
	ret += appendLine(ret, "look",
		Action_LOAD, Action_GET, Action_COPY_FROM, Action_LINK_TO,
	)
	ret += "touch @look SAVE POST DELETE COPY_TO\n"
	return ret
}

func ParseActionsFile(data []byte) (ret []groupfile.Entry[GroupName, Action], _ error) {
	return groupfile.Parse(data, ParseGroupName, ParseAction)
}

func WriteActionsFile(w io.Writer, actions []groupfile.Entry[GroupName, Action]) error {
	return groupfile.Write(w, actions, func(a Action) string { return string(a) })
}

// ObjectSet is something that Actions are performed on.
// It can be a specific OID, or a set of names defined by a regular expression.
type ObjectSet struct {
	// ByOID is a specific OID
	ByOID *blobcache.OID
	// All refers to all possible objects
	All *struct{}
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
	if string(x) == "ALL" {
		return ObjectSet{All: &struct{}{}}, nil
	}
	if len(x) == hex.EncodedLen(len(blobcache.OID{})) {
		oid, err := blobcache.ParseOID(string(x))
		if err != nil {
			return ObjectSet{}, fmt.Errorf("invalid object: %s", x)
		}
		return ObjectSet{ByOID: &oid}, nil
	}
	return ObjectSet{}, fmt.Errorf("could not parse object set: %s", x)
}

func ParseObjectsFile(data []byte) (ret []groupfile.Entry[GroupName, ObjectSet], _ error) {
	return groupfile.Parse(data, ParseGroupName, ParseObject)
}

func WriteObjectsFile(w io.Writer, ents []groupfile.Entry[GroupName, ObjectSet]) error {
	return groupfile.Write(w, ents, func(o ObjectSet) string { return o.String() })
}

func DefaultObjectsFile() (ret string) {
	ret += "all ALL\n"
	return ret
}

type Grant struct {
	Subject groupfile.Member[GroupName, Identity]
	Action  groupfile.Member[GroupName, Action]
	Object  groupfile.Member[GroupName, ObjectSet]
}

func (g *Grant) Equals(other Grant) bool {
	// Equality by string formatting of members
	return g.Subject.Format(func(i Identity) string { return i.String() }) == other.Subject.Format(func(i Identity) string { return i.String() }) &&
		g.Action.Format(func(a Action) string { return string(a) }) == other.Action.Format(func(a Action) string { return string(a) }) &&
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
		Action:  verb,
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

func WriteGrantsFile(w io.Writer, grants []Grant) error {
	bw := bufio.NewWriter(w)
	for _, g := range grants {
		if _, err := fmt.Fprintf(bw, "%s %s %s\n",
			g.Subject.Format(func(i Identity) string { return i.String() }),
			g.Action.Format(func(a Action) string { return string(a) }),
			g.Object.Format(func(o ObjectSet) string { return o.String() }),
		); err != nil {
			return err
		}
	}
	return bw.Flush()
}

func DefaultGrantsFile() (ret string) {
	ret += "@admin @all @all\n"
	return ret
}

var _ bclocal.Policy = &Policy{}

type Policy struct {
	idens   map[GroupName][]Member[Identity]
	actions map[GroupName][]Member[Action]
	objects map[GroupName][]Member[ObjectSet]

	grants []Grant

	// indexes
	iden2Grant      map[inet256.ID][]uint16
	anyoneGrants    []uint16
	allObjectGrants []uint16
	vol2Grant       map[blobcache.OID][]uint16
	actionsClosure  map[GroupName]blobcache.ActionSet
}

func (p *Policy) CanConnect(peer blobcache.PeerID) bool {
	if len(p.anyoneGrants) > 0 {
		return true
	}
	_, exists := p.iden2Grant[peer]
	return exists
}

func (p *Policy) OpenFiat(peer blobcache.PeerID, target blobcache.OID) blobcache.ActionSet {
	volGrants := slices.Clone(p.vol2Grant[target])
	volGrants = append(volGrants, p.allObjectGrants...)
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
		rights |= p.expandActionMember(grant.Action)
	}
	// Open should never include Action_VOLUME_CREATE in the returned handle rights.
	rights &^= blobcache.Action_VOLUME_CREATE
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
		rights := p.expandActionMember(grant.Action)
		if rights&Action_CREATE.ToSet() != 0 {
			return true
		}
	}
	return false
}

// findCommon finds the common elements of two sorted slices.
func findCommon[T cmp.Ordered](a, b []T) iter.Seq[T] {
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

func buildIndex[T any](membership []Membership[T]) map[GroupName][]Member[T] {
	idx := make(map[GroupName][]Member[T])
	for _, m := range membership {
		if m.Member.Empty != nil {
			idx[m.Group] = idx[m.Group] // make sure there is something there
		} else {
			idx[m.Group] = append(idx[m.Group], m.Member)
		}
	}
	return idx
}

func NewPolicy(idens []Membership[Identity], actions []Membership[Action], objects []Membership[ObjectSet], grants []Grant) (*Policy, error) {
	p := &Policy{
		idens:   buildIndex(idens),
		actions: buildIndex(actions),
		objects: buildIndex(objects),
		grants:  grants,
		// idenMemberships:   slices.Clone(idens),
		// actionMemberships: slices.Clone(actions),
		// objectMemberships: slices.Clone(objects),
		iden2Grant:     make(map[inet256.ID][]uint16),
		vol2Grant:      make(map[blobcache.OID][]uint16),
		actionsClosure: make(map[GroupName]blobcache.ActionSet),
	}

	// validate grants
	for _, g := range grants {
		if g.Subject.GroupRef != nil {
			if _, ok := p.idens[*g.Subject.GroupRef]; !ok {
				return nil, fmt.Errorf("subject group %v not found", *g.Subject.GroupRef)
			}
		}
		if g.Action.GroupRef != nil {
			if _, ok := p.actions[*g.Action.GroupRef]; !ok {
				return nil, fmt.Errorf("action group %v not found", *g.Action.GroupRef)
			}
		}
		if g.Object.GroupRef != nil {
			if _, ok := p.objects[*g.Object.GroupRef]; !ok {
				return nil, fmt.Errorf("object group %v not found", *g.Object.GroupRef)
			}
		}
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
		includesAll, oids := p.expandObjectMember(g.Object)
		if includesAll {
			p.allObjectGrants = append(p.allObjectGrants, uint16(gi))
		}
		for _, oid := range oids {
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
	p.allObjectGrants = slices.Compact(p.allObjectGrants)
	for k := range p.vol2Grant {
		s := p.vol2Grant[k]
		slices.Sort(s)
		s = slices.Compact(s)
		p.vol2Grant[k] = s
	}
	return p, nil
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
		if id == Everyone {
			everyone = true
		}
		peers[id] = struct{}{}
	}
	visit(m)
	out := make([]inet256.ID, 0, len(peers))
	for pid := range peers {
		out = append(out, pid)
	}
	slices.SortFunc(out, func(a, b inet256.ID) int { return bytes.Compare(a[:], b[:]) })
	return everyone, out
}

func (p *Policy) expandObjectMember(m Member[ObjectSet]) (bool, []blobcache.OID) {
	seen := make(map[GroupName]bool)
	oids := make(map[blobcache.OID]struct{})
	var includesAll bool
	var visit func(Member[ObjectSet])
	visit = func(mx Member[ObjectSet]) {
		switch {
		case mx.GroupRef != nil:
			if seen[*mx.GroupRef] {
				return
			}
			seen[*mx.GroupRef] = true
			for _, sub := range p.objects[*mx.GroupRef] {
				visit(sub)
			}
			return
		case mx.Unit != nil:
			obj := *mx.Unit
			if obj.ByOID != nil {
				oids[*obj.ByOID] = struct{}{}
			} else if obj.All != nil {
				includesAll = true
			}
		}
	}
	visit(m)
	out := make([]blobcache.OID, 0, len(oids))
	for oid := range oids {
		out = append(out, oid)
	}
	// keep stable order; not strictly necessary
	slices.SortFunc(out, func(a, b blobcache.OID) int { return a.Compare(b) })
	return includesAll, out
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
					ret |= sub.Unit.ToSet()
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
	return m.Unit.ToSet()
}

// Management and enumeration helpers used by admin CLI

func (p *Policy) AllGrants() iter.Seq[Grant] {
	return func(yield func(Grant) bool) {
		for _, g := range p.grants {
			if !yield(g) {
				return
			}
		}
	}
}

func (p *Policy) AllIdentityGroups() iter.Seq[string] {
	return func(yield func(string) bool) {
		for group := range p.idens {
			if !yield(string(group)) {
				return
			}
		}
	}
}

func (p *Policy) AllActionGroups() iter.Seq[string] {
	return func(yield func(string) bool) {
		for group := range p.actions {
			if !yield(string(group)) {
				return
			}
		}
	}
}

func (p *Policy) AllObjectGroups() iter.Seq[string] {
	return func(yield func(string) bool) {
		for group := range p.objects {
			if !yield(string(group)) {
				return
			}
		}
	}
}

// IsDefined returns true if the identity is a defined group, or a peer.
func (p *Policy) IsIdentityDefined(iden Identity) bool {
	switch {
	case iden == Everyone:
		return true
	default:
		return false
	}
}

func (p *Policy) IdentityMembersOf(group string) iter.Seq[Identity] {
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

// LoadIdentitiesFile loads the identities file from the filesystem.
// p should be the path to the identities file.
func LoadIdentitiesFile(dir *os.Root, p string) ([]Entry[Identity], error) {
	data, err := dir.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return ParseIdentitiesFile(data)
}

// LoadActionsFile loads the actions file from the filesystem.
// p should be the path to the actions file.
func LoadActionsFile(dir *os.Root, p string) ([]Entry[Action], error) {
	data, err := dir.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return ParseActionsFile(data)
}

// LoadObjectsFile loads the objects file from the filesystem.
// p should be the path to the objects file.
func LoadObjectsFile(dir *os.Root, p string) ([]Entry[ObjectSet], error) {
	data, err := dir.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return ParseObjectsFile(data)
}

// LoadGrantsFile loads the grants file from the filesystem.
// p should be the path to the grants file.
func LoadGrantsFile(dir *os.Root, p string) ([]Grant, error) {
	f, err := dir.Open(p)
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
// stateDir should be the path to the state directory.
func LoadPolicy(dir *os.Root) (*Policy, error) {
	idenPath := IdentitiesFilename
	actionPath := ActionsFilename
	objectPath := ObjectsFilename
	grantsPath := GrantsFilename
	idens, err := LoadIdentitiesFile(dir, idenPath)
	if err != nil {
		return nil, fmt.Errorf("loading IDENTITIES file: %w", err)
	}
	acts, err := LoadActionsFile(dir, actionPath)
	if err != nil {
		return nil, fmt.Errorf("loading ACTIONS file: %w", err)
	}
	objs, err := LoadObjectsFile(dir, objectPath)
	if err != nil {
		return nil, fmt.Errorf("loading OBJECTS file: %w", err)
	}
	grants, err := LoadGrantsFile(dir, grantsPath)
	if err != nil {
		return nil, fmt.Errorf("loading GRANTS file: %w", err)
	}
	return NewPolicy(mFromE(idens), mFromE(acts), mFromE(objs), grants)
}

func ptr[T any](x T) *T {
	return &x
}

func mFromE[T any](ents []Entry[T]) (ret []Membership[T]) {
	for _, ent := range ents {
		for _, m := range ent.Memberships() {
			ret = append(ret, m)
		}
	}
	return ret
}
