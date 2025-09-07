package blobcachecmd

import (
	"fmt"

	"blobcache.io/blobcache/src/internal/blobcached"
	"go.brendoncarroll.net/star"
)

var groupsCmd = star.Command{
	Metadata: star.Metadata{
		Short: "list all groups and their members",
	},
	Flags: []star.AnyParam{stateDirParam},
	Pos:   []star.AnyParam{},
	F: func(c star.Context) error {
		stateDir := stateDirParam.Load(c)
		policy, err := blobcached.LoadPolicy(stateDir)
		if err != nil {
			return err
		}
		for groupName := range policy.AllGroups() {
			c.Printf("%s:\n", groupName)
			for member := range policy.MembersOf(groupName) {
				c.Printf("  %s\n", member.String())
			}
		}
		return nil
	},
}

var addMemCmd = star.Command{
	Metadata: star.Metadata{
		Short: "add a member to a group",
	},
	Flags: []star.AnyParam{stateDirParam},
	Pos:   []star.AnyParam{groupNameParam, memberNameParam},
	F: func(c star.Context) error {
		stateDir := stateDirParam.Load(c)
		policy, err := blobcached.LoadPolicy(stateDir)
		if err != nil {
			return err
		}
		groupName := groupNameParam.Load(c)
		memberName := memberNameParam.Load(c)
		if !policy.IsIdentityDefined(memberName) {
			return fmt.Errorf("identity %s is not defined", memberName.String())
		}
		if policy.AddMember(groupName, memberName) {
			c.Printf("Added %s to %s\n", memberName, groupName)
		} else {
			c.Printf("Already a member of %s\n", groupNameParam.Load(c))
		}
		return blobcached.SavePolicy(stateDir, policy)
	},
}

var rmMemCmd = star.Command{
	Metadata: star.Metadata{
		Short: "remove a member from a group",
	},
	Flags: []star.AnyParam{stateDirParam},
	Pos:   []star.AnyParam{groupNameParam, memberNameParam},
	F: func(c star.Context) error {
		stateDir := stateDirParam.Load(c)
		policy, err := blobcached.LoadPolicy(stateDir)
		if err != nil {
			return err
		}
		groupName := groupNameParam.Load(c)
		member := memberNameParam.Load(c)
		if policy.RemoveMember(groupName, member) {
			c.Printf("Removed %s from %s\n", member.String(), groupName)
		} else {
			c.Printf("Not a member of %s\n", groupName)
		}
		return blobcached.SavePolicy(stateDir, policy)
	},
}

var grantCmd = star.Command{
	Flags: []star.AnyParam{stateDirParam},
	Pos:   []star.AnyParam{subjectParam, verbParam, objectParam},
	F: func(c star.Context) error {
		stateDir := stateDirParam.Load(c)
		policy, err := blobcached.LoadPolicy(stateDir)
		if err != nil {
			return err
		}
		g := blobcached.Grant{
			Subject: subjectParam.Load(c),
			Verb:    verbParam.Load(c),
			Object:  objectParam.Load(c),
		}
		if policy.AddGrant(g) {
			c.Printf("Granted (%v) \n", g)
		} else {
			c.Printf("Grant (%v) already exists\n", g)
		}
		return blobcached.SavePolicy(stateDir, policy)
	},
}

var revokeCmd = star.Command{
	Flags: []star.AnyParam{stateDirParam},
	Pos:   []star.AnyParam{subjectParam, verbParam, objectParam},
	F: func(c star.Context) error {
		stateDir := stateDirParam.Load(c)
		policy, err := blobcached.LoadPolicy(stateDir)
		if err != nil {
			return err
		}
		g := blobcached.Grant{
			Subject: subjectParam.Load(c),
			Verb:    verbParam.Load(c),
			Object:  objectParam.Load(c),
		}
		if policy.RemoveGrant(g) {
			c.Printf("Revoked (%v)\n", g)
		} else {
			c.Printf("Grant (%v) does not exist\n", g)
		}
		return blobcached.SavePolicy(stateDir, policy)
	},
}

var groupNameParam = star.Param[string]{
	Name:  "group",
	Parse: star.ParseString,
}

var memberNameParam = star.Param[blobcached.Identity]{
	Name: "member",
	Parse: func(x string) (blobcached.Identity, error) {
		return blobcached.ParseIdentity([]byte(x))
	},
}

var subjectParam = star.Param[blobcached.Identity]{
	Name: "subject",
	Parse: func(x string) (blobcached.Identity, error) {
		return blobcached.ParseIdentity([]byte(x))
	},
}

var verbParam = star.Param[blobcached.Action]{
	Name: "verb",
	Parse: func(x string) (blobcached.Action, error) {
		return blobcached.ParseAction([]byte(x))
	},
}

var objectParam = star.Param[blobcached.ObjectSet]{
	Name: "object",
	Parse: func(x string) (blobcached.ObjectSet, error) {
		return blobcached.ParseObject([]byte(x))
	},
}
