// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"sync"
)

type iamDummyStore struct {
	sync.RWMutex
	*iamCache
	usersSysType UsersSysType
}

func newIAMDummyStore(usersSysType UsersSysType) *iamDummyStore {
	return &iamDummyStore{
		iamCache:     newIamCache(),
		usersSysType: usersSysType,
	}
}

func (ids *iamDummyStore) rlock() *iamCache {
	ids.RLock()
	return ids.iamCache
}

func (ids *iamDummyStore) runlock() {
	ids.RUnlock()
}

func (ids *iamDummyStore) lock() *iamCache {
	ids.Lock()
	return ids.iamCache
}

func (ids *iamDummyStore) unlock() {
	ids.Unlock()
}

func (ids *iamDummyStore) getUsersSysType() UsersSysType {
	return ids.usersSysType
}

func (ids *iamDummyStore) loadPolicyDoc(_ context.Context, policy string, m map[string]PolicyDoc) error {
	v, ok := ids.iamPolicyDocsMap[policy]
	if !ok {
		return errNoSuchPolicy
	}
	m[policy] = v
	return nil
}

func (ids *iamDummyStore) loadPolicyDocs(_ context.Context, m map[string]PolicyDoc) error {
	for k, v := range ids.iamPolicyDocsMap {
		m[k] = v
	}
	return nil
}

func (ids *iamDummyStore) loadUser(_ context.Context, user string, _ IAMUserType, _ map[string]UserIdentity) error {
	u, ok := ids.iamUsersMap[user]
	if !ok {
		return errNoSuchUser
	}
	ids.iamUsersMap[user] = u
	return nil
}

func (ids *iamDummyStore) loadUsers(_ context.Context, _ IAMUserType, m map[string]UserIdentity) error {
	for k, v := range ids.iamUsersMap {
		m[k] = v
	}
	return nil
}

func (ids *iamDummyStore) loadGroup(_ context.Context, group string, m map[string]GroupInfo) error {
	g, ok := ids.iamGroupsMap[group]
	if !ok {
		return errNoSuchGroup
	}
	m[group] = g
	return nil
}

func (ids *iamDummyStore) loadGroups(_ context.Context, m map[string]GroupInfo) error {
	for k, v := range ids.iamGroupsMap {
		m[k] = v
	}
	return nil
}

func (ids *iamDummyStore) loadMappedPolicy(_ context.Context, name string, _ IAMUserType, isGroup bool, m map[string]MappedPolicy) error {
	if isGroup {
		g, ok := ids.iamGroupPolicyMap[name]
		if !ok {
			return errNoSuchPolicy
		}
		m[name] = g
	} else {
		u, ok := ids.iamUserPolicyMap[name]
		if !ok {
			return errNoSuchPolicy
		}
		m[name] = u
	}
	return nil
}

func (ids *iamDummyStore) loadMappedPolicies(_ context.Context, _ IAMUserType, isGroup bool, m map[string]MappedPolicy) error {
	if !isGroup {
		for k, v := range ids.iamUserPolicyMap {
			m[k] = v
		}
	} else {
		for k, v := range ids.iamGroupPolicyMap {
			m[k] = v
		}
	}
	return nil
}

func (ids *iamDummyStore) saveIAMConfig(_ context.Context, _ /*item*/ interface{}, _ /*path*/ string, _ /*opts*/ ...options) error {
	return nil
}

func (ids *iamDummyStore) loadIAMConfig(_ context.Context, _ /*item*/ interface{}, _ /*path*/ string) error {
	return nil
}

func (ids *iamDummyStore) deleteIAMConfig(_ context.Context, _ /*path*/ string) error {
	return nil
}

func (ids *iamDummyStore) savePolicyDoc(_ context.Context, _ /*policyName*/ string, _ /*p*/ PolicyDoc) error {
	return nil
}

func (ids *iamDummyStore) saveMappedPolicy(_ context.Context, _ /*name*/ string, _ IAMUserType, _ /*isGroup*/ bool, _ MappedPolicy, _ ...options) error {
	return nil
}

func (ids *iamDummyStore) saveUserIdentity(_ context.Context, _ /*name*/ string, _ IAMUserType, _ UserIdentity, _ ...options) error {
	return nil
}

func (ids *iamDummyStore) saveGroupInfo(_ context.Context, _ /*group*/ string, _ GroupInfo) error {
	return nil
}

func (ids *iamDummyStore) deletePolicyDoc(_ context.Context, _ /*policyName*/ string) error {
	return nil
}

func (ids *iamDummyStore) deleteMappedPolicy(_ context.Context, _ /*name*/ string, _ IAMUserType, _ /*isGroup*/ bool) error {
	return nil
}

func (ids *iamDummyStore) deleteUserIdentity(_ context.Context, _ /*name*/ string, _ IAMUserType) error {
	return nil
}

func (ids *iamDummyStore) deleteGroupInfo(_ context.Context, _ /*name*/ string) error {
	return nil
}
