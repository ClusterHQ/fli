/*
 * Copyright 2016 ClusterHQ
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package volumeset

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"time"

	"github.com/ClusterHQ/fli/meta/attrs"
	"github.com/ClusterHQ/fli/meta/util"
	"github.com/pborman/uuid"
)

type (
	// ID ...
	ID string

	// VolumeSet ...
	VolumeSet struct {
		ID               ID          `json:"id"`
		Attrs            attrs.Attrs `json:"attrs"`
		CreationTime     time.Time   `json:"creation_time"`
		Size             uint64      `json:"size"`
		Creator          string      `json:"creator"`
		CreatorUsername  string      `json:"creator_username"`
		Owner            string      `json:"owner"`
		OwnerUsername    string      `json:"owner_username"`
		Name             string      `json:"name"`
		Prefix           string      `json:"prefix"`
		LastModifiedTime time.Time   `json:"last_modified_time"` // last time the meta data is modified
		LastSnapshotTime time.Time   `json:"last_snapshot_time"` // last time a snapshot is taken on this volume
		NumSnapshots     int         `json:"num_snapshots"`
		NumBranches      int         `json:"num_branches"`
		Description      string      `json:"description"`
	}

	// Query ..
	Query struct {
		// TODO: Combine ID and IDs into one? Any UI impact?
		// Query by ID
		ID  ID   `json:"id"`
		IDs []ID `json:"ids"`

		// Pagination
		Offset    int    `json:"offset"`
		Limit     int    `json:"limit"`
		SortBy    string `json:"sortby"`
		OrderType string `json:"order"`

		// Group 1 attributes.
		Name            string `json:"name"`
		Prefix          string `json:"prefix"`
		Creator         string `json:"creator"`
		CreatorUsername string `json:"creator_username"`
		Owner           string `json:"owner"`
		OwnerUsername   string `json:"owner_username"`

		// For now, the query will try to match entries that matches
		// all attributes present in here.
		Attr attrs.Attrs `json:"attr"`

		Search string `json:"search"`
	}
)

const (
	nilID = ""

	// ASC means ascending order for the query result.
	ASC = "asc"
	// DESC means descending order for the query result.
	DESC = "desc"

	// OrderByTime indicates sortby creation time.
	OrderByTime = "creation_time"
	// OrderBySize indicates sortby size.
	OrderBySize = "size"
)

// NewRandomID generates a new unused snapshot identifier at random.
func NewRandomID() ID {
	return ID(uuid.New())
}

// NewID creates a ID from a string returned from a
// previous call to ID.String.
func NewID(s string) ID {
	return ID(s)
}

// String ...
func (id ID) String() string {
	return string(id)
}

// MarshalJSON is a json helper.
func (id ID) MarshalJSON() ([]byte, error) {
	return json.Marshal(id.String())
}

// UnmarshalJSON is a json helper.
func (id *ID) UnmarshalJSON(in []byte) error {
	var str string
	err := json.Unmarshal(in, &str)
	if err != nil {
		return err
	}
	*id = NewID(str)
	return nil
}

// NilID returns an empty ID.
func NilID() ID {
	return ID(nilID)
}

// IsNilID ...
func (id ID) IsNilID() bool {
	return id == nilID
}

// Equals ..
func (id ID) Equals(target ID) bool {
	return id == target
}

// Copy ...
func (vs *VolumeSet) Copy() *VolumeSet {
	if vs == nil {
		return nil
	}
	volset := *vs
	volset.Attrs = vs.Attrs.Copy()
	return &volset
}

//MetaEqual compares two volume sets for equality.
func (vs *VolumeSet) MetaEqual(that *VolumeSet) bool {
	return (that != nil &&
		vs.Name == that.Name &&
		vs.Prefix == that.Prefix &&
		vs.Description == that.Description &&
		vs.ID.Equals(that.ID) &&
		vs.Owner == that.Owner && //ignore OwnerUsername because we assume it matches owner uuid.
		reflect.DeepEqual(vs.Attrs, that.Attrs))
}

// Matches ..
func (q Query) Matches(vs VolumeSet) bool {
	if !vs.Attrs.Matches(q.Attr) {
		return false
	}

	if !q.ID.IsNilID() && !q.ID.Equals(vs.ID) {
		return false
	}

	if q.Name != "" && q.Name != vs.Name {
		return false
	}

	if q.Prefix != "" && q.Prefix != vs.Prefix {
		return false
	}

	if q.Creator != "" && q.Creator != vs.Creator {
		return false
	}

	if q.CreatorUsername != "" && q.CreatorUsername != vs.CreatorUsername {
		return false
	}

	if q.Owner != "" && q.Owner != vs.Owner {
		return false
	}

	if q.OwnerUsername != "" && q.OwnerUsername != vs.OwnerUsername {
		return false
	}

	if q.Search != "" &&
		!strings.Contains(vs.Name, q.Search) &&
		!strings.Contains(vs.CreatorUsername, q.Search) &&
		!strings.Contains(vs.OwnerUsername, q.Search) &&
		!vs.Attrs.Contains(q.Search) {
		return false
	}

	return true
}

// Validate ..
func (q Query) Validate() error {
	switch q.SortBy {
	case "", OrderBySize, OrderByTime:
	default:
		return errors.New("Not valud sortby value")
	}

	switch q.OrderType {
	case "", ASC, DESC:
	default:
		return errors.New("Not valid order type")
	}

	if q.Offset < 0 {
		return errors.New("Offset cannot be negative")
	}

	if q.Limit < 0 {
		return errors.New("Limit cannot be negative")
	}

	if ret := uuid.Parse(q.ID.String()); !q.ID.IsNilID() && ret == nil {
		return errors.New("VolumeSet ID is not valid UUID format")
	}

	if !q.ID.IsNilID() && (len(q.IDs) != 0) {
		return errors.New("Cannot query by ID and by IDs (list) at the same time")
	}

	return nil
}

// StoreKnownKeys stores all known keys to attributes
func (vs *VolumeSet) StoreKnownKeys() {
	delete(vs.Attrs, attrs.Description)
	if !util.IsEmptyString(vs.Description) {
		if vs.Attrs == nil {
			vs.Attrs = make(attrs.Attrs, 0)
		}
		vs.Attrs.SetKey(attrs.Description, vs.Description)
	}

	delete(vs.Attrs, attrs.Name)
	if !util.IsEmptyString(vs.Name) {
		if vs.Attrs == nil {
			vs.Attrs = make(attrs.Attrs, 0)
		}
		vs.Attrs.SetKey(attrs.Name, vs.Name)
	}

	delete(vs.Attrs, attrs.Prefix)
	if !util.IsEmptyString(vs.Prefix) {
		if vs.Attrs == nil {
			vs.Attrs = make(attrs.Attrs, 0)
		}
		vs.Attrs.SetKey(attrs.Prefix, vs.Prefix)
	}
}

// RetrieveKnownKeys retrives all known keys from attributes
func (vs *VolumeSet) RetrieveKnownKeys() {
	v, exist := vs.Attrs[attrs.Description]
	if exist {
		vs.Description = v
	}
	delete(vs.Attrs, attrs.Description)

	v, exist = vs.Attrs[attrs.Name]
	if exist {
		vs.Name = v
	}
	delete(vs.Attrs, attrs.Name)

	v, exist = vs.Attrs[attrs.Prefix]
	if exist {
		vs.Prefix = v
	}
	delete(vs.Attrs, attrs.Prefix)
}

// SetOwnerUUID sets owner uuid from name and current client and creator
func (vs *VolumeSet) SetOwnerUUID(currentUser string) error {
	//owner uuid is set, nothing to do
	if vs.Owner != "" {
		return nil
	}

	//owneruuid and ownername will always match. so we have no name either.
	// so just use the current user. if user is not there, set it to creator.
	vs.Owner = currentUser

	if vs.Owner == "" {
		// No current user id or 'nobody', so set the owner uuid to creator.
		err := vs.SetCreatorUUID(currentUser)
		if err != nil {
			return err
		}

		vs.Owner = vs.Creator
	}

	return nil
}

// SetCreatorUUID sets creator uuid given the current user
func (vs *VolumeSet) SetCreatorUUID(currentUser string) error {
	//if uuid is already there, nothing to do
	if vs.Creator != "" {
		return nil
	}

	//client can not change creator, so we know it is going to be the current user
	vs.Creator = currentUser
	return nil
}
