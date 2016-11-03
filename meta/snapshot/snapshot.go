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

package snapshot

// Duplication of implementation between this and volumeset.ID seems unfortunate.
// It's nice to have distinct types to avoid accidental mixups between the two
// but there should be a way to accomplish that without two copies of the code.

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"time"

	"github.com/ClusterHQ/go/meta/attrs"
	"github.com/ClusterHQ/go/meta/blob"
	"github.com/ClusterHQ/go/meta/util"
	"github.com/ClusterHQ/go/meta/volumeset"
	"github.com/pborman/uuid"
)

type (
	// ID is the opaque, globally unique identifier of a snapshot. An ID can be
	// used across time and scopes of control to refer to a particular snapshot.
	ID string

	// Snapshot represents a point-in-time consistent version of a volumeset.  It can
	// be unambiguously addressed with a ID, snapshot.ID pair.  It exists in
	// a forest-like structure with other snapshots in the same volumeset.
	Snapshot struct {
		ID               ID           `json:"id"`
		ParentID         *ID          `json:"parent_id"`
		VolSetID         volumeset.ID `json:"volsetid"`
		CreationTime     time.Time    `json:"creation_time"`
		Size             uint64       `json:"size"`
		Creator          string       `json:"creator"`
		CreatorName      string       `json:"creator_name"`
		Owner            string       `json:"owner"`
		OwnerName        string       `json:"owner_name"`
		Attrs            attrs.Attrs  `json:"attrs"`
		Name             string       `json:"name"`
		LastModifiedTime time.Time    `json:"last_modified_time"` // last time the meta data is modified
		Description      string       `json:"description"`

		// Local fields that should nt be synced to another MDS
		// TODO: This json skip only works for a remote MDS which passes objects marshalled by JSON.
		// It doesn't work for a local MDS.
		// BlobID is the current blob id
		BlobID blob.ID `json:"-"`

		// PrevBlobID is the blob id at the time this object is created
		PrevBlobID blob.ID `json:"-"`

		// Note: Following fields are used by different logic including sync, adding new snapshots, etc.
		//       They are exposed and can be used for other purposes, for example, show to UI.
		//       These fields are immutable. Also, they are only valid after a read from the MDS.
		//       (For example, after calling ImportSnapshot(), the snapshots passed for import may not have
		//       the right depth)
		Depth       int    `json:"depth"`
		NumChildren int    `json:"number_children"`
		IsTip       bool   `json:"is_tip"`      // True if the snapshot is the end of a branch(may not be a leaf)
		BranchName  string `json:"branch_name"` // valid only if 'IsTip' is true
	}
	// Query ..
	Query struct {
		// Query by ID
		ID  ID   `json:"id"`
		IDs []ID `json:"ids"`

		// Pagination ..
		Offset int `json:"offset"`
		Limit  int `json:"limit"`

		Name        string `json:"name"`
		Creator     string `json:"creator"`
		CreatorName string `json:"creator_username"`
		Owner       string `json:"owner"`
		OwnerName   string `json:"owner_username"`

		// Flag to signal orders
		SortBy    string `json:"sortby"`
		OrderType string `json:"order"`

		// For now, the query will try to match entries that matches
		// all attributes present in here.
		Attr attrs.Attrs `json:"attr"`

		// Query snapshot within a particular snapshot.
		VolSetID volumeset.ID `json:"volset_id"`

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

// Equals ..
func (id *ID) Equals(that *ID) bool {
	return (id == nil && that == nil) || (id != nil && that != nil && *id == *that)
}

// NilID returns an empty ID.
func NilID() ID {
	return ID(nilID)
}

// IsNilID ...
func (id ID) IsNilID() bool {
	return id == nilID
}

// HasParent ...
func (sn Snapshot) HasParent() bool {
	return sn.ParentID != nil
}

// Equals returns true if a and b contain exactly the same data, false otherwise.
func (sn *Snapshot) Equals(that *Snapshot) bool {
	if that == nil {
		return false
	}

	if !sn.VolSetID.Equals(that.VolSetID) {
		return false
	}

	if !(&sn.ID).Equals(&that.ID) {
		return false
	}

	if sn.Name != that.Name || sn.Description != that.Description {
		return false
	}

	// Let nil compare equal to an empty description.
	empty := len(sn.Attrs) == 0 && len(that.Attrs) == 0
	if !empty && !reflect.DeepEqual(sn.Attrs, that.Attrs) {
		return false
	}

	return sn.ParentID.Equals(that.ParentID)
}

// Copy ...
func (sn *Snapshot) Copy() *Snapshot {
	if sn == nil {
		return nil
	}
	volset := *sn
	volset.Attrs = sn.Attrs.Copy()
	return &volset
}

// Matches ..
func (q *Query) Matches(ss Snapshot) bool {
	if !ss.Attrs.Matches(q.Attr) {
		return false
	}

	if q.ID != nilID && q.ID != ss.ID {
		return false
	}

	if q.Name != "" && q.Name != ss.Name {
		return false
	}

	if !q.VolSetID.IsNilID() && !q.VolSetID.Equals(ss.VolSetID) {
		return false
	}

	if q.Creator != "" && q.Creator != ss.Creator {
		return false
	}

	if q.CreatorName != "" && q.CreatorName != ss.CreatorName {
		return false
	}

	if q.Owner != "" && q.Owner != ss.Owner {
		return false
	}

	if q.OwnerName != "" && q.OwnerName != ss.OwnerName {
		return false
	}

	if q.Search != "" &&
		!strings.Contains(ss.Name, q.Search) &&
		!strings.Contains(ss.Creator, q.Search) &&
		!strings.Contains(ss.Owner, q.Search) &&
		!ss.Attrs.Contains(q.Search) {
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

	if ret := uuid.Parse(q.VolSetID.String()); !q.VolSetID.IsNilID() && ret == nil {
		return errors.New("Volumeset ID specified is not valid UUID format")
	}

	if ret := uuid.Parse(q.ID.String()); !q.ID.IsNilID() && ret == nil {
		return errors.New("Snapshot ID is not valid UUID format")
	}

	if !q.ID.IsNilID() && (len(q.IDs) != 0) {
		return errors.New("Cannot query by ID and by IDs (list) at the same time")
	}

	return nil
}

// StoreKnownKeys stores all known keys to attributes
func (sn *Snapshot) StoreKnownKeys() {
	delete(sn.Attrs, attrs.Description)
	if !util.IsEmptyString(sn.Description) {
		if sn.Attrs == nil {
			sn.Attrs = make(attrs.Attrs, 0)
		}
		sn.Attrs.SetKey(attrs.Description, sn.Description)
	}

	delete(sn.Attrs, attrs.Name)
	if !util.IsEmptyString(sn.Name) {
		if sn.Attrs == nil {
			sn.Attrs = make(attrs.Attrs, 0)
		}
		sn.Attrs.SetKey(attrs.Name, sn.Name)
	}
}

// RetrieveKnownKeys retrives all known keys from attributes
func (sn *Snapshot) RetrieveKnownKeys() {
	v, exist := sn.Attrs[attrs.Description]
	if exist {
		sn.Description = v
	}
	delete(sn.Attrs, attrs.Description)

	v, exist = sn.Attrs[attrs.Name]
	if exist {
		sn.Name = v
	}
	delete(sn.Attrs, attrs.Name)
}

// SetOwnerUUID sets owner uuid from name and current client and creator
func (sn *Snapshot) SetOwnerUUID(currentUser string) error {
	//owner uuid is set, nothing to do
	if sn.Owner != "" {
		return nil
	}

	//owneruuid and ownername will always match. so we have no name either.
	// so just use the current user. if user is not there, set it to creator.
	sn.Owner = currentUser

	if sn.Owner == "" {
		// No current user id or 'nobody'.
		// We first set creator, then assign the owner to creator.
		err := sn.SetCreatorUUID(currentUser)
		if err != nil {
			return err
		}

		sn.Owner = sn.Creator
	}

	return nil
}

// SetCreatorUUID sets creator uuid given the current user
func (sn *Snapshot) SetCreatorUUID(currentUser string) error {
	//if uuid is already there, nothing to do
	if sn.Creator != "" {
		return nil
	}

	//client can not change creator, so we know it is going to be the current user
	sn.Creator = currentUser
	return nil
}
