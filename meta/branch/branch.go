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

package branch

import (
	"strings"

	"github.com/ClusterHQ/go/errors"
	"github.com/ClusterHQ/go/meta/snapshot"
	"github.com/ClusterHQ/go/meta/volumeset"
	"github.com/pborman/uuid"
)

// A branch is a string of snapshots start from an empty base and end at a snapshot.
// The end snapshot can have children or have no child.
// The last snapshot is called tip of the branch. If the tip has no child, it is called a leaf.(So a leaf is guaranteed
// to be a tip).
// A branch is identified by either a branch name or a snapshot that has no child(with or without a branch name).
// A branch is created and extended(grow) by creating volumes from a snapshot followed by taking snapshots of the
// volumes.
// So how does a snapshot becomes a branch?
// 1. All leaf snapshots are automatically become branches.
// 2. Internal snapshots can become branches when user instruct us to do so
type (
	// ID is the opaque, globally unique identifier of a branch.
	ID string

	// Branch defines a branch object
	Branch struct {
		// ID of the branch. It is internal, used for identifying branches during sync.
		ID ID `json:"id"`

		// Name of the branch.
		Name string `json:"name"`

		// Tip is the last snapshot on the branch
		Tip *snapshot.Snapshot `json:"tip"`

		// HasChild is true if the tip has child(ren)
		HasChild bool `json:"has_child"`
	}

	// Query ..
	Query struct {
		// Query by ID
		ID ID `json:"id"`

		Name     string       `json:"name"`
		VolSetID volumeset.ID `json:"volset_id"`
		// Pagination ..
		Offset int `json:"offset"`
		Limit  int `json:"limit"`

		Search string `json:"search"`
	}
)

const (
	nilID = ""
)

// Matches ..
func (q Query) Matches(b *Branch) bool {
	if q.Name != "" && q.Name != b.Name {
		return false
	}

	if !(q.VolSetID.IsNilID()) && !(q.VolSetID.Equals(b.Tip.VolSetID)) {
		return false
	}

	if !(q.ID.IsNilID()) && !(q.ID.Equals(&b.ID)) {
		return false
	}

	if q.Search != "" && !strings.Contains(b.Name, q.Search) {
		return false
	}

	return true
}

// Validate ..
func (q Query) Validate() error {
	// VolSetID is mandatory.
	if q.VolSetID.IsNilID() {
		return errors.New("VolumeSet ID is mandatory for query")
	}

	return nil
}

// NewRandomID generates a new unused snapshot identifier at random.
func NewRandomID() ID {
	return ID(uuid.New())
}

// String ...
func (id ID) String() string {
	return string(id)
}

// NewID creates a ID from a string returned from a previous call to ID.String.
func NewID(s string) ID {
	return ID(s)
}

// IsNilID ...
func (id ID) IsNilID() bool {
	return id == nilID
}

// Equals ..
func (id *ID) Equals(that *ID) bool {
	return (id == nil && that == nil) || (id != nil && that != nil && *id == *that)
}

// SortableBranchesByTipDepth is a helper for sorting branches by their tip's depth
type (
	SortableBranchesByTipDepth []*Branch
)

func (b SortableBranchesByTipDepth) Len() int {
	return len(b)
}

func (b SortableBranchesByTipDepth) Less(i, j int) bool {
	return b[i].Tip.Depth < b[j].Tip.Depth
}

func (b SortableBranchesByTipDepth) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}
