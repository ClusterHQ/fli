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

package volume

import (
	"time"

	"github.com/ClusterHQ/fli/meta/attrs"
	"github.com/ClusterHQ/fli/meta/snapshot"
	"github.com/ClusterHQ/fli/meta/volumeset"
	"github.com/ClusterHQ/fli/securefilepath"
)

type (
	// ID is the local volume ID
	ID string

	// Volume ...
	Volume struct {
		ID           ID
		VolSetID     volumeset.ID
		BaseID       *snapshot.ID
		MntPath      securefilepath.SecureFilePath
		Attrs        attrs.Attrs
		CreationTime time.Time
		Size         uint64
		Name         string
	}

	// Query ..
	Query struct {
		Limit int

		// For now, the query will try to match entries that matches
		// all attributes present in here.
		Attr   attrs.Attrs
		FromID *ID
	}
)

const nilID = ""

// NewID creates a ID from a string returned from a
// previous call to ID.String.
func NewID(s string) ID {
	return ID(s)
}

// NilID returns an empty ID.
func NilID() ID {
	return ID(nilID)
}

// String ...
func (id ID) String() string {
	return string(id)
}

// Equals ..
func (id ID) Equals(target ID) bool {
	return id == target
}

// IsNilID ...
func (id ID) IsNilID() bool {
	return id == nilID
}

// HasBase ...
func (v Volume) HasBase() bool {
	return v.BaseID != nil
}

// Copy ...
func (v *Volume) Copy() *Volume {
	if v == nil {
		return nil
	}
	vol := *v
	vol.Attrs = v.Attrs.Copy()
	return &vol
}

// Matches ..
func (q *Query) Matches(vol Volume) bool {
	return vol.Attrs.Matches(q.Attr)
}
