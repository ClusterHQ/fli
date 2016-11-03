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

package metastore

import (
	"github.com/ClusterHQ/fli/meta/branch"
	"github.com/ClusterHQ/fli/meta/snapshot"
	"github.com/ClusterHQ/fli/meta/volumeset"
)

type (
	// ResolveStatus ..
	ResolveStatus uint32

	// VSMetaConflict - array of these is used for
	// reporting conflicts in volumeset metadata
	VSMetaConflict struct {
		Tgt  *volumeset.VolumeSet `json:"target"`
		Cur  *volumeset.VolumeSet `json:"current"`
		Init *volumeset.VolumeSet `json:"init"`
	}

	// SnapMetaConflict - array of these is used for
	// reporting conflicts in snap metadata
	SnapMetaConflict struct {
		Tgt  *snapshot.Snapshot `json:"target"`
		Cur  *snapshot.Snapshot `json:"current"`
		Init *snapshot.Snapshot `json:"init"`
	}

	// BranchMetaConflict - array of these is used
	// for reporting conflicts in branch metadata
	BranchMetaConflict struct {
		Tgt, Cur, Init *branch.Branch
	}

	// MdsTriplet holds 3 stores for
	// syncs and conflict resolution. Used for push and conflict resolution.
	MdsTriplet struct {
		Tgt, Cur, Init Syncable
	}

	// MdsTuple holds 2 stores for pull operations.
	// Pull has no conflict resolution
	MdsTuple struct {
		Tgt, Cur Syncable
	}
)

const (
	// UseCurrent ...
	UseCurrent ResolveStatus = 1
	// UseTgtNoConflict ...
	UseTgtNoConflict ResolveStatus = 2
	// UseTgtConflict ...
	UseTgtConflict ResolveStatus = 3
)

// IsEmpty ...
func (v VSMetaConflict) IsEmpty() bool {
	return v.Tgt == nil && v.Cur == nil && v.Init == nil
}

// IsEmpty ...
func (s SnapMetaConflict) IsEmpty() bool {
	return s.Tgt == nil && s.Cur == nil && s.Init == nil
}

// IsEmpty ...
func (b BranchMetaConflict) IsEmpty() bool {
	return b.Tgt == nil && b.Cur == nil && b.Init == nil
}
