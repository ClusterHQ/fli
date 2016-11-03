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

package snapshottest

import (
	"math/rand"
	"time"

	"github.com/ClusterHQ/go/meta/attrs"
	"github.com/ClusterHQ/go/meta/snapshot"
	"github.com/ClusterHQ/go/meta/volumeset"
)

// GetRandomSnapshot ..
func GetRandomSnapshot() *snapshot.Snapshot {
	return GetRandomSnapshotWithVolumeSet(volumeset.NewRandomID())
}

// GetRandomSnapshotWithVolumeSet ..
func GetRandomSnapshotWithVolumeSet(vsid volumeset.ID) *snapshot.Snapshot {
	return &snapshot.Snapshot{
		ID:               snapshot.NewRandomID(),
		CreationTime:     time.Now(),
		Size:             uint64(rand.Uint32()),
		LastModifiedTime: time.Now(),
		Description:      "Random Description",
		Attrs:            attrs.Attrs{},
		VolSetID:         vsid,
	}
}
