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

package volumesettest

import (
	"math/rand"
	"time"

	"github.com/ClusterHQ/go/meta/attrs"
	"github.com/ClusterHQ/go/meta/volumeset"
	"github.com/pborman/uuid"
)

// GetRandomVolumeSet will generate a random VS for testing.
func GetRandomVolumeSet() *volumeset.VolumeSet {
	creator := uuid.New()
	return &volumeset.VolumeSet{
		ID:           volumeset.NewRandomID(),
		CreationTime: time.Now(),
		// Database has a limit on integer, so left shift by 1 to make
		// sure rand.Uint32() does not go over the limit.
		Size:             uint64(rand.Uint32() >> 1),
		LastModifiedTime: time.Now(),
		LastSnapshotTime: time.Now(),
		Description:      "Random Description",
		Attrs:            attrs.Attrs{},
		Creator:          creator,
		Owner:            creator,
	}
}
