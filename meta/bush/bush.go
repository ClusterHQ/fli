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

package bush

import (
	"github.com/ClusterHQ/go/meta/snapshot"
	"github.com/ClusterHQ/go/meta/volumeset"
)

type (
	// Bush is an internal object, not exposed to users.
	// A bush has a single root and many branches, all branches are based on that single root.
	// A bush can be the unit of rebalancing, it can also be used as data server placement.
	Bush struct {
		// VolSetID is the volume set's id this bush belongs to
		VolSetID volumeset.ID

		// Root is the snapshot's ID of the bush's root
		Root snapshot.ID

		// DataSrvID is data server's ID. Blobs of all snapshots belong to the same bush go to the same
		// data server. In another word, bush is the data server allocation unit
		DataSrvID int
	}
)
