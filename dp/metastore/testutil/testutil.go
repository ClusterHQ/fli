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

package testutil

import (
	"github.com/ClusterHQ/fli/dp/metastore"
	"github.com/ClusterHQ/fli/errors"
	"github.com/ClusterHQ/fli/meta/attrs"
	"github.com/ClusterHQ/fli/meta/volumeset"
	"github.com/pborman/uuid"
)

// VolumeSetTest assigns a random ID, random owner/creator to the given volume set
// and calls meta data storage to create a new record
func VolumeSetTest(mds metastore.Syncable, n string, p string, a attrs.Attrs, d string) (*volumeset.VolumeSet, error) {
	if a == nil {
		return nil, errors.New("Attributes for a volume set can be empty but can't be nil")
	}

	creator := uuid.New()

	return metastore.VolumeSet(mds, n, p, a, d, creator, creator)
}
