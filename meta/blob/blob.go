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

package blob

// ID is an opaque *locally* unique identifier for a blob.  A blob is a data
// layer-provided sequence of bytes which can be used to reconstitute a
// snapshot of a volume.  An ID can only be used with the data layer
// instance from which it was obtained.
type ID string

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
