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

package hash

import (
	"crypto/sha256"
	"hash"

	dlhash "github.com/ClusterHQ/go/dl/hash"
)

type (
	// Factory is a helper for creating a new sha hash factory
	Factory struct {
	}
)

var (
	_ dlhash.Factory = Factory{}
)

// New creates a new hash
func (f Factory) New() hash.Hash {
	return sha256.New()
}

// Type returns the hash's type
func (f Factory) Type() dlhash.Type {
	return dlhash.SHA256
}
