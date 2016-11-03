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

package noop

import (
	"hash"
	"io"

	dlhash "github.com/ClusterHQ/fli/dl/hash"
)

type (
	// Factory is a helper for creating a new sha hash factory
	Factory struct {
	}

	// noOpHash implements the hash.hash interface
	noOpHash struct {
		io.Writer
	}
)

var (
	_ dlhash.Factory = Factory{}
	_ hash.Hash      = noOpHash{}
	_ io.Writer      = noOpHash{}
)

// New creates a new hash
func (f Factory) New() hash.Hash {
	return &noOpHash{}
}

// Type returns the hash's type
func (f Factory) Type() dlhash.Type {
	return dlhash.NoOp
}

// Write is the no op hash 's implementation of io.Writer
func (h noOpHash) Write(p []byte) (n int, err error) {
	return len(p), nil
}

// Sum is the no op hash 's implementation of hash.Hash
func (h noOpHash) Sum(b []byte) []byte {
	return nil
}

// Reset is the no op hash 's implementation of hash.Hash
func (h noOpHash) Reset() {
}

// Size is the no op hash 's implementation of hash.Hash
func (h noOpHash) Size() int {
	return 0
}

// BlockSize is the no op hash 's implementation of hash.Hash
func (h noOpHash) BlockSize() int {
	return 0
}
