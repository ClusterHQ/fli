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

import "hash"

type (
	// Type defines the kind of hash
	Type int

	// Factory defines the record hash factory interface
	Factory interface {
		New() hash.Hash
		Type() Type
	}
)

const (
	// NoOp type for no op hash
	NoOp Type = iota

	// SHA256 type for SHA2 hashing
	SHA256

	// MD5 type for MD5 hashing
	MD5

	// Adler32 type for adler32 CRC
	Adler32
)
