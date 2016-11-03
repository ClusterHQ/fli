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

package encdec

import (
	"io"

	"github.com/ClusterHQ/fli/dl/record"
)

type (
	// Type defines the kind of encoder/decoder
	Type int

	// Encoder defines the record encode interface
	Encoder interface {
		Encode([]record.Record) error
	}

	// Decoder defines the record decode interface
	Decoder interface {
		Decode() ([]record.Record, error)
	}

	// Factory defines the record encode/decode factory interface
	// Note: Assumption is made that there is only one encoder at a time so it can write data to the target without
	// worrying about concurrent writes to the same stream. Multiplexing is not supported.
	Factory interface {
		NewEncoder(io.Writer) Encoder
		NewDecoder(io.Reader) Decoder
		Type() Type
	}
)

const (
	// Binary type for binary encoder/decoder
	Binary Type = iota

	// Gob type for Gob encoder/decoder
	Gob
)
