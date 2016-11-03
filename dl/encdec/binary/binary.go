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

package binary

import (
	"io"
	"time"

	"encoding/binary"
	"github.com/ClusterHQ/fli/dl/encdec"
	"github.com/ClusterHQ/fli/dl/record"
	"github.com/ClusterHQ/fli/errors"
)

type (
	encoder struct {
		target io.Writer
	}

	decoder struct {
		src io.Reader
	}

	// Factory is a helper for creating a new record encode/decode factory
	Factory struct {
	}
)

var (
	_ encdec.Factory = Factory{}
	_ encdec.Encoder = &encoder{}
	_ encdec.Decoder = &decoder{}
)

// NewEncoder returns a new encoder
func (f Factory) NewEncoder(target io.Writer) encdec.Encoder {
	return &encoder{target: target}
}

// NewDecoder returns a new decoder
func (f Factory) NewDecoder(src io.Reader) encdec.Decoder {
	return &decoder{src: src}
}

// Type returns the encoder/decoder's type
func (f Factory) Type() encdec.Type {
	return encdec.Binary
}

// Encode implements encoder
func (e *encoder) Encode(recs []record.Record) error {
	err := binary.Write(e.target, binary.LittleEndian, uint64(len(recs)))
	if err != nil {
		return err
	}

	for _, r := range recs {
		err = r.ToBinary(e.target)
		if err != nil {
			return err
		}
	}

	return nil
}

// Decode implements decoder
func (d *decoder) Decode() ([]record.Record, error) {
	var (
		numrecs uint64
		recType uint64
		r       record.Record
		recs    []record.Record
	)
	err := binary.Read(d.src, binary.LittleEndian, &numrecs)
	if err != nil {
		return nil, err
	}

	for i := 0; i < int(numrecs); i++ {
		err := binary.Read(d.src, binary.LittleEndian, &recType)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		switch record.Type(recType) {
		case record.TypeMkdir:
			r = record.NewMkdir("", 0)
		case record.TypePwrite:
			r = record.NewPwrite("", nil, 0)
		case record.TypeHardlink:
			r = record.NewHardlink("", "")
		case record.TypeSymlink:
			r = record.NewSymlink("", "")
		case record.TypeTruncate:
			r = record.NewTruncate("", 0)
		case record.TypeChown:
			r = record.NewChown("", 0, 0)
		case record.TypeCreate:
			r = record.NewCreate("", 0)
		case record.TypeRemove:
			r = record.NewRemove("")
		case record.TypeSetxattr:
			r = record.NewSetXattr("", "", nil)
		case record.TypeRmxattr:
			r = record.NewRmXattr("", "")
		case record.TypeRename:
			r = record.NewRename("", "")
		case record.TypeMknod:
			r = record.NewMknod("", 0, 0)
		case record.TypeChmod:
			r = record.NewChmod("", 0)
		case record.TypeSetmtime:
			r = record.NewSetMtime("", time.Time{})
		case record.TypeEOT:
			r = record.NewEOT()
		}

		if r == nil {
			// Failed to decode, likely something went wrong
			return nil, errors.New("Failed to read records from soruce")
		}
		err = r.FromBinary(d.src)
		if err != nil {
			return nil, err
		}

		recs = append(recs, r)
	}

	return recs, nil
}
