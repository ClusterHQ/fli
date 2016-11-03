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

package gob

import (
	"encoding/gob"
	"fmt"
	"io"

	"github.com/ClusterHQ/go/dl/encdec"
	"github.com/ClusterHQ/go/dl/record"
)

type (
	// recordsHeader is the header for an array of records
	recordsHeader struct {
		NumRecs int // How many records are in already-encoded record list
	}

	encoder struct {
		enc *gob.Encoder
	}

	decoder struct {
		dec *gob.Decoder
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

// register registers all record types in order to be able to decoded/encoded by gob
func init() {
	gob.Register(&record.Mkdir{})
	gob.Register(&record.Pwrite{})
	gob.Register(&record.Hardlink{})
	gob.Register(&record.Symlink{})
	gob.Register(&record.Truncate{})
	gob.Register(&record.Chown{})
	gob.Register(&record.Create{})
	gob.Register(&record.Remove{})
	gob.Register(&record.Setxattr{})
	gob.Register(&record.Rmxattr{})
	gob.Register(&record.Rename{})
	gob.Register(&record.Mknod{})
	gob.Register(&record.Chmod{})
	gob.Register(&record.Setmtime{})
	gob.Register(&record.EOT{})
}

// NewEncoder returns a new encoder
func (f Factory) NewEncoder(target io.Writer) encdec.Encoder {
	return newEnc(target)
}

// NewDecoder returns a new decoder
func (f Factory) NewDecoder(src io.Reader) encdec.Decoder {
	return newDec(src)
}

// Type returns the encoder/decoder's type
func (f Factory) Type() encdec.Type {
	return encdec.Gob
}

// newEnc returns a new gob encoder to user. It will act on a passed-in object that implements io.Writer interface
func newEnc(w io.Writer) *encoder {
	return &encoder{enc: gob.NewEncoder(w)}
}

// newDec returns a new gob decoder to caller. It will act on a passed-in object that implements io.Reader interface
func newDec(r io.Reader) *decoder {
	return &decoder{dec: gob.NewDecoder(r)}
}

// encRecs encodes records
func (g *encoder) encRecs(recs []record.Record) error {
	return g.enc.Encode(&recs)
}

// encRecHdr encodes a RecHeader object
func (g *encoder) encRecHdr(h recordsHeader) error {
	return g.enc.Encode(h)
}

// decRecs decodes []records.Record
func (g *decoder) decRecs() ([]record.Record, error) {
	var recs []record.Record
	err := g.dec.Decode(&recs)
	if err != nil {
		return nil, err
	}
	return recs, nil
}

// decRecHdr decodes an encoded ChunkHeader object.
func (g *decoder) decRecHdr() (recordsHeader, error) {
	var hdr recordsHeader
	err := g.dec.Decode(&hdr)
	return hdr, err
}

// Encode implements encoder
func (g *encoder) Encode(recs []record.Record) error {
	hdr := recordsHeader{NumRecs: len(recs)}
	err := g.encRecHdr(hdr)
	if err != nil {
		return err
	}

	err = g.encRecs(recs)
	return err
}

// Decode implements decoder
func (g *decoder) Decode() ([]record.Record, error) {
	var (
		recs []record.Record
		hdr  recordsHeader
	)

	hdr, err := g.decRecHdr()
	if err != nil && err != io.EOF {
		return nil, err
	}

	recs, err = g.decRecs()
	if err != nil {
		return nil, err
	}

	if len(recs) != hdr.NumRecs {
		err = fmt.Errorf("Erorr: unpacked unexpected number of records. Expected %d, got %d",
			hdr.NumRecs, len(recs))
		return nil, err
	}

	return recs, nil
}
