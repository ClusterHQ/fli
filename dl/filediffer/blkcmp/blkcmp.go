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

package blkcmp

import (
	"bytes"
	"io"

	"github.com/ClusterHQ/go/dl/blobdiffer"
	dlhash "github.com/ClusterHQ/go/dl/hash"
	"github.com/ClusterHQ/go/dl/record"
)

// Factory is a helper for creating a new file differ factory
type Factory struct {
}

var (
	_ blobdiffer.FileDiffer = Factory{}
)

const (
	// number of bytes per file read
	bytesPerRead int = (1 << 20)

	// number of bytes per pwrite record
	blockSize int = (1 << 8)
)

// DiffContents reads the files in a chunk, compares block by block,
// generates for all blocks that are different between
// the two files. Caller is expected to close the records channel.
func (factory Factory) DiffContents(f1, f2 blobdiffer.FileInfo, target string, records chan<- record.Record,
	hf dlhash.Factory) error {
	var buf1, buf2 []byte
	buf1 = make([]byte, bytesPerRead)
	buf2 = make([]byte, bytesPerRead)

	var (
		err    error
		off    int64 // marker of the current read buffer
		n1, n2 int   // total number of bytes in the current read buffer of file1 and file2
	)

	// Loop through file 2 chunk by chunk
	for ; off < f2.Size(); off += int64(n2) {
		// Read a chunk from file 2
		n2, err = f2.Read(buf2)
		if err != nil && err != io.EOF {
			return err
		}

		// Read from file 1 if there are enough bytes left
		if off < f1.Size() {
			n1, err = f1.Read(buf1)
			if err != nil && err != io.EOF {
				return err
			}
		} else {
			n1 = 0
		}

		// Loop through a chunk block by block
		for idx := 0; idx < n2; idx += blockSize {
			var cnt int
			// # of bytes left in this chunk(either a full block or the left over)
			// pwrite records will always have blockSize as the # of bytes or the reminder of the bytes if
			// is reaches the end of the file
			if n2 < idx+blockSize {
				cnt = n2 - idx
			} else {
				cnt = blockSize
			}

			var same bool
			// Compare bytes if file 1 has enough data
			if n1 >= idx+cnt {
				same = bytes.Compare(buf1[idx:idx+cnt], buf2[idx:idx+cnt]) == 0
			} else {
				same = false
			}
			if !same {
				r := record.NewPwrite(target,
					buf2[idx:idx+cnt],
					uint64(off+int64(idx)))
				err := record.Send(r, records, hf)
				if err != nil {
					return err
				}
			}
		}

		// Allocate a new buffer for the next read while the old buffer is in use by pwrite
		buf2 = make([]byte, bytesPerRead)
	}

	return nil
}
