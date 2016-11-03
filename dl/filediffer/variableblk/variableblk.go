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

// Package variableblk provides a file diff method based on the following algorithm:
// Scan the files and compare byte by byte. For a byte stream that is the same, it is skipped if it reaches certain
// length; the bytes are included in the pwrite even though they are the same, this is to avoid sending smaller pwrites.
// For bytes stream that is not the same, generates a pwrite as large as possible(pre-defined length).
package variableblk

import (
	"io"

	"github.com/ClusterHQ/fli/dl/blobdiffer"
	dlhash "github.com/ClusterHQ/fli/dl/hash"
	"github.com/ClusterHQ/fli/dl/record"
)

// Factory is a helper for creating a new file differ factory
type Factory struct {
}

var (
	_ blobdiffer.FileDiffer = Factory{}
)

const (
	// Number of bytes per read
	bytesPerRead int = (1 << 20)

	// Max number of bytes per pwrite record
	maxBlockSize int = (1 << 17)

	// Minimal number of bytes to skip in case of the bytes are the same in both files(to avoid sending small
	// pwrites)
	minSkipSize int = (1 << 8)
)

// DiffContents reads the files in a chunk, compares byte by byte,
// generates a pwrite for every unmatched variable sized blocks.
// Caller is expected to close the records channel.
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

		// Loop through a chunk
		for idx := 0; idx < n2; {
			if n1 <= idx {
				// File 1 is done, generate a pwrite, length = min(blockSize, n2 - idx)
				var cnt int
				if n2 < idx+maxBlockSize {
					cnt = n2 - idx
				} else {
					cnt = maxBlockSize
				}
				r := record.NewPwrite(target,
					buf2[idx:idx+cnt],
					uint64(off+int64(idx)))
				err := record.Send(r, records, hf)
				if err != nil {
					return err
				}
				idx += cnt
				continue
			}

			// File 1 is not done yet
			// If first bytes are the same, skip all same bytes
			if buf1[idx] == buf2[idx] {
				for ; idx < n1 && idx < n2 && buf1[idx] == buf2[idx]; idx++ {
				}
			}

			if idx == n2 {
				// Reached end of chunk of file 2
				break
			}

			m1 := idx     // marker of starting point of the current byte the is different
			m2 := idx + 1 // marker of starting point of a same block that is tracked
			for ; idx < n1 && idx < n2 && idx-m1 < maxBlockSize; idx++ {
				if buf1[idx] != buf2[idx] {
					if idx-m2 > minSkipSize {
						// Found a same byte stream that can be skipped
						break
					}
					// Reset m2; same byte stream is too short and the bytes are included in the
					// next pwrite
					m2 = idx + 1
				}
			}

			r := record.NewPwrite(target,
				buf2[m1:m2],
				uint64(off+int64(m1)))
			err := record.Send(r, records, hf)
			if err != nil {
				return err
			}
		}

		// Allocate a new buffer for the next read while the old buffer is in use by pwrite
		buf2 = make([]byte, bytesPerRead)
	}

	return nil
}
