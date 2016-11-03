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

package record_test

import (
	"testing"
	"time"

	dlhash "github.com/ClusterHQ/fli/dl/hash"
	"github.com/ClusterHQ/fli/dl/hash/adler32"
	"github.com/ClusterHQ/fli/dl/hash/md5"
	dlsha "github.com/ClusterHQ/fli/dl/hash/sha256"
	"github.com/ClusterHQ/fli/dl/record"
	"github.com/stretchr/testify/require"
)

// TestType make sure each record return the correct type
func TestType(t *testing.T) {
	allrecords := []struct {
		r  record.Record
		id record.Type
	}{
		{record.NewMkdir("", 0), record.TypeMkdir},
		{record.NewPwrite("", nil, 0), record.TypePwrite},
		{record.NewHardlink("", ""), record.TypeHardlink},
		{record.NewSymlink("", ""), record.TypeSymlink},
		{record.NewTruncate("", 0), record.TypeTruncate},
		{record.NewChown("", 0, 0), record.TypeChown},
		{record.NewCreate("", 0), record.TypeCreate},
		{record.NewRemove(""), record.TypeRemove},
		{record.NewSetXattr("", "", nil), record.TypeSetxattr},
		{record.NewRmXattr("", ""), record.TypeRmxattr},
		{record.NewRename("", ""), record.TypeRename},
		{record.NewMknod("", 0, 0), record.TypeMknod},
		{record.NewChmod("", 0), record.TypeChmod},
		{record.NewSetMtime("", time.Now()), record.TypeSetmtime},
		{record.NewEOT(), record.TypeEOT},
	}

	alltypes := make(map[int]int)
	for _, r := range allrecords {
		require.Equal(t, r.id, r.r.Type())
		alltypes[int(r.id)] = int(r.r.Type())
	}
	require.Equal(t, len(allrecords), len(alltypes))
}

// testChecksum tests record checksum implementation
func testChecksum(t *testing.T, hf dlhash.Factory) {
	recs := []record.Record{
		record.NewMkdir("test", 0),
		record.NewPwrite("test", nil, 0),
		record.NewHardlink("test", ""),
		record.NewSymlink("test", ""),
		record.NewTruncate("test", 0),
		record.NewChown("test", 0, 0),
		record.NewCreate("test", 0),
		record.NewRemove("test"),
		record.NewSetXattr("test", "", nil),
		record.NewRmXattr("test", ""),
		record.NewRename("test", ""),
		record.NewMknod("test", 0, 0),
		record.NewChmod("test", 0),
		record.NewSetMtime("test", time.Now()),
		record.NewEOT(),
	}

	h := hf.New()
	emptyChksum := h.Sum(nil)

	// Get checksum on original
	var chksumOriginal [][]byte
	for _, r := range recs {
		actual, err := r.Chksum(hf)
		require.NoError(t, err)
		require.NotEqual(t, []byte(nil), actual)
		require.NotEqual(t, emptyChksum, actual)
		chksumOriginal = append(chksumOriginal, actual)
	}

	// Modify field 1
	recs = []record.Record{
		record.NewMkdir("test1", 0),
		record.NewPwrite("test1", nil, 0),
		record.NewHardlink("test1", ""),
		record.NewSymlink("test1", ""),
		record.NewTruncate("test1", 0),
		record.NewChown("test1", 0, 0),
		record.NewCreate("test1", 0),
		record.NewRemove("test1"),
		record.NewSetXattr("test1", "", nil),
		record.NewRmXattr("test1", ""),
		record.NewRename("test1", ""),
		record.NewMknod("test1", 0, 0),
		record.NewChmod("test1", 0),
		record.NewSetMtime("test1", time.Now()),
	}
	for idx, r := range recs {
		actual, err := r.Chksum(hf)
		require.NoError(t, err)
		require.NotEqual(t, chksumOriginal[idx], actual)
	}

	// Modify field 2
	recs = []record.Record{
		record.NewMkdir("test", 2),
		record.NewPwrite("test", []byte{0}, 0),
		record.NewHardlink("test", "2"),
		record.NewSymlink("test", "2"),
		record.NewTruncate("test", 2),
		// Note: Chown translate UID/GID, so internally it is converted into the same string when using 'bad'
		// UID/GID numbers, can't verify the conversion.
		record.NewChown("test2", 2, 0),
		record.NewCreate("test", 2),
		record.NewRemove("test2"),
		record.NewSetXattr("test", "2", nil),
		record.NewRmXattr("test", "2"),
		record.NewRename("test", "2"),
		record.NewMknod("test", 2, 0),
		record.NewChmod("test", 2),
		// Note: No need to modify, time will change
		record.NewSetMtime("test", time.Now()),
	}
	for idx, r := range recs {
		actual, err := r.Chksum(hf)
		require.NoError(t, err)
		require.NotEqual(t, chksumOriginal[idx], actual)
	}

	// Modify field 3
	recs = []record.Record{
		record.NewMkdir("test3", 3),
		record.NewPwrite("test", nil, 3),
		record.NewHardlink("test3", "3"),
		record.NewSymlink("test3", "3"),
		record.NewTruncate("test3", 3),
		// Note: Chown translate UID/GID, so internally it is converted into the same string when using 'bad'
		// UID/GID numbers, can't verify the conversion.
		record.NewChown("test3", 0, 0),
		record.NewCreate("test3", 3),
		record.NewRemove("test3"),
		record.NewSetXattr("test", "", []byte{9}),
		record.NewRmXattr("test3", "3"),
		record.NewRename("test3", "3"),
		record.NewMknod("test", 0, 3),
		record.NewChmod("test3", 3),
		// Note: No need to modify, time will change
		record.NewSetMtime("test", time.Now()),
	}
	for idx, r := range recs {
		actual, err := r.Chksum(hf)
		require.NoError(t, err)
		require.NotEqual(t, chksumOriginal[idx], actual)
	}
}

// TestChecksum tests record checksum implementation
func TestChecksum(t *testing.T) {
	testChecksum(t, dlsha.Factory{})
	testChecksum(t, md5.Factory{})
	testChecksum(t, adler32.Factory{})
}
