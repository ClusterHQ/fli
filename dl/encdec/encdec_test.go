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

package encdec_test

import (
	"bytes"
	"os"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/ClusterHQ/fli/dl/encdec"
	dlbin "github.com/ClusterHQ/fli/dl/encdec/binary"
	dlgob "github.com/ClusterHQ/fli/dl/encdec/gob"
	dlhash "github.com/ClusterHQ/fli/dl/hash"
	"github.com/ClusterHQ/fli/dl/hash/adler32"
	"github.com/ClusterHQ/fli/dl/hash/md5"
	"github.com/ClusterHQ/fli/dl/hash/noop"
	dlsha "github.com/ClusterHQ/fli/dl/hash/sha256"
	"github.com/ClusterHQ/fli/dl/record"
	"github.com/stretchr/testify/require"
)

func encodeAndDecode(t *testing.T, recs []record.Record, ed encdec.Factory, hf dlhash.Factory) {
	var bbuf bytes.Buffer
	enc := ed.NewEncoder(&bbuf)
	dec := ed.NewDecoder(&bbuf)

	err := enc.Encode(recs)
	require.NoError(t, err)

	recsDecoded, err := dec.Decode()
	require.NoError(t, err)
	for _, r := range recsDecoded {
		chksum, err := r.Chksum(hf)
		require.NoError(t, err)
		r.SetChksum(chksum)
	}

	require.True(t, reflect.DeepEqual(recs, recsDecoded))
}

func test(t *testing.T, ed encdec.Factory, hf dlhash.Factory) {
	data := []byte{0xde, 0xad, 0xbe, 0xef}
	filename := "test_grouped_file"
	offset := uint64(64)
	path := "test_grouped_mkdir"
	mode := record.DefaultCreateMode
	oldFilename := "test_grouped_old_file"
	newFilename := "test_grouped_new_file"
	size := int64(2048)
	uid := 123
	gid := 456
	time := time.Now()
	mkmode := syscall.S_IFCHR | uint32(os.FileMode(0666))
	dev := 259
	attr := "user.testing"
	val := "some_data"
	longpath := "this/is//my/looooooooooooooooooooooooooooooooooooooooooooooooooooooongpath"
	strangepath := "/dir1-0/dir$0%9/dir123_qwerty/09file"

	mkdir := record.NewMkdir(path, mode)
	pwrite := record.NewPwrite(filename, data, offset)
	hardlink := record.NewHardlink(oldFilename, newFilename)
	symlink := record.NewSymlink(oldFilename, newFilename)
	truncate := record.NewTruncate(filename, size)
	chown := record.NewChown(filename, uid, gid)
	create := record.NewCreate(filename, mode)
	remove := record.NewRemove(longpath)
	mtime := record.NewSetMtime(path, time)
	mknod := record.NewMknod(path, mkmode, dev)
	chmod := record.NewChmod(path, mode)
	rename := record.NewRename(oldFilename, newFilename)
	rmxattr := record.NewRmXattr(path, attr)
	setxattr := record.NewSetXattr(path, attr, []byte(val))
	rmStrangeFile := record.NewRemove(strangepath)
	eot := record.NewEOT()

	allrecords := []record.Record{
		mkdir,
		pwrite,
		hardlink,
		symlink,
		truncate,
		chown,
		create,
		remove,
		mtime,
		mknod,
		chmod,
		rename,
		rmxattr,
		setxattr,
		rmStrangeFile,
		eot,
	}
	for _, r := range allrecords {
		chksum, err := r.Chksum(hf)
		require.NoError(t, err)
		r.SetChksum(chksum)
	}

	encodeAndDecode(
		t,
		[]record.Record{
			mkdir,
			pwrite,
			hardlink,
			symlink,
			truncate,
			chown,
			create,
			remove,
			setxattr,
			rmxattr,
			rename,
			mknod,
			chmod,
			mtime,
			rmStrangeFile,
			eot,
		},
		ed,
		hf,
	)

	encodeAndDecode(
		t,
		[]record.Record{
			mkdir,
		},
		ed,
		hf,
	)

	encodeAndDecode(
		t,
		[]record.Record{
			mkdir,
			create,
			remove,
			setxattr,
		},
		ed,
		hf,
	)

	encodeAndDecode(
		t,
		nil,
		ed,
		hf,
	)
}

// TestEncDec test of encoding & decoding various records and various array size
func TestEncDec(t *testing.T) {
	test(t, dlbin.Factory{}, dlsha.Factory{})
	test(t, dlgob.Factory{}, dlsha.Factory{})
	test(t, dlbin.Factory{}, md5.Factory{})
	test(t, dlgob.Factory{}, md5.Factory{})
	test(t, dlbin.Factory{}, adler32.Factory{})
	test(t, dlgob.Factory{}, adler32.Factory{})
	test(t, dlbin.Factory{}, noop.Factory{})
	test(t, dlgob.Factory{}, noop.Factory{})
}
