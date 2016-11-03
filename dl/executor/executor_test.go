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

package executor_test

import (
	"fmt"
	"testing"

	"github.com/ClusterHQ/go/dl/executor"
	"github.com/ClusterHQ/go/dl/record"
	"github.com/ClusterHQ/go/dl/testutils"
	"github.com/stretchr/testify/require"
)

func createRecs(prefix string) []record.Record {
	directory := prefix + "_dir"
	data := []byte{0xde, 0xad, 0xbe, 0xef}
	filename := prefix + "_old_file"
	symlink := prefix + "_new_file_symlink"
	hardlink := prefix + "_new_file_hardlink"
	chown := prefix + "_new_chown_file"
	offset := uint64(64)
	newsize := int64(2048)
	uid := 123
	gid := 456
	xattr := "user.testingExecutor"
	xattrVal := "testingExecutorData"

	recs := []record.Record{record.NewMkdir(directory, record.DefaultCreateMode),
		record.NewCreate(filename, record.DefaultCreateMode),
		record.NewChmod(filename, 0750),
		record.NewPwrite(filename, data, offset),
		record.NewHardlink(filename, hardlink),
		record.NewSymlink(filename, symlink),
		record.NewTruncate(filename, newsize),
		record.NewSetXattr(filename, xattr, []byte(xattrVal)),
		record.NewRmXattr(filename, xattr),
		record.NewRemove(filename),
	}

	if testutils.RunningAsRoot() {
		fmt.Println("Running as root, will test chown functionality.")
		recs = append(recs, record.NewCreate(chown, record.DefaultCreateMode), record.NewChown(chown, uid, gid))
	} else {
		fmt.Println("Not running as superuser - skipping chown test.")
	}

	return recs
}

func TestStdout(t *testing.T) {
	testutils.SetupTest()
	recs := createRecs("TestStdout")
	etor := executor.NewStdoutExecutor()
	err := etor.Execute("doesnotmatter", recs)
	testutils.ShutdownTest()
	require.NoError(t, err)
	require.False(t, etor.EOT())
}

func TestCommon(t *testing.T) {
	testutils.SetupTest()
	root := testutils.TestRootDir
	recs := createRecs("TestCommon")
	e := executor.NewCommonExecutor()
	err := e.Execute(root, recs)
	testutils.ShutdownTest()
	require.NoError(t, err)
	require.False(t, e.EOT())
}

func TestSafe(t *testing.T) {
	testutils.SetupTest()
	root := testutils.TestRootDir
	recs := createRecs("TestSafe")
	e := executor.NewSafeExecutor()
	err := e.Execute(root, recs)
	testutils.ShutdownTest()
	require.NoError(t, err)
	require.False(t, e.EOT())
}

func TestStdoutEOT(t *testing.T) {
	testutils.SetupTest()
	recs := createRecs("TestStdout")
	recs = append(recs, record.NewEOT())
	e := executor.NewStdoutExecutor()
	err := e.Execute("doesnotmatter", recs)
	testutils.ShutdownTest()
	require.NoError(t, err)
	require.True(t, e.EOT())
}

func TestCommonEOT(t *testing.T) {
	testutils.SetupTest()
	root := testutils.TestRootDir
	recs := createRecs("TestCommon")
	recs = append(recs, record.NewEOT())
	e := executor.NewCommonExecutor()
	err := e.Execute(root, recs)
	testutils.ShutdownTest()
	require.NoError(t, err)
	require.True(t, e.EOT())
}

func TestSafeEOT(t *testing.T) {
	testutils.SetupTest()
	root := testutils.TestRootDir
	recs := createRecs("TestSafe")
	recs = append(recs, record.NewEOT())
	e := executor.NewSafeExecutor()
	err := e.Execute(root, recs)
	testutils.ShutdownTest()
	require.NoError(t, err)
	require.True(t, e.EOT())
}

func TestStdoutDupEOT(t *testing.T) {
	testutils.SetupTest()
	recs := createRecs("TestStdout")
	recs = append([]record.Record{record.NewEOT()}, recs...)
	recs = append(recs, record.NewEOT())
	e := executor.NewStdoutExecutor()
	err := e.Execute("doesnotmatter", recs)
	testutils.ShutdownTest()
	require.Error(t, err)
}

func TestCommonDupEOT(t *testing.T) {
	testutils.SetupTest()
	root := testutils.TestRootDir
	recs := createRecs("TestCommon")
	recs = append([]record.Record{record.NewEOT()}, recs...)
	recs = append(recs, record.NewEOT())
	e := executor.NewCommonExecutor()
	err := e.Execute(root, recs)
	testutils.ShutdownTest()
	require.Error(t, err)
}

func TestSafeDupEOT(t *testing.T) {
	testutils.SetupTest()
	root := testutils.TestRootDir
	recs := createRecs("TestSafe")
	recs = append([]record.Record{record.NewEOT()}, recs...)
	recs = append(recs, record.NewEOT())
	e := executor.NewSafeExecutor()
	err := e.Execute(root, recs)
	testutils.ShutdownTest()
	require.Error(t, err)
}
