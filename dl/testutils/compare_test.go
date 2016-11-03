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

package testutils_test

import (
	"os"
	"testing"

	"github.com/ClusterHQ/fli/dl/testutils"
)

// TestSuccessCompare validates if the CompareTree can compare 2 similar directories and validate the result.
func TestSuccessCompare(t *testing.T) {
	err := testutils.CompareTree(".", ".")
	if err != nil {
		t.Fatal(err, ":found mismatch.")
	}
}

// TestFailedCompare validates if the CompareTree can compare 2 dissimilar directories and validate the result.
func TestFailedCompare(t *testing.T) {
	err := testutils.CompareTree(".", "..")
	if err == nil {
		t.Fatal(err, ":match found.")
	}
}

func TestFileCompare(t *testing.T) {
	/* TEST CASE # 1: COMPARE TWO FILES - SAME SIZE DIFFERENT CONTENT */
	filenameA := testutils.TestRootDir + "/file1.out"
	fptrA, err := os.Create(filenameA)
	if err != nil {
		t.Fatal(err, ":os.Create() failed for ", filenameA)
	}
	defer fptrA.Close()

	bufA := []byte{0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10}
	if _, err := fptrA.WriteAt(bufA, int64(0)); err != nil {
		t.Fatal(err, ":WriteAt() failed for ", filenameA)
	}

	filenameB := testutils.TestRootDir + "/file2.out"
	fptrB, err := os.Create(filenameB)
	if err != nil {
		t.Fatal(err, ":os.Create() failed for ", filenameB)
	}
	defer fptrB.Close()

	bufB := []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55}
	if _, err := fptrB.WriteAt(bufB, int64(0)); err != nil {
		t.Fatal(err, ":WriteAt() failed for ", filenameB)
	}

	// Compare 2 files with same size but different content
	if err := testutils.CompareTree(filenameA, filenameB); err == nil {
		t.Fatal("Match shouldn't be found")
	}

	/* TEST CASE # 2: COMPARE TWO FILES - SAME CONTENT DIFFERENT MODES */
	filenameC := testutils.TestRootDir + "/file3.out"
	fptrC, err := os.Create(filenameC)
	if err != nil {
		t.Fatal(err, ":os.Create() failed for ", filenameC)
	}
	defer fptrC.Close()

	bufC := []byte{0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10}
	if _, err := fptrB.WriteAt(bufC, int64(0)); err != nil {
		t.Fatal(err, ":WriteAt() failed for ", filenameC)
	}

	if err := os.Chmod(filenameC, 0777); err != nil {
		t.Fatal(err, ": os.Chmod() failed for ", filenameC)
	}

	// Compare 2 file with different modes
	if err := testutils.CompareTree(filenameA, filenameC); err == nil {
		t.Fatal("Match shouldn't be found")
	}

	/* TEST CASE # 3: COMPARE TWO FILES - DIFFERENT CONTENTS */
	filenameD := testutils.TestRootDir + "/file4.out"
	fptrD, err := os.Create(filenameD)
	if err != nil {
		t.Fatal(err, ":os.Create() failed for ", filenameD)
	}
	defer fptrD.Close()

	bufD := []byte{0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10}
	if _, err := fptrB.WriteAt(bufD, int64(0)); err != nil {
		t.Fatal(err, ":WriteAt() failed for ", filenameD)
	}

	// Compare 2 file with different sizes
	if err := testutils.CompareTree(filenameA, filenameD); err == nil {
		t.Fatal("Match shouldn't be found")
	}

	/* TEST CASE # 4: COMPARE TWO SYMLINKS - SAME SYMLINKS */
	symlinkA := testutils.TestRootDir + "/symlink1.link"
	if err := os.Symlink(filenameA, symlinkA); err != nil {
		t.Fatal(err, ": os.Link() failed for ", symlinkA)
	}
	symlinkB := testutils.TestRootDir + "/symlink2.link"
	if err := os.Symlink(filenameA, symlinkB); err != nil {
		t.Fatal(err, ": os.Link() failed for ", symlinkB)
	}

	// Compare 2 symlinks that match
	if err := testutils.CompareTree(symlinkA, symlinkB); err != nil {
		t.Fatal(err, ": Found mismatch")
	}

	/* TEST CASE # 5: COMPARE TWO SYMLINKS - DIFFERENT TARGET LINKS */
	symlinkC := testutils.TestRootDir + "/symlink3.link"
	if err := os.Symlink(filenameB, symlinkC); err != nil {
		t.Fatal(err, ": os.Link() failed for ", symlinkB)
	}
	// Compare 2 symlinks that don't match
	if err := testutils.CompareTree(symlinkA, symlinkC); err == nil {
		t.Fatal("Match shouldn't be found")
	}
}
