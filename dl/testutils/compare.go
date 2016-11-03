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

package testutils

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// fileInfo provides an extended set of functionality to os.FileInfo
type fileInfo struct {
	os.FileInfo
	rootPath string // root path for the file/directory
	relPath  string // relative path to the root path
}

// fullpath returns the full path that includes root and the file path
func (f fileInfo) fullpath() string {
	return filepath.Join(f.rootPath, f.relPath)
}

// isSymLink checks if the file/directory is a symbolic link
func (f fileInfo) isSymLink() bool {
	return f.Mode()&os.ModeSymlink == os.ModeSymlink
}

// compareSymLinks compares to check to symbolic links passed as fileInfo structs
//
// NOTE: That the symbolic link is relative to the root for each of the file and don't point to the same
//       file or directory
func compareSymLinks(finfoA fileInfo, finfoB fileInfo) error {
	targetA, err := os.Readlink(finfoA.fullpath())
	if err != nil {
		fmt.Println(err, ":Readlink failed.")
		fmt.Println("File: ", finfoA.fullpath())
		return err
	}

	targetB, err := os.Readlink(finfoB.fullpath())
	if err != nil {
		fmt.Println(err, ":Readlink failed.")
		fmt.Println("File: ", finfoB.fullpath())
		return err
	}

	// TODO: The target links may be outside the root.
	if targetA != targetB { // just compare the symlink name, don't go deeper
		fmt.Println("symlink mismatched for:")
		fmt.Println("Symlink: ", finfoA.fullpath())
		fmt.Println("Traget: ", targetA)
		fmt.Println("Symlink: ", finfoA.fullpath())
		fmt.Println("Target: ", targetB)
		return errors.New("target mismatch")
	}

	return nil
}

// compareFile compares two files byte by byte
func compareFiles(finfoA fileInfo, finfoB fileInfo) error {
	if finfoA.Size() != finfoB.Size() {
		fmt.Println("File: ", finfoB.fullpath())
		fmt.Println("File: ", finfoB.fullpath())
		return errors.New("different file sizes")
	}

	fptrA, err := os.Open(finfoA.fullpath())
	if err != nil {
		fmt.Println(err, ":open() failed")
		fmt.Println("File: ", finfoA.fullpath())
		return err
	}
	defer fptrA.Close()

	fptrB, err := os.Open(finfoB.fullpath())
	if err != nil {
		fmt.Println(err, ":open failed")
		fmt.Println("File: ", finfoB.fullpath())
		return err
	}
	defer fptrB.Close()

	chunkSz := int64(1 << 12) // 4K blocks
	bufA := make([]byte, chunkSz)
	bufB := make([]byte, chunkSz)

	// file Size of the both files should be same here.
	fileSz := finfoA.Size()

	for fileOff := int64(0); fileOff < fileSz; fileOff += chunkSz {
		_, errA := fptrA.ReadAt(bufA, fileOff)
		_, errB := fptrB.ReadAt(bufB, fileOff)

		// Ignore EOF because both files are of the same size
		if errA != nil && errA != io.EOF {
			fmt.Println(errA, ": error while reading.")
			fmt.Println("File: ", finfoA.fullpath())
			return errA
		}

		if errB != nil && errB != io.EOF {
			fmt.Println(errB, ": error while reading.")
			fmt.Println("File: ", finfoB.fullpath())
			return errB
		}

		if !bytes.Equal(bufA, bufB) {
			fmt.Println("File bytes mismatch for")
			fmt.Println("File: ", finfoA.fullpath())
			fmt.Println("File: ", finfoB.fullpath())
			return errors.New("File bytes mismatch")
		}
	}

	return nil
}

// deepCompare does a deep compare of the two files.
//
// For directory it does a size compare along with modes and attributes.
// For file it does a byte by byte comparison of the files.
func deepCompare(finfoA fileInfo, finfoB fileInfo) error {
	if finfoA.Size() != finfoB.Size() ||
		finfoA.Mode() != finfoB.Mode() {
		fmt.Println("FileInfo mismatch for ")
		fmt.Println("File: ", finfoA.fullpath())
		fmt.Println("File: ", finfoB.fullpath())
		fmt.Println("Details:")
		fmt.Println("A: ", finfoA.Size(), ",", finfoA.Mode())
		fmt.Println("B: ", finfoB.Size(), ",", finfoB.Mode())
		return errors.New("File info mismatch")
	}

	// at this point if they are directories they match.
	if finfoA.IsDir() {
		return nil
	}

	// compare and verify if symlinks
	if finfoA.isSymLink() {
		err := compareSymLinks(finfoA, finfoB)
		if err != nil {
			return err
		}

		return nil
	}

	// If we are here, fptrA & fptrB are files
	err := compareFiles(finfoA, finfoB)
	return err
}

// compareRoot function is an handler method invoked by filepath.Walk() when it visits a file or directory.
// The function compares the current file/directory being walked to corresponding file/directory in rootB.
func compareRoot(rootA string, rootB string) filepath.WalkFunc {
	return func(path string, infoA os.FileInfo, errP error) error {
		if errP != nil {
			fmt.Println(errP, ":filepath.Walk() failed.")
			fmt.Println("File: ", filepath.Join(rootA, path))
			return errP
		}

		// relPath is the relative path of path from rootA
		relPath, err := filepath.Rel(rootA, path)
		if err != nil {
			fmt.Println(err, ":filepath method failed.")
			fmt.Println("File: ", path)
			return err
		}

		infoB, err := os.Lstat(filepath.Join(rootB, relPath))
		if err != nil {
			fmt.Println(err, ":os.Lstat() failed.")
			fmt.Println("File: ", filepath.Join(rootB, relPath))
			return err
		}

		// compare the internals of file info
		err = deepCompare(fileInfo{infoA, rootA, relPath}, fileInfo{infoB, rootB, relPath})
		return err
	}
}

// CompareTree does deep compare the two paths rootA & rootB and returns true if they match else returns false.
func CompareTree(rootA string, rootB string) error {
	// Following method walks through the rootA and matches it to rootB
	// NOTE: This will detect files/directories in rootA but not in rootB
	err := filepath.Walk(rootA, compareRoot(rootA, rootB))
	if err != nil {
		return err
	}

	// Following method walks through the rootB and matches it to rootA
	// NOTE: This will detect files/directories in rootB but not in rootA
	err = filepath.Walk(rootB, compareRoot(rootB, rootA))
	return err
}
