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
	"fmt"
	"io"
	"os"
	"os/user"
	"strconv"
	"testing"
)

// TestRootDir defines the root directory used by tests to write temporary test data
const TestRootDir string = ",scratch"

// SetupTest is the first function Go test framework calls when testing the 'datalayer' package.
// we do setup here.
func SetupTest() error {
	//create a test directory
	os.MkdirAll(TestRootDir, 0777)

	// if test root directory is not empty, we bail out.
	fp, err := os.Open(TestRootDir)
	if err != nil {
		fmt.Println("Error: could not open test directory", TestRootDir)
		return err
	}
	defer fp.Close()

	_, err = fp.Readdirnames(1)

	if err != io.EOF {
		fmt.Println("Error: expected empty test directory", TestRootDir,
			" But it is either a file or a non-empty directory.")
		return err
	}

	return nil
}

// ShutdownTest cleans up after running the tests
func ShutdownTest() {
	//remove the contents of the test root directory. then create test rood dir again
	os.RemoveAll(TestRootDir)
}

// RunningAsRoot returns true if it is running as a super user
func RunningAsRoot() bool {
	//are we running as root
	usr, err := user.Current()
	if err != nil {
		fmt.Println("Failed to get current user with the following error:", err)
		return false
	}

	uid, err := strconv.Atoi(usr.Uid)
	if err != nil {
		fmt.Println("Failed to get uid with the following error:", err)
		return false
	}

	gid, err := strconv.Atoi(usr.Gid)
	if err != nil {
		fmt.Println("Failed to get gid with the following error:", err)
		return false
	}

	return uid == 0 && gid == 0
}

// MainTestEntry is the main test function that gets called.
// it performs setup, runs tests, and if they passed, performs the teardown.
func MainTestEntry(m *testing.M) {
	//setup
	fmt.Println("Setting up")
	err := SetupTest()
	if err != nil {
		fmt.Println("Setup failed with the following error:", err, "\nExiting.")
		os.Exit(1)
	}

	//run all tests
	retCode := m.Run()
	fmt.Println("Tests finished running. Exit code is", retCode)

	//shutdown
	if retCode == 0 {
		fmt.Println("Performing cleanup.")
		ShutdownTest()
	} else {
		fmt.Println("Not cleaning up because tests failed.")
	}
	os.Exit(retCode)
}
