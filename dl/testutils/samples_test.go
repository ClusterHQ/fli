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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/ClusterHQ/fli/dl/executor"
	"github.com/ClusterHQ/fli/dl/record"
	"github.com/ClusterHQ/fli/dl/testutils"
)

// How to use Snapshot Test Framework
//
// Purpose
// The framework is designed to run tests for generating the diff and/or applying the diff. The diff is generated
// between a destination and source snapshot. The diff is applied on the source snapshot and compared against the
// destination snapshot to validate the result. A single sample data contains source and destination snapshots and each
// sample data represents a test-case. Developers can add samples data to this data-sets, the framework would run the
// test and validate the results.
//
// Where do the sample data-set exists?
// The sample data-set is kept in the github.com/ClusterHQ/fli/datalayer/samples directory. Each sample data is
// identified by the name of the file (without the extension). This sample data should be tar'ed and gzip'ed with ".tgz"
// extension.
// For example, in github.com/ClusterHQ/fli/datalayer/samples/sample1.tgz the sample data is "sample1" and the framework
// will extract the contents by un-gzip'ing and un-tar'ing the file.
//
// How to add a sample data?
// A typical sample data directory looks like this:
// sample##
// |- goFunc
// |- snapA
// |- snapB
//
// The goFunc is a simple text file that contains the function definition that returns a record slice. Here is the
// function signature you should use:
// func createRecSetSample##() []records
// Look at sample1.tgz to understand how to write the goFunc file.
// The goFunc file is optional if you want to test the diff generation.

// snapA and snapB are two snapshot from the same lineage where snapA is a source snapshot and snapB is the destination
// snapshot. From a unit test perspective each snapshot is just a directory.
// Once the sample## is complete simply tar and gzip the file to github.com/ClusterHQ/fli/datalayer/samples/sample##.tgz
// (NOTE: Remember to check-in the new file into your git.)
//
// How to test your new sample data?
// The framework is not completely automated, and you need to update this file to reference the desired sample data.
// (TODO: Future.)
// Once you have added the sample##.tgz file the samples_test.go need to be update with the latest sample information.
// To do this:
// 1. Add the function in sample##/goFunc to the end of the file.
// 2. Update var samples []sampleTestData = []sampleTestData{}
//    This is the list of tests the framework runs and validates. The new test is added here with the all the relevant

// sampleTestData groups all the attributes for a sample
// TODO: This can be extended or be converted into an interface where getRecsFunc() could be used to generate a diff.
type sampleTestData struct {
	tarFile     string                 // name of the sample tar file
	tarDir      string                 // dirname of the sample after untar
	getRecsFunc func() []record.Record // func pointer that returns set of records representing a diff
}

// Global Const
// IMPORTANT: Don't change testDataDir & samples unless the ClusterHQ/fli/datalayer/samples changes.
const testDataDir string = "testdata" // all the test data needed to validate unit tests

// Global
// TODO: This should be automated to extract the information at runtime?
var samples = []sampleTestData{
	{"sample1.tgz", "sample1", createRecSetSample1},
	{"sample2.tgz", "sample2", createRecSetSample2},
}

// samplesTestSetup untars the sample file to generate the snapA and snapB to run the tests
func (s *sampleTestData) sampleTestSetup() error {
	tarFile := filepath.Join(testDataDir, s.tarFile)

	err := testutils.Untar(tarFile, testutils.TestRootDir)
	if err != nil {
		fmt.Println(err, ":unable to untar", tarFile)
		return err

	}
	return nil
}

// sampleTestShutdown removes all the untar'ed files
func (s *sampleTestData) sampleTestShutdown() {
	os.Remove(filepath.Join(testutils.TestRootDir, s.tarDir))
}

// TestSamples validates the tests for a defined sample test cases. For each sample it applies rec[] on snapA that
// creates snapB. It compares the actual snapB with the expected snapB that validates the test.
// NOTE: Look at README.md in ClusterHQ/fli/datalayer/samples/README.md
func TestSamples(t *testing.T) {
	fmt.Println("*** Starting Tests")
	for _, s := range samples {
		fmt.Println("*** Running", s.tarDir, "now")

		snapA := "snapA"
		snapB := "snapB"

		err := s.sampleTestSetup()
		if err != nil {
			t.Fatal(err, ":sampleTestSetup() failed")
		}

		rootA := filepath.Join(testutils.TestRootDir, s.tarDir, snapA)
		rootB := filepath.Join(testutils.TestRootDir, s.tarDir, snapB)

		e := executor.NewCommonExecutor()

		// apply diff (snapB - snapA) on snapA to generate snapB again
		// TODO: Validate if diff needs to be generated here or function already exists that gives the []records
		err = e.Execute(rootA, s.getRecsFunc())
		if err != nil {
			t.Fatal(err, ":iterator execute tests failed")
		}

		err = testutils.CompareTree(rootA, rootB)
		if err != nil {
			t.Fatal(err, ":compare failed.")
		}

		s.sampleTestShutdown()
	}
	fmt.Println("*** Finished Tests")
}

// IMPORTANT: DO NOT CHANGE THIS - IT IS COPIED FROM ClusterHQ/fli/datalayer/samples/*.tgz FILES. //

// For samples/sample1.tgz //////////////////////////

// createRecSetSample1 is from the samples/sample1.tgz where this method returns the slice of records that represent a
// diff between sample1/snapB and sample1/snapA.
func createRecSetSample1() []record.Record {
	h1data := []byte{
		0x65, 0x20, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x73, 0x20, 0x62, 0x65, 0x79, 0x6f, 0x6e, 0x64,
		0x20, 0x74, 0x68, 0x69, 0x73, 0x20, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x20, 0x6f, 0x6e, 0x6c, 0x79, 0x20,
		0x61, 0x70, 0x70, 0x65, 0x61, 0x72, 0x73, 0x20, 0x74, 0x6f, 0x20, 0x62, 0x65, 0x20, 0x53, 0x6e, 0x61,
		0x70, 0x73, 0x68, 0x6f, 0x74, 0x20, 0x42, 0x2e, 0x20, 0x49, 0x74, 0x20, 0x69, 0x73, 0x20, 0x63, 0x72,
		0x69, 0x74, 0x69, 0x63, 0x61, 0x6c, 0x20, 0x66, 0x6f, 0x72, 0x20, 0x74, 0x68, 0x65, 0x20, 0x74, 0x65,
		0x73, 0x74, 0x20, 0x74, 0x6f, 0x20, 0x67, 0x65, 0x6e, 0x65, 0x72, 0x61, 0x74, 0x65, 0x20, 0x74, 0x68,
		0x69, 0x73, 0x20, 0x66, 0x72, 0x6f, 0x6d, 0x20, 0x74, 0x68, 0x65, 0x20, 0x72, 0x65, 0x63, 0x6f, 0x72,
		0x64, 0x20, 0x61, 0x72, 0x72, 0x61, 0x79, 0x20, 0x67, 0x65, 0x6e, 0x65, 0x72, 0x61, 0x74, 0x65, 0x64,
		0x2e, 0x20, 0x54, 0x68, 0x61, 0x6e, 0x6b, 0x73, 0x21, 0x0a,
	}
	h1offset := uint64(121)

	f2data := []byte{
		0x53, 0x61, 0x6e, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x20, 0x42, 0x20, 0x73, 0x68, 0x6f, 0x75, 0x6c, 0x64,
		0x20, 0x68, 0x61, 0x76, 0x65, 0x20, 0x69, 0x6e, 0x73, 0x69, 0x64, 0x65, 0x20, 0x6f, 0x66, 0x20, 0x69,
		0x74, 0x2e, 0x0a,
	}
	f2offset := uint64(0)

	rec := []record.Record{
		record.NewHardlink("h1", "/hardlink_h1"),  // new hardlink
		record.NewMkdir("/d1", 0755),              // new dirent
		record.NewRemove("/d2"),                   // remove dirent
		record.NewPwrite("/h1", h1data, h1offset), // write to the middle of the file
		record.NewRemove("/f1"),                   // remove file
		record.NewCreate("/f2", 0644),             // create file
		record.NewPwrite("/f2", f2data, f2offset), // write to the new file
	}

	return rec
}

// For samples/sample2.tgz //////////////////////////

// createRecSetSample2 is from the samples/sample2.tgz where this method returns the slice of records that represent a
// diff between sample2/snapB and sample2/snapA.
func createRecSetSample2() []record.Record {
	f2data := []byte{
		0x53, 0x61, 0x6e, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x20, 0x42, 0x20, 0x73, 0x68, 0x6f, 0x75, 0x6c, 0x64,
		0x20, 0x68, 0x61, 0x76, 0x65, 0x20, 0x69, 0x6e, 0x73, 0x69, 0x64, 0x65, 0x20, 0x6f, 0x66, 0x20, 0x69,
		0x74, 0x2e, 0x0a,
	}
	f2offset := uint64(0)

	rec := []record.Record{
		record.NewSymlink("s1", "/symlink_s1"),    // new symlink
		record.NewMkdir("/d1", 0755),              // new dirent
		record.NewRemove("/d2"),                   // remove dirent
		record.NewRemove("/f1"),                   // remove file
		record.NewCreate("/f2", 0644),             // create file
		record.NewPwrite("/f2", f2data, f2offset), // write to the new file
	}

	return rec
}

func TestMain(m *testing.M) {
	testutils.MainTestEntry(m)
}
