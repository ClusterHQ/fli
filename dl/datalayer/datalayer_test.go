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

// Package datalayer_test tests different datalayer implementations
package datalayer_test

import (
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"testing"

	"github.com/ClusterHQ/fli/dl/blobdiffer"
	"github.com/ClusterHQ/fli/dl/datalayer"
	"github.com/ClusterHQ/fli/dl/filediffer/variableblk"
	"github.com/ClusterHQ/fli/dl/fs"
	"github.com/ClusterHQ/fli/dl/testutils"
	"github.com/ClusterHQ/fli/dl/zfs"
	"github.com/ClusterHQ/fli/errors"
	"github.com/ClusterHQ/fli/meta/snapshot"
	"github.com/ClusterHQ/fli/meta/volumeset"
	"github.com/ClusterHQ/fli/securefilepath"
	"github.com/stretchr/testify/assert"
)

// basicTests tests volume creation, snapshot creation, volume deletion and snapshot deletion
func basicTests(t *testing.T, s datalayer.Storage) error {
	// Create a new volume set
	vsid := volumeset.NewRandomID()
	p1, err := s.EmptyBlobID(vsid)
	if err != nil {
		t.Error("Getting empty blob failed", err)
		return err
	}

	// Try again and see if we will get the same path
	p2, err := s.EmptyBlobID(vsid)
	assert.NoError(t, err)
	assert.Equal(t, p1, p2)

	vid, _, err := s.CreateVolume(vsid, p1, datalayer.AutoMount)
	if err != nil {
		t.Error("Create volume from empty blob failed", err)
		return err
	}

	// Creates a snapshot
	blobid, err := s.CreateSnapshot(vsid, snapshot.NewRandomID(), vid)
	if err != nil {
		t.Error("Create snapshot failed", err)
		return err
	}

	// Make sure the snapshot is created
	ok, err := s.SnapshotExists(blobid)
	if err != nil {
		t.Error("Check snapshot exist failed with error", err)
		return err
	}

	if !ok {
		t.Error("Create snapshot failed to create")
		return err
	}

	// Destroys the snapshot
	err = s.DestroySnapshot(blobid)
	if err != nil {
		t.Error("Destroy snapshot failed", err)
		return err
	}

	// Make sure the snapshot is removed
	ok, err = s.SnapshotExists(blobid)
	if err != nil {
		t.Error("Check snapshot exist after delete failed with error", err)
		return err
	}

	if ok {
		t.Error("Destroy snapshot failed, still exist")
		return err
	}

	err = s.DestroyVolume(vsid, vid)
	if err != nil {
		t.Error("Destroy volume failed", err)
		return err
	}

	// Delete volume set
	err = s.DestroyVolumeSet(vsid)
	if err != nil {
		t.Error("Destroy volume set failed", err)
		return err
	}

	return nil
}

func writeRandomData(path string, size int64) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	r := rand.New(rand.NewSource(99))
	_, err = io.CopyN(f, r, size)
	if err != nil {
		return err
	}
	return nil
}

// spaceTests tests methods that query space usage
func spaceTests(t *testing.T, s datalayer.Storage) error {
	// Create a new volume set
	vsid := volumeset.NewRandomID()
	p, _ := s.EmptyBlobID(vsid)
	vid, vpath, _ := s.CreateVolume(vsid, p, datalayer.AutoMount)

	// Create a snapshot
	blobid1, _ := s.CreateSnapshot(vsid, snapshot.NewRandomID(), vid)

	totalSpace1, err := s.GetTotalSpace()
	if err != nil {
		t.Error("GetTotalSpace failed", err)
		return err
	}
	log.Printf("totalSpace1.Used = %d totalSpace1.Available = %d\n", totalSpace1.Used, totalSpace1.Available)

	vsSpace1, err := s.GetVolumesetSpace(vsid)
	if err != nil {
		t.Error("GetVolumesetSpace failed", err)
		return err
	}
	log.Printf("vsSpace1.Used = %d vsSpace1.Available = %d\n", vsSpace1.Used, vsSpace1.Available)

	snapSpace1, err := s.GetSnapshotSpace(blobid1)
	if err != nil {
		t.Error("GetSpaceSpace failed for the first blob", err)
		return err
	}
	log.Printf("snapSpace1.LogicalSize = %d snapSpace1.DeltaFromPrevious = %d\n", snapSpace1.LogicalSize, snapSpace1.DeltaFromPrevious)

	dataFile, err := vpath.Child("test_file")
	if err != nil {
		t.Error("Failed to generate data file name:", err)
		return err
	}

	const fileSize = 4 * 128 * 1024
	err = writeRandomData(dataFile.Path(), fileSize)
	if err != nil {
		t.Error("Failed to create a data file", err)
		return err
	}

	// Create another snapshot
	blobid2, _ := s.CreateSnapshot(vsid, snapshot.NewRandomID(), vid)

	totalSpace2, _ := s.GetTotalSpace()
	log.Printf("totalSpace2.Used = %d totalSpace2.Available = %d\n", totalSpace2.Used, totalSpace2.Available)
	if totalSpace2.Used < totalSpace1.Used+fileSize {
		err = errors.Errorf("Total used space %d is less than expected %d", totalSpace2.Used, totalSpace1.Used+fileSize)
		t.Error(err)
		return err
	}
	if totalSpace2.Available > totalSpace1.Available-fileSize {
		err = errors.Errorf("Total available space %d is greater than expected %d", totalSpace2.Available, totalSpace1.Available-fileSize)
		t.Error(err)
		return err
	}

	vsSpace2, _ := s.GetVolumesetSpace(vsid)
	log.Printf("vsSpace2.Used = %d vsSpace2.Available = %d\n", vsSpace2.Used, vsSpace2.Available)
	if vsSpace2.Used < vsSpace1.Used+fileSize {
		err = errors.Errorf("Volumeset used space %d is less than expected %d", vsSpace2.Used, vsSpace1.Used+fileSize)
		t.Error(err)
		return err
	}
	if vsSpace2.Available > vsSpace1.Available-fileSize {
		err = errors.Errorf("Volumeset available space %d is greater than expected %d", vsSpace2.Available, vsSpace1.Available-fileSize)
		t.Error(err)
		return err
	}

	snapSpace21, _ := s.GetSnapshotSpace(blobid1)
	if snapSpace1.LogicalSize != snapSpace21.LogicalSize {
		err = errors.New("Snapshot's logical size changed after creating another snapshot")
		t.Error(err)
		return err
	}
	if snapSpace1.DeltaFromPrevious != snapSpace21.DeltaFromPrevious {
		err = errors.New("Snapshot's new size changed after creating another snapshot")
		t.Error(err)
		return err
	}

	snapSpace2, err := s.GetSnapshotSpace(blobid2)
	log.Printf("snapSpace2.LogicalSize = %d snapSpace2.DeltaFromPrevious = %d\n", snapSpace2.LogicalSize, snapSpace2.DeltaFromPrevious)
	if snapSpace2.LogicalSize < snapSpace1.LogicalSize+fileSize {
		err = errors.Errorf("Snapshot logical size %d is less than expected %d", snapSpace2.LogicalSize, snapSpace1.LogicalSize+fileSize)
		t.Error(err)
		return err
	}
	if snapSpace2.DeltaFromPrevious < fileSize {
		err = errors.Errorf("Snapshot new size %d is less than expected %d", snapSpace2.DeltaFromPrevious, fileSize)
		t.Error(err)
		return err
	}

	// Delete volume set
	err = s.DestroyVolumeSet(vsid)
	if err != nil {
		t.Error("Destroy volume set failed", err)
		return err
	}

	return nil
}

// deleteVolumeSet ...
func deleteVolumeSet(t *testing.T, s datalayer.Storage) error {
	// Create a new volume set
	vsid := volumeset.NewRandomID()
	p1, err := s.EmptyBlobID(vsid)
	if err != nil {
		t.Error("Getting empty blob failed", err)
		return err
	}

	vid, _, err := s.CreateVolume(vsid, p1, datalayer.AutoMount)
	if err != nil {
		t.Error("Create volume from empty blob failed", err)
		return err
	}

	// Creates a snapshot
	blobid1, err := s.CreateSnapshot(vsid, snapshot.NewRandomID(), vid)
	if err != nil {
		t.Error("Create snapshot failed", err)
		return err
	}

	// Creates another snapshot
	blobid2, err := s.CreateSnapshot(vsid, snapshot.NewRandomID(), vid)
	if err != nil {
		t.Error("Create snapshot failed", err)
		return err
	}

	// Delete volume set
	err = s.DestroyVolumeSet(vsid)
	if err != nil {
		t.Error("Destroy volume set failed", err)
		return err
	}

	// Make sure the snapshots are removed
	ok, err := s.SnapshotExists(blobid1)
	if err != nil {
		t.Error("Check snapshot exist after delete failed with error", err)
		return err
	}
	if ok {
		t.Error("Destroy snapshot failed, still exist")
		return err
	}
	ok, err = s.SnapshotExists(blobid2)
	if err != nil {
		t.Error("Check snapshot exist after delete failed with error", err)
		return err
	}
	if ok {
		t.Error("Destroy snapshot failed, still exist")
		return err
	}

	return nil
}

func TestZFS(t *testing.T) {
	// Skip if not running as super user
	if !testutils.RunningAsRoot() {
		t.Skip("Not running as superuser, skip ZFS tests.")
	}

	s, err := zfs.New("chq", blobdiffer.Factory{FileDiffer: variableblk.Factory{}})
	if err != nil {
		t.Fatal(err)
	}

	err = basicTests(t, s)
	if err != nil {
		switch e := err.(type) {
		case errors.Error:
			t.Fatal(e.Error(), e.GetStackTrace())
		default:
			t.Fatal(e.Error())
		}
	}
	err = spaceTests(t, s)
	if err != nil {
		switch e := err.(type) {
		case errors.Error:
			t.Fatal(e.Error(), e.GetStackTrace())
		default:
			t.Fatal(e.Error())
		}
	}

	err = deleteVolumeSet(t, s)
	if err != nil {
		switch e := err.(type) {
		case errors.Error:
			t.Fatal(e.Error(), e.GetStackTrace())
		default:
			t.Fatal(e.Error())
		}
	}

	zfs.Close()
}

func TestFS(t *testing.T) {
	name, err := ioutil.TempDir("", "datalayer_test_fs-")
	if err != nil {
		t.Fatal("Failed to allocate temporary directory for fs tests", err)
	}
	path, err := securefilepath.New(name)
	if err != nil {
		t.Fatal("Failed to construct temporary path for fs tests", err, path)
	}
	s, err := fs.New(path)
	if err != nil {
		t.Fatal("Failed to construct fs storage implementation", err, path)
	}

	err = basicTests(t, s)
	if err != nil {
		switch e := err.(type) {
		case errors.Error:
			t.Fatal(e.Error(), e.GetStackTrace())
		default:
			t.Fatal(e.Error())
		}
	}
}
