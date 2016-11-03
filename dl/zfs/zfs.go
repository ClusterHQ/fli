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

package zfs

import (
	"fmt"
	"strings"

	"github.com/ClusterHQ/fli/dl/datalayer"
	"github.com/ClusterHQ/fli/errors"
	"github.com/ClusterHQ/fli/meta/blob"
	"github.com/ClusterHQ/fli/meta/volume"
	"github.com/ClusterHQ/fli/meta/volumeset"
	"github.com/ClusterHQ/fli/securefilepath"
	uuidPkg "github.com/pborman/uuid"
)

// ZFS storage implementation
// On each ZFS storage, one zpool is created as the root of all Voluminous data sets.
// A voluminous snapshot is a ZFS snapshot and it is created by taking a snapshot from a volume.
// A volume is a mutable file system on ZFS created by cloning an existing snapshot.
//
// A volume set is created by:
// zfs create zpool/uuid(of volume set)
// And a snapshot is taken right after the creation of the file system for a volume set:
// zfs snapshot zpool/uuid(of volume set)@empty
// this empty snapshot is the base for all new volumes
//
// A volume is created by:
// zfs clone zpool/uuid(of volume set)@empty zpool/uuid_of_volume_set/uuid_of_volume
// Mount point for the volume is zpool/uuid_of_volume_set/uuid_of_volume
//
// A snapshot is created by:
// zfs snapshot zpool/uuid_of_volume_set/uuid_of_volume@snap_uuid
// Snapshot's blob id is: zpool/uuid_of_volume_set/uuid_of_volume/.zfs/snapshot/uuid(of snapshot)
//
// Delete a volume set:
// zfs destroy -R zpool/uuid_of_volume_set
//
// No auto create ZFS pool yet, manual creation is required until we have auto config of a ZFS storage backend.
// To prepare a ZFS pool, create a zpool using the name defined by 'zpool', take a snapshot of the newly created
// zpool, this will be used as the default volume root.
const (
	snapshotRoot = ".zfs"
	snapshotDir  = "snapshot"
)

var (
	_ datalayer.Storage = ZFS{}
)

// ZFS hides implementation details of the ZFS backend from caller
type ZFS struct {
	blobDiffer datalayer.BlobDifferFactory
	zpool      string
}

// New creates a new ZFS storage backend
func New(zpool string, blobDiffer datalayer.BlobDifferFactory) (ZFS, error) {
	// Must call this first
	initialize()
	zfs := ZFS{zpool: zpool, blobDiffer: blobDiffer}
	if err := zfs.Validate(); err != nil {
		return zfs, err
	}

	return zfs, nil
}

// Close should be called when shutting down the ZFS storage backend
func Close() {
	finish()
}

// ZFS implements datalayer.Storage interface

// BlobDiffer is the ZFS implementation of the Storage interface
func (z ZFS) BlobDiffer() datalayer.BlobDifferFactory {
	return z.blobDiffer
}

// EmptyBlobID is the ZFS implementation of the Storage interface
func (z ZFS) EmptyBlobID(vsid volumeset.ID) (blob.ID, error) {
	// Check if volume set base exists
	fs := z.volumesetPath(vsid)
	base := fs + "@empty"
	if !exists(base) {
		// Volume set's base has not been established, created it
		err := createFileSystem(fs)
		if err != nil {
			return blob.NilID(), err
		}

		// Create a snapshot off the newly created filesystem, this is the root of all snapshots/volumes of
		// this volume set
		err = snapshot([]string{base})
		if err != nil {
			return blob.NilID(), err
		}
	}
	return blob.NewID(base), nil
}

// volumesetPath form a zfs filesystem path for a volume set
func (z ZFS) volumesetPath(vsid volumeset.ID) string {
	return strings.Join([]string{z.zpool, vsid.String()}, "/")
}

// volumePath form a zfs filesystem path for a volume
func (z ZFS) volumePath(vsid volumeset.ID, vid volume.ID) string {
	return strings.Join([]string{z.zpool, vsid.String(), vid.String()}, "/")
}

// CreateVolume is the ZFS implements of the Storage interface
func (z ZFS) CreateVolume(vsid volumeset.ID, b blob.ID) (volume.ID, securefilepath.SecureFilePath, error) {
	vid := volume.NewID(uuidPkg.New())

	err := clone(z.volumePath(vsid, vid), b.String())
	if err != nil {
		return volume.NilID(), nil, err
	}

	path, err := z.volumeMountPointPath(vsid, vid)
	// TODO clean up on a failure.
	return vid, path, err
}

// GetVolumeForSnapshot is the ZFS implements of the Storage interface
func (z ZFS) GetVolumeForSnapshot(vsid volumeset.ID, b blob.ID) (volume.ID, securefilepath.SecureFilePath, error) {
	var vid volume.ID

	vsname := z.volumesetPath(vsid)

	s := strings.Split(b.String(), "@")
	if len(s) != 2 {
		return vid, nil, errors.Errorf("Invalid blob id %v", b)
	}

	vname := s[0]
	sname := s[1]
	if vname == vsname && sname == "empty" {
		// Special case, starting from the original empty blob.
		return z.CreateVolume(vsid, b)
	}
	if !strings.HasPrefix(vname, vsname+"/") {
		return vid, nil, errors.Errorf("Blob id %v does not match volumeset %v", b, vsid)
	}

	// The blob must exit.
	exists, err := z.SnapshotExists(b)
	if err != nil {
		return vid, nil, err
	}
	if !exists {
		return vid, nil, errors.Errorf("Blob %v does not exist", b)
	}

	written, err := getUint64Property(vname, "written@"+sname)
	if err != nil {
		return vid, nil, err
	}
	if written > 0 {
		// The volume has been modified since the snapshot.
		return z.CreateVolume(vsid, b)
	}

	vid = volume.NewID(strings.TrimPrefix(vname, vsname+"/"))
	path, err := z.volumeMountPointPath(vsid, vid)
	// TODO clean up on a failure.
	return vid, path, err
}

// CreateSnapshot is the ZFS implementation of the Storage interface
func (z ZFS) CreateSnapshot(vsid volumeset.ID, vid volume.ID) (blob.ID, error) {
	path := strings.Join([]string{z.volumePath(vsid, vid), uuidPkg.New()}, "@")
	err := snapshot([]string{path})
	if err != nil {
		return blob.NilID(), err
	}

	return blob.NewID(path), err
}

// DestroyVolume is the ZFS implementation of the Storage interface
func (z ZFS) DestroyVolume(vsid volumeset.ID, vid volume.ID) error {
	// By rolling back to the most recent snapshot, we "destroy" the volume.
	return rollback(z.volumePath(vsid, vid))
}

// DestroySnapshot is the ZFS implementation of the Storage interface
func (z ZFS) DestroySnapshot(b blob.ID) error {
	return destroySnapshot([]string{b.String()}, false)
}

// SnapshotExists is the ZFS implementation of Storage interface
func (z ZFS) SnapshotExists(b blob.ID) (bool, error) {
	return exists(b.String()), nil
}

// BlobMountPointPath is the ZFS implementation of Storage interface
func (z ZFS) BlobMountPointPath(b blob.ID) (securefilepath.SecureFilePath, error) {
	s1 := strings.Split(b.String(), "/")
	// Leave out the last one
	s2 := s1[:len(s1)-1]
	// Last one has the snapshot name
	s3 := strings.Split(s1[len(s1)-1], "@")
	if len(s3) != 2 {
		return nil, fmt.Errorf("ZFS.BlobMountPointPath invalid blob id %v", b)
	}

	// Build secure file path for ZFS snapshot
	path, err := securefilepath.New("/")
	if err != nil {
		return nil, err
	}

	for _, segment := range append(s2, []string{s3[0], snapshotRoot, snapshotDir, s3[1]}...) {
		path, err = path.Child(segment)
		if err != nil {
			return nil, err
		}
	}

	return path, nil
}

// volumeMountPointPath returns a path where the volume is mounted.
func (z ZFS) volumeMountPointPath(vsid volumeset.ID, vid volume.ID) (securefilepath.SecureFilePath, error) {
	path, err := securefilepath.New("/")
	if err != nil {
		return nil, err
	}

	// Build secure file path for ZFS volume
	for _, segment := range []string{z.zpool, vsid.String(), vid.String()} {
		path, err = path.Child(segment)
		if err != nil {
			return nil, err
		}
	}

	return path, err
}

// DestroyVolumeSet ...
func (z ZFS) DestroyVolumeSet(vsid volumeset.ID) error {
	return destroyFileSystem(z.volumesetPath(vsid))
}

// GetSnapshotSpace ...
func (z ZFS) GetSnapshotSpace(b blob.ID) (datalayer.SnapshotSpace, error) {
	return getSnapshotSpace(b.String())
}

// GetTotalSpace ...
func (z ZFS) GetTotalSpace() (datalayer.DiskSpace, error) {
	return getFSAndDescendantsSpace(z.zpool)
}

// GetVolumesetSpace ...
func (z ZFS) GetVolumesetSpace(vsid volumeset.ID) (datalayer.DiskSpace, error) {
	return getFSAndDescendantsSpace(z.volumesetPath(vsid))
}

// Validate ...
func (z ZFS) Validate() error {
	err := validate(z.zpool)
	if err != nil {
		return err
	}

	return nil
}
