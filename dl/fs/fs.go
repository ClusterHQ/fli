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

package fs

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/ClusterHQ/fli/dl/blobdiffer"
	"github.com/ClusterHQ/fli/dl/datalayer"
	"github.com/ClusterHQ/fli/dl/filediffer/variableblk"
	"github.com/ClusterHQ/fli/meta/blob"
	"github.com/ClusterHQ/fli/meta/snapshot"
	"github.com/ClusterHQ/fli/meta/volume"
	"github.com/ClusterHQ/fli/meta/volumeset"
	"github.com/ClusterHQ/fli/securefilepath"
	"github.com/pborman/uuid"
)

type fsStorage struct {
	// blobs is the persistent storage for blobs
	blobs securefilepath.SecureFilePath

	// volumes is the transient location for volumes
	volumes securefilepath.SecureFilePath
}

var (
	// Even though this is called emptyID, the content of this is not empty.
	emptyID = blob.NewID(uuid.New())

	_ datalayer.Storage = &fsStorage{}
)

func newRandomID() string {
	return uuid.New()
}

// New creates a new Storage implementation which uses plain files and
// directories on the filesystem. Storage costs scale with the total VolumeSet
// size and the number of snapshots and volumes. Snapshotting cost
// scales with volume size. Certain operations that probably should be
// atomic aren't atomic.
func New(path securefilepath.SecureFilePath) (datalayer.Storage, error) {
	blobs, err := path.Child("blobs")
	if err != nil {
		return nil, err
	}
	volumes, err := path.Child("workingcopies")
	if err != nil {
		return nil, err
	}
	s := &fsStorage{blobs, volumes}
	err = s.initialize()
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *fsStorage) initialize() error {
	log.Printf("Here: %s\n", emptyID)
	child, err := s.blobs.Child(emptyID.String())
	if err != nil {
		return err
	}
	log.Printf("initialize %v\n", child)
	err = os.MkdirAll(child.Path(), 0700)
	if err != nil {
		return err
	}
	return nil
}

func (s *fsStorage) Version() string {
	return "Fake FS"
}
func (s *fsStorage) EmptyBlobID(vsid volumeset.ID) (blob.ID, error) {
	return emptyID, nil
}

func (s *fsStorage) CreateVolume(
	vsid volumeset.ID,
	id blob.ID,
	_ datalayer.MountType,
) (volume.ID, securefilepath.SecureFilePath, error) {
	exists, err := s.SnapshotExists(id)
	if err != nil {
		return volume.NilID(), nil, fmt.Errorf("Failed to check if snapshot exists: %v", err)
	}

	if !exists {
		return volume.NilID(), nil, errors.New("No such blob")
	}

	blobPath, err := s.blobPath(id)
	if err != nil {
		return volume.NilID(), nil, err
	}

	// uuid used directly as volume id
	vid := volume.NewID(uuid.New())

	parentPath, err := s.volumes.Child(vid.String())
	if err != nil {
		return volume.NilID(), nil, fmt.Errorf("Failed to get child path: %v", err)
	}
	volumePath, err := parentPath.Child("working-copy")
	if err != nil {
		return volume.NilID(), nil, fmt.Errorf("Failed to get volume mount path failed: %v", err)
	}

	os.MkdirAll(volumePath.Parent().Path(), 0700)
	err = copyTree(blobPath, volumePath)
	return vid, volumePath, err
}

func (s *fsStorage) CreateSnapshot(vsid volumeset.ID, ssid snapshot.ID, vid volume.ID) (blob.ID, error) {
	blobID := blob.NewID(newRandomID())
	blobPath, err := s.blobs.Child(blobID.String())
	if err != nil {
		return blob.NilID(), err
	}
	vol, err := s.volumeMountPointPath(vid)
	if err != nil {
		return blob.NilID(), err
	}
	err = copyTree(vol, blobPath)
	if err != nil {
		return blob.NilID(), err
	}
	return blobID, nil
}

func (s *fsStorage) DestroyVolume(vsid volumeset.ID, id volume.ID) error {
	path, err := s.volumeMountPointPath(id)
	if err != nil {
		return err
	}
	// XXX Not atomic but this code may not need to work so well as that...
	// Go up above the "working-copy" path.
	return os.RemoveAll(path.Parent().Path())
}

func (s *fsStorage) DestroyVolumeSet(vsid volumeset.ID) error {
	// TODO: Implement me
	return nil
}

func (s *fsStorage) DestroySnapshot(id blob.ID) error {
	path, err := s.blobPath(id)
	if err != nil {
		return err
	}
	// XXX Not atomic but this code may not need to work so well as that...
	return os.RemoveAll(path.Path())
}

func (s *fsStorage) MountBlob(id blob.ID) (string, error) {
	p, err := s.blobPath(id)
	if err != nil {
		return "", err
	}
	return p.Path(), nil
}

func (s *fsStorage) Unmount(_ string) error {
	return nil
}

func (s *fsStorage) volumeMountPointPath(id volume.ID) (securefilepath.SecureFilePath, error) {
	parentPath, err := s.volumes.Child(id.String())
	if err != nil {
		return nil, err
	}
	return parentPath.Child("working-copy")
}

func (s *fsStorage) blobPath(id blob.ID) (securefilepath.SecureFilePath, error) {
	return s.blobs.Child(id.String())
}

func (s *fsStorage) SnapshotExists(id blob.ID) (bool, error) {
	b, err := s.blobPath(id)
	if err != nil {
		return false, fmt.Errorf("blobPath(%#v) failed: %v", id, err)
	}
	exists, err := b.Exists()
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (s *fsStorage) BlobDiffer() datalayer.BlobDifferFactory {
	return blobdiffer.Factory{
		FileDiffer: variableblk.Factory{},
	}
}

func copyTree(source, destination securefilepath.SecureFilePath) error {
	log.Printf("Copying tree %s to %s", source.Path(), destination.Path())
	return filepath.Walk(source.Path(), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		outputPath, err := securefilepath.New(
			destination.Path() + path[len(source.Path()):],
		)
		stat, err := os.Stat(path)
		if err != nil {
			return err
		}
		if stat.IsDir() {
			return os.Mkdir(outputPath.Path(), 0700)
		}

		input, err := os.Open(path)
		if err != nil {
			return err
		}
		defer input.Close()

		if err != nil {
			return err
		}
		output, err := os.Create(outputPath.Path())
		if err != nil {
			return err
		}
		defer output.Close()

		_, err = io.Copy(output, input)
		if err == nil {
			log.Printf("Copied from %s to %s", path, outputPath.Path())
		} else {
			log.Printf("Failed copying from %s to %s: %s", path, outputPath.Path(), err)
		}
		return err
	})
}

// GetSnapshotSpace ...
func (s *fsStorage) GetSnapshotSpace(b blob.ID) (datalayer.SnapshotSpace, error) {
	// TODO implement me...
	return datalayer.SnapshotSpace{}, nil
}

// GetTotalSpace ...
func (s *fsStorage) GetTotalSpace() (datalayer.DiskSpace, error) {
	// TODO implement me...
	return datalayer.DiskSpace{}, nil
}

// GetVolumesetSpace ...
func (s *fsStorage) GetVolumesetSpace(vsid volumeset.ID) (datalayer.DiskSpace, error) {
	// TODO implement me...
	return datalayer.DiskSpace{}, nil
}
