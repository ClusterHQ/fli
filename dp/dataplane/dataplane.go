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

package dataplane

import (
	"errors"
	"time"

	"github.com/ClusterHQ/fli/dl/datalayer"
	"github.com/ClusterHQ/fli/dp/metastore"
	"github.com/ClusterHQ/fli/meta/attrs"
	"github.com/ClusterHQ/fli/meta/blob"
	"github.com/ClusterHQ/fli/meta/branch"
	"github.com/ClusterHQ/fli/meta/snapshot"
	"github.com/ClusterHQ/fli/meta/util"
	"github.com/ClusterHQ/fli/meta/volume"
	"github.com/ClusterHQ/fli/meta/volumeset"
)

type (
	// BlobUploader sends a local blob identified by delta of the blob id and the base blob id. The receiver of the blob
	// is identified by the token.
	BlobUploader interface {
		UploadBlobDiff(
			vsid volumeset.ID,
			base blob.ID,
			blob blob.ID,
			token string,
			dspuburl string,
		) error
	}

	// BlobDownloader receives a blob from the sender identified by the token, blob received is applied to the base.
	BlobDownloader interface {
		DownloadBlobDiff(
			vsid volumeset.ID,
			ssid snapshot.ID,
			base blob.ID,
			token string,
			dspuburl string,
		) (blob.ID, uint64, error)
	}
)

func updateStorageUsage(mds metastore.Store, s datalayer.Storage, vsid volumeset.ID) error {
	space, err := s.GetVolumesetSpace(vsid)
	if err != nil {
		return err
	}

	return mds.SetVolumeSetSize(vsid, space.Used)
}

// forking returns true if a new branch is needed based on the given new snapshot parameters
func forking(mds metastore.Client, branchName string, syncMode metastore.SyncMode,
	parentSnapID *snapshot.ID) (bool, error) {
	if parentSnapID == nil {
		return true, nil
	}

	parentSnap, err := metastore.GetSnapshot(mds, *parentSnapID)
	if err != nil {
		return false, err
	}

	if !util.IsEmptyString(branchName) {
		if parentSnap.BranchName == branchName {
			return false, nil
		}
		return true, nil
	}

	if syncMode == metastore.AutoSync {
		if parentSnap.NumChildren > 0 {
			return true, nil
		}
		return false, nil
	}

	return true, nil
}

// Snapshot takes a snapshot of a volume by ask storage layer to take a snapshot followed by updating meta data store
func Snapshot(
	mds metastore.Client,
	s datalayer.Storage,
	volid volume.ID,
	branchName string,
	syncMode metastore.SyncMode,
	name string,
	attrs attrs.Attrs,
	desc string,
) (*snapshot.Snapshot, error) {
	vol, err := mds.GetVolume(volid)
	if err != nil {
		return nil, err
	}

	if vol.HasBase() {
		_, err = metastore.GetSnapshot(mds, *vol.BaseID)
		if err != nil {
			return nil, err
		}
	}

	ssid := snapshot.NewRandomID()
	// TODO: Make sure this is the right sequence in term of MDS consistence(transaction)
	blobid, err := s.CreateSnapshot(vol.VolSetID, ssid, volid)
	if err != nil {
		return nil, err
	}

	space, err := s.GetSnapshotSpace(blobid)
	if err != nil {
		return nil, err
	}

	vsid := vol.VolSetID

	var sn *snapshot.Snapshot
	fork, err := forking(mds, branchName, syncMode, vol.BaseID)
	if err != nil {
		return nil, err
	}

	if fork {
		sn, err = metastore.SnapshotFork(
			mds,
			vsid,
			ssid,
			branchName,
			vol.BaseID,
			blobid,
			attrs,
			name,
			space.LogicalSize,
			desc,
		)
	} else {
		sn, err = metastore.SnapshotExtend(
			mds,
			ssid,
			vol.BaseID,
			blobid,
			attrs,
			name,
			space.LogicalSize,
			desc,
		)
	}
	if err != nil {
		return nil, err
	}

	// TODO: This can be avoided if all datalayer supports ZFS like snapshot(fs mount continues after snapshot)
	// then we don't need destroy and re-create the volume, just need to update the volume's parent
	// TODO: May need to call data layer (if needed)
	err = mds.DeleteVolume(volid)
	if err != nil {
		return nil, err
	}

	newVol := vol.Copy()
	newVol.BaseID = &sn.ID
	err = mds.ImportVolume(newVol)
	if err != nil {
		return nil, err
	}

	err = updateStorageUsage(mds, s, vsid)
	return sn, err
}

// createVolume creates a volume based on the given blob id
func createVolume(mds metastore.Client, s datalayer.Storage, vsid volumeset.ID, sid *snapshot.ID, blobid blob.ID, name string) (*volume.Volume, error) {
	vid, mntPath, err := s.CreateVolume(vsid, blobid, datalayer.AutoMount)
	if err != nil {
		return nil, err
	}

	// Updates meta data
	vol := &volume.Volume{
		ID:           vid,
		VolSetID:     vsid,
		BaseID:       sid,
		MntPath:      mntPath,
		Attrs:        attrs.Attrs{},
		CreationTime: time.Now(),
		Name:         name,
	}
	err = mds.ImportVolume(vol)
	if err != nil {
		return nil, err
	}

	return vol, nil
}

// CreateEmptyVolume ...
func CreateEmptyVolume(mds metastore.Client, s datalayer.Storage, vsid volumeset.ID, name string) (*volume.Volume, error) {
	blobid, err := s.EmptyBlobID(vsid)
	if err != nil {
		return nil, err
	}
	return createVolume(mds, s, vsid, nil, blobid, name)
}

// CreateVolumeByBranch ...
func CreateVolumeByBranch(mds metastore.Client, s datalayer.Storage, vsid volumeset.ID,
	branch string, name string) (*volume.Volume, error) {
	sn, err := mds.GetTip(vsid, branch)
	if err != nil {
		return nil, err
	}

	blobid, err := metastore.GetBlobID(mds, sn.ID)
	if err != nil {
		return nil, err
	}
	return createVolume(mds, s, vsid, &sn.ID, blobid, name)
}

// CreateVolumeFromSnapshot ...
func CreateVolumeFromSnapshot(mds metastore.Client, s datalayer.Storage, sid snapshot.ID,
	name string) (*volume.Volume, error) {
	sn, err := metastore.GetSnapshot(mds, sid)
	if err != nil {
		return nil, err
	}

	return createVolume(mds, s, sn.VolSetID, &sid, sn.BlobID, name)
}

// DeleteVolume ...
func DeleteVolume(mds metastore.Client, s datalayer.Storage, vid volume.ID) error {
	vs, err := mds.GetVolume(vid)
	if err != nil {
		return err
	}

	// Remove from MDS first
	err = mds.DeleteVolume(vid)
	if err != nil {
		return err
	}

	// Remove from storage
	// TODO: block ZFS's error can't delete volume set(used by other snapshots)
	err = s.DestroyVolume(vs.VolSetID, vid)
	if err != nil {
		return err
	}

	return updateStorageUsage(mds, s, vs.VolSetID)
}

// DeleteBlob ...
func DeleteBlob(mds metastore.Client, s datalayer.Storage, snapid snapshot.ID) error {
	snap, err := metastore.GetSnapshot(mds, snapid)
	if err != nil {
		return err
	}

	if snap.BlobID.Equals(blob.NilID()) {
		// Don't have blob, nothing to do
		return nil
	}

	blobid := snap.BlobID
	snap.BlobID = blob.NilID()
	snap.Size = 0
	_, err = mds.UpdateSnapshot(snap, nil)
	if err != nil {
		return err
	}

	// TODO: Needs a transaction log in case a crash happens here
	err = s.DestroySnapshot(blobid)
	if err != nil {
		return err
	}

	return updateStorageUsage(mds, s, snap.VolSetID)
}

// DeleteVolumeSet ...
func DeleteVolumeSet(mds metastore.Client, s datalayer.Storage, vsid volumeset.ID) error {
	// Remove meta data
	err := mds.DeleteVolumeSet(vsid)
	if err != nil {
		return err
	}

	// TODO: Needs a transaction log in case a crash happens here
	return s.DestroyVolumeSet(vsid)
}

// UploadBlobDiff ...
func UploadBlobDiff(mds metastore.Store, sender BlobUploader, vsid volumeset.ID, base *snapshot.ID, target snapshot.ID,
	token string, dspuburl string) error {
	var baseBlobID blob.ID
	if base != nil {
		var err error
		baseBlobID, err = metastore.GetBlobID(mds, *base)
		if err != nil {
			return err
		}
	} else {
		baseBlobID = blob.NilID()
	}
	targetBlobID, err := metastore.GetBlobID(mds, target)
	if err != nil {
		return err
	}

	return sender.UploadBlobDiff(vsid, baseBlobID, targetBlobID, token, dspuburl)
}

// DownloadBlobDiff is the orchestrator of blob download. It finds the base blob is, download the diff and updates
// the newly downloaded blob's id in meta data store.
func DownloadBlobDiff(
	mds metastore.Client,
	receiver BlobDownloader,
	vsid volumeset.ID,
	base *snapshot.ID,
	target snapshot.ID,
	token string,
	dspuburl string,
) error {
	var (
		baseBlobID = blob.NilID()
		err        error
		snap       *snapshot.Snapshot
	)

	if base != nil && !base.IsNilID() {
		snap, err = metastore.GetSnapshot(mds, *base)
		if err != nil {
			return err
		}
		baseBlobID = snap.BlobID
	}

	targetBlobID, size, err := receiver.DownloadBlobDiff(
		vsid,
		target,
		baseBlobID,
		token,
		dspuburl,
	)
	if err != nil {
		return err
	}

	err = metastore.SetBlobIDAndSize(mds, target, targetBlobID, size)
	if err != nil {
		return err
	}

	return mds.SetVolumeSetSize(vsid, size)
}

// DeleteBranch deletes a branch identified by a branch's tip snapshot ID.
// This operation is only allowed on the client side and only on unsynced tip. This function doesn't check this and it
// is the caller's responsibility to verify that the tip is not synced.
// The branch is deleted from tip to the point where a split happens or all the way to the root.
// Either all of the snapshots are deleted or none is deleted.
// Blobs associated with the snapshots are also removed from the data storage layer.
func DeleteBranch(mds metastore.Client, s datalayer.Storage, vsid volumeset.ID, tip *branch.Branch) error {
	// if tip has pending volume set, can't delete it, needs to return the user a nice message
	numVolumes, err := mds.NumVolumes(tip.Tip.ID)
	if err != nil {
		return err
	}

	if numVolumes > 0 {
		return errors.New("Branch tip has pending volume, please delete the volume first")
	}

	var (
		snap        *snapshot.Snapshot
		tobeDeleted []*snapshot.Snapshot
	)

	for snap = tip.Tip; snap != nil; {
		numVolumes, err := mds.NumVolumes(snap.ID)
		if err != nil {
			return err
		}

		if numVolumes > 0 {
			break
		}

		numChildren, err := mds.NumChildren(snap.ID)
		if err != nil {
			return err
		}

		if numChildren > 1 {
			break
		}

		tobeDeleted = append(tobeDeleted, snap)

		if snap.ParentID != nil {
			snap, err = metastore.GetSnapshot(mds, *snap.ParentID)
			if err != nil {
				// NOTE The parent here could have been deleted if it belonged
				// to another branch and the current child may have become an
				// orphan
				_, ok := err.(*metastore.ErrSnapshotNotFound)
				if !ok {
					return err
				}
			}
		} else {
			snap = nil
		}
	}

	err = mds.DeleteSnapshots(tobeDeleted, snap)
	if err != nil {
		return err
	}

	// Delete blobs
	for _, snap := range tobeDeleted {
		if !snap.BlobID.Equals(blob.NilID()) {
			s.DestroySnapshot(snap.BlobID)
		}
	}

	return updateStorageUsage(mds, s, vsid)
}
