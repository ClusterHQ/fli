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

package metastore

import (
	"strings"
	"time"

	"github.com/ClusterHQ/fli/errors"
	"github.com/ClusterHQ/fli/meta/attrs"
	"github.com/ClusterHQ/fli/meta/blob"
	"github.com/ClusterHQ/fli/meta/branch"
	"github.com/ClusterHQ/fli/meta/bush"
	"github.com/ClusterHQ/fli/meta/snapshot"
	"github.com/ClusterHQ/fli/meta/volume"
	"github.com/ClusterHQ/fli/meta/volumeset"
)

type (
	// SyncMode is a type for volume set sync mode
	SyncMode bool

	// SnapshotPair holds a pair of snapshots. It is a helper struct for updating a batch of snapshots.
	SnapshotPair struct {
		// Cur is the current and latest version of the snapshot
		Cur *snapshot.Snapshot
		// Init is the version that Cur is based on
		Init *snapshot.Snapshot
	}

	// MetadataStorage is a storage system for volumesets and a small amount of additional arbitrary (string)
	// key/value data.

	// Meta data operation interface groups
	// Meta data storage interfaces that are used by different part of the system. For example, data plane
	// server never creates volume meta data, so it can have a MDS that doesn't support working meta
	// create/delete, etc. Another example is restful transfer MDS doesn't use GetKVs()/SetKVs(), so it
	// doesn't have to implement the KV interface.

	// Syncable supports all volumeset related methods and can be used for sync meta data between two
	// meta data storages. This is used by restful MDS to transfer meta from one MDS to another.
	Syncable interface {
		// ImportVolumeSet creates a new volumeset.
		ImportVolumeSet(vs *volumeset.VolumeSet) error

		// GetVolumeSets ..
		GetVolumeSets(q volumeset.Query) ([]*volumeset.VolumeSet, error)

		// GetTip returns the tip of the named branch, or an error if the branch doesn't exist.
		GetTip(vsid volumeset.ID, branch string) (*snapshot.Snapshot, error)

		// GetBranches returns the names of all the branches in the volumeset.
		GetBranches(q branch.Query) ([]*branch.Branch, error)

		// ForkBranch creates a new branch with the given snapshots, branch name can be empty.
		// The new branch is forked off the first snapshot's parent; parent can be nil which means this is a
		// brand new branch.
		// A new random branch id is generated.
		// Assumes the given snapshots belong to the same lineage(snapshots[i] is the parent of snapshots[i-1]).
		ForkBranch(branchName string, snapshots ...*snapshot.Snapshot) error

		// ExtendBranch extends an existing branch with the given snapshots.
		ExtendBranch(snapshots ...*snapshot.Snapshot) error

		// ImportBranch is similar to ForkBranch() but with a given branch id.
		ImportBranch(branchID branch.ID, branchName string, snapshots ...*snapshot.Snapshot) error

		// UpdateVolumeSet updates a volume set's meta data based on the given set of
		// volume sets. Current is what the caller's current desired value, initial is the caller's
		// had before the change to current.
		// Based on the two volume set(or one, initial can be empty), target decides what to do
		// based on the three way sync protocol, details in postgres or sqlite3 implementation.
		UpdateVolumeSet(vsCur, vsInit *volumeset.VolumeSet) (VSMetaConflict, error)

		// PullVolumeSet is almost identical to UpdateVolumeSet except it doesn't actully update target.
		PullVolumeSet(vsCur, vsInit *volumeset.VolumeSet) (VSMetaConflict, error)

		// UpdateSnapshots updates an array of snapshots similar to UpdateVolumeSets
		UpdateSnapshots([]*SnapshotPair) ([]SnapMetaConflict, error)

		// PullSnapshots is almost identical to UpdateSnapshots except it doesn't actully update target.
		PullSnapshots([]*SnapshotPair) ([]SnapMetaConflict, error)

		// GetSnapshots ...
		GetSnapshots(q snapshot.Query) ([]*snapshot.Snapshot, error)

		// GetSnapshotIDs returns IDs of all snapshots in a volume set.
		// Returns empty [] when there is no snapshot in the volume set.
		GetSnapshotIDs(vsid volumeset.ID) ([]snapshot.ID, error)
	}

	// Store is the basic MDS who supports all interfaces but client side of things (like volume).
	// It can be used by dataplane server.
	Store interface {
		Syncable

		// SetVolumeSetSize updates a volume set's size
		// Note: Didn't want to overload UpdateVolumeSet() because want this be explicitly called.
		SetVolumeSetSize(vsid volumeset.ID, size uint64) error

		// UpdateSnapshot updates one snapshot
		UpdateSnapshot(snapCur, snapInit *snapshot.Snapshot) (SnapMetaConflict, error)

		// DeleteVolumeSet irrevocably discards the data associated with the
		// given unique identifier
		DeleteVolumeSet(vsid volumeset.ID) error

		// RenameBranch changes the name of an existing branch.
		RenameBranch(vsid volumeset.ID, oldName, newName string) error

		// ImportBush creates a new bush
		ImportBush(bush *bush.Bush) error

		// GetBush retrieves a bush by any snapshot id
		GetBush(snapid snapshot.ID) (*bush.Bush, error)

		// DeleteBush deletes a bush
		DeleteBush(root snapshot.ID) error

		// GetBlobIDs looks up a blob previously associated with a snapshot.  If there
		// is no such association, a nil blob identifier is returned.
		GetBlobIDs(snapids []snapshot.ID) (map[snapshot.ID]blob.ID, error)

		// SetBlobIDAndSize sets a snapshot's blob id and disk space used by the blob
		SetBlobIDAndSize(snapshot.ID, blob.ID, uint64) error
	}

	// Client supports volume, and it can be used a client like dpcli.
	Client interface {
		Store

		// ImportVolume ...
		ImportVolume(v *volume.Volume) error

		// GetVolume ...
		GetVolume(vid volume.ID) (*volume.Volume, error)

		// DeleteVolume ...
		DeleteVolume(vid volume.ID) error

		// GetVolumes returns all volumes that belongs to the given volumeset ordered by the
		// volume's ID in ascending order
		GetVolumes(vsid volumeset.ID) ([]*volume.Volume, error)

		// UpdateVolume ...
		UpdateVolume(v *volume.Volume) error

		// NumVolumes returns the number of volumes created from the given snapshot
		NumVolumes(snapid snapshot.ID) (int, error)

		// NumChildren returns number of snapshots created from the given snapshot
		NumChildren(snapid snapshot.ID) (int, error)

		// DeleteSnapshots deletes a series of snapshots
		DeleteSnapshots(snaps []*snapshot.Snapshot, tip *snapshot.Snapshot) error
	}

	// Server supports disk space usage, likely used by a volume hub server.
	Server interface {
		Store

		// DiskSpaceUsage returns disk space used by all given volume sets
		DiskSpaceUsage(vsids []volumeset.ID) (uint64, error)
	}
)

const (
	// AutoSync means branch tip grow/new while doing volumeset sync is done programmatically
	AutoSync SyncMode = true

	// ManualSync means branch tip grow/new while doing volumeset sync is done based on the user's input
	ManualSync SyncMode = false
)

// ValidateBranchName ...
func ValidateBranchName(name string) error {
	if strings.ContainsAny(name, ":*") {
		return errors.Errorf("illegal branch name '%s'", name)
	}
	return nil
}

// VolumeSet assigns a random ID, random owner/creator to the given volume set
// and calls meta data storage to create a new record
func VolumeSet(mds Syncable, n string, p string, a attrs.Attrs, d string, o, c string) (*volumeset.VolumeSet, error) {
	if a == nil {
		return nil, errors.New("Attributes for a volume set can be empty but can't be nil")
	}

	volset := volumeset.VolumeSet{
		ID:               volumeset.NewRandomID(),
		Attrs:            a.Copy(),
		CreationTime:     time.Now(),
		LastModifiedTime: time.Now(),
		Name:             n,
		Prefix:           p,
		Description:      d,
		Creator:          c,
		Owner:            o,
	}

	err := mds.ImportVolumeSet(&volset)
	if err != nil {
		return nil, err
	}

	// If db does any manipulations, this will reflect them.
	rv, err := GetVolumeSet(mds, volset.ID)
	if err != nil {
		return nil, err
	}

	return rv, nil
}

// GetVolumeSetBySnapID retrieves a volumeset id by a snapshot id(assume snapshot id is globally unique)
func GetVolumeSetBySnapID(mds Store, sid snapshot.ID) (volumeset.ID, error) {
	sn, err := GetSnapshot(mds, sid)
	if err == nil {
		return sn.VolSetID, nil
	}
	return volumeset.NilID(), &ErrVolumeSetNotFound{}
}

// SnapshotFork creates a new snapshot and makes it the tip of the indicated branch name.
func SnapshotFork(
	mds Store,
	vsid volumeset.ID,
	ssid snapshot.ID,
	branchName string,
	parentID *snapshot.ID,
	blobid blob.ID,
	a attrs.Attrs,
	name string,
	size uint64,
	desc string,
) (*snapshot.Snapshot, error) {
	if a == nil {
		return nil, errors.New("Attributes for a volume set can be empty but can't be nil")
	}

	if parentID != nil {
		// Make sure parent belongs to the given volume set
		parent, err := GetSnapshot(mds, *parentID)
		if err != nil {
			return nil, err
		}
		if !parent.VolSetID.Equals(vsid) {
			return nil, errors.New("Parent does not belong to the given volume set")
		}
	}

	sn := snapshot.Snapshot{
		VolSetID:         vsid,
		ID:               ssid,
		ParentID:         parentID,
		Attrs:            a.Copy(),
		CreationTime:     time.Now(),
		LastModifiedTime: time.Now(),
		Name:             name,
		BlobID:           blobid,
		PrevBlobID:       blobid,
		Size:             size,
		Description:      desc,
	}

	return &sn, mds.ForkBranch(branchName, &sn)
}

// SnapshotExtend creates a new snapshot and makes it the new tip of an existing branch.
func SnapshotExtend(
	mds Store,
	ssid snapshot.ID,
	parentID *snapshot.ID,
	blobid blob.ID,
	a attrs.Attrs,
	name string,
	size uint64,
	desc string,
) (*snapshot.Snapshot, error) {
	if a == nil {
		return nil, errors.New("Attributes for a volume set can be empty but can't be nil")
	}

	if parentID == nil {
		return nil, errors.New("Can't extend when parent is nil, use fork")
	}

	parent, err := GetSnapshot(mds, *parentID)
	if err != nil {
		return nil, err
	}
	if !parent.IsTip {
		return nil, errors.New("Can't extend when parent is not a tip, use fork")
	}

	sn := snapshot.Snapshot{
		VolSetID:         parent.VolSetID,
		ID:               ssid,
		ParentID:         parentID,
		Attrs:            a.Copy(),
		CreationTime:     time.Now(),
		LastModifiedTime: time.Now(),
		Name:             name,
		BlobID:           blobid,
		PrevBlobID:       blobid,
		Size:             size,
		Description:      desc,
	}

	return &sn, mds.ExtendBranch(&sn)
}

// FindNonMissingSnapshot walks the given snapshot list backwards, returns the first snapshot that the meta data
// store doesn't have, it can be used as the upload base.
//
// Assumptions:
// 1. baseCandidateIDs[] is a list of snapshots from oldest to newest
// 2. If a store has blob A, then it has all blobs older than A
func FindNonMissingSnapshot(
	mds Store,
	targetID snapshot.ID,
	baseCandidateIDs []snapshot.ID,
) (*snapshot.ID, blob.ID, error) {
	blobIDs, err := mds.GetBlobIDs(append(baseCandidateIDs, targetID))
	if err != nil {
		return nil, blob.NilID(), err
	}

	// Check if we already have that snapshot's blob; if we do, decline.
	blobID, ok := blobIDs[targetID]
	if !ok {
		return nil, blob.NilID(), &ErrSnapshotNotFound{}
	}
	if !blobID.IsNilID() {
		return nil, blob.NilID(), &ErrAlreadyHaveBlob{}
	}

	// Otherwise, find the newest base candidate for which we have a blob and select it.
	for i := len(baseCandidateIDs) - 1; i > -1; i-- {
		blobID, ok = blobIDs[baseCandidateIDs[i]]
		if !ok {
			return nil, blob.NilID(), &ErrSnapshotNotFound{}
		}
		if !blobID.IsNilID() {
			return &baseCandidateIDs[i], blobID, nil
		}
	}

	// Nothing matched, start from empty.
	return nil, blob.NilID(), nil
}

// RequestBlobDiff asks the MDS to share a blob diff which will produce
// the blob for a particular snapshot.  The MDS can select from a slice
// of base blobs on which to base the diff.  It can also decline to share the
// diff entirely.
func RequestBlobDiff(
	mds Store,
	targetID snapshot.ID,
	baseCandidateIDs []snapshot.ID,
) (*snapshot.ID, blob.ID, blob.ID, error) {
	blobIDs, err := mds.GetBlobIDs(append(baseCandidateIDs, targetID))
	if err != nil {
		return nil, blob.NilID(), blob.NilID(), err
	}

	// If we don't have that snapshot's blob, decline.
	targetBlobID, ok := blobIDs[targetID]
	if !ok {
		return nil, blob.NilID(), blob.NilID(), &ErrSnapshotNotFound{}
	}
	if targetBlobID.IsNilID() {
		return nil, blob.NilID(), blob.NilID(), errors.Errorf("Base blob for snapshot %v not found.", targetID)
	}

	// Otherwise, find the newest base candidate for which we have a blob and select it.
	for i := len(baseCandidateIDs) - 1; i > -1; i-- {
		baseBlobID, ok := blobIDs[baseCandidateIDs[i]]
		if !ok {
			return nil, blob.NilID(), blob.NilID(), &ErrSnapshotNotFound{}
		}
		if !baseBlobID.IsNilID() {
			return &baseCandidateIDs[i], baseBlobID, targetBlobID, nil
		}
	}

	// Nothing matched.  Offer a diff from the beginning of time.
	return nil, blob.NilID(), targetBlobID, nil
}

// GetVolumeSets ..
func GetVolumeSets(mds Syncable, q volumeset.Query) ([]*volumeset.VolumeSet, error) {
	volsets := []*volumeset.VolumeSet{}
	vss, err := mds.GetVolumeSets(q)
	if err != nil {
		return nil, err
	}

	for _, vs := range vss {
		if q.Matches(*vs) {
			volsets = append(volsets, vs)
		}
	}

	return volsets, nil
}

// GetAllVolumes ..
func GetAllVolumes(mds Client) ([]*volume.Volume, error) {
	vss, err := mds.GetVolumeSets(volumeset.Query{})
	if err != nil {
		return nil, err
	}

	var allVols []*volume.Volume
	for _, vs := range vss {
		vols, err := mds.GetVolumes(vs.ID)
		if err != nil {
			return nil, err
		}
		allVols = append(vols, allVols...)
	}

	return allVols, nil
}

// GetSnapshot is a wrapper for reading a snapshot by ID.
func GetSnapshot(mds Syncable, id snapshot.ID) (*snapshot.Snapshot, error) {
	q := snapshot.Query{ID: id}
	snaps, err := GetSnapshots(mds, q)
	if err != nil {
		return nil, err
	}

	if len(snaps) == 0 {
		return nil, &ErrSnapshotNotFound{}
	}

	return snaps[0], nil
}

// GetSnapshots gets all the mds's snapshots and filters them based on query.
func GetSnapshots(mds Syncable, q snapshot.Query) ([]*snapshot.Snapshot, error) {
	snapshots := []*snapshot.Snapshot{}
	snaps, err := mds.GetSnapshots(q)
	if err != nil {
		return nil, err
	}

	for _, s := range snaps {
		if q.Matches(*s) {
			snapshots = append(snapshots, s)
		}
	}

	return snapshots, nil
}

// GetBranch ..
func GetBranch(mds Syncable, vsid volumeset.ID, branchid branch.ID) (*branch.Branch, error) {
	q := branch.Query{ID: branchid, VolSetID: vsid}
	branches, err := GetBranches(mds, q)
	if err != nil {
		return nil, err
	}

	if len(branches) == 0 {
		return nil, &ErrBranchNotFound{}
	}

	return branches[0], nil
}

// GetBranches ..
func GetBranches(mds Syncable, q branch.Query) ([]*branch.Branch, error) {
	if err := q.Validate(); err != nil {
		return nil, err
	}

	// A valid branch query must have a volumeset ID.
	branches, err := mds.GetBranches(q)
	if err != nil {
		return nil, err
	}

	var allBranches = []*branch.Branch{}
	for _, branch := range branches {
		if q.Matches(branch) {
			allBranches = append(allBranches, branch)
		}
	}

	return allBranches, nil
}

// GetVolumeSet ..
func GetVolumeSet(mds Syncable, id volumeset.ID) (*volumeset.VolumeSet, error) {
	vss, err := GetVolumeSets(mds, volumeset.Query{ID: id})
	if err != nil {
		return nil, err
	}

	switch len(vss) {
	case 0:
		return nil, &ErrVolumeSetNotFound{}
	case 1:
		return vss[0], nil
	default:
		return nil, errors.Errorf("%d volume sets returned whereas 1 is expected", len(vss))
	}
}

// GetBushes all bushes of a volume set
// Note: This is slow, since it is not used often, ok to do it this way for now instead of going to DB
func GetBushes(mds Store, vsid volumeset.ID) ([]*bush.Bush, error) {
	branches, err := mds.GetBranches(branch.Query{VolSetID: vsid})
	if err != nil {
		return nil, err
	}

	bushes := make(map[snapshot.ID]*bush.Bush)
	for _, branch := range branches {
		b, err := mds.GetBush(branch.Tip.ID)
		if err != nil {
			return nil, err
		}
		bushes[b.Root] = b
	}

	var ret []*bush.Bush
	for _, b := range bushes {
		ret = append(ret, b)
	}

	return ret, nil
}

// GetSnapshotsByBranch finds all snapshots that belongs to a particular
// branch. The order will be from tip to the root.
func GetSnapshotsByBranch(mds Syncable, q branch.Query) ([]*snapshot.Snapshot, error) {
	branches, err := GetBranches(mds, q)
	if err != nil {
		return nil, err
	}

	if len(branches) == 0 {
		return nil, &ErrBranchNotFound{}
	}

	if len(branches) != 1 {
		return nil, errors.Errorf("Query obtained more than one branch: %+v", q)
	}

	tip := branches[0].Tip
	snaps := []*snapshot.Snapshot{tip}

	for cur := tip; cur.HasParent(); {
		cur, err = GetSnapshot(mds, *cur.ParentID)
		if err != nil {
			return nil, err
		}

		snaps = append(snaps, cur)
	}

	return snaps, nil
}

// UpdateVolumeSet calls MDS's update volume set, it might update some of the volume set's field before
// calling, for example, set volume set's last motified time to current time.
func UpdateVolumeSet(mds Syncable, vs *volumeset.VolumeSet) error {
	vs.LastModifiedTime = time.Now()
	_, err := mds.UpdateVolumeSet(vs, nil)
	return err
}

// UpdateSnapshot calls MDS's update snapshot, it might update some of the snapshott's field before
// calling, for example, set last motified time to current time.
func UpdateSnapshot(mds Store, snap *snapshot.Snapshot) error {
	snap.LastModifiedTime = time.Now()
	_, err := mds.UpdateSnapshot(snap, nil)
	return err
}

// RenameBranch wraps the MDS's rename name branch
func RenameBranch(mds Store, vsid volumeset.ID, oldName, newName string) error {
	return mds.RenameBranch(vsid, oldName, newName)
}

// UpdateVolume calls MDS client's update volume
func UpdateVolume(mds Client, v *volume.Volume) error {
	return mds.UpdateVolume(v)
}

// GetVolume calls MDS client's update volume
func GetVolume(mds Client, vid volume.ID) (*volume.Volume, error) {
	return mds.GetVolume(vid)
}

// GetVolumes calls MDS client's update volumes
func GetVolumes(mds Client, vsid volumeset.ID) ([]*volume.Volume, error) {
	return mds.GetVolumes(vsid)
}

// GetBlobID returns a snapshot's blob id
func GetBlobID(mds Store, snapid snapshot.ID) (blob.ID, error) {
	ids, err := mds.GetBlobIDs([]snapshot.ID{snapid})
	if err != nil {
		return blob.NilID(), err
	}
	return ids[snapid], err
}
