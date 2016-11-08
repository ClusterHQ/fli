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

// Package sync provides utilities necessary to synchronize metadata across dataplanes.
package sync

import (
	"fmt"
	"log"
	"sort"

	"github.com/ClusterHQ/fli/dp/metastore"
	"github.com/ClusterHQ/fli/errors"
	"github.com/ClusterHQ/fli/meta/blob"
	"github.com/ClusterHQ/fli/meta/branch"
	"github.com/ClusterHQ/fli/meta/snapshot"
	"github.com/ClusterHQ/fli/meta/volumeset"
)

// Error is an interface for synchronization errors that can be
// returned by the functions in this package.
type Error interface {
	error
	GetBranch() string
}

// HistoryDivergedError is an error reported when a branch can not be
// synchronized because its histories (snapshot chains) in repositories
// has diverged.
type HistoryDivergedError struct {
	branch    string
	sourceTip snapshot.ID
	targetTip snapshot.ID
}

func (e *HistoryDivergedError) Error() string {
	return fmt.Sprintf("history of branch %s has diverged: source tip %s, target tip %s",
		e.branch, e.sourceTip, e.targetTip)
}

// GetBranch returns a name of the failed branch.
func (e *HistoryDivergedError) GetBranch() string {
	return e.branch
}

// Internal errors.
var (
	errSnapshotNotInBranch = errors.New("branch does not contain expected snapshot")
)

// listSnapshots returns all snapshots in a range between (upto, tip] snapshots.
// ('tip' is included in the returned snapshot lists, 'upto' is not)
// Returned snapshots are ordered from oldest to newest
// NB: This functionality can be optimized by the following extensions to VolumeSet interface:
// - A method to check if a given snapshot belongs to a certain branch.
//   Alternatively, a method that lists all branches that contain the given snapshot, so that
//   we can check that the branch is in the list.
func listSnapshots(mds metastore.Syncable, tip *snapshot.Snapshot, upTo *snapshot.ID) ([]*snapshot.Snapshot, error) {
	var snapshots []*snapshot.Snapshot

	for sn := tip; upTo == nil || sn.ID != *upTo; {
		snapshots = append(snapshots, sn)
		if sn.ParentID == nil {
			if upTo != nil {
				// We were looking for the snapshot ID but it hasn't been found.
				return nil, errSnapshotNotInBranch
			}
			// The chain is exhausted.
			break
		}

		var err error
		sn, err = metastore.GetSnapshot(mds, *sn.ParentID)
		if err != nil {
			// XXX Must never happen?
			return nil, err
		}
	}

	// Reverse the slice, so that the first snapshot is a first element and the tip is the last one.
	for i, j := 0, len(snapshots)-1; i < j; i, j = i+1, j-1 {
		snapshots[i], snapshots[j] = snapshots[j], snapshots[i]
	}
	return snapshots, nil
}

// findSharedBranchpoint starts from the tip of a branch in the source volumeset, walks backwards of the branch, stops
// until it finds a snapshot that also exists in the target volumeset, or reaches the beginning of the branch.
func findSharedBranchpoint(mdsSrc metastore.Syncable, mdsTarget metastore.Syncable,
	sourceTip *snapshot.Snapshot) (*snapshot.Snapshot, error) {
	// Follow the snapshot ancestry chain.
	for id := &sourceTip.ID; id != nil; {
		sn, err := metastore.GetSnapshot(mdsSrc, *id)
		if err != nil {
			// XXX Must never happen?
			return nil, err
		}
		_, err = metastore.GetSnapshot(mdsTarget, sn.ID)
		if err != nil {
			if _, ok := err.(*metastore.ErrSnapshotNotFound); ok {
				id = sn.ParentID
				continue
			}

			return nil, err
		}

		return sn, nil
	}

	// None of the snapshots in the chain is known by the target.
	return nil, nil
}

// This algorithm implements a best effort, optimistic approach based on the fact
// that metadata changes should be append-only.
// We keep a static view of the source metadata ignoring any changes to it that
// happen concurrently with the synchronization.
// We get target information required for the synchronization and then try to perform
// the update using that information.  If the update fails we check if the failure
// happened because the target's metadata changed and, if so, refetch the metadata
// and try again.  This iteration must converge because at each step the target's
// state either gets closer to the (static) state of the sources or divereges from
// it resulting in a fatal error.
func pushBranch(mdsSrc metastore.Syncable, mdsTarget metastore.Syncable, vsid volumeset.ID, b branch.Branch) error {
	var (
		targetTipID *snapshot.ID
		targetTip   *snapshot.Snapshot
		newBranch   = false
	)
	sourceTip := b.Tip

	// Look for the branch on target in order to detect divergence.
	targetBranch, err := metastore.GetBranch(mdsTarget, vsid, b.ID)
	if err != nil {
		if _, ok := err.(*metastore.ErrBranchNotFound); !ok {
			return err
		}

		newBranch = true
		targetTip, err = findSharedBranchpoint(mdsSrc, mdsTarget, sourceTip)
		if err != nil {
			log.Printf("Failed to find shared branch point for volumeset %s branch %+v", vsid, b)
			return err
		}
	} else {
		targetTip = targetBranch.Tip
	}

	if targetTip != nil {
		targetTipID = &targetTip.ID
	}

	newSnaps, err := listSnapshots(mdsSrc, sourceTip, targetTipID)
	if err == nil {
		// Do nothing is no snapshots to push
		if len(newSnaps) == 0 {
			return nil
		}

		// Clear blob IDs and size before sending to the receiving MDS
		for _, snap := range newSnaps {
			snap.BlobID = blob.NilID()
			snap.PrevBlobID = blob.NilID()
			snap.Size = 0
		}

		if newBranch {
			log.Printf("Synced %d snapshot(s) to volumeset %s, new branch %s, (name = \"%s\")",
				len(newSnaps), vsid, b.ID.String(), b.Name)
			return mdsTarget.ImportBranch(b.ID, b.Name, newSnaps...)
		}

		log.Printf("Synced %d snapshot(s) to volumeset %s, existing branch %s, (name = \"%s\")",
			len(newSnaps), vsid, b.ID.String(), b.Name)
		return mdsTarget.ExtendBranch(newSnaps...)
	}

	// Check if we could not find the target's tip in the source's history or we've got some other error.
	if err != errSnapshotNotInBranch {
		log.Printf("Problem listing snapshots: %v", err)
		return err
	}
	// There are two possibilities now:
	// - the source is actually behind the target
	// - the source and the target have diverged
	_, err = listSnapshots(mdsTarget, targetTip, &sourceTip.ID)
	if err == nil {
		// The source's tip is in the target's history, so the source is just behind
		// ther target and there is nothing to do.
		return nil
	}

	if err != errSnapshotNotInBranch {
		log.Printf("Unhandled failure while listing snapshots on target: %v", err)
		return err
	}

	log.Print("Detected diverging histories while trying to push")
	return &HistoryDivergedError{
		branch:    b.Name,
		sourceTip: sourceTip.ID,
		targetTip: targetTip.ID,
	}
}

// Push all branches in a volumeset one by one.
func pushVolumeSet(mdsSrc metastore.Syncable, mdsTarget metastore.Syncable, vsid volumeset.ID) error {
	branches, err := metastore.GetBranches(mdsSrc, branch.Query{VolSetID: vsid})
	if err != nil {
		return err
	}

	sort.Sort(branch.SortableBranchesByTipDepth(branches))
	for _, b := range branches {
		for {
			err = pushBranch(mdsSrc, mdsTarget, vsid, *b)
			if err == nil {
				// This branch has been successfully pushed.
				break
			}

			if _, ok := err.(*metastore.ErrSnapshotImportMismatch); !ok {
				return err
			}

			// We've got the error because by the time we've built a list of snapshots
			// to push the target's tip has changed.  So, now retry the operation: get
			// the target's tip, build the list of snapshots and push it.
			log.Printf("Pushing volumeset %s branch %+v encountered snapshot mismatch", vsid, b)
		}
	}

	return nil
}

// PushMetadata is a function that ensures that the target storage contains a volumeset with the same ID as
// sourceVolumeSet and that all metadata in sourceVolumeSet is also present in the target volumeset.
// In other words, after PushMetadata() completes without an error the target metadata should be a subset of the
// source metadata (or possibly exactly the same).
//
// The synchronization is done in one direction.
//
// The metadata on either side is not locked, so the synchronization property should hold for the source metadata that
// was present before PushMetadata is called.  There are no guarantees for any metadata that is being added
// concurrently with PushMetadata.
//
// Some open questions:
//
// Q1. Should this function have any filters, so that only selected branches are synchronized?
//
// Q2. At present there is only basic handling of renamed branchs:
// a renamed branch is pushed as if it was a new branch with history that contains all of the history of the original
// branch.  So, after the push there will two branches on the target side, one with the old name and the other with the
// new one.  Should we be smarter about that?  Should there be a method to remove a branch?
// TODO: This is exported for test only, not export may be?
func PushMetadata(source metastore.Syncable, target metastore.Syncable, vsid volumeset.ID) error {
	_, err := metastore.GetVolumeSet(target, vsid)
	if err != nil {
		if _, ok := err.(*metastore.ErrVolumeSetNotFound); !ok {
			return err
		}

		srcvs, err := metastore.GetVolumeSet(source, vsid)
		if err != nil {
			return err
		}

		// Not exist, create
		err = target.ImportVolumeSet(srcvs)
		if err != nil {
			if _, ok := err.(*metastore.ErrVolumeSetAlreadyExists); !ok {
				return err
			}
		}
	}

	return pushVolumeSet(source, target, vsid)
}

// MetadataSync syncs the volumeset between the metadata stores.
// 1. Push new snapshots from current to target
// 2. Pull new snapshots from target to current
// 3. Pull new snapshots from current to initial(including locally newly created and pulled from target)
// 4. Sync meta data including both existing and new among all three stores. This is done after new snapshots
//    are synced first because during sync, target might change some of the meta fields, for example,
//    creator, owner, etc.
func MetadataSync(storeTgt, storeCur, storeInit metastore.Syncable, vsid volumeset.ID) (MetaConflicts, error) {
	log.Println("Syncing meta data of new objects ...")
	var (
		tgtNotFound bool
		curNotFound bool
	)

	_, errTgt := metastore.GetVolumeSet(storeTgt, vsid)
	if errTgt != nil {
		if _, ok := errTgt.(*metastore.ErrVolumeSetNotFound); !ok {
			return MetaConflicts{}, errTgt
		}

		tgtNotFound = true
	}

	_, errCur := metastore.GetVolumeSet(storeCur, vsid)
	if errCur != nil {
		if _, ok := errCur.(*metastore.ErrVolumeSetNotFound); !ok {
			return MetaConflicts{}, errCur
		}

		curNotFound = true
	}

	// Volumeset-id not found in neither mds, err
	if tgtNotFound && curNotFound {
		return MetaConflicts{}, errors.Errorf("VolumeSet (%s) not found anywhere.", vsid.String())
	}

	if errCur == nil {
		log.Println("Pushing meta data to remote ...")
		err := PushMetadata(storeCur, storeTgt, vsid)
		if err != nil {
			return MetaConflicts{}, err
		}
	}

	if errTgt == nil {
		_, err := metastore.GetVolumeSet(storeTgt, vsid)
		if err != nil {
			return MetaConflicts{}, err
		}

		log.Println("Pulling meta data from remote ...")
		err = PushMetadata(storeTgt, storeCur, vsid)
		if err != nil {
			return MetaConflicts{}, err
		}
	}

	// Bring all the new objects within vs from cur to init
	log.Println("Syncing meta data locally ...")
	err := PushMetadata(storeCur, storeInit, vsid)
	if err != nil {
		return MetaConflicts{}, err
	}

	log.Println("Syncing meta data of existing objects ...")
	s := metastore.MdsTriplet{
		Tgt:  storeTgt,
		Cur:  storeCur,
		Init: storeInit,
	}

	vsMetaConflicts, err := UpdateTgtVSMeta(s, vsid)
	if err != nil {
		return MetaConflicts{}, err
	}

	snapMetaConflicts, err := UpdateTgtSnapMeta(s, vsid)
	if err != nil {
		return MetaConflicts{}, err
	}

	branchMetaConflicts, err := UpdateTgtBranchMeta(s, vsid)
	if err != nil {
		return MetaConflicts{}, err
	}

	conflicts := MetaConflicts{
		VsC: vsMetaConflicts,
		SnC: snapMetaConflicts,
		BrC: branchMetaConflicts,
	}

	return conflicts, nil
}
