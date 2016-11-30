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

package sync

import (
	"log"

	"github.com/ClusterHQ/fli/dp/metastore"
	"github.com/ClusterHQ/fli/errors"
	"github.com/ClusterHQ/fli/meta/snapshot"
	"github.com/ClusterHQ/fli/meta/volumeset"
)

// MetaConflicts - list of conflicts for vs, snaps, branches.
// Used for reporting to the user.
type MetaConflicts struct {
	VsC []metastore.VSMetaConflict
	SnC []metastore.SnapMetaConflict
	BrC []metastore.BranchMetaConflict
}

func volSetMeta(
	s metastore.MdsTriplet,
	vsid volumeset.ID,
	pullOnly bool,
) ([]metastore.VSMetaConflict, error) {
	var (
		vsCur, vsInit *volumeset.VolumeSet
		err           error
		c             metastore.VSMetaConflict
	)

	vsCur, err = metastore.GetVolumeSet(s.Cur, vsid)
	if err != nil {
		return nil, err
	}

	if vsCur == nil {
		return nil, nil
	}

	if s.Init != nil {
		vsInit, err = metastore.GetVolumeSet(s.Init, vsid)
		if err != nil {
			if _, ok := err.(*metastore.ErrVolumeSetNotFound); !ok {
				return nil, err
			}
		}
	}

	if pullOnly {
		c, err = s.Tgt.PullVolumeSet(vsCur, vsInit)
	} else {
		c, err = s.Tgt.UpdateVolumeSet(vsCur, vsInit)
	}
	if err != nil {
		return nil, err
	}

	if c.IsEmpty() {
		if !vsCur.MetaEqual(vsInit) {
			_, err = s.Init.UpdateVolumeSet(vsCur, nil)
		}
		return nil, err
	}

	_, err = s.Cur.UpdateVolumeSet(c.Tgt, nil)
	if err != nil {
		return nil, err
	}

	_, err = s.Init.UpdateVolumeSet(c.Tgt, nil)
	if err != nil {
		return nil, err
	}

	return []metastore.VSMetaConflict{c}, nil
}

// snapshotMeta upates the metadata of the snapshots common between source and target
func snapshotMeta(
	s metastore.MdsTriplet,
	vsid volumeset.ID,
	pullOnly bool,
) ([]metastore.SnapMetaConflict, error) {
	// Note: There is different cases for meta sync:
	//       1. Source and target are quiet different in term of snapshots.
	//       2. Source and target largely have the same snapshots.
	//       In case #1, reading only the snapshot IDs are quiet fast and then only read the common snapshots.
	//       In case #2, reading all snapshots(not IDs) in one call might be faster because eventually all
	//       snapshots will be read anyway.
	//       Hard to say which approach is better, but definitely reading snapshot IDs first and only reading in the
	//       necessary snapshots approach uses less memory.
	//       To switch between the two approaches, use the correct method GetSnapshotIDs() or
	//       GetSnapshots(), and build the map accordingly.
	snapsCur, err := metastore.GetSnapshots(s.Cur, snapshot.Query{
		VolSetID: vsid,
	})
	if err != nil {
		return nil, err
	}

	var snapsInit []*snapshot.Snapshot
	if s.Init != nil {
		snapsInit, err = metastore.GetSnapshots(s.Init, snapshot.Query{
			VolSetID: vsid,
		})
		if err != nil {
			return nil, err
		}
	}

	snapidsTgt, err := s.Tgt.GetSnapshotIDs(vsid)
	if err != nil {
		return nil, err
	}

	// Build look up maps(which snapshot exists on a MDS) for quick look up later
	tgtSnapIDMap := make(map[snapshot.ID]int)
	for _, id := range snapidsTgt {
		tgtSnapIDMap[id] = 0
	}

	initSnapIDMap := make(map[snapshot.ID]int)
	for idx, snap := range snapsInit {
		initSnapIDMap[snap.ID] = idx
	}

	var snapPairs []*metastore.SnapshotPair
	for _, snap := range snapsCur {
		if _, ok := tgtSnapIDMap[snap.ID]; !ok {
			continue
		}

		var snapInit *snapshot.Snapshot
		if s.Init != nil {
			idx, ok := initSnapIDMap[snap.ID]
			if !ok {
				return nil, errors.Errorf("Faield to find a snapshot locally that is expected to exist.")
			}
			snapInit = snapsInit[idx]
		}

		snapPairs = append(
			snapPairs,
			&metastore.SnapshotPair{
				Cur:  snap,
				Init: snapInit,
			},
		)
	}

	if len(snapPairs) == 0 {
		return nil, nil
	}

	var conflicts []metastore.SnapMetaConflict
	if pullOnly {
		conflicts, err = s.Tgt.PullSnapshots(snapPairs)
	} else {
		conflicts, err = s.Tgt.UpdateSnapshots(snapPairs)
	}
	if err != nil {
		return nil, err
	}

	if s.Init == nil {
		return conflicts, nil
	}

	// Map for which snapshot on initial has been updated
	snapUpdated := make(map[snapshot.ID]int)

	// Collect all snapshots needs to be updated first and then go to DB once in one batch update
	var (
		updatePairCur  []*metastore.SnapshotPair
		updatePairInit []*metastore.SnapshotPair
	)

	for _, c := range conflicts {
		updatePairCur = append(
			updatePairCur,
			&metastore.SnapshotPair{
				Cur:  c.Tgt,
				Init: nil,
			},
		)
		updatePairInit = append(
			updatePairInit,
			&metastore.SnapshotPair{
				Cur:  c.Tgt,
				Init: nil,
			},
		)
		snapUpdated[c.Tgt.ID] = 0
	}

	// Update initial with current if it has not been updated by conflicts
	for _, pair := range snapPairs {
		if _, ok := snapUpdated[pair.Cur.ID]; ok {
			continue
		}

		if !pair.Cur.Equals(pair.Init) {
			updatePairInit = append(
				updatePairInit,
				&metastore.SnapshotPair{
					Cur:  pair.Cur,
					Init: nil,
				},
			)
		}
	}

	if len(updatePairCur) != 0 {
		_, err = s.Cur.UpdateSnapshots(updatePairCur)
		if err != nil {
			return nil, err
		}
	}

	if len(updatePairInit) != 0 {
		_, err = s.Init.UpdateSnapshots(updatePairInit)
		if err != nil {
			return nil, err
		}
	}

	return conflicts, nil
}

func branchMeta(
	s metastore.MdsTriplet,
	vsid volumeset.ID,
) ([]metastore.BranchMetaConflict, error) {
	// TODO: to be implemented
	return []metastore.BranchMetaConflict{}, nil
}

// CheckVSConflict checks and returns action code for sync based on the set of volume set objects
// Conflicts for snapshots and volumeset metadata are resolved as the following:
// init == target       current == init      target == current   state                action
// true||false          true||false          true                tgt won't change     none
// true                 false                false               update               use current
// false                false                false               conflict             use tgt, report conflict
// false                true                 false               tgt data changed     use tgt
//                                                               but not current's
// for the purpose of one-way sync no action is the same as 'use tgt'
func CheckVSConflict(vsTgt, vsCur, vsInit *volumeset.VolumeSet) metastore.ResolveStatus {
	if vsCur.MetaEqual(vsInit) {
		if vsCur.MetaEqual(vsTgt) {
			return metastore.NoAction
		}
		return metastore.UseTgtNoConflict
	}

	if vsTgt.MetaEqual(vsInit) {
		return metastore.UseCurrent
	}

	if vsTgt.MetaEqual(vsCur) {
		return metastore.UseTgtNoConflict
	}

	return metastore.UseTgtConflict
}

// CheckSnapConflict ...
func CheckSnapConflict(snapTgt, snapCur, snapInit *snapshot.Snapshot) metastore.ResolveStatus {
	if snapCur.Equals(snapInit) {
		if snapCur.Equals(snapTgt) {
			return metastore.NoAction
		}
		return metastore.UseTgtNoConflict
	}

	if snapTgt.Equals(snapInit) {
		return metastore.UseCurrent
	}

	if snapTgt.Equals(snapCur) {
		return metastore.UseTgtNoConflict
	}

	return metastore.UseTgtConflict
}

// Report prints out conflicts.
func (c *MetaConflicts) Report() {
	if c.HasConflicts() == false {
		log.Println("No conflicts were detected.")
		return
	}

	for _, v := range c.VsC {
		if v.Cur.MetaEqual(v.Init) || v.Cur.MetaEqual(v.Tgt) {
			continue
		}
		log.Println("Volume set conflict: ")
		log.Println("  Initial version:", v.Init)
		log.Println("  Current version (overwritten by target one): ", v.Cur)
		log.Println("  Target version:", v.Tgt)
	}

	for _, s := range c.SnC {
		if s.Cur.Equals(s.Init) || s.Cur.Equals(s.Tgt) {
			continue
		}
		log.Println("Snapshot conflict: ")
		log.Println("  Initial version:", s.Init)
		log.Println("  Current version (overwritten by target one): ", s.Cur)
		log.Println("  Target version:", s.Tgt)
	}

	// TODO: Branch conflicts
}

// HasConflicts returns true if there are conflicts.
// COnflicts if current == initial && target != current
func (c *MetaConflicts) HasConflicts() bool {
	for _, v := range c.VsC {
		if !v.Cur.MetaEqual(v.Init) && !v.Cur.MetaEqual(v.Tgt) {
			return true
		}
	}

	for _, s := range c.SnC {
		if !s.Cur.Equals(s.Init) && !s.Cur.Equals(s.Tgt) {
			return true
		}
	}

	// TODO: Branch conflicts
	return false
}
