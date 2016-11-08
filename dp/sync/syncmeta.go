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
	meta "github.com/ClusterHQ/fli/dp/metastore"
	"github.com/ClusterHQ/fli/errors"
	"github.com/ClusterHQ/fli/meta/snapshot"
	"github.com/ClusterHQ/fli/meta/volumeset"
)

// MetaConflicts - list of conflicts for vs, snaps, branches.
// Used for reporting to the user.
type MetaConflicts struct {
	VsC []meta.VSMetaConflict
	SnC []meta.SnapMetaConflict
	BrC []meta.BranchMetaConflict
}

// UpdateTgtVSMeta updates meta data of a volumeset using the three way sync
func UpdateTgtVSMeta(s meta.MdsTriplet, vsid volumeset.ID) ([]meta.VSMetaConflict, error) {
	var (
		vsCur, vsInit *volumeset.VolumeSet
		err           error
		cnfl          []meta.VSMetaConflict
	)

	vsCur, err = meta.GetVolumeSet(s.Cur, vsid)
	if err != nil {
		return nil, err
	}

	if vsCur == nil {
		return nil, nil
	}

	if s.Init != nil {
		vsInit, err = meta.GetVolumeSet(s.Init, vsid)
		if err != nil {
			if _, ok := err.(*metastore.ErrVolumeSetNotFound); !ok {
				return nil, err
			}
		}
	}

	vsConfl, err := s.Tgt.UpdateVolumeSet(vsCur, vsInit)
	if err != nil {
		return nil, err
	}

	if !vsConfl.IsEmpty() {
		cnfl = append(cnfl, vsConfl)
		_, err := s.Cur.UpdateVolumeSet(vsConfl.Tgt, nil)
		if err != nil {
			return nil, err
		}

		_, err = s.Init.UpdateVolumeSet(vsConfl.Tgt, nil)
		if err != nil {
			return nil, err
		}
	}

	return cnfl, nil
}

// UpdateTgtSnapMeta upates the metadata of the snapshots common between source and target
func UpdateTgtSnapMeta(s meta.MdsTriplet, vsid volumeset.ID) ([]meta.SnapMetaConflict, error) {
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

	conflicts, err := s.Tgt.UpdateSnapshots(snapPairs)
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

// UpdateTgtBranchMeta updates branch meta data
func UpdateTgtBranchMeta(s meta.MdsTriplet, vsid volumeset.ID) ([]meta.BranchMetaConflict, error) {
	// TODO: to be implemented
	return []meta.BranchMetaConflict{}, nil
}

// CheckVSConflict ...
/**********************************************************************************************
*
*Conflicts for snapshots and volumeset metadata are resolved thusly:
*
* init == target       current == init      target == current       state              action
*
* true||false            true||false            true                tgt won't change     none
* true                   false                  false               update               use current
* false                  false                  false               conflict             use tgt, report conflict
* false                  true                   false               tgt data changed     use tgt
*                                                                   but not client's
*
* for the purpose of one-way sync no action is the same a 'use tgt'
************************************************************************************************/
func CheckVSConflict(vsTgt, vsCur, vsInit *volumeset.VolumeSet) metastore.ResolveStatus {
	if vsCur.MetaEqual(vsInit) {
		if vsCur.MetaEqual(vsTgt) {
			return meta.NoAction
		}
		return meta.UseTgtNoConflict
	}

	if vsTgt.MetaEqual(vsInit) {
		return meta.UseCurrent
	}

	if vsTgt.MetaEqual(vsCur) {
		return meta.UseTgtNoConflict
	}

	return meta.UseTgtConflict
}

// CheckSnapConflict ...
func CheckSnapConflict(snapTgt, snapCur, snapInit *snapshot.Snapshot) metastore.ResolveStatus {
	if snapCur.Equals(snapInit) {
		if snapCur.Equals(snapTgt) {
			return meta.NoAction
		}
		return meta.UseTgtNoConflict
	}

	if snapTgt.Equals(snapInit) {
		return meta.UseCurrent
	}

	if snapTgt.Equals(snapCur) {
		return meta.UseTgtNoConflict
	}

	return meta.UseTgtConflict
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
