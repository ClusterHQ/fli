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
	"fmt"
	"log"

	"github.com/ClusterHQ/fli/dp/dataplane"
	"github.com/ClusterHQ/fli/dp/metastore"
	"github.com/ClusterHQ/fli/errors"
	"github.com/ClusterHQ/fli/meta/snapshot"
	"github.com/ClusterHQ/fli/meta/volumeset"
)

// BlobAccepter is an entity which can negotiate about which blob diffs it would like to receive.
type BlobAccepter interface {
	// OfferBlobDiff suggests to a peer that we might have some data they
	// want.  If the peer wants it, they can return a base snapshot ID to
	// use and a token which can be used to perform the upload.  Otherwise
	// they an decline with an empty token or an error.
	OfferBlobDiff(vsid volumeset.ID, targetID snapshot.ID, baseCandidateIDs []snapshot.ID) (*snapshot.ID, string, string, error)
}

// PushDataForAllSnapshots sends all blobs available in source which target is willing to take.
func PushDataForAllSnapshots(mds metastore.Store, vsid volumeset.ID, sender dataplane.BlobUploader, target BlobAccepter) error {
	snapshots, err := NewSnapshotIterator(mds, vsid)
	if err != nil {
		return err
	}
	return pushData(mds, sender, target, snapshots)
}

// PushDataForCertainSnapshots sends the blobs associated any of the specified
// snapshots which the target is willing to take.
//
// Note: The snapshots should be ordered from oldest to newest (wherever there
// is an ancestor/descendant relationship) to avoid unnecessary blob transfer.
func PushDataForCertainSnapshots(mds metastore.Store, sender dataplane.BlobUploader, target BlobAccepter, pushSnapshots []snapshot.ID) error {
	if len(pushSnapshots) == 0 {
		// Nothing to push
		return nil
	}
	// Figure out which volumeset this snapshot belongs to
	vsid, err := metastore.GetVolumeSetBySnapID(mds, pushSnapshots[0])
	if err != nil {
		return err
	}

	// Reverse the snapshot slice so LazySnapshotLoader can use it like a stack.
	snapshots := make([]snapshot.ID, 0, len(pushSnapshots))
	for i := len(pushSnapshots) - 1; i > -1; i-- {
		snapshots = append(snapshots, pushSnapshots[i])
	}
	return pushData(mds, sender, target, NewLazyLoadSnapshotIterator(mds, vsid, snapshots))
}

// PushDataForQualifyingSnapshots sends all blobs associated with snapshots
// allowed by a given predicate and which the target is willing to take.
func PushDataForQualifyingSnapshots(mds metastore.Store, vsid volumeset.ID, sender dataplane.BlobUploader, target BlobAccepter, shouldPush SnapshotPredicate) error {
	allSnapshots, err := NewSnapshotIterator(mds, vsid)
	if err != nil {
		return err
	}
	someSnapshots := NewFilterSnapshotIterator(allSnapshots, shouldPush)
	return pushData(mds, sender, target, someSnapshots)
}

// pushData sends all blobs associated with snapshots in the given iterator and
// which the target is willing to take.
func pushData(mds metastore.Store, sender dataplane.BlobUploader, target BlobAccepter, snapshots SnapshotIterator) error {
	for {
		sn, err := snapshots.Next()
		if err != nil {
			if IsStopIteration(err) {
				// All done
				break
			}
			return err
		}

		blobID, err := metastore.GetBlobID(mds, sn.ID)
		if err != nil {
			return err
		}
		if blobID.IsNilID() {
			// can't push if have no blob locally
			continue
		}

		blobs, err := blobsAlreadyHave(mds, sn)
		if err != nil {
			return err
		}

		baseID, token, dspuburl, err := target.OfferBlobDiff(sn.VolSetID, sn.ID, blobs)
		if err != nil {
			log.Printf("Snapshot %s offer rejected by target: %v", sn.ID, err)
			continue
		}

		err = dataplane.UploadBlobDiff(mds, sender, sn.VolSetID, baseID, sn.ID, token, dspuburl)
		if err != nil {
			return errors.Errorf("Upload snapshot %s failed: %v", sn.ID, err)
		}

		log.Printf("Uploaded snapshot %s\n", sn.ID.String())
	}
	return nil
}

// blobsAlreadyHave returns all snapshots the MDS already have
func blobsAlreadyHave(mds metastore.Store, s *snapshot.Snapshot) ([]snapshot.ID, error) {
	ancestors, err := ancestry(mds, s.ID, 20)
	if err != nil {
		return nil, err
	}

	blobful, err := worthOffering(mds, ancestors)
	if err != nil {
		return nil, err
	}

	return blobful, nil
}

// ancestry returns a slice containing the snapshot history leading up to the given snapshot.
func ancestry(mds metastore.Store, id snapshot.ID, limit int) ([]*snapshot.Snapshot, error) {
	var result []*snapshot.Snapshot
	for i := 0; i < limit; i++ {
		sn, err := metastore.GetSnapshot(mds, id)
		if err != nil {
			return nil, err
		}
		result = append(result, sn)

		if sn.ParentID == nil {
			break
		}

		id = *sn.ParentID
	}
	return result, nil
}

// Find the snapshots for which a blob exists (and so a blob diff could be
// constructed starting from them).
func worthOffering(mds metastore.Store, snapshots []*snapshot.Snapshot) ([]snapshot.ID, error) {
	var result []snapshot.ID
	for i := range snapshots {
		sn := snapshots[len(snapshots)-i-1]
		blobID, err := metastore.GetBlobID(mds, sn.ID)
		if err != nil {
			return nil, fmt.Errorf("Failed to get blob id of snapshot %v, error %v", sn.ID, err)
		}
		if !blobID.IsNilID() {
			// Has a blob, consider it
			result = append(result, sn.ID)
		}
	}
	return result, nil
}
