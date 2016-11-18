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

	"github.com/ClusterHQ/fli/dp/dataplane"
	"github.com/ClusterHQ/fli/dp/metastore"
	"github.com/ClusterHQ/fli/meta/snapshot"
	"github.com/ClusterHQ/fli/meta/volumeset"
)

// BlobSpewer is an entity which can negotiate about which blob diffs it is willing to transmit.
type BlobSpewer interface {
	RequestBlobDiff(vsid volumeset.ID, targetID snapshot.ID, baseCandidateIDs []snapshot.ID) (*snapshot.ID, string, string, error)
}

// PullDataForAllSnapshots retrieves all blobs missing on target which source
// is willing to provide.
func PullDataForAllSnapshots(source BlobSpewer, mds metastore.Client, vsid volumeset.ID,
	receiver dataplane.BlobDownloader) error {
	snapshots, err := NewSnapshotIterator(mds, vsid)
	if err != nil {
		return err
	}
	return pullData(source, mds, receiver, snapshots)
}

// PullDataForCertainSnapshots retrieves the blobs associated with any of the
// specified snapshots which are missing on target and which source is willing
// to provide.
func PullDataForCertainSnapshots(source BlobSpewer, mds metastore.Client, receiver dataplane.BlobDownloader,
	pullSnapshots []snapshot.ID) error {
	if len(pullSnapshots) == 0 {
		return nil
	}
	// Figure out which volumeset this snapshot belongs to
	vsid, err := metastore.GetVolumeSetBySnapID(mds, pullSnapshots[0])
	if err != nil {
		return err
	}

	// Reverse the snapshot slice so LazySnapshotLoader can use it like a stack.
	snapshotStack := make([]snapshot.ID, 0, len(pullSnapshots))
	for i := len(pullSnapshots) - 1; i > -1; i-- {
		snapshotStack = append(snapshotStack, pullSnapshots[i])
	}
	return pullData(source, mds, receiver, NewLazyLoadSnapshotIterator(mds, vsid, snapshotStack))
}

// PullDataForQualifyingSnapshots downloads all blobs associated with snapshots
// allowed by a given predicate and which the source is willing to send.
func PullDataForQualifyingSnapshots(source BlobSpewer, mds metastore.Client, vsid volumeset.ID,
	receiver dataplane.BlobDownloader, shouldPull SnapshotPredicate) error {
	allSnapshots, err := NewSnapshotIterator(mds, vsid)
	if err != nil {
		return err
	}
	someSnapshots := NewFilterSnapshotIterator(allSnapshots, shouldPull)
	return pullData(source, mds, receiver, someSnapshots)
}

// pullData retrieves all blobs missing on target which source is willing to
// provide and which are associated with snapshots from the given snapshot
// iterator.
//
// Note: Only snapshots for which the metadata already exists on the target
// will have their blobs considered for pull.
func pullData(source BlobSpewer, mds metastore.Client, receiver dataplane.BlobDownloader,
	snapshots SnapshotIterator) error {
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

		if !blobID.IsNilID() {
			// Already have
			continue
		}

		blobs, err := blobsAlreadyHave(mds, sn)
		if err != nil {
			return err
		}

		baseID, token, dspuburl, err := source.RequestBlobDiff(sn.VolSetID, sn.ID, blobs)
		if err != nil {
			log.Printf("Snapshot %s request rejected by target: %v", sn.ID, err)
			continue
		}

		err = dataplane.DownloadBlobDiff(
			mds,
			receiver,
			sn.VolSetID,
			baseID,
			sn.ID,
			token,
			dspuburl,
		)
		if err != nil {
			return err
		}

		log.Printf("Downloaded snapshot %s\n", sn.ID.String())
	}
	return nil
}
