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
	"sort"

	"github.com/ClusterHQ/go/dp/metastore"
	"github.com/ClusterHQ/go/meta/branch"
	"github.com/ClusterHQ/go/meta/snapshot"
	"github.com/ClusterHQ/go/meta/volumeset"
)

type (
	// SnapshotIterator is an interface that allows simpler iteration over some collection of snapshots.
	SnapshotIterator interface {
		Next() (*snapshot.Snapshot, error)
	}

	// StopIteration is the error returned by SnapshotIterator.Next when all snapshots have been seen.
	StopIteration struct {
		error
	}

	snapshotSet map[snapshot.ID]bool

	snapshotStack struct {
		stack []*snapshot.Snapshot
	}

	snapshotIterator struct {
		mds       metastore.Store
		vsid      volumeset.ID
		branches  *snapshotStack
		seen      snapshotSet
		snapshots *snapshotStack
	}

	// SnapshotPredicate is a function which makes a true/false decision about an
	// individual snapshot - for example, whether to push its blob or not.
	SnapshotPredicate func(snapshot *snapshot.Snapshot) bool

	// FilterIterator is a SnapshotIterator which loads snapshots from another
	// SnapshotIterator but only passes through values which satisfy a predicate.
	FilterIterator struct {
		Snapshots SnapshotIterator
		Predicate SnapshotPredicate
	}

	// LazySnapshotLoader is a SnapshotIterator which only loads snapshots by ID
	// when a Next() call demands them.
	//
	// Note: IDs is used as a stack and so snapshots are produced in reverse order.
	LazySnapshotLoader struct {
		mds         metastore.Store
		vsid        volumeset.ID
		snapshotIDs []snapshot.ID
	}

	branchIterator struct {
		mds      metastore.Store
		vsid     volumeset.ID
		position *snapshot.Snapshot
	}
)

var (
	_ SnapshotIterator = &snapshotIterator{}
	_ SnapshotIterator = &branchIterator{}
	_ SnapshotIterator = &LazySnapshotLoader{}
	_ SnapshotIterator = &FilterIterator{}
)

// IsStopIteration is the preferred way to detect the StopIteration error case.
func IsStopIteration(err error) bool {
	_, ok := err.(*StopIteration)
	return ok
}

// ToSlice returns a slice of snapshots containing all of the snapshots
// produced by a SnapshotIterator, in the order they are produced.
func ToSlice(i SnapshotIterator) ([]*snapshot.Snapshot, error) {
	var result []*snapshot.Snapshot
	for {
		value, err := i.Next()
		if err != nil {
			if IsStopIteration(err) {
				break
			}
			return nil, err
		}
		result = append(result, value)
	}
	return result, nil
}

// Next returns the next oldest reachable snapshot by following the
// Snapshot.ParentID pointer.
func (i *branchIterator) Next() (*snapshot.Snapshot, error) {
	if i.position == nil {
		return nil, &StopIteration{}
	}

	result := i.position
	if result.ParentID == nil {
		i.position = nil
	} else {
		next, err := metastore.GetSnapshot(i.mds, *result.ParentID)
		if err != nil {
			return nil, err
		}
		i.position = next
	}
	return result, nil
}

func (i *branchIterator) VolumeSetID() volumeset.ID {
	return i.vsid
}

// Next returns the next reachable snapshots by inspecting the stack for the current branch or by inspecting the next
// branch.
func (i *snapshotIterator) Next() (*snapshot.Snapshot, error) {
	for {
		result, ok := i.snapshots.pop()
		if ok {
			return result, nil
		}
		err := i.nextBranch()
		if err != nil {
			return nil, err
		}
	}
}

func (i *snapshotIterator) nextBranch() error {
	branch, ok := i.branches.peek()
	if !ok {
		return &StopIteration{}
	}
	iterator, err := NewBranchSnapshotIterator(i.mds, i.vsid, branch)
	if err != nil {
		return err
	}
	for {
		sn, err := iterator.Next()
		if err != nil {
			if IsStopIteration(err) {
				break
			}
			return err
		}
		if i.seen.has(sn.ID) {
			break
		}
		i.snapshots.push(sn)
		i.seen.add(sn.ID)
	}
	i.branches.pop()
	return nil
}

func (s snapshotSet) has(id snapshot.ID) bool {
	_, ok := s[id]
	return ok
}

func (s snapshotSet) add(id snapshot.ID) {
	s[id] = true
}

func (s *snapshotStack) pop() (*snapshot.Snapshot, bool) {
	value, ok := s.peek()
	if ok {
		s.stack = s.stack[:len(s.stack)-1]
	}
	return value, ok
}

func (s *snapshotStack) push(value *snapshot.Snapshot) {
	s.stack = append(s.stack, value)
}

func (s *snapshotStack) peek() (*snapshot.Snapshot, bool) {
	if 0 < len(s.stack) {
		return s.stack[len(s.stack)-1], true
	}
	return nil, false
}

// Next loads the next snapshot by ID and returns it.
func (i *LazySnapshotLoader) Next() (*snapshot.Snapshot, error) {
	if 0 < len(i.snapshotIDs) {
		next := i.snapshotIDs[len(i.snapshotIDs)-1]
		i.snapshotIDs = i.snapshotIDs[:len(i.snapshotIDs)-1]
		return metastore.GetSnapshot(i.mds, next)
	}
	return nil, &StopIteration{}
}

// VolumeSetID ...
func (i *LazySnapshotLoader) VolumeSetID() volumeset.ID {
	return i.vsid
}

// Next loads values from the wrapped iterator until one satisfies the
// predicate, then returns that one.
func (i *FilterIterator) Next() (*snapshot.Snapshot, error) {
	for {
		value, err := i.Snapshots.Next()
		if err != nil {
			return nil, err
		}
		if i.Predicate(value) {
			return value, nil
		}
		log.Printf("Snapshot %s rejected by filter, discarding", value.ID)
	}
}

type (
	sortableBranches []*branch.Branch
)

func (b sortableBranches) Len() int {
	return len(b)
}

func (b sortableBranches) Less(i, j int) bool {
	return b[i].Name < b[j].Name
}

func (b sortableBranches) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

// NewSnapshotIterator returns an iterator over all of the snapshots in a volumeset, starting at the root and
// proceeding depth-first out to the branch tips.  Snapshots which are reachable in the history of more than branch tip
// are only returned by the iterator once. Branches are visited in alphabetic order.
func NewSnapshotIterator(mds metastore.Store, vsid volumeset.ID) (SnapshotIterator, error) {
	branches, err := metastore.GetBranches(mds, branch.Query{VolSetID: vsid})
	if err != nil {
		return nil, err
	}

	sort.Sort(sort.Reverse(sortableBranches(branches)))

	var tips []*snapshot.Snapshot
	for _, b := range branches {
		tips = append(tips, b.Tip)
	}

	iterator := &snapshotIterator{
		mds:       mds,
		vsid:      vsid,
		branches:  &snapshotStack{stack: tips},
		seen:      make(snapshotSet),
		snapshots: &snapshotStack{},
	}
	return iterator, nil
}

// NewBranchSnapshotIterator returns an iterator over all of the snapshots which are in the history of a specified
// branch, starting at the tip and proceeding to the root.
func NewBranchSnapshotIterator(mds metastore.Store, vsid volumeset.ID, tip *snapshot.Snapshot) (SnapshotIterator, error) {
	// verify tip is a valid snapshot
	_, err := metastore.GetSnapshot(mds, tip.ID)
	if err != nil {
		return nil, err
	}

	return &branchIterator{mds: mds, vsid: vsid, position: tip}, nil
}

// NewLazyLoadSnapshotIterator returns an iterator over all of the snapshots which are in the history of a specified
// branch, starting at the tip and proceeding to the root.
func NewLazyLoadSnapshotIterator(mds metastore.Store, vsid volumeset.ID, snapshotIDs []snapshot.ID) SnapshotIterator {
	return &LazySnapshotLoader{mds: mds, vsid: vsid, snapshotIDs: snapshotIDs}
}

// NewFilterSnapshotIterator returns an iterator over all of the snapshots which
// are in the history of a specified branch, starting at the tip and
// proceeding to the root.
func NewFilterSnapshotIterator(snapshots SnapshotIterator, predicate SnapshotPredicate) SnapshotIterator {
	return &FilterIterator{Snapshots: snapshots, Predicate: predicate}
}
