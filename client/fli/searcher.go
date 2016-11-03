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

package fli

import (
	"path/filepath"
	"regexp"
	"strings"

	"github.com/ClusterHQ/fli/dp/metastore"
	"github.com/ClusterHQ/fli/errors"
	"github.com/ClusterHQ/fli/meta/branch"
	"github.com/ClusterHQ/fli/meta/snapshot"
	"github.com/ClusterHQ/fli/meta/volume"
	"github.com/ClusterHQ/fli/meta/volumeset"
	"github.com/gobwas/glob"
)

// isID returns a true or false if the string is UUID.
// Uses regular expression to validate the input string.
func isUUID(s string) (bool, error) {
	p := `^\w{8,8}-\w{4,4}-\w{4,4}-\w{4,4}-\w{12,12}$`
	isID, err := regexp.MatchString(p, s)
	if err != nil {
		return isID, errors.New(err)
	}

	isShrunkID, err := isShrunkUUID(s)
	if err != nil {
		return (isID || isShrunkID), err
	}

	return (isID || isShrunkID), nil
}

func isShrunkUUID(s string) (bool, error) {
	p := `^\w{8,8}-\w{4,4}$`
	ret, err := regexp.MatchString(p, s)
	if err != nil {
		return ret, errors.New(err)
	}

	return ret, nil
}

// FindVolumesets reports whether the search matches the VolumeSets in mds
// search syntax is
//	search:
//		c8980031-b0ba
//		e6296a4a-b481-406c-9d33-ae074c6df78b
//		/chq/volset/name
//		/chq/volset*
//		/chq/volset?
//		(empty string)
func FindVolumesets(mds metastore.Syncable, search string) ([]*volumeset.VolumeSet, error) {
	var (
		vsFound = []*volumeset.VolumeSet{}
	)

	check, err := isUUID(search)
	if err != nil {
		return vsFound, err
	}

	if !check {
		// Search string is not a UUID or Shrunk UUID
		vs, err := metastore.GetVolumeSets(mds, volumeset.Query{})
		if err != nil {
			return vsFound, errors.New(err)
		}

		var g glob.Glob
		switch {
		case search == "":
			g = glob.MustCompile("*")
		case search[0] == '/':
			g = glob.MustCompile(search + "*")
		default:
			g = glob.MustCompile("*" + search)
		}

		for _, v := range vs {
			fullname := filepath.Join(v.Prefix, v.Name)
			if g.Match(fullname) {
				vsFound = append(vsFound, v)
			}
		}
	} else {
		// Search is an id then simply search for that ID
		isShrunkID, err := isShrunkUUID(search)
		if err != nil {
			return vsFound, err
		}

		if isShrunkID {
			volsets, err := metastore.GetVolumeSets(mds, volumeset.Query{})
			if err != nil {
				return vsFound, errors.New(err)
			}

			for _, v := range volsets {
				if ShrinkUUIDs(v.ID.String()) == search {
					vsFound = append(vsFound, v)
				}
			}
		} else {
			vs, err := metastore.GetVolumeSet(mds, volumeset.NewID(search))
			if err != nil {
				_, ok := err.(*metastore.ErrVolumeSetNotFound)
				if !ok {
					return vsFound, errors.New(err)
				}
			} else {
				vsFound = append(vsFound, vs)
			}
		}

	}

	if len(vsFound) == 0 {
		return vsFound, &ErrVolSetNotFound{Name: search}
	}

	return vsFound, nil
}

// FindBranches reports whether the search matches the Branches in mds
// search syntax is
//	search:
//		{volumeset}:{branch}
//		{branch}
//	volumeset:
//		c8980031-b0ba
//		e6296a4a-b481-406c-9d33-ae074c6df78b
//		/chq/volset/name
//		/chq/volset*
//		/chq/volset?
//		(empty string)
//	branch:
//		br*
//		*
//		(empty string)
func FindBranches(mds metastore.Syncable, search string) ([]*branch.Branch, error) {
	var (
		brFound = []*branch.Branch{}
	)

	vsname := ""
	brname := ""

	if strings.Contains(search, ":") {
		splitSearch := strings.Split(search, ":")
		if len(splitSearch) != 2 {
			return brFound, &ErrInvalidSearch{search}
		}

		// The first part is VolSet and second part is branch
		vsname = splitSearch[0]
		brname = splitSearch[1]
	} else {
		vsname = "*"
		brname = search
	}

	// If branch name passed it empty then search for everything
	if brname == "" {
		brname = "*"
	}

	vs, err := FindVolumesets(mds, vsname)
	if err != nil {
		return brFound, err
	}

	var g glob.Glob
	g = glob.MustCompile(brname)
	for _, v := range vs {
		brs, err := metastore.GetBranches(mds, branch.Query{VolSetID: v.ID})
		if err != nil {
			return brFound, errors.New(err)
		}

		for _, br := range brs {
			if g.Match(br.Name) {
				brFound = append(brFound, br)
			}
		}
	}

	if len(brFound) == 0 {
		return brFound, &ErrBranchNotFound{Name: search}
	}

	return brFound, nil
}

// FindSnapshots reports whether the search matches the Snapshots in mds
// search syntax is
//	search:
//		{volumeset}:{snapshot}
//		{snapshot}
//	volumeset:
//		c8980031-b0ba
//		e6296a4a-b481-406c-9d33-ae074c6df78b
//		/chq/volset/name
//		/chq/volset*
//		/chq/volset?
//		(empty string)
//	snapshot:
//		c8980031-b0ba
//		e6296a4a-b481-406c-9d33-ae074c6df78b
//		snap*
//		*
//		(empty string)
func FindSnapshots(mds metastore.Syncable, search string) ([]*snapshot.Snapshot, error) {
	var (
		snapFound = []*snapshot.Snapshot{}
	)

	check, err := isUUID(search)
	if err != nil {
		return snapFound, err
	}

	if !check {
		vsname := ""
		snapname := ""

		if strings.Contains(search, ":") {
			splitSearch := strings.Split(search, ":")
			if len(splitSearch) != 2 {
				return snapFound, &ErrInvalidSearch{search}
			}

			// The first part is VolSet and second part is branch
			vsname = splitSearch[0]
			snapname = splitSearch[1]
		} else {
			vsname = "*"
			snapname = search
		}

		if snapname == "" {
			snapname = "*"
		}

		check, err := isUUID(snapname)
		if err != nil {
			return snapFound, err
		}

		vs, err := FindVolumesets(mds, vsname)
		if err != nil {
			return snapFound, err
		}

		if !check {
			var g glob.Glob
			g = glob.MustCompile(snapname)
			for _, v := range vs {
				// Find the snapshot name in every VolSet found
				snaps, err := metastore.GetSnapshots(mds, snapshot.Query{VolSetID: v.ID})
				if err != nil {
					return snapFound, errors.New(err)
				}

				for _, snap := range snaps {
					if g.Match(snap.Name) {
						snapFound = append(snapFound, snap)
					}
				}
			}
		} else {
			// Search is an id then simply search for that ID
			isShrunkID, err := isShrunkUUID(snapname)
			if err != nil {
				return snapFound, err
			}

			if isShrunkID {
				for _, v := range vs {
					// Find the snapshot name in every VolSet found
					snaps, err := metastore.GetSnapshots(mds, snapshot.Query{VolSetID: v.ID})
					if err != nil {
						return snapFound, errors.New(err)
					}

					for _, snap := range snaps {
						if ShrinkUUIDs(snap.ID.String()) == snapname {
							snapFound = append(snapFound, snap)
						}
					}
				}
			} else {
				snap, err := metastore.GetSnapshot(mds, snapshot.NewID(snapname))
				if err != nil {
					_, ok := err.(*metastore.ErrSnapshotNotFound)
					if !ok {
						return snapFound, errors.New(err)
					}
				} else {
					snapFound = append(snapFound, snap)
				}
			}
		}
	} else {
		// Search is an id then simply search for that ID
		isShrunkID, err := isShrunkUUID(search)
		if err != nil {
			return snapFound, err
		}

		if isShrunkID {
			snaps, err := metastore.GetSnapshots(mds, snapshot.Query{})
			if err != nil {
				return snapFound, errors.New(err)
			}

			for _, snap := range snaps {
				if ShrinkUUIDs(snap.ID.String()) == search {
					snapFound = append(snapFound, snap)
				}
			}
		} else {
			snap, err := metastore.GetSnapshot(mds, snapshot.NewID(search))
			if err != nil {
				_, ok := err.(*metastore.ErrSnapshotNotFound)
				if !ok {
					return snapFound, errors.New(err)
				}
			} else {
				snapFound = append(snapFound, snap)
			}
		}

	}

	if len(snapFound) == 0 {
		return snapFound, &ErrSnapshotNotFound{Name: search}
	}

	return snapFound, nil
}

// FindVolumes reports whether the search matches the Volume in mds
// search syntax is
//	search:
//		{volumeset}:{volume}
//		{volume}
//	volumeset:
//		c8980031-b0ba
//		e6296a4a-b481-406c-9d33-ae074c6df78b
//		/chq/volset/name
//		/chq/volset*
//		/chq/volset?
//		(empty string)
//	volume:
//		c8980031-b0ba
//		e6296a4a-b481-406c-9d33-ae074c6df78b
//		vol*
//		*
//		(empty string)
func FindVolumes(mds metastore.Client, search string) ([]*volume.Volume, error) {
	var (
		volFound = []*volume.Volume{}
	)

	check, err := isUUID(search)
	if err != nil {
		return volFound, err
	}

	if !check {
		vsname := ""
		volname := ""

		if strings.Contains(search, ":") {
			splitSearch := strings.Split(search, ":")
			if len(splitSearch) != 2 {
				return volFound, &ErrInvalidSearch{search}
			}

			// The first part is VolSet and second part is branch
			vsname = splitSearch[0]
			volname = splitSearch[1]
		} else {
			vsname = "*"
			volname = search
		}

		if volname == "" {
			volname = "*"
		}

		check, err := isUUID(volname)
		if err != nil {
			return volFound, err
		}

		vs, err := FindVolumesets(mds, vsname)
		if err != nil {
			return volFound, err
		}

		if !check {
			var g glob.Glob
			g = glob.MustCompile(volname)
			for _, v := range vs {
				// Find the volume name in every VolSet found
				vols, err := mds.GetVolumes(v.ID)
				if err != nil {
					return volFound, errors.New(err)
				}

				for _, vol := range vols {
					if g.Match(vol.Name) {
						volFound = append(volFound, vol)
					}
				}
			}
		} else {
			// Search is an id then simply search for that ID
			isShrunkID, err := isShrunkUUID(volname)
			if err != nil {
				return volFound, err
			}

			if isShrunkID {
				for _, v := range vs {
					// Find the volume name in every VolSet found
					vols, err := mds.GetVolumes(v.ID)
					if err != nil {
						// TODO: Handle error here and return fli error here
						return volFound, errors.New(err)
					}

					for _, vol := range vols {
						if ShrinkUUIDs(vol.ID.String()) == volname {
							volFound = append(volFound, vol)
						}
					}
				}
			} else {
				vol, err := mds.GetVolume(volume.NewID(volname))
				if err != nil {
					_, ok := err.(*metastore.ErrVolumeNotFound)
					if !ok {
						return volFound, errors.New(err)
					}
				} else {
					volFound = append(volFound, vol)
				}
			}
		}
	} else {
		// Search is an id then simply search for that ID
		isShrunkID, err := isShrunkUUID(search)
		if err != nil {
			return volFound, err
		}

		if isShrunkID {
			vols, err := metastore.GetAllVolumes(mds)
			if err != nil {
				return volFound, errors.New(err)
			}

			for _, vol := range vols {
				if ShrinkUUIDs(vol.ID.String()) == search {
					volFound = append(volFound, vol)
				}
			}
		} else {
			vol, err := mds.GetVolume(volume.NewID(search))
			if err != nil {
				_, ok := err.(*metastore.ErrVolumeNotFound)
				if !ok {
					return volFound, errors.New(err)
				}
			} else {
				volFound = append(volFound, vol)
			}
		}

	}

	if len(volFound) == 0 {
		return volFound, &ErrVolumeNotFound{Name: search}
	}

	return volFound, nil
}

// FindAll ...
func FindAll(mds metastore.Client, search string) ([]*snapshot.Snapshot, []*branch.Branch, []*volume.Volume, error) {
	brFound := []*branch.Branch{}
	snapFound := []*snapshot.Snapshot{}
	volFound := []*volume.Volume{}

	brFound, err := FindBranches(mds, search)
	if err != nil {
		_, isNotFound := err.(*ErrBranchNotFound)
		if !isNotFound {
			return snapFound, brFound, volFound, err
		}
	}

	snapFound, err = FindSnapshots(mds, search)
	if err != nil {
		_, isNotFound := err.(*ErrSnapshotNotFound)
		if !isNotFound {
			return snapFound, brFound, volFound, err
		}
	}

	volFound, err = FindVolumes(mds, search)
	if err != nil {
		_, isNotFound := err.(*ErrVolumeNotFound)
		if !isNotFound {
			return snapFound, brFound, volFound, err
		}
	}

	return snapFound, brFound, volFound, nil
}
