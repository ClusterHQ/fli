// +build !libzfs_core

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

package zfs

import (
	"log"
	"os/exec"
	"strconv"
	"strings"

	"github.com/ClusterHQ/fli/dl/datalayer"
)

// ZFS backend shell command interface implementation

// run executes one ZFS call using the given arguments synchronously and
// returns the output of the exec call and error if any
func run(args ...string) ([]byte, error) {
	// log.Printf("zfs.cli.run(%#v)", args)
	return exec.Command("zfs", args...).Output()
}

// list returns a slice of strings giving the names of all filesystems beneath
// the given pool.
func list(root string) ([]string, error) {
	o, err := run("list", "-H", "-o", "name", "-r", root)
	if err != nil {
		return nil, err
	}
	names := strings.Split(string(o), "\n")
	// Drop the final empty element that follows the final newline.
	return names[:len(names)-1], nil
}

func version() string {
	if o, err := exec.Command("modinfo", "zfs").Output(); err == nil {
		// Dumps a lot of info here, just split it at new line
		res := strings.Split(string(o), "\n")

		for _, r := range res {
			// ZFS version information starts with 'version:' so look for that
			if strings.HasPrefix(r, "version:") {
				// Get the version and return
				return strings.TrimSpace(strings.TrimLeft(r, "version:"))
			}
		}
	}

	// Didn't find the version info
	return ""
}

// exists returns true if the specified file system path exists
func exists(fs string) bool {
	_, err := run([]string{"list", fs}...)
	return err == nil
}

// destroy deletes the specified file system mount
func destroy(fs string) error {
	o, err := run([]string{"destroy", fs}...)
	if err != nil {
		exited, ok := err.(*exec.ExitError)
		if ok {
			log.Printf("destroy failed: %s", string(exited.Stderr))
		} else {
			log.Printf("destroy failed: %#v %#v", o, err)
		}
		return err
	}
	return nil
}

func destroySnapshot(fs []string, deferDel bool) error {
	return destroy(fs[0])
}

func clone(path string, b string) error {
	// Form volume ID (format: zppol/uuid)
	o, err := run([]string{"clone", b, path}...)
	if err != nil {
		exited, ok := err.(*exec.ExitError)
		if ok {
			log.Printf("zfs clone (create volume): %s", string(exited.Stderr))
		} else {
			log.Printf("zfs clone (create volume) failed: %#v %#v", o, err)
		}
	}

	return err
}

func rollback(fs string) error {
	// This is a hack for now.  Today, this is used to roll back in the
	// case where we need to destroy a volume.  Due to the limitation
	// of the zfs cli, we cannot just roll back to the most recent snapshot
	// for the give fs/VolumeSet if there is no snapshot present. Here, we
	// use destroy to accomplish this.
	// TODO: not returning the error on purpose for now. We don't have a good way to tell the difference between
	//       a failed call or failed because of ZFS's limitation that the working copy has dependant snapshots.
	//       Will take care of this once we have the lib version which hopefully can return the error.
	//       We can parse the command line return for now, but it is a hack.
	//       Also, blocking the return error message so user won't see it until we have a way to log it without
	//       user seen it.
	/*
		o, err := run([]string{"destroy", fs}...)
		if err != nil {
			exited, ok := err.(*exec.ExitError)
			if ok {
				log.Printf("rollback failed: %s", string(exited.Stderr))
			} else {
				log.Printf("rollback failed: %#v %#v", o, err)
			}
			return err
		}
	*/
	run([]string{"destroy", fs}...)
	return nil
}

func snapshot(path []string) error {
	o, err := run([]string{"snapshot", path[0]}...)
	if err != nil {
		log.Println(o)
	}

	return err
}

func initialize() {
	return
}

func finish() {
	return
}

func createFileSystem(path string) error {
	o, err := run([]string{"create", path}...)
	if err != nil {
		exited, ok := err.(*exec.ExitError)
		if ok {
			log.Printf("zfs create (create filesystem): %s", string(exited.Stderr))
		} else {
			log.Printf("zfs create (create filesystem) failed: %#v %#v", o, err)
		}
	}

	return nil
}

func destroyFileSystem(path string) error {
	o, err := run([]string{"destroy", "-rR", path}...)
	if err != nil {
		exited, ok := err.(*exec.ExitError)
		if ok {
			log.Printf("zfs destroy filesystem: %s", string(exited.Stderr))
		} else {
			log.Printf("zfs destroy filesystem failed: %#v %#v", o, err)
		}
	}

	return nil
}

// Get values of properties specified as a coma separated list
// for a given VolumeSet.
func getProperties(ds string, propList string) ([]string, error) {
	o, err := run([]string{"get", "-Hp", "-o", "value", propList, ds}...)
	if err != nil {
		log.Printf("zfs get failed: %#v %#v", o, err)
		return nil, err
	}
	// Note that the last array element is an empty string after the last new line.
	lines := strings.Split(string(o), "\n")
	return lines[0 : len(lines)-1], nil
}

func getSnapshotSpace(snap string) (datalayer.SnapshotSpace, error) {
	lines, err := getProperties(snap, "logicalreferenced,written")
	if err != nil {
		return datalayer.SnapshotSpace{}, err
	}
	var written uint64
	referenced, err := strconv.ParseUint(lines[0], 10, 64)
	if err == nil {
		written, err = strconv.ParseUint(lines[1], 10, 64)
	}
	return datalayer.SnapshotSpace{LogicalSize: referenced, DeltaFromPrevious: written}, err
}

func getFSAndDescendantsSpace(fs string) (datalayer.DiskSpace, error) {
	lines, err := getProperties(fs, "used,avail")
	if err != nil {
		return datalayer.DiskSpace{}, err
	}
	var avail uint64
	used, err := strconv.ParseUint(lines[0], 10, 64)
	if err == nil {
		avail, err = strconv.ParseUint(lines[1], 10, 64)
	}
	return datalayer.DiskSpace{Used: used, Available: avail}, err
}

func getUint64Property(ds, prop string) (uint64, error) {
	lines, err := getProperties(ds, prop)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(lines[0], 10, 64)
}

func validate(zpool string) error {
	err := exec.Command("modprobe", "zfs").Run()
	if err != nil {
		return &ErrZfsNotFound{}
	}

	err = exec.Command("which", "zfs").Run()
	if err != nil {
		return &ErrZfsUtilsNotFound{}
	}

	exists := exists(zpool)
	if !exists {
		return &ErrZpoolNotFound{Zpool: zpool}
	}

	return nil
}
