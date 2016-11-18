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

// Package attrcmp provides a file attr diff method.
package attrcmp

import (
	"bytes"
	"os"
	"reflect"
	"strconv"
	"strings"
	"syscall"

	dlhash "github.com/ClusterHQ/fli/dl/hash"
	"github.com/ClusterHQ/fli/dl/record"
	"github.com/ClusterHQ/fli/errors"
	"github.com/pkg/xattr"
)

//Returns Xattrs of a file as a map
func getXattrs(path string) (map[string][]byte, error) {
	attrNames, err := xattr.Listxattr(path)
	if err != nil {
		return nil, err
	}

	m := make(map[string][]byte)

	//if no xattrs, we return empty map and no errors
	for _, attrName := range attrNames {
		//get the value of the xattr
		val, err := xattr.Getxattr(path, attrName)
		if err != nil {
			return nil, err

		}

		//assign xattr's value to its name in a map
		m[attrName] = val
	}

	return m, nil
}

type getXattrsFn func(path string) (map[string][]byte, error)

func diffXattrs(f1 string, f2 string, target string, getXattrs getXattrsFn, records chan<- record.Record,
	hf dlhash.Factory) error {
	var xattr1, xattr2 map[string][]byte
	var err error

	if f1 != "" {
		xattr1, err = getXattrs(f1)
		if err != nil {
			return err
		}
	}

	//f2 should never be ""
	xattr2, err = getXattrs(f2)
	if err != nil {
		return err
	}

	if reflect.DeepEqual(xattr1, xattr2) {
		return nil
	}

	// xattrs are not the same. we need to iterate over hashes.
	// Find the attributes that we need to drop. they will be present in attr1, but absent in attr2
	for attrname := range xattr1 {
		if _, ok := xattr2[attrname]; !ok {
			err = record.Send(record.NewRmXattr(target, attrname), records, hf)
			if err != nil {
				return err
			}
		}
	}

	// Iterate over xattr2 and for all the values that are either abscent from xattr1 or are different,
	// need to be added.
	for attrname, val2 := range xattr2 {
		if val1, ok := xattr1[attrname]; !ok || !bytes.Equal(val2, val1) {
			err = record.Send(record.NewSetXattr(target, attrname, val2), records, hf)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

//DiffXattrs diffs xattrs and if they are not the same,
// generates a series of setxattr() records that will
//produce the target xattrs. the syscalls are sent to records channel
//Caller is expected to close the records channel.
func DiffXattrs(f1 string, f2 string, target string, records chan<- record.Record, hf dlhash.Factory) error {
	return diffXattrs(f1, f2, target, getXattrs, records, hf)
}

//DiffAttrs diffs attrs (mode, atime, ctime) and if they are not the same, generates
//a setattr record that produces the target mode, atime, ctime.
// target argument is the relative path from the mount point of snapshot2.
func DiffAttrs(f1 string, f2 string, target string, records chan<- record.Record, hf dlhash.Factory) error {
	var lStat1, lStat2 syscall.Stat_t
	var newFile bool
	var err error

	if f2 == "" {
		return errors.New("Target file name is empty")
	}

	// File1 can be empty when we are pushing the first snapshot (no base snapshot to apply diffs to).
	if f1 != "" {
		newFile = false
		err = syscall.Lstat(f1, &lStat1)
		if err != nil {
			return err
		}
	} else {
		newFile = true
	}

	err = syscall.Lstat(f2, &lStat2)
	if err != nil {
		return err
	}

	if newFile || lStat1.Uid != lStat2.Uid || lStat1.Gid != lStat2.Gid {
		err = record.Send(record.NewChown(target, int(lStat2.Uid), int(lStat2.Gid)), records, hf)
		if err != nil {
			return err
		}
	}

	if newFile || lStat1.Mode != lStat2.Mode {
		err = record.Send(record.NewChmod(target, os.FileMode(lStat2.Mode)), records, hf)
		if err != nil {
			return err
		}
	}

	//set mtime on target to that of f2 regardless,
	//because if we check change in mtime, we still need to do 2 syscalls
	stat2, err := os.Stat(f2)
	if err != nil {
		return err
	}

	return record.Send(record.NewSetMtime(target, stat2.ModTime()), records, hf)
}

//GetStrXattr returns the extended attribute's value as a string
func GetStrXattr(path string, name string) (string, error) {
	xattr, err := xattr.Getxattr(path, record.XattrPrefix+name)
	if err != nil {
		return "", err
	}
	return string(xattr), nil
}

//GetUintXattr returns the extended attribute's value as an unsigned integer
func GetUintXattr(path string, name string) (uint, error) {
	var val uint64
	xattr, err := GetStrXattr(path, name)
	if err == nil {
		val, err = strconv.ParseUint(xattr, 0, 32)
	}
	return uint(val), err
}

func getUserGroupModeFromXattrs(path string) (string, string, uint, error) {
	var user, group string
	mode, err := GetUintXattr(path, "mode")
	if err == nil {
		user, err = GetStrXattr(path, "user")
	}
	if err == nil {
		group, err = GetStrXattr(path, "group")
	}
	if err != nil {
		return "", "", 0, err
	}
	return user, group, mode, nil
}

func getXattrsFromXattrs(path string) (map[string][]byte, error) {
	prefix := record.XattrPrefix + "xattr."
	var result map[string][]byte
	xattrs, err := xattr.Listxattr(path)
	if err != nil {
		return nil, err
	}
	for _, attr := range xattrs {
		if strings.HasPrefix(attr, prefix) {
			name := strings.TrimPrefix(attr, prefix)
			result[name], err = xattr.Getxattr(path, attr)
			if err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

// DiffAttrsInXattrs finds differences between file attributes that are stored as extended attributes.
func DiffAttrsInXattrs(f1 string, f2 string, target string, records chan<- record.Record, hf dlhash.Factory) error {
	var uid1, gid1 string
	var mode1 uint
	var err error
	if f1 != "" {
		uid1, gid1, mode1, err = getUserGroupModeFromXattrs(f1)
		if err != nil {
			return err
		}
	}

	uid2, gid2, mode2, err := getUserGroupModeFromXattrs(f2)
	if err != nil {
		return err
	}

	if uid1 != uid2 || gid1 != gid2 {
		err = record.Send(record.NewChownByNames(target, uid2, gid2), records, hf)
		if err != nil {
			return err
		}
	}

	if mode1 != mode2 {
		err = record.Send(record.NewChmod(target, os.FileMode(mode2)), records, hf)
		if err != nil {
			return err
		}
	}

	// set mtime on target to that of f2 regardless,
	// because if we check change in mtime, we still need to do 2 syscalls
	stat2, err := os.Stat(f2)
	if err != nil {
		return err
	}

	return record.Send(record.NewSetMtime(target, stat2.ModTime()), records, hf)
}

//DiffXattrsInXattrs finds differences between extended file attributes that are stored
//as extended attributes with prefixed names for safety reasons.
func DiffXattrsInXattrs(f1 string, f2 string, target string, records chan<- record.Record, hf dlhash.Factory) error {
	return diffXattrs(f1, f2, target, getXattrsFromXattrs, records, hf)
}
