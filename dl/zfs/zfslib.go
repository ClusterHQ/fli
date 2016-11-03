// +build libzfs_core

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

/*
#cgo pkg-config: libzfs_core
#cgo LDFLAGS: -lzfs_core -lnvpair -luutil
#include <libzfs_core.h>
#include <libnvpair.h>
*/
import "C"

import (
	"fmt"
	"log"
	"os/exec"
	"path/filepath"
	"unsafe"

	"github.com/ClusterHQ/fli/dl/datalayer"
)

// Init the libzfs_core. This should be called before using any
// libzfs_core operations.
func initialize() {
	log.Printf("Initiating Lib ZFS core.")
	C.libzfs_core_init()
}

// Finish with the libzfs_core. This should be called after the system
// is done using the libzfs_core.
func finish() {
	log.Printf("Clean up Lib ZFS core.")
	C.libzfs_core_fini()
}

// Clone from a snapshot. Wrapper for the libzfs_clone.
// TODO: the properties arg are ignored for now, which should be reintroduced.
func clone(dest string, src string) error {
	props, err := nvlistAlloc(C.NV_UNIQUE_NAME, 0)
	if err != nil {
		return err
	}

	defer nvlistFree(props)
	cdest, csrc := C.CString(dest), C.CString(src)
	defer C.free(unsafe.Pointer(cdest))
	defer C.free(unsafe.Pointer(csrc))
	ret := C.lzc_clone(cdest, csrc, props)
	if ret != 0 {
		return fmt.Errorf("Failed libzfs_core clone: %d", ret)
	}

	return nil
}

// Exists examine whether the given path to a snapshot exists. Wrapper to the
// lzc_exists.
func exists(path string) bool {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	// The return type from lzc_exists is boolean_t. Although
	// conceptually, it is equivalent of the golang bool type, the type is
	// not considered to be matching. Therefore, we need to do the clumpsy
	// way of comparing it to the B_TURE value to convert the returned
	// value to golang bool type.
	return (C.lzc_exists(cpath) == C.B_TRUE)
}

// Snapshot takes snapshots. Wrapper for lzc_snapshot.
func snapshot(snapshotNames []string) error {
	snapshots, err := nvlistAlloc(C.NV_UNIQUE_NAME, 0)
	if err != nil {
		return err
	}

	defer nvlistFree(snapshots)
	for _, ssName := range snapshotNames {
		err := nvlistAddBoolean(snapshots, ssName)
		if err != nil {
			return err
		}
	}

	// TODO: For now, we let the properties to be empty.
	props, err := nvlistAlloc(C.NV_UNIQUE_NAME, 0)
	if err != nil {
		return err
	}

	defer nvlistFree(props)
	var errList *C.nvlist_t
	ret := C.lzc_snapshot(snapshots, props, &errList)
	if ret != 0 {
		ret2 := processErrorList(errList)
		if ret2 != nil {
			return ret2
		}
		return fmt.Errorf("Failed libzfs_core snapshot: %d", ret)
	}
	return nil
}

// DestroySnapshot destroies the snapshots passed in. Wrapper for lzc_destroy_snaps
func destroySnapshot(snapshotNames []string, deferDelete bool) error {
	snapshots, err := nvlistAlloc(C.NV_UNIQUE_NAME, 0)
	if err != nil {
		return err
	}

	defer nvlistFree(snapshots)
	for _, ssName := range snapshotNames {
		err := nvlistAddBoolean(snapshots, ssName)
		if err != nil {
			return err
		}
	}

	var errList *C.nvlist_t
	var cdeferDelete C.boolean_t
	// This is not exactly clean, but since the input needs to be a
	// C.boolean_t, there is no easy translation between golang bool
	// type into the C type.
	if deferDelete {
		cdeferDelete = C.B_TRUE
	} else {
		cdeferDelete = C.B_FALSE
	}

	ret := C.lzc_destroy_snaps(snapshots, cdeferDelete, &errList)
	if ret != 0 {
		ret2 := processErrorList(errList)
		if ret2 != nil {
			return ret2
		}
		return fmt.Errorf("Failed libzfs_core snapshot: %d", ret)
	}
	return nil
}

// Rollback is the wrapper for lzc_rollback
func rollback(name string) error {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	// For now, we only need to roll back to the most recent snapshot.
	ret := C.lzc_rollback(cname, nil, 0)
	if ret != 0 {
		return fmt.Errorf("Failed to rollback'%s': %d", name, ret)
	}

	return nil
}

// Destroy is the wrapper for lzc_destroy_one
func destroy(name string) error {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	// The only allowed property is the defer, but we leave it empty for
	// now
	props, err := nvlistAlloc(C.NV_UNIQUE_NAME, 0)
	if err != nil {
		return err
	}

	defer nvlistFree(props)
	ret := C.lzc_destroy_one(cname, props)
	if ret != 0 {
		return fmt.Errorf("Failed to destroy '%s': %d", name, ret)
	}

	return nil
}

// List is the wrapper for lzc_list_iter
func list(zpool string) ([]string, error) {
	// TODO: Not Implemented
	return nil, fmt.Errorf("Not Implemented")
}

// Process the error list returned from the libzfs_core functions in the types
// of nvlist_t. The keys are the snapshot names that failed and the values are
// the errno corresponding to each failed snapshot.
func processErrorList(errList *C.nvlist_t) error {
	if nvlistEmpty(errList) {
		return nil
	}

	defer nvlistFree(errList)
	var elem *C.nvpair_t
	var errno C.int32_t
	for {
		elem = C.nvlist_next_nvpair(errList, elem)
		if elem == nil {
			break
		}

		s := C.nvpair_name(elem)
		C.nvpair_value_int32(elem, &errno)
		log.Printf("Failed Snapshot '%s':%d\n", C.GoString(s), int(errno))
	}

	return fmt.Errorf("ZFS core Lib returned error")
}

// NvlistAlloc is a wrapper for nvlist_alloc.
func nvlistAlloc(nvflag int, kmflag int) (*C.nvlist_t, error) {
	var cnvlist *C.nvlist_t
	ret := C.nvlist_alloc(&cnvlist, C.uint_t(nvflag), C.int(kmflag))
	if ret != 0 {
		return nil, fmt.Errorf("Failed to allocation nvlist")
	}

	return cnvlist, nil
}

// NvlistFree is a wrapper for nvlist_free.
func nvlistFree(cnvlist *C.nvlist_t) {
	C.nvlist_free(cnvlist)
}

// NvlistAddBoolean is a wrapper for nvlist_add_boolean.
func nvlistAddBoolean(cnvlist *C.nvlist_t, name string) error {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	errno := C.nvlist_add_boolean(cnvlist, cname)
	if errno != 0 {
		return fmt.Errorf("Failed to add boolean: %d", errno)
	}

	return nil
}

// NvlistEmpty is a wrapper for nvlist_empty.
func nvlistEmpty(cnvlist *C.nvlist_t) bool {
	return (C.nvlist_empty(cnvlist) == C.B_TRUE)
}

func createFileSystem(path string) error {
	props, err := nvlistAlloc(C.NV_UNIQUE_NAME, 0)
	if err != nil {
		return err
	}
	defer nvlistFree(props)

	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	ret := C.lzc_create(cpath, C.LZC_DATSET_TYPE_ZFS, props)
	if ret != 0 {
		return fmt.Errorf("Failed libzfs_core create: %d", ret)
	}
	return nil
}

func destroyFileSystem(path string) error {
	// TODO: complete this code
	return fmt.Errorf("Not Implemented")
}

func getSnapshotSpace(snap string) (datalayer.SnapshotSpace, error) {
	// TODO: complete this code
	return datalayer.SnapshotSpace{}, fmt.Errorf("Not Implemented")
}

func getFSAndDescendantsSpace(fs string) (datalayer.DiskSpace, error) {
	// TODO: complete this code
	return datalayer.DiskSpace{}, fmt.Errorf("Not Implemented")
}

func validate(zpool string) error {
	err := exec.Command("modprobe", "zfs").Run()
	if err != nil {
		return &ErrZfsNotFound{}
	}

	if !exists(filepath.Join([]string{"", zpool})) {
		return &ErrZpoolNotFound{Zpool: zpool}
	}

	return nil
}
