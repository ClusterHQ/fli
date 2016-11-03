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

package cli

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ClusterHQ/go/dl/datalayer"
	dlbin "github.com/ClusterHQ/go/dl/encdec/binary"

	"github.com/ClusterHQ/go/dp/dataplane"
	"github.com/ClusterHQ/go/dp/metastore"
	"github.com/ClusterHQ/go/dp/sync"

	"github.com/ClusterHQ/go/mdsimpls/restfulstorage"
	"github.com/ClusterHQ/go/mdsimpls/sqlite3storage"

	"github.com/ClusterHQ/go/meta/blob"
	"github.com/ClusterHQ/go/meta/branch"
	"github.com/ClusterHQ/go/meta/snapshot"
	"github.com/ClusterHQ/go/meta/volume"
	"github.com/ClusterHQ/go/meta/volumeset"

	"github.com/ClusterHQ/go/protocols"

	"github.com/ClusterHQ/go/securefilepath"

	"github.com/ClusterHQ/go/dl/blobdiffer"
	"github.com/ClusterHQ/go/dl/encdec"
	"github.com/ClusterHQ/go/dl/executor"
	"github.com/ClusterHQ/go/dl/filediffer/variableblk"
	dlhash "github.com/ClusterHQ/go/dl/hash"
	"github.com/ClusterHQ/go/dl/hash/adler32"
	"github.com/ClusterHQ/go/dl/zfs"
	"github.com/ClusterHQ/go/vh/cauthn"
)

//go:generate go run ../cmd/cligen/main.go -yaml cmd.yml -output cmds_gen.go -package cli

type (
	// handlerImpl implements the auto generated handlerIface
	handlerImpl struct {
		mdsCur  metastore.Client
		mdsInit metastore.Client
		stor    datalayer.Storage
		exec    executor.Executor
		jsCfg   *JSONFileConfig
		ed      encdec.Factory // TODO: hard code for now
		hf      dlhash.Factory // TODO: hard code for now
	}

	// JSONOp object that is marshalled and unmarshalled to generate json output
	JSONOp struct {
		Str string
		Tab [][]string
	}
)

var (
	// NOTE: fetched from auto generate by cligen
	_ handlerIface = &handlerImpl{}

	// need for sync & push/pull
	_ dataplane.BlobUploader   = &handlerImpl{}
	_ dataplane.BlobDownloader = &handlerImpl{}
)

// UploadBlobDiff ...
func (h *handlerImpl) UploadBlobDiff(vsid volumeset.ID, base blob.ID, targetBlobID blob.ID, t string,
	dspuburl string) error {
	return datalayer.UploadBlobDiff(h.stor, h.ed, h.hf, vsid, base, targetBlobID, t, dspuburl)
}

// DownloadBlobDiff ...
func (h *handlerImpl) DownloadBlobDiff(vsid volumeset.ID, base blob.ID, t string, dspuburl string) (blob.ID, uint64, error) {
	return datalayer.DownloadBlobDiff(h.stor, h.ed, vsid, base, t, h.exec, h.hf, dspuburl)
}

// Initialize dpcli sqlite store and re-create it if it already exists with force flag set
func (h handlerImpl) handlerDpcliInit(force bool, zpool string, args []string, jsonF bool) ([]byte, error) {
	var js = JSONOp{}

	if len(args) > 0 {
		return convert2Json(js, errors.New("Invalid arguments"))
	}

	js.Str = "Validating meta-data store path... "

	cfg, err := h.jsCfg.ReadConfig()
	if err != nil {
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}

	if cfg.ZPool != "" {
		if !force {
			js.Str += "zpool already set in configuration file, please use --force to force update this."
			return convert2Json(js, nil)
		}
	}
	cfg.ZPool = zpool

	if cfg.SQLiteDBCur == "" {
		// SQLLiteFile is not set in configuration file, update it
		cfg.SQLiteDBCur = filepath.Join(getHomeDir(), mdsFileCur)
	}

	if cfg.SQLiteDBInit == "" {
		// SQLLiteFile is not set in configuration file, update it
		cfg.SQLiteDBInit = filepath.Join(getHomeDir(), mdsFileInit)
	}

	// update all the above changes here
	if err := h.jsCfg.UpdateConfig(cfg); err != nil {
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}

	// TODO: refactor the code to have a function that opens the two dbs.
	// TODO: might have to change cli output to remove the mention of two dbs.
	pathCur, err := securefilepath.New(cfg.SQLiteDBCur)
	if err != nil {
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}

	pathInit, err := securefilepath.New(cfg.SQLiteDBInit)
	if err != nil {
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}

	js.Str += "[OK]\n"

	existsCur, err := pathCur.Exists()
	if err != nil {
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}

	existsInit, err := pathInit.Exists()
	if err != nil {
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}

	if (existsCur && !existsInit) || (!existsCur && existsInit) {
		js.Str += "Inconsistent state - only one of two databases exists. Removing it."
		if existsCur {
			if err := os.Remove(cfg.SQLiteDBCur); err != nil {
				return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
			}
			existsCur = false
		} else {
			if err := os.Remove(cfg.SQLiteDBInit); err != nil {
				return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
			}
			existsInit = false
		}
		js.Str += "[OK]\n"
	}

	if existsCur {
		// check if force flag is set before trying to re-create
		if force {
			js.Str += "Metadata store 1 already exists, forcing remove... "
			if err := os.Remove(cfg.SQLiteDBCur); err != nil {
				return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
			}
			existsCur = false
			js.Str += "[OK]\n"
		} else {
			err = fmt.Errorf("internal error: Cannot create meta-data store 1 at %v because a previous"+
				" meta-data store already exists.", pathCur.Path())
			return convert2Json(js, err)
		}
	}

	if existsInit {
		// check if force flag is set before trying to re-create
		if force {
			js.Str += "Metadata store 2 already exists, forcing remove... "
			if err := os.Remove(cfg.SQLiteDBInit); err != nil {
				return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
			}
			existsInit = false
			js.Str += "[OK]\n"
		} else {
			err = fmt.Errorf("internal error: Cannot create meta-data store 2 at %v because a previous"+
				" meta-data store already exists.", pathInit.Path())
			return convert2Json(js, err)
		}
	}

	js.Str += "Creating meta-data stores... "
	if _, err = sqlite3storage.Create(pathCur); err != nil {
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}

	if _, err = sqlite3storage.Create(pathInit); err != nil {
		os.Remove(cfg.SQLiteDBCur) //we either want two dbs or none
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}
	js.Str += "[OK]\n"

	return convert2Json(js, nil)
}

func (h handlerImpl) handlerCreateVolumeset(attributes string, desc string, args []string, jsonF bool) ([]byte, error) {
	var js = JSONOp{}

	if len(args) > 1 {
		return convert2Json(js, errors.New("Invalid arguments"))
	}

	vsname := ""
	if len(args) == 1 {
		vsname = args[0]
	}

	h.initialize()

	attr, err := convStr2Attr(attributes)
	if err != nil {
		return convert2Json(js, err)
	}

	js.Str = "Creating new volumeset... "

	var prefix string
	if strings.Compare(vsname, "") != 0 {
		if filepath.Dir(vsname) == "." || filepath.Dir(vsname) == "/" {
			prefix = ""
		} else {
			prefix = filepath.Dir(vsname)
		}

		vsname = filepath.Base(vsname)
	}

	vs, err := metastore.VolumeSet(h.mdsCur, vsname, prefix, attr, desc, "", "")
	if err != nil {
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}

	js.Str += "[OK]\n"
	js.Str += fmt.Sprintf("New Volumeset: %s\n", vs.ID.String())

	return convert2Json(js, nil)
}

func (h handlerImpl) handlerCreateVolume(volumesetF, branch, snapshotF, attributes string,
	args []string, jsonF bool) ([]byte, error) {
	var (
		js   = JSONOp{}
		err  error
		vol  *volume.Volume
		vsID volumeset.ID
	)

	if len(args) > 1 {
		return convert2Json(js, errors.New("Invalid arguments"))
	}

	vname := ""
	if len(args) == 1 {
		vname = args[0]
	}

	h.initialize()

	attr, err := convStr2Attr(attributes)
	if err != nil {
		return convert2Json(js, fmt.Errorf("%s - %s", err.Error(), attributes))
	}

	if snapshotF == "" {
		if volumesetF == "" {
			return convert2Json(js, errors.New("No volumeset identifier passed."))
		}

		vsFiltered, err := h.getMatchingVolumesets(volumesetF)
		if err != nil {
			return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
		}

		if len(vsFiltered) > 1 {
			// more than one match found
			js.Tab = volumesetTabOutput(vsFiltered)
			js.Str = "Found more than one matching volumeset.\n"

			return convert2Json(js, nil)
		} else if len(vsFiltered) == 0 {
			js.Str = "Volumeset (" + volumesetF + ") not found\n"
			return convert2Json(js, nil)
		}

		vsID = vsFiltered[0].ID

		if branch == "" {
			js.Str = "Creating empty volume..."
			vol, err = dataplane.CreateEmptyVolume(h.mdsCur, h.stor, vsID, vname)
			if err != nil {
				return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
			}
		} else {
			js.Str = "Creating volume off branch " + branch + "..."
			vol, err = dataplane.CreateVolumeByBranch(h.mdsCur,
				h.stor, vsID, branch, vname)
			if err != nil {
				return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
			}
		}
	} else {
		snapFiltered, err := h.getMatchingSnapshots(snapshotF)
		if len(snapFiltered) > 1 {
			// more than one match found
			js.Tab = snapshotTabOutput(snapFiltered)
			js.Str = "Found more than one matching snapshots.\n"
		} else if len(snapFiltered) == 0 {
			js.Str = "Snapshot (" + snapshotF + ") not found\n"

			return convert2Json(js, nil)
		}

		js.Str = "Creating volume off snapshot " + snapFiltered[0].ID.String() + "..."
		vsID = snapFiltered[0].VolSetID
		vol, err = dataplane.CreateVolumeFromSnapshot(h.mdsCur, h.stor, snapFiltered[0].ID, vname)
		if err != nil {
			return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
		}
	}

	vol.Attrs = attr
	if err := h.mdsCur.UpdateVolume(vol); err != nil {
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}

	js.Str += "[OK]\n"
	js.Str += fmt.Sprintf("Volumeset: %s\n", vsID.String())
	js.Str += fmt.Sprintf("Volume ID: %s\n", vol.ID.String())
	js.Str += fmt.Sprintf("Volume Path: %s\n", vol.MntPath.Path())

	return convert2Json(js, nil)
}

func (h handlerImpl) handlerCreateSnapshot(branch, volumeF, attributes string, desc string, args []string,
	jsonF bool) ([]byte, error) {
	var js = JSONOp{}

	if len(args) > 1 {
		return convert2Json(js, errors.New("Invalid arguments"))
	}

	snapname := ""
	if len(args) == 1 {
		snapname = args[0]
	}

	h.initialize()

	if volumeF == "" {
		return convert2Json(js, errors.New("Missing volume flag value."))
	}

	attr, err := convStr2Attr(attributes)
	if err != nil {
		return convert2Json(js, err)
	}

	volID := volume.NewID(volumeF)

	js.Str = fmt.Sprintf("Creating snapshot of volume (%s)... ", volID.String())
	snap, err := dataplane.Snapshot(h.mdsCur, h.stor, volID, branch, metastore.AutoSync, snapname, attr, desc)
	if err != nil {
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}

	js.Str += "[OK]\n"
	js.Str += fmt.Sprintf("New Snapshot ID: %s\n", snap.ID.String())

	return convert2Json(js, nil)
}

func (h handlerImpl) handlerShowVolumeset(args []string, jsonF bool) ([]byte, error) {
	var (
		js  = JSONOp{}
		vsl = []*volumeset.VolumeSet{}
		err error
	)

	h.initialize()

	if len(args) == 0 {
		vsl, err = metastore.GetVolumeSets(h.mdsCur, volumeset.Query{})
		if err != nil {
			return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
		}
	} else {
		for _, arg := range args {
			vs, err := h.getMatchingVolumesets(arg)
			if err != nil {
				return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
			}
			vsl = append(vsl, vs...)
		}
	}

	js.Tab = volumesetTabOutput(vsl)

	return convert2Json(js, nil)
}

func (h handlerImpl) handlerShowVolume(volumesetF string, args []string, jsonF bool) ([]byte, error) {
	var (
		parent string
		vols   []*volume.Volume
		err    error
		js     = JSONOp{}
	)

	if len(args) > 0 {
		return convert2Json(js, errors.New("Invalid arguments"))
	}

	h.initialize()

	if volumesetF == "" {
		vols, err = metastore.GetAllVolumes(h.mdsCur)
		if err != nil {
			return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
		}
	} else {
		vsFiltered, err := h.getMatchingVolumesets(volumesetF)
		if err != nil {
			return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
		}

		for _, vs := range vsFiltered {
			vls, err := h.mdsCur.GetVolumes(vs.ID)
			if err != nil {
				return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
			}

			vols = append(vols, vls...)
		}
	}

	js.Tab = append(js.Tab, []string{"NAME", "ID", "MOUNTED", "VOLUMESET ID", "PARENT", "CREATED", "ATTRIBUTES"})
	for _, vol := range vols {
		if vol.HasBase() {
			parent = vol.BaseID.String()
		} else {
			parent = "-"
		}

		js.Tab = append(js.Tab, []string{vol.Name, vol.ID.String(), vol.MntPath.Path(), vol.VolSetID.String(), parent,
			vol.CreationTime.Format(time.RFC822), convAttr2Str(vol.Attrs)})
	}

	return convert2Json(js, nil)
}

func (h handlerImpl) handlerShowSnapshot(volumesetF string, args []string, jsonF bool) ([]byte, error) {
	var js = JSONOp{}

	if len(args) > 0 {
		return convert2Json(js, errors.New("Invalid arguments"))
	}

	h.initialize()

	volset := []*volumeset.VolumeSet{}
	if volumesetF == "" {
		vsList, err := metastore.GetVolumeSets(h.mdsCur, volumeset.Query{})
		if err != nil {
			return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
		}
		volset = vsList
	} else {

		vsFiltered, err := h.getMatchingVolumesets(volumesetF)
		if err != nil {
			return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
		}

		if len(vsFiltered) > 1 {
			// more than one match found
			js.Tab = volumesetTabOutput(vsFiltered)
			js.Str = "Found more than one matching volumeset.\n"

			return convert2Json(js, nil)
		} else if len(vsFiltered) == 0 {
			js.Str = "Volumeset (" + volumesetF + ") not found\n"
			return convert2Json(js, nil)
		}

		volset = vsFiltered
	}

	js.Tab = append(js.Tab, []string{"BRANCH", "NAME", "ID", "SIZE", "ATTRIBUTES", "DESCRIPTION"})

	for _, vs := range volset {
		branches, err := metastore.GetBranches(h.mdsCur, branch.Query{VolSetID: vs.ID})
		if err != nil {
			return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
		}

		for _, b := range branches {
			var showBranch = true
			// Trace all snapshots from tip
			snap := b.Tip
			for {
				var bname = " "
				if showBranch {
					bname = b.Name
					showBranch = false
				}

				js.Tab = append(js.Tab, []string{bname, snap.Name, snap.ID.String(), strconv.FormatUint(snap.Size, 10) + "B", convAttr2Str(snap.Attrs), snap.Description})
				if snap.ParentID == nil {
					break
				}

				snap, err = metastore.GetSnapshot(h.mdsCur, *snap.ParentID)
				if err != nil {
					return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
				}
			}
		}
	}

	return convert2Json(js, nil)
}

func (h handlerImpl) handlerShowBranch(volumesetF string, args []string, jsonF bool) ([]byte, error) {
	var js = JSONOp{}

	h.initialize()

	if volumesetF == "" {
		return convert2Json(js, errors.New("Missing volumeset flag value."))
	}

	vsFiltered, err := h.getMatchingVolumesets(volumesetF)
	if err != nil {
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}

	if len(vsFiltered) > 1 {
		// more than one match found
		js.Tab = volumesetTabOutput(vsFiltered)
		js.Str = "Found more than one matching volumeset.\n"

		return convert2Json(js, nil)
	} else if len(vsFiltered) == 0 {
		js.Str = "Volumeset (" + volumesetF + ") not found\n"
		return convert2Json(js, nil)
	}

	vs := vsFiltered[0]

	js.Tab = append(js.Tab, []string{"BRANCH"})

	branches, err := metastore.GetBranches(h.mdsCur, branch.Query{VolSetID: vs.ID})
	if err != nil {
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}

	for _, b := range branches {
		// don't show no-named branches
		if b.Name == "" {
			continue
		}

		if len(args) > 0 {
			// get the regexpr to be listed
			expr := args[0]
			matched, err := regexp.MatchString(expr, b.Name)
			if err != nil {
				return convert2Json(js, err)
			}
			if !matched {
				continue
			}
		}

		js.Tab = append(js.Tab, []string{b.Name})
	}

	return convert2Json(js, nil)
}

func (h handlerImpl) handlerRemoveVolumeset(args []string, jsonF bool) ([]byte, error) {
	var js = JSONOp{}

	if len(args) == 0 {
		return convert2Json(js, errors.New("Missing Volumeset ID argument"))
	}

	h.initialize()

	var vsFiltered = []*volumeset.VolumeSet{}

	for _, arg := range args {
		vsl, err := h.getMatchingVolumesets(arg)
		if err != nil {
			return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
		}

		if len(vsl) > 0 {
			vsFiltered = append(vsFiltered, vsl...)
		}
	}

	for _, vs := range vsFiltered {
		js.Str = fmt.Sprintf("Removing volumeset(%s)...", vs.ID.String())
		if err := dataplane.DeleteVolumeSet(h.mdsCur, h.stor, vs.ID); err != nil {
			return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
		}

		js.Str += "[OK]\n"
	}

	return convert2Json(js, nil)
}

func (h handlerImpl) handlerRemoveVolume(args []string, jsonF bool) ([]byte, error) {
	var js = JSONOp{}

	if len(args) == 0 {
		return convert2Json(js, errors.New("Missing Volume ID argument"))
	}

	h.initialize()

	for _, vID := range args {
		js.Str = fmt.Sprintf("Removing volume(%s)...", vID)
		if err := dataplane.DeleteVolume(h.mdsCur, h.stor, volume.NewID(vID)); err != nil {
			return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
		}

		js.Str += "[OK]\n"
	}

	return convert2Json(js, nil)
}

func (h handlerImpl) handlerRemoveSnapshot(args []string, jsonF bool) ([]byte, error) {
	var js = JSONOp{}

	if len(args) == 0 {
		return convert2Json(js, errors.New("Missing Snapshot ID argument"))
	}

	h.initialize()

	for _, snap := range args {
		snapFiltered, err := h.getMatchingSnapshots(snap)
		if err != nil {
			return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
		}

		for _, s := range snapFiltered {
			js.Str = fmt.Sprintf("Removing snapshot(%s)...", s.ID.String())
			if err := dataplane.DeleteBlob(h.mdsCur, h.stor, s.ID); err != nil {
				return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
			}

			js.Str += "[OK]\n"
		}
	}

	return convert2Json(js, nil)
}

func (h handlerImpl) handlerRemoveBranch(volumesetF string, args []string, jsonF bool) ([]byte, error) {
	var js = JSONOp{}

	if len(args) == 0 {
		return convert2Json(js, errors.New("Missing branch  argument"))
	}

	if volumesetF == "" {
		return convert2Json(js, errors.New("Missing volumeset flag"))
	}

	h.initialize()

	vsFiltered, err := h.getMatchingVolumesets(volumesetF)
	if err != nil {
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}

	if len(vsFiltered) > 1 {
		// more than one match found
		js.Tab = volumesetTabOutput(vsFiltered)
		js.Str = "Found more than one matching volumeset.\n"

		return convert2Json(js, nil)
	} else if len(vsFiltered) == 0 {
		js.Str = "Volumeset (" + volumesetF + ") not found\n"

		return convert2Json(js, nil)
	}

	vsID := vsFiltered[0].ID
	for _, arg := range args {
		// for all the branches to be deleted
		bQuery := branch.Query{Name: arg, VolSetID: vsID}
		branches, err := metastore.GetBranches(h.mdsCur, bQuery)
		if err != nil {
			return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
		}

		if len(branches) != 1 {
			return convert2Json(js, fmt.Errorf("internal error: branch %s not found", arg))
		}

		js.Str = fmt.Sprintf("Removing branch(%s)...", branches[0].Name)
		if err := dataplane.DeleteBranch(h.mdsCur, h.stor, vsID, branches[0]); err != nil {
			return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
		}
		js.Str += "[OK]\n"

		break // move to matching the next argument

	}

	return convert2Json(js, nil)
}

func (h handlerImpl) handlerUpdateVolumeset(name string, attributes string, desc string, args []string, jsonF bool) ([]byte, error) {
	var js = JSONOp{}

	if len(args) == 0 {
		return convert2Json(js, fmt.Errorf("Missing Volumeset ID or name"))
	}

	h.initialize()

	vsFiltered, err := h.getMatchingVolumesets(args[0])
	if err != nil {
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}

	if len(vsFiltered) > 1 {
		// more than one match found
		js.Tab = volumesetTabOutput(vsFiltered)
		js.Str = "Found more than one matching volumeset.\n"

		return convert2Json(js, nil)
	} else if len(vsFiltered) == 0 {
		js.Str = "Volumeset (" + args[0] + ") not found\n"

		return convert2Json(js, nil)
	}

	vsID := vsFiltered[0].ID

	vsCur, err := metastore.GetVolumeSet(h.mdsCur, vsID)
	if err != nil {
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}

	// if we have extracted the name from the arguments
	if name != "" {
		var prefix string
		if filepath.Dir(name) == "." || filepath.Dir(name) == "/" {
			prefix = ""
		} else {
			prefix = filepath.Dir(name)
		}

		name = filepath.Base(name)

		vsCur.Name = name
		vsCur.Prefix = prefix
	}

	attr, err := convStr2Attr(attributes)
	if err != nil {
		return convert2Json(js, err)
	}
	vsCur.Attrs = updateAttributes(attr, vsCur.Attrs)
	vsCur.LastModifiedTime = time.Now()

	js.Str = "Update volumeset (" + vsID.String() + ")... "
	if _, err = h.mdsCur.UpdateVolumeSet(vsCur, nil); err != nil {
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}
	js.Str += "[OK]\n"

	return convert2Json(js, nil)
}

func (h handlerImpl) handlerUpdateVolume(attributes string, args []string, jsonF bool) ([]byte, error) {
	var js = JSONOp{}

	if len(args) != 1 {
		return convert2Json(js, errors.New("Missing Volume ID"))
	}

	h.initialize()

	vol, err := h.mdsCur.GetVolume(volume.NewID(args[0]))
	if err != nil {
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}

	attr, err := convStr2Attr(attributes)
	if err != nil {
		return convert2Json(js, err)
	}
	vol.Attrs = updateAttributes(attr, vol.Attrs)

	js.Str = "Update volume (" + vol.ID.String() + ")..."
	if err := h.mdsCur.UpdateVolume(vol); err != nil {
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}

	js.Str += "[OK]\n"

	return convert2Json(js, nil)
}

func (h handlerImpl) handlerUpdateSnapshot(name, attributes string, desc string, args []string, jsonF bool) ([]byte, error) {
	var js = JSONOp{}

	if len(args) != 1 {
		return convert2Json(js, errors.New("Missing Snapshot ID"))
	}

	h.initialize()

	snapFiltered, err := h.getMatchingSnapshots(args[0])
	if len(snapFiltered) > 1 {
		// more than one match found
		js.Tab = snapshotTabOutput(snapFiltered)
		js.Str = "Found more than one matching snapshots.\n"
	} else if len(snapFiltered) == 0 {
		js.Str = "Snapshot (" + args[0] + ") not found\n"

		return convert2Json(js, nil)
	}

	snap := snapFiltered[0]

	attr, err := convStr2Attr(attributes)
	if err != nil {
		return convert2Json(js, err)
	}
	snap.Attrs = updateAttributes(attr, snap.Attrs)

	if name != "" {
		snap.Name = name
	}

	if desc != "" {
		snap.Description = desc
	}

	js.Str = "Update snapshot (" + snap.ID.String() + ")... "
	snap.LastModifiedTime = time.Now()
	if _, err = h.mdsCur.UpdateSnapshot(snap, nil); err != nil {
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}

	js.Str += "[OK]\n"

	return convert2Json(js, nil)
}

func (h handlerImpl) handlerUpdateBranch(volumesetF string, args []string, jsonF bool) ([]byte, error) {
	var js = JSONOp{}

	if volumesetF == "" {
		return convert2Json(js, errors.New("Missing volumeset flag"))
	}

	if len(args) != 2 {
		return convert2Json(js, errors.New("Incorrect number of arguments passed."))
	}

	h.initialize()

	vsFiltered, err := h.getMatchingVolumesets(volumesetF)
	if err != nil {
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}

	if len(vsFiltered) > 1 {
		// more than one match found
		js.Tab = volumesetTabOutput(vsFiltered)
		js.Str = "Found more than one matching volumeset.\n"

		return convert2Json(js, nil)
	} else if len(vsFiltered) == 0 {
		js.Str = "Volumeset (" + volumesetF + ") not found\n"
		return convert2Json(js, nil)
	}

	br, err := metastore.GetBranches(h.mdsCur, branch.Query{VolSetID: vsFiltered[0].ID, Name: args[0]})
	if err != nil {
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}
	// old branch found
	if len(br) == 0 {
		return convert2Json(js, fmt.Errorf("Branch (%v) not found", args[0]))
	}

	br, err = metastore.GetBranches(h.mdsCur, branch.Query{VolSetID: vsFiltered[0].ID, Name: args[1]})
	if err != nil {
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}
	// new branch name already exists
	if len(br) != 0 {
		return convert2Json(js, fmt.Errorf("New branch (%v) already exists", args[1]))
	}

	if err := h.mdsCur.RenameBranch(vsFiltered[0].ID, args[0], args[1]); err != nil {
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}

	return convert2Json(js, nil)
}

func (h handlerImpl) handlerPushVolumeset(vhub, tokenfile string, args []string, jsonF bool) ([]byte, error) {
	var (
		js  = JSONOp{}
		err error
	)

	h.initialize()

	if len(args) == 0 {
		return convert2Json(js, errors.New("No volumeset identifiers passed."))
	}

	mdVhub, err := h.getRestfulMDStore(vhub, tokenfile)
	if err != nil {
		return convert2Json(js, err)
	}

	for _, vsID := range args {
		vsFiltered, err := h.getMatchingVolumesets(vsID)
		if err != nil {
			return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
		}

		if len(vsFiltered) > 1 {
			// more than one match found
			js.Tab = volumesetTabOutput(vsFiltered)
			js.Str = "Found more than one matching volumeset.\n"

			return convert2Json(js, nil)
		} else if len(vsFiltered) == 0 {
			js.Str = "Volumeset (" + vsID + ") not found\n"

			return convert2Json(js, nil)
		}

		js.Str = fmt.Sprintf("Pushing unsynchronized snapshots to Volume-Hub (%s)... ", vhub)
		if err = sync.PushDataForAllSnapshots(h.mdsCur, vsFiltered[0].ID, &h, mdVhub); err != nil {
			return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
		}
		js.Str += "[OK]\n"
	}

	return convert2Json(js, nil)
}

func (h handlerImpl) handlerPushSnapshot(vhub, tokenfile string, args []string, jsonF bool) ([]byte, error) {
	var (
		js  = JSONOp{}
		err error
	)

	h.initialize()

	mdVhub, err := h.getRestfulMDStore(vhub, tokenfile)
	if err != nil {
		return convert2Json(js, err)
	}

	for _, arg := range args {
		snaps, err := h.getMatchingSnapshots(arg)
		if err != nil {
			return convert2Json(js, err)
		}

		for _, snap := range snaps {
			js.Str = fmt.Sprintf("Pushing snapshots (%s) to Volume-Hub (%s)... ", snap.ID.String(), vhub)
			if err = sync.PushDataForCertainSnapshots(h.mdsCur, &h, mdVhub, []snapshot.ID{snap.ID}); err != nil {
				return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
			}
			js.Str += "[OK]\n"
		}
	}

	return convert2Json(js, nil)
}

func (h handlerImpl) handlerPullVolumeset(vhub, tokenfile string, args []string, jsonF bool) ([]byte, error) {
	var (
		js  = JSONOp{}
		err error
	)

	h.initialize()

	mdVhub, err := h.getRestfulMDStore(vhub, tokenfile)
	if err != nil {
		return convert2Json(js, err)
	}

	for _, vsID := range args {
		vsFiltered, err := h.getMatchingVolumesets(vsID)
		if err != nil {
			return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
		}

		if len(vsFiltered) > 1 {
			// more than one match found
			js.Tab = volumesetTabOutput(vsFiltered)
			js.Str = "Found more than one matching volumeset.\n"

			return convert2Json(js, nil)
		} else if len(vsFiltered) == 0 {
			js.Str = "Volumeset (" + vsID + ") not found\n"

			return convert2Json(js, nil)
		}

		js.Str = fmt.Sprintf("Pulling unsynchronized snapshots from Volume-Hub (%s)... ", vhub)
		if err = sync.PullDataForAllSnapshots(mdVhub, h.mdsCur, vsFiltered[0].ID, &h); err != nil {
			return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
		}
	}
	js.Str += "[OK]\n"

	return convert2Json(js, nil)
}

func (h handlerImpl) handlerPullSnapshot(vhub, tokenfile string, args []string, jsonF bool) ([]byte, error) {
	var (
		js  = JSONOp{}
		err error
	)

	h.initialize()

	mdVhub, err := h.getRestfulMDStore(vhub, tokenfile)
	if err != nil {
		return convert2Json(js, err)
	}

	for _, arg := range args {
		snaps, err := h.getMatchingSnapshots(arg)
		if err != nil {
			return convert2Json(js, err)
		}

		for _, snap := range snaps {
			js.Str = fmt.Sprintf("Pulling snapshots (%s) from Volume-Hub (%s)... ", snap.ID.String(), vhub)
			if err = sync.PullDataForCertainSnapshots(mdVhub, h.mdsCur, &h, []snapshot.ID{snap.ID}); err != nil {
				return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
			}
			js.Str += "[OK]\n"
		}
	}

	return convert2Json(js, nil)
}

func (h handlerImpl) handlerSyncVolumeset(vhub, tokenfile string, args []string, jsonF bool) ([]byte, error) {
	var (
		js  = JSONOp{}
		err error
	)

	h.initialize()

	mdVhub, err := h.getRestfulMDStore(vhub, tokenfile)
	if err != nil {
		return convert2Json(js, err)
	}

	for _, vs := range args {
		var vsID volumeset.ID

		check, err := isID(vs)
		if err != nil {
			return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
		}

		if check {
			vsID = volumeset.NewID(vs)
		} else {
			// for non ID search we need to find the volumset locally first
			vsFiltered, err := h.getMatchingVolumesets(vs)
			if err != nil {
				fmt.Println("vsnf0")
				return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
			}

			if len(vsFiltered) == 0 {
				// didn't anything locally - then seach remote from matching vs
				vhVS, err := mdVhub.GetVolumeSets(volumeset.Query{})
				if err != nil {
					fmt.Println("vsnf1")
					return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
				}

				// find the volumeset by name from vhub volumeset
				vsFiltered, err = matchVolumeSetName(vs, vhVS)
				if err != nil {
					fmt.Println("vsnf1")
					return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
				}
			}

			if len(vsFiltered) > 1 {
				// more than one match found
				js.Tab = volumesetTabOutput(vsFiltered)
				js.Str = "Found more than one matching volumeset.\n"

				return convert2Json(js, nil)
			} else if len(vsFiltered) == 0 {
				// oops no volumeset on VHUB or locally may be a client CLI error
				js.Str = "Volumeset (" + vs + ") not found\n"

				return convert2Json(js, nil)
			}

			// we found something locally lets use the ID to sync it
			vsID = vsFiltered[0].ID
		}

		js.Str = fmt.Sprintf("Syncing meta-data for volumeset (%s) with Volume-Hub (%s)... ", vs, vhub)
		conflicts, err := sync.MetadataSync(mdVhub, h.mdsCur, h.mdsInit, vsID)
		if err != nil {
			return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
		}
		conflicts.Report()

		js.Str += "[OK]\n"
	}

	return convert2Json(js, nil)
}

// Set volume-hub URL to config file
func (h handlerImpl) handlerSetVolumehub(args []string, jsonF bool) ([]byte, error) {
	var js = JSONOp{}

	if len(args) == 0 {
		return convert2Json(js, errors.New("Missing Volume Hub argument"))
	}

	vhub := strings.Trim(args[0], " ")

	if !strings.HasPrefix(vhub, "http://") {
		vhub = "http://" + vhub
	}

	cfg, err := h.jsCfg.ReadConfig()
	if err != nil {
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}

	cfg.VhubURL = vhub
	if err := h.jsCfg.UpdateConfig(cfg); err != nil {
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}

	js.Str = "Volume Hub URL set to " + vhub + "\n"
	return convert2Json(js, nil)
}

// Get volume-hub URL from config file
func (h handlerImpl) handlerGetVolumehub(args []string, jsonF bool) ([]byte, error) {
	var js = JSONOp{}

	cfg, err := h.jsCfg.ReadConfig()
	if err != nil {
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}

	js.Str = "Volume Hub URL: " + cfg.VhubURL + "\n"

	return convert2Json(js, nil)
}

func (h handlerImpl) handlerSetTokenfile(args []string, jsonP bool) ([]byte, error) {
	var js = JSONOp{}

	if len(args) == 0 {
		return convert2Json(js, errors.New("Missing Volume Hub token file argument"))
	}

	vhutfile := strings.Trim(args[0], " ")

	if !filepath.IsAbs(vhutfile) {
		return convert2Json(js, fmt.Errorf("internal error: %s is not absolute path", vhutfile))
	}
	if _, err := os.Stat(vhutfile); os.IsNotExist(err) {
		return convert2Json(js, fmt.Errorf("internal error: %s does not exists", vhutfile))
	}

	cfg, err := h.jsCfg.ReadConfig()
	if err != nil {
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}

	cfg.Tokenfile = vhutfile
	if err := h.jsCfg.UpdateConfig(cfg); err != nil {
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}

	js.Str = "Volume Hub Token file set to " + vhutfile + "\n"
	return convert2Json(js, nil)
}

func (h handlerImpl) handlerGetTokenfile(args []string, jsonP bool) ([]byte, error) {
	var js = JSONOp{}

	cfg, err := h.jsCfg.ReadConfig()
	if err != nil {
		return convert2Json(js, fmt.Errorf("internal error: %s", err.Error()))
	}

	js.Str = "Volume Hub token file: " + cfg.Tokenfile + "\n"

	return convert2Json(js, nil)
}

func (h *handlerImpl) initialize() {
	cfg, err := h.jsCfg.ReadConfig()
	exitOnError(err)

	pathCur, err := securefilepath.New(cfg.SQLiteDBCur)
	exitOnError(err)

	h.mdsCur, err = sqlite3storage.Open(pathCur)
	exitOnError(err)

	// TODO: look into deferring opening of the INIT store until doing the sync
	// since no other operations need it.
	pathInit, err := securefilepath.New(cfg.SQLiteDBInit)
	exitOnError(err)

	h.mdsInit, err = sqlite3storage.Open(pathInit)
	exitOnError(err)

	h.stor, err = zfs.New(cfg.ZPool, blobdiffer.Factory{FileDiffer: variableblk.Factory{}})
	exitOnError(err)

	h.exec = executor.NewCommonExecutor()
	h.ed = dlbin.Factory{}
	h.hf = adler32.Factory{}
}

func (h handlerImpl) getRestfulMDStore(vhub, tokenfile string) (*restfulstorage.MetadataStorage, error) {
	cfg, err := h.jsCfg.ReadConfig()
	if err != nil {
		return nil, fmt.Errorf("internal error: %s", err.Error())
	}

	if vhub == "" {
		// --vhub overides dpcli set --vhub, first check --vhub flag
		vhub = cfg.VhubURL
		if vhub == "" {
			// checkout for set --vhub flag
			return nil, errors.New("Missing vhub flag value.")
		}
	}

	if tokenfile == "" {
		tokenfile = cfg.Tokenfile
		if tokenfile == "" {
			return nil, errors.New("Missing tokenfile flag value.")
		}
	}

	uVhub, err := url.Parse(vhub)
	if err != nil {
		return nil, fmt.Errorf("internal error: %s", err.Error())
	}

	vhut := cauthn.VHUT{}
	err = vhut.InitFromFile(tokenfile)
	if err != nil {
		return nil, fmt.Errorf("internal error: %s", err.Error())
	}

	client := protocols.GetClient()
	mdVhub, err := restfulstorage.Create(client, uVhub, &vhut)
	if err != nil {
		return nil, fmt.Errorf("internal error: %s", err.Error())
	}

	return mdVhub, nil
}

func (h handlerImpl) getMatchingVolumesets(vsp string) ([]*volumeset.VolumeSet, error) {
	check, err := isID(vsp)
	if err != nil {
		return []*volumeset.VolumeSet{}, err
	}

	if check {
		vs, err := metastore.GetVolumeSet(h.mdsCur, volumeset.NewID(vsp))

		// If no matching VolSet found just return empty volset slice without error
		if err != nil {
			if _, ok := err.(*metastore.ErrVolumeSetNotFound); ok {
				return []*volumeset.VolumeSet{}, nil
			}
		}

		return []*volumeset.VolumeSet{vs}, err
	}

	// fetch volumesets
	vsList, err := metastore.GetVolumeSets(h.mdsCur, volumeset.Query{})
	if err != nil {
		return vsList, err
	}

	vsFiltered, err := matchVolumeSetName(vsp, vsList)
	if err != nil {
		return vsFiltered, err
	}

	return vsFiltered, nil
}

func (h handlerImpl) getMatchingSnapshots(snap string) ([]*snapshot.Snapshot, error) {
	snapList := []*snapshot.Snapshot{}

	// if snap is an ID
	check, err := isID(snap)
	if err != nil {
		return snapList, err
	}

	if check {
		s, err := metastore.GetSnapshot(h.mdsCur, snapshot.NewID(snap))
		if err != nil {
			return snapList, err
		}

		snapList = append(snapList, s)
		return snapList, err
	}

	p := strings.Split(snap, "@")
	if len(p) != 2 {
		return snapList, fmt.Errorf("Invalid snapshot format (%v)", snap)
	}

	check, err = isID(p[1])
	if err != nil {
		return snapList, err
	}

	if check {
		return snapList, fmt.Errorf("Invalid snapshot format (%v). Snapshot name cannot be an ID (%v)", snap, p[1])
	}

	vsFiltered, err := h.getMatchingVolumesets(p[0])
	if err != nil {
		return snapList, err
	}

	for _, vs := range vsFiltered {
		snaps, err := metastore.GetSnapshots(h.mdsCur, snapshot.Query{VolSetID: vs.ID, Name: p[1]})
		if err != nil {
			return snapList, err
		}

		snapList = append(snapList, snaps...)
	}

	return snapList, nil
}

func (h handlerImpl) getMatchingBranches(br string) ([]*branch.Branch, error) {
	brList := []*branch.Branch{}

	p := strings.Split(br, "@")
	if len(p) != 2 {
		return brList, fmt.Errorf("Invalid branch format (%v)", br)
	}

	vsFiltered, err := h.getMatchingVolumesets(p[0])
	if err != nil {
		return brList, err
	}

	for _, vs := range vsFiltered {
		brs, err := metastore.GetBranches(h.mdsCur, branch.Query{VolSetID: vs.ID, Name: p[1]})
		if err != nil {
			return brList, err
		}

		brList = append(brList, brs...)
	}

	return brList, nil
}

// Execute defines the root cli generated from the cligen. This consumes the arguments and calls the handlers accordingly.
func Execute() error {
	js := NewJSONFileConfig(filepath.Join(getHomeDir(), configFile))

	// create a generate executor that invokes POSIX syscalls
	h := handlerImpl{jsCfg: js}

	if !h.jsCfg.Exists() {
		err := h.jsCfg.CreateFile()
		exitOnError(err)
	}

	cmd := newDpcliCmd(h)

	// XXX Should this be controlled externally? Not sure yet.
	cmd.SetUsageTemplate(usageTemplate)
	cmd.SetOutput(os.Stdout)
	cmd.SilenceErrors = false

	// parse and execute dpcli2
	return cmd.Execute()
}
