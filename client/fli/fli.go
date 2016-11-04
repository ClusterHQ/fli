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
	"fmt"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"github.com/ClusterHQ/fli/dl/blobdiffer"
	"github.com/ClusterHQ/fli/dl/datalayer"
	"github.com/ClusterHQ/fli/dl/encdec"
	"github.com/ClusterHQ/fli/dl/executor"
	"github.com/ClusterHQ/fli/dl/filediffer/variableblk"
	dlhash "github.com/ClusterHQ/fli/dl/hash"
	"github.com/ClusterHQ/fli/dl/zfs"
	"github.com/ClusterHQ/fli/dp/metastore"
	"github.com/ClusterHQ/fli/errors"
	"github.com/ClusterHQ/fli/mdsimpls/sqlite3storage"
	"github.com/ClusterHQ/fli/meta/attrs"
	"github.com/ClusterHQ/fli/meta/blob"
	"github.com/ClusterHQ/fli/meta/branch"
	"github.com/ClusterHQ/fli/meta/snapshot"
	"github.com/ClusterHQ/fli/meta/volume"
	"github.com/ClusterHQ/fli/meta/volumeset"
	"github.com/ClusterHQ/fli/securefilepath"
	"golang.org/x/net/context"
)

//go:generate go run ../cmd/fligen/main.go -yaml cmd.yml -output cmds_gen.go -package fli

const (
	configDir      = ".fli"
	configFile     = "config"
	mdsFileCurrent = "mds_current"
	mdsFileInitial = "mds_initial"
	logDir         = "/var/log/fli"

	byteSz     = 1.0
	kilobyteSz = 1024 * byteSz
	megabyteSz = 1024 * kilobyteSz
	gigabyteSz = 1024 * megabyteSz
	terabyteSz = 1024 * gigabyteSz

	// CommandCtxKeys
	urlKey         cmdCtxKey = "url"
	tokenKey       cmdCtxKey = "token"
	attributesKey  cmdCtxKey = "attributes"
	descriptionKey cmdCtxKey = "description"
	zpoolKey       cmdCtxKey = "zpool"
	branchKey      cmdCtxKey = "branch"
	nameKey        cmdCtxKey = "name"
)

type (
	// Result ...
	Result struct {
		Str string
		Tab [][]string
	}

	// CmdOutput ...
	CmdOutput struct {
		Op []Result
	}

	volumesetObjects struct {
		volset *volumeset.VolumeSet
		brs    []*branch.Branch
		snaps  []*snapshot.Snapshot
		vols   []*volume.Volume
	}

	blobDiff struct {
		store datalayer.Storage
		ed    encdec.Factory
		hf    dlhash.Factory
	}

	cmdCtxKey string
)

var (
	_ CommandHandler = &Handler{}

	usageTemplate = `Usage:{{if .Runnable}}
{{multiUseLine .CommandPath .Use}}{{end}}{{if .HasSubCommands }}
  {{ .CommandPath}} COMMAND{{end}}{{if gt .Aliases 0}}

Aliases:
  {{.NameAndAliases}}{{end}}{{if .HasExample}}

Examples:
{{ .Example }}{{end}}{{ if .HasLocalFlags}}
Options:

{{.LocalFlags.FlagUsages | trimRightSpace}}{{end}}{{if .HasAvailableSubCommands}}

Commands:{{range .Commands}}{{if .IsAvailableCommand}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{ if .HasInheritedFlags}}

Global Flags:
{{.InheritedFlags.FlagUsages | trimRightSpace}}{{end}}{{- if .HasSubCommands }}

Run '{{.CommandPath}} COMMAND --help' for more information on a command.
{{- end}}
`
)

// UploadBlobDiff ...
func (b blobDiff) UploadBlobDiff(vsid volumeset.ID, base blob.ID, targetBlobID blob.ID, t string,
	dspuburl string) error {
	return datalayer.UploadBlobDiff(b.store, b.ed, b.hf, vsid, base, targetBlobID, t, dspuburl)
}

// DownloadBlobDiff ...
func (b blobDiff) DownloadBlobDiff(vsid volumeset.ID, base blob.ID, t string, dspuburl string) (blob.ID, uint64, error) {
	return datalayer.DownloadBlobDiff(b.store, b.ed, vsid, base, t, executor.NewCommonExecutor(),
		b.hf, dspuburl)
}

// getHomeDir gets the full path of the current user's home dir, if unable to fetch the user home dir path then
// returns alias to home dir "~"
func getHomeDir() string {
	dir := "~"
	u, err := user.Current()
	if err == nil {
		dir = u.HomeDir
	}

	return dir
}

func readableSize(sz uint64) string {
	unitSz := "B"
	fmtSz := float32(sz)

	switch {
	case sz >= terabyteSz:
		unitSz = "TB"
		fmtSz = float32(sz) / terabyteSz
	case sz >= gigabyteSz:
		unitSz = "GB"
		fmtSz = float32(sz) / gigabyteSz
	case sz >= megabyteSz:
		unitSz = "MB"
		fmtSz = float32(sz) / megabyteSz
	case sz >= kilobyteSz:
		unitSz = "KB"
		fmtSz = float32(sz) / kilobyteSz
	case sz == 0:
		unitSz = ""
	}

	fmtSzStr := ""

	if float32(uint64(fmtSz)) == fmtSz {
		// remove decimal point if its .0
		fmtSzStr = fmt.Sprintf("%d %s", uint64(fmtSz), unitSz)
	} else {
		fmtSzStr = fmt.Sprintf("%.1f %s", fmtSz, unitSz)
	}

	return fmtSzStr
}

// convStrToAttr convert a string key=value,key=value into attrs.Attrs struct
func convStrToAttr(attrStr string) (attrs.Attrs, error) {
	attr := attrs.Attrs{}

	if attrStr == "" {
		return attr, nil
	}

	for _, val := range strings.Split(attrStr, ",") {
		kv := strings.Split(val, "=")
		if len(kv) != 2 {
			return attrs.Attrs{}, &ErrInvalidAttrFormat{str: val}
		}

		attr[kv[0]] = kv[1]
	}

	return attr, nil
}

// convAttrToStr convert attrs.Attrs to a string with the following format key=value,key=value
func convAttrToStr(attr attrs.Attrs) string {
	kvList := []string{}

	for key, value := range attr {
		kv := strings.Join([]string{key, value}, "=")
		kvList = append(kvList, kv)
	}

	return strings.Join(kvList, ",")
}

// updateAttributes does a union of two attr.Attrs into a single attr.Attrs where key is case-insensitive
// and matching keys update the value from nAttr struct
func updateAttributes(nAttr attrs.Attrs, oAttr attrs.Attrs) attrs.Attrs {
	var newAttr = attrs.Attrs{}

	// Copy old attributes to attributes that will be returned by this function
	// then to udpate the old attributes with new attributes values
	// loop the new attributes and compare the key with old attributes
	// if match is found update a global key, value variables a break from old
	// attributes
	// if match not found then we global key, value will match the outer new attribute
	// loop so just add it
	for k, v := range oAttr {
		newAttr[k] = v
	}

	for k, v := range nAttr {
		var key, value = k, v
		for iK := range oAttr {
			if strings.Compare(strings.ToLower(k), strings.ToLower(iK)) == 0 {
				key, value = iK, v
				break
			}
		}

		newAttr[key] = value
	}

	return newAttr
}

func getMds(mdsPath string) (metastore.Client, error) {
	pathCur, err := securefilepath.New(mdsPath)
	if err != nil {
		return nil, err
	}

	exists, err := pathCur.Exists()
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, errors.Errorf("Metadata store not found\nRun 'fli setup --zpool=<ZFS ZPOOL>' to setup the environment")
	}

	return sqlite3storage.Open(pathCur)
}

func getStorage(zpool string) (datalayer.Storage, error) {
	store, err := zfs.New(zpool, blobdiffer.Factory{FileDiffer: variableblk.Factory{}})
	if err != nil {
		return store, handleZFSErr(err)
	}

	return store, err
}

func validateName(name string) error {
	isID, err := isUUID(name)
	if err != nil {
		return err
	}

	if isID {
		return errors.Errorf("Name (%s) can not be an UUID format", name)
	}

	if strings.Contains(name, ":") {
		return errors.Errorf("Name (%s) can not contain ':'", name)
	}

	if strings.Contains(name, " ") {
		return errors.Errorf("Name (%s) can not contain whitespaces", name)
	}

	return nil
}

func splitVolumeSetName(vsName string) (string, string) {
	var prefix string

	if filepath.Dir(vsName) == "." || filepath.Dir(vsName) == "/" {
		prefix = ""
	} else {
		prefix = filepath.Dir(vsName)
	}

	vsName = filepath.Base(vsName)

	return prefix, vsName
}

func snapshotTable(tabCount int, full bool, snaps []*snapshot.Snapshot) [][]string {
	if len(snaps) == 0 {
		return [][]string{}
	}

	header := []string{
		"SNAPSHOT ID",
		"OWNER",
		"CREATOR",
		"CREATED",
		"SIZE",
		"DESCRIPTION",
		"ATTRIBUTES",
		"NAME",
	}

	leadingTabs := ""
	if tabCount > 0 {
		for i := 0; i < tabCount; i++ {
			leadingTabs += "    "
		}

		header = append([]string{leadingTabs}, header...)
	}

	tab := append([][]string{}, header)

	for _, snap := range snaps {
		id := snap.ID.String()
		owner := snap.Owner
		creator := snap.Creator

		if !full {
			id = ShrinkUUIDs(id)
			owner = ShrinkUUIDs(snap.Owner)
			creator = ShrinkUUIDs(snap.Creator)
		}

		row := []string{
			id,
			owner,
			creator,
			snap.CreationTime.Format(time.Stamp),
			readableSize(snap.Size),
			snap.Description,
			convAttrToStr(snap.Attrs),
			snap.Name,
		}

		if tabCount > 0 {
			row = append([]string{leadingTabs}, row...)
		}

		tab = append(tab, row)
	}

	return tab
}

func volumesetTable(tabCount int, full bool, volsets []*volumeset.VolumeSet) [][]string {
	if len(volsets) == 0 {
		return [][]string{}
	}

	header := []string{
		"VOLUMESET ID",
		"CREATOR",
		"CREATED",
		"SIZE",
		"DESCRIPTION",
		"ATTRIBUTES",
		"NAME",
	}

	leadingTabs := ""
	if tabCount > 0 {
		for i := 0; i < tabCount; i++ {
			leadingTabs += "    "
		}

		header = append([]string{leadingTabs}, header...)
	}

	tab := append([][]string{}, header)

	for _, volset := range volsets {
		name := volset.Name
		id := volset.ID.String()
		creator := volset.Creator

		if !full {
			id = ShrinkUUIDs(id)
			creator = ShrinkUUIDs(volset.Creator)
		}

		if volset.Prefix != "" {
			name = volset.Prefix + "/" + name
		}

		row := []string{
			id,
			creator,
			volset.CreationTime.Format(time.Stamp),
			readableSize(volset.Size),
			volset.Description,
			convAttrToStr(volset.Attrs),
			name,
		}

		if tabCount > 0 {
			row = append([]string{leadingTabs}, row...)
		}

		tab = append(tab, row)
	}

	return tab
}

func branchTable(tabCount int, full bool, branches []*branch.Branch) [][]string {
	if len(branches) == 0 {
		return [][]string{}
	}

	header := []string{
		"BRANCH",
		"SNAPSHOT TIP"}

	leadingTabs := ""
	if tabCount > 0 {
		for i := 0; i < tabCount; i++ {
			leadingTabs += "    "
		}

		header = append([]string{leadingTabs}, header...)
	}

	tab := append([][]string{}, header)
	for _, br := range branches {
		row := []string{br.Name, br.Tip.Name}
		if tabCount > 0 {
			row = append([]string{leadingTabs}, row...)
		}
		tab = append(tab, row)
	}

	return tab
}

func volumeTables(tabCount int, full bool, vols []*volume.Volume) [][]string {
	if len(vols) == 0 {
		return [][]string{}
	}

	header := []string{
		"VOLUME ID",
		"CREATED",
		"SIZE",
		"MOUNT POINT",
		"NAME",
	}

	leadingTabs := ""
	if tabCount > 0 {
		for i := 0; i < tabCount; i++ {
			leadingTabs += "    "
		}

		header = append([]string{leadingTabs}, header...)
	}
	tab := append([][]string{}, header)

	for _, vol := range vols {
		id := vol.ID.String()

		if !full {
			id = ShrinkUUIDs(id)
		}

		row := []string{
			id,
			vol.CreationTime.Format(time.Stamp),
			readableSize(vol.Size),
			vol.MntPath.Path(),
			vol.Name,
		}
		if tabCount > 0 {
			row = append([]string{leadingTabs}, row...)
		}
		tab = append(tab, row)
	}

	return tab
}

func displayObjects(vsObj volumesetObjects, full bool) []Result {

	result := []Result{
		{
			Tab: volumesetTable(0, full, []*volumeset.VolumeSet{vsObj.volset}),
		},
	}

	if len(vsObj.brs) > 0 {
		res := Result{Tab: branchTable(1, full, vsObj.brs)}
		result = append(result, res)
	}

	if len(vsObj.snaps) > 0 {
		res := Result{Tab: snapshotTable(1, full, vsObj.snaps)}
		result = append(result, res)
	}

	if len(vsObj.vols) > 0 {
		res := Result{Tab: volumeTables(1, full, vsObj.vols)}
		result = append(result, res)
	}

	return result
}

func handleZFSErr(err error) error {
	switch err.(type) {
	case *zfs.ErrZfsNotFound:
		return errors.Errorf(`Missing ZFS kernel module
Visit %s for instructions to install ZFS`, flockerHubRef)
	case *zfs.ErrZfsUtilsNotFound:
		return errors.Errorf(`Missing ZFS utilities
Visit %s for instructions to install ZFS utilities`, flockerHubRef)
	case *zfs.ErrZpoolNotFound:
		return errors.Errorf(`%s
Visit https://clusterhq.com/ for instructions to create ZPOOL`, err.Error())

	}

	return err
}

// ShrinkUUIDs reduces the UUID from 32 char long string to 12 char long ID
func ShrinkUUIDs(UUID string) string {
	if UUID == "" || len(UUID) != (32+4) {
		// DEFAULT UUID SIZE IS 32 + 4 DASHES
		return UUID
	}

	return UUID[:8] + "-" + UUID[len(UUID)-4:]
}

// Execute ...
func Execute() {
	os.MkdirAll(logDir, (os.ModeDir | 0755))

	logFile := filepath.Join(logDir, "fli.log")
	fp, err := os.OpenFile(logFile, (os.O_CREATE | os.O_WRONLY | os.O_APPEND), 0666)
	if err != nil {
		fmt.Printf("Failed to set the log output file (%v)", logFile)
		fmt.Println(err.Error())
		os.Exit(1)
	}

	log.SetOutput(fp)

	// Create the configDir if it doesn't exist
	os.MkdirAll(filepath.Join(getHomeDir(), configDir), (os.ModeDir | os.ModePerm))

	cfgFile := filepath.Join(getHomeDir(), configDir, configFile)
	cfg := NewConfig(cfgFile)
	if !cfg.Exists() {
		if err := cfg.CreateFile(); err != nil {
			fmt.Printf("Failed to create configuration file (%v)\n", err.Error())
			os.Exit(1)
		}
	}

	params, err := cfg.ReadConfig()
	if err != nil {
		fmt.Printf("Failed to read configuration file (%v)\n", err.Error())
		os.Exit(1)
	}

	handler := NewHandler(
		params,
		cfgFile,
		filepath.Join(getHomeDir(), configDir, mdsFileCurrent),
		filepath.Join(getHomeDir(), configDir, mdsFileInitial),
	)

	url := flockerHubURL
	if params.FlockerHubURL != "" {
		url = params.FlockerHubURL
	}

	ctx := context.WithValue(context.Background(), urlKey, url)
	ctx = context.WithValue(ctx, tokenKey, params.AuthTokenFile)

	cmd := newFliCmd(ctx, handler)

	cmd.SetUsageTemplate(usageTemplate)
	cmd.SetOutput(os.Stdout)
	cmd.SilenceErrors = false

	// parse and execute fli
	cmd.Execute()
}
