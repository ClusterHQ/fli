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
	"bytes"
	"encoding/json"
	"time"

	"github.com/ClusterHQ/fli/meta/branch"
	"github.com/ClusterHQ/fli/meta/snapshot"
	"github.com/ClusterHQ/fli/meta/volume"
	"github.com/ClusterHQ/fli/meta/volumeset"
)

type (
	// Result interfaces the cmd output of each of the handlers. Each of
	// the handlers should write their own cmd output format and
	// should be able to output it as string or JSON string.
	Result interface {
		// String turns result into regular human readable string.
		String() string
		// JSON turns result into JSON string.
		JSON() string
	}

	// CmdResult ...
	CmdResult struct {
		Str string
		Tab [][]string
	}

	// CmdOutput is the default Result.
	CmdOutput struct {
		Op []CmdResult
	}
)

func (out CmdOutput) String() string {
	var strBuf bytes.Buffer

	for _, o := range out.Op {
		if o.Str != "" {
			strBuf.WriteString(o.Str)
			strBuf.WriteString("\n")
		}

		strBuf.WriteString(genTable(o.Tab))
	}

	return strBuf.String()
}

// JSON is unimplemented for now.
func (out CmdOutput) JSON() string {
	// TODO: Unimplemented
	return "Command executed but output JSON is unimplemented\n"
}

type (
	// ListResult represent the result from list command.
	ListResult struct {
		full   bool
		vsObjs []*volumesetObjects
		vols   []*volumeset.VolumeSet
	}
)

var _ Result = &ListResult{}

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
		"SNAPSHOT TIP",
	}

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

func displayObjectDefault(vsObj *volumesetObjects, full bool) []CmdResult {
	result := []CmdResult{
		{
			Tab: volumesetTable(0, full, []*volumeset.VolumeSet{vsObj.volset}),
		},
	}

	if len(vsObj.brs) > 0 {
		res := CmdResult{Tab: branchTable(1, full, vsObj.brs)}
		result = append(result, res)
	}

	if len(vsObj.snaps) > 0 {
		res := CmdResult{Tab: snapshotTable(1, full, vsObj.snaps)}
		result = append(result, res)
	}

	if len(vsObj.vols) > 0 {
		res := CmdResult{Tab: volumeTables(1, full, vsObj.vols)}
		result = append(result, res)
	}

	return result
}

func volumesetTableJSON(vss []*volumeset.VolumeSet) []map[string]string {
	resultMap := []map[string]string{}
	for _, vs := range vss {
		m := map[string]string{}
		m["VOLUMESET ID"] = vs.ID.String()
		m["CREATOR"] = vs.Creator
		m["CREATED"] = vs.CreationTime.Format(time.Stamp)
		m["SIZE"] = readableSize(vs.Size)
		m["DESCRIPTION"] = vs.Description
		m["ATTRIBUTES"] = convAttrToStr(vs.Attrs)
		m["NAME"] = vs.Name
		resultMap = append(resultMap, m)
	}

	return resultMap
}

func snapshotTableJSON(snaps []*snapshot.Snapshot) []map[string]string {
	resultMap := []map[string]string{}
	for _, snap := range snaps {
		m := map[string]string{}
		m["SNAPSHOT ID"] = snap.ID.String()
		m["OWNER"] = snap.Owner
		m["CREATOR"] = snap.Creator
		m["CREATED"] = snap.CreationTime.Format(time.Stamp)
		m["SIZE"] = readableSize(snap.Size)
		m["DESCRIPTION"] = snap.Description
		m["ATTRIBUTES"] = convAttrToStr(snap.Attrs)
		m["NAME"] = snap.Name
		resultMap = append(resultMap, m)
	}

	return resultMap
}

func branchTableJSON(branches []*branch.Branch) []map[string]string {
	resultMap := []map[string]string{}
	for _, br := range branches {
		m := map[string]string{}
		m["BRANCH"] = br.Name
		m["SNAPSHOT TIP"] = br.Tip.Name
		resultMap = append(resultMap, m)
	}

	return resultMap
}

func volumeTableJSON(vols []*volume.Volume) []map[string]string {
	resultMap := []map[string]string{}
	for _, vol := range vols {
		m := map[string]string{}
		m["VOLUME ID"] = vol.ID.String()
		m["CREATED"] = vol.CreationTime.Format(time.Stamp)
		m["NAME"] = vol.Name
		m["SIZE"] = readableSize(vol.Size)
		m["MOUNT POINT"] = vol.MntPath.Path()
		resultMap = append(resultMap, m)
	}

	return resultMap
}

func (r ListResult) String() string {
	cmdOut := CmdOutput{}
	switch {
	case len(r.vsObjs) != 0:
		for _, vsObj := range r.vsObjs {
			out := displayObjectDefault(vsObj, r.full)
			cmdOut.Op = append(cmdOut.Op, out...)
		}

	case len(r.vols) != 0:
		cmdOut.Op = append(
			cmdOut.Op,
			CmdResult{
				Tab: volumesetTable(0, r.full, r.vols),
			},
		)
	}

	return cmdOut.String()
}

type (
	listResultObj struct {
		Volsets  []map[string]string `json:",omitempty"`
		Branches []map[string]string `json:",omitempty"`
		Snaps    []map[string]string `json:",omitempty"`
		Vols     []map[string]string `json:",omitempty"`
	}
)

// JSON translates the list result to JSON string.
func (r ListResult) JSON() string {
	switch {
	case len(r.vsObjs) != 0:
		result := []listResultObj{}
		for _, vsObj := range r.vsObjs {
			var resultObj listResultObj

			resultObj.Volsets = volumesetTableJSON([]*volumeset.VolumeSet{vsObj.volset})
			if len(vsObj.snaps) > 0 {
				resultObj.Snaps = snapshotTableJSON(vsObj.snaps)
			}
			if len(vsObj.brs) > 0 {
				resultObj.Branches = branchTableJSON(vsObj.brs)
			}
			if len(vsObj.vols) > 0 {
				resultObj.Vols = volumeTableJSON(vsObj.vols)
			}

			result = append(result, resultObj)
		}

		b, _ := json.Marshal(result)
		return string(b[:])
	case len(r.vols) != 0:
		b, _ := json.Marshal(volumesetTableJSON(r.vols))
		return string(b[:])
	}

	return ""
}
