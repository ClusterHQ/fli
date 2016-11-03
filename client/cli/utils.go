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
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"strings"

	"strconv"
	"time"

	"github.com/ClusterHQ/go/meta/attrs"
	"github.com/ClusterHQ/go/meta/snapshot"
	"github.com/ClusterHQ/go/meta/volumeset"
)

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

// convert2Json converts a JsonOp to json byte slice and handle error generate during marshalling json
func convert2Json(js JSONOp, err error) ([]byte, error) {
	ret, lErr := json.Marshal(js)
	if lErr != nil {
		if err != nil {
			err = fmt.Errorf("%s\ninternal error: %s", err.Error(), lErr.Error())
		} else {
			err = lErr
		}
	}

	return ret, err
}

// convert attrs.Attrs to a string with the following format key=value,key=value
func convAttr2Str(attr attrs.Attrs) string {
	var attrStr = ""

	for key, value := range attr {
		attrStr += key + "=" + value + ","
	}

	// for loop results in k=v,k1=v1, - the below code removes the last , from the stirng
	var strLen = len(attrStr)
	if strLen > 0 {
		attrStr = attrStr[:strLen-1]
	}

	return attrStr
}

// convert a string key=value,key=value into attrs.Attrs struct
func convStr2Attr(attrStr string) (attrs.Attrs, error) {
	var attrs = attrs.Attrs{}
	var err error

	err = nil
	if len(attrStr) == 0 {
		return attrs, err
	}

	var attr = strings.Split(attrStr, ",")
	for _, val := range attr {
		var v = strings.Split(val, "=")
		if len(v) != 2 {
			return attrs, fmt.Errorf("Invalid attribute format")
		}

		attrs[v[0]] = v[1]
	}

	return attrs, err
}

// updateAttributes does a union of two attr.Attrs into a single attr.Attrs where key is case-insensitive
// and matching keys update the value from nAttr struct
func updateAttributes(nAttr attrs.Attrs, oAttr attrs.Attrs) attrs.Attrs {
	var newAttr = attrs.Attrs{}

	// copy old attributes to attributes that will be returned by this function
	// then to udpate the old attributes with new attributes values
	// loop the new attributes and compare the key with old attributes
	// if match is found update a global key, value variables a break from old
	// attributes
	// if match not found then we global key, value will match the outer new attribute
	// loop so just add it

	// TODO: Find a better way to copy here.
	for k, v := range oAttr {
		newAttr[k] = v
	}

	for k, v := range nAttr {
		var key, value = k, v
		for iK := range oAttr {
			if strings.Compare(strings.ToLower(k), strings.ToLower(iK)) == 0 {
				key = iK
				value = v

				break
			}
		}

		newAttr[key] = value
	}

	return newAttr
}

func exitOnError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "internal error: %s", err.Error())
		os.Exit(1)
	}
}

// reverse slice of stirngs
func reverse(s []string) []string {
	n := len(s)
	p := make([]string, n)
	_ = copy(p, s)

	for i := 0; i < n/2; i++ {
		p[i], p[n-1-i] = p[n-1-i], p[i]
	}

	return p
}

func compare(s []string, p []string) bool {
	if len(s) != len(p) {
		return false
	}

	for o := range s {
		if s[o] != p[o] {
			return false
		}
	}

	return true
}

// volumesetTabOutput prints the volumeset table for cli output
func volumesetTabOutput(vs []*volumeset.VolumeSet) [][]string {
	var tab [][]string
	tab = append(tab, []string{"NAME", "ID", "CREATOR", "CREATED", "SIZE", "ATTRIBUTES", "DESCRIPTION"})

	for _, v := range vs {
		var name = ""

		if v.Prefix != "" && v.Name != "" {
			name = v.Prefix + "/" + v.Name
		} else if v.Prefix == "" && v.Name != "" {
			name = v.Name
		} else {
			name = ""
		}

		tab = append(tab,
			[]string{name, v.ID.String(), v.CreatorUsername,
				v.CreationTime.Format(time.RFC822), strconv.FormatUint(v.Size, 10) + "B",
				convAttr2Str(v.Attrs), v.Description})
	}

	return tab
}

// snapshotTabOutput prints the snapshot table for cli output
func snapshotTabOutput(snaps []*snapshot.Snapshot) [][]string {
	var tab [][]string
	tab = append(tab, []string{"NAME", "ID", "VOLUMESET", "SIZE", "ATTRIBUTES", "DESCRIPTION"})

	for _, snap := range snaps {
		tab = append(tab, []string{snap.Name, snap.ID.String(), snap.VolSetID.String(), strconv.FormatUint(snap.Size, 10) + "B", convAttr2Str(snap.Attrs), snap.Description})
	}

	return tab
}
