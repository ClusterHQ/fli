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
	"regexp"
	"strings"

	"github.com/ClusterHQ/go/meta/volumeset"
)

func isID(s string) (bool, error) {
	p := `\w{8,8}-\w{4,4}-\w{4,4}-\w{4,4}-\w{12,12}`
	return regexp.MatchString(p, s)
}

func split(s string) []string {
	// if the s starts with '/' means we need to do compares from the start of the prefix
	p := strings.Split(s, "/")
	if s[0] == '/' {
		// if s start with '/' then drop the first empty string
		return p[1:]
	}

	return p
}

func matchVolumeSetName(vsp string, vsPtr []*volumeset.VolumeSet) ([]*volumeset.VolumeSet, error) {
	res := []*volumeset.VolumeSet{}
	pName := split(vsp)

	// if the whole prefix is given then do a reverse search starting from the name
	if vsp[0] != '/' {
		pName = reverse(pName)
	}

	// for every volumeset find the matches
	for _, vsP := range vsPtr {
		sName := []string{vsP.Name}
		if vsP.Prefix != "" {
			// split the name by '/'
			sName = split(vsP.Prefix + "/" + vsP.Name)
		}

		// if the whole prefix is given then do a reverse search starting from the name
		if vsp[0] != '/' {
			sName = reverse(sName)
		}

		// search Name length should be equal or less the Volume Set name
		// pName matches sName
		if len(pName) <= len(sName) && compare(pName, sName[:len(pName)]) {
			// found a match
			res = append(res, vsP)
		}
	}

	return res, nil
}
