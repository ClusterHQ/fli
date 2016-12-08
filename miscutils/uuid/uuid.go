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

package uuid

import (
	"regexp"

	"github.com/ClusterHQ/fli/errors"
)

// IsUUID returns a true if the string is UUID.
// Uses regular expression to validate the input string.
func IsUUID(s string) (bool, error) {
	p := `^\w{8,8}-\w{4,4}-\w{4,4}-\w{4,4}-\w{12,12}$`
	isID, err := regexp.MatchString(p, s)
	if err != nil {
		return isID, errors.New(err)
	}

	isShrunkID, err := IsShrunkUUID(s)
	if err != nil {
		return (isID || isShrunkID), err
	}

	return (isID || isShrunkID), nil
}

// IsShrunkUUID returns true if the UUID is in the shrunk format
func IsShrunkUUID(s string) (bool, error) {
	p := `^\w{8,8}-\w{4,4}$`
	ret, err := regexp.MatchString(p, s)
	if err != nil {
		return ret, errors.New(err)
	}

	return ret, nil
}

// ShrinkUUID reduces the UUID from 32 char long string to 12 char long ID
// Note: Default UUID size is 32 + 4 DASHES
func ShrinkUUID(id string) string {
	if id == "" || len(id) != (32+4) {
		return id
	}

	return id[:8] + "-" + id[len(id)-4:]
}
