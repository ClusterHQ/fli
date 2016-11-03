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

import "bytes"

type (
	// ErrZfsNotFound ...
	ErrZfsNotFound struct{}

	// ErrZfsUtilsNotFound ...
	ErrZfsUtilsNotFound struct{}

	// ErrZpoolNotFound ...
	ErrZpoolNotFound struct {
		Zpool string
	}
)

var (
	_ error = &ErrZfsNotFound{}
	_ error = &ErrZpoolNotFound{}
	_ error = &ErrZfsUtilsNotFound{}
)

func (e ErrZfsNotFound) Error() string {
	return "ZFS kernel module not found"
}

func (e ErrZpoolNotFound) Error() string {
	var errBuf bytes.Buffer

	errBuf.WriteString("Missing ")
	errBuf.WriteString(e.Zpool)
	errBuf.WriteString(" zpool")

	return errBuf.String()
}

func (e ErrZfsUtilsNotFound) Error() string {
	return "ZFS utils not found"
}
