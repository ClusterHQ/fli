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

import "bytes"

type (
	// ErrBranchNotFound ...
	ErrBranchNotFound struct {
		Name string
	}
	// ErrVolSetNotFound ...
	ErrVolSetNotFound struct {
		Name string
	}
	// ErrInvalidSearch ..
	ErrInvalidSearch struct {
		search string
	}
	// ErrSnapshotNotFound ...
	ErrSnapshotNotFound struct {
		Name string
	}
	// ErrVolumeNotFound ...
	ErrVolumeNotFound struct {
		Name string
	}
	// ErrConfigFileNotFound ...
	ErrConfigFileNotFound struct{}

	// ErrInvalidArgs ...
	ErrInvalidArgs struct{}

	// ErrMissingFlag ...
	ErrMissingFlag struct {
		FlagName string
	}

	// ErrInvalidAttrFormat ...
	ErrInvalidAttrFormat struct {
		str string
	}
)

var (
	_ error = &ErrBranchNotFound{}
	_ error = &ErrVolSetNotFound{}
	_ error = &ErrInvalidSearch{}
	_ error = &ErrSnapshotNotFound{}
	_ error = &ErrBranchNotFound{}
	_ error = &ErrConfigFileNotFound{}
	_ error = &ErrInvalidArgs{}
	_ error = &ErrMissingFlag{}
	_ error = &ErrInvalidAttrFormat{}
)

func (e ErrBranchNotFound) Error() string {
	var errBuf bytes.Buffer

	errBuf.WriteString("Branch '")
	errBuf.WriteString(e.Name)
	errBuf.WriteString("' not found")

	return errBuf.String()
}

func (e ErrVolSetNotFound) Error() string {
	var errBuf bytes.Buffer

	errBuf.WriteString("Volumeset '")
	errBuf.WriteString(e.Name)
	errBuf.WriteString("' not found")

	return errBuf.String()
}

func (e ErrInvalidSearch) Error() string {
	var errBuf bytes.Buffer

	errBuf.WriteString("Invalid search string '")
	errBuf.WriteString(e.search)
	errBuf.WriteString("'")
	return errBuf.String()
}

func (e ErrSnapshotNotFound) Error() string {
	var errBuf bytes.Buffer

	errBuf.WriteString("Snapshot '")
	errBuf.WriteString(e.Name)
	errBuf.WriteString("' not found")

	return errBuf.String()
}

func (e ErrVolumeNotFound) Error() string {
	var errBuf bytes.Buffer

	errBuf.WriteString("Volume '")
	errBuf.WriteString(e.Name)
	errBuf.WriteString("' not found")

	return errBuf.String()
}

func (e ErrConfigFileNotFound) Error() string {
	return "Configuration file not found"
}

func (e ErrInvalidArgs) Error() string {
	return "Invalid arguments passed for the command"
}

func (e ErrMissingFlag) Error() string {
	var errBuf bytes.Buffer

	errBuf.WriteString("Missing --")
	errBuf.WriteString(e.FlagName)

	return errBuf.String()
}

func (e ErrInvalidAttrFormat) Error() string {
	var errBuf bytes.Buffer

	errBuf.WriteString("Invalid attribute format - ")
	errBuf.WriteString(e.str)

	return errBuf.String()
}
