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

package metastore

// ErrAlreadyHaveBlob is returned when an offer is made for a blob diff
// but the blob data is already present and the offer is declined.
type ErrAlreadyHaveBlob struct{}

func (e *ErrAlreadyHaveBlob) Error() string {
	return "Already have blob"
}

// ErrVolumeSetNotFound ....
type ErrVolumeSetNotFound struct{}

func (e *ErrVolumeSetNotFound) Error() string {

	return "Volumeset not found"
}

// ErrVolumeSetAlreadyExists ...
type ErrVolumeSetAlreadyExists struct{}

func (e *ErrVolumeSetAlreadyExists) Error() string {
	return "Volumeset already exists"
}

// ErrSnapshotNotFound ...
type ErrSnapshotNotFound struct{}

func (e *ErrSnapshotNotFound) Error() string {
	return "Snapshot not found"
}

// ErrBranchNotFound ...
type ErrBranchNotFound struct{}

func (e *ErrBranchNotFound) Error() string {
	return "Branch not found in volumeset"
}

// ErrSnapshotImportMismatch ...
type ErrSnapshotImportMismatch struct{}

func (e *ErrSnapshotImportMismatch) Error() string {
	return "can not import snapshot because of mismatching tip"
}

// ErrVolumeNotFound ..
type ErrVolumeNotFound struct{}

func (e *ErrVolumeNotFound) Error() string {
	return "Volume not found"
}

var (
	_ error = &ErrAlreadyHaveBlob{}
	_ error = &ErrVolumeSetNotFound{}
	_ error = &ErrVolumeSetAlreadyExists{}
	_ error = &ErrSnapshotNotFound{}
	_ error = &ErrBranchNotFound{}
	_ error = &ErrSnapshotImportMismatch{}
	_ error = &ErrVolumeNotFound{}
)
