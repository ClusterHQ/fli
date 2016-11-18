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

package token

import (
	"time"

	"github.com/ClusterHQ/fli/meta/blob"
	"github.com/ClusterHQ/fli/meta/snapshot"
	"github.com/ClusterHQ/fli/meta/volumeset"
)

type (
	// Type represents the token type.
	Type int

	// Server is data server's name or IP that a client connects to
	Server string

	// Key is a network transferable version of a token; it has two parts:
	// 1. Data server's IP address or name
	// 2. Encrypted UploadToken/DownloadToken
	Key string

	// Note: These structures might come in useful as internal implementation
	// details for the transfer token, or they may not.  Consider them hints about
	// constraints on the design for the tokens.  At least some of these fields
	// will need to be recoverable by the data plane and data layer from the
	// tokens.
	//
	// This information is left here because the tokens themselves, as expressed in
	// the public interface of the data layer, however, is merely an opaque byte
	// sequence which is not very informative and the use of which is not very
	// discoverable.

	// UploadToken contains parameters which allow the upload of a single blob diff
	// which can be applied onto a pre-negotiated blob on a particular remote data
	// layer server to create a blob which contains the data for a particular
	// pre-negotiated snapshot.
	// Note: The fields are exported because it is need for GOB encoding/decoding
	UploadToken struct {
		// VolumeSetID is the volume set ID the blob belongs to
		VolSetID volumeset.ID

		// SnapshotID is the snapshot ID the blob belongs to
		SnapshotID snapshot.ID

		// BaseBlobID identifies the blob on the receiving side
		// where the diff can be applied.  This doesn't change the sending
		// side's behavior but the receiving side needs to know which blob to
		// apply the diff to.
		BaseBlobID blob.ID

		// ExpirationTime lets data layer reject stale uploads cheaply (without
		// talking to data plane, without actually accepting the blob diff).
		// An optimization / some minor protection against a DoS.
		//
		// Data layer and data plane should have sync'd clocks.
		ExpirationTime time.Time
	}

	// DownloadToken contains parameters which all the download of a single blob
	// diff which can be applied onto a pre-negotiated blob on the local data layer
	// to create a blob which contains the data for a particular pre-negotiated
	// snapshot.
	DownloadToken struct {
		// RemoteBaseBlobID identifies the blob on the sending side on which
		// the diff will be based.  This doesn't change the receiving side's
		// behavior but the sending side needs to know this value to generate
		// the diff.
		RemoteBaseBlobID blob.ID

		// RemoteTargetBlobID identifies the blob on the sending side which
		// will be the target of the diff.  This doesn't change the receiving
		// side's behavior but the sending side needs to know this value to
		// generate the diff.
		RemoteTargetBlobID blob.ID

		// ExpirationTime lets data layer reject stale downloads cheaply.
		ExpirationTime time.Time
	}
)

const (
	// Upload represents upload token.
	Upload Type = iota
	// Download represents download token.
	Download
)

// String ...
func (s Server) String() string {
	return string(s)
}

// String ...
func (k Key) String() string {
	return string(k)
}

// NewUploadToken returns a new upload token
func NewUploadToken(
	vsid volumeset.ID,
	ssid snapshot.ID,
	b blob.ID,
	expiresInSecond time.Duration,
) UploadToken {
	return UploadToken{
		VolSetID:       vsid,
		SnapshotID:     ssid,
		BaseBlobID:     b,
		ExpirationTime: time.Now().Add(expiresInSecond * time.Second),
	}
}

// NewDownloadToken returns a new upload token
func NewDownloadToken(
	base,
	target blob.ID,
	expiresInSecond time.Duration,
) DownloadToken {
	return DownloadToken{
		RemoteBaseBlobID:   base,
		RemoteTargetBlobID: target,
		ExpirationTime:     time.Now().Add(expiresInSecond * time.Second),
	}
}
