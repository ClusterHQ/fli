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
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"time"

	"github.com/ClusterHQ/go/meta/blob"
	"github.com/ClusterHQ/go/meta/volumeset"
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

func init() {
	gob.Register(DownloadToken{})
	gob.Register(UploadToken{})
}

// String ...
func (s Server) String() string {
	return string(s)
}

// String ...
func (k Key) String() string {
	return string(k)
}

// encrypt encrypts a token
// Note: Exact method to be determined
func encrypt(token interface{}) ([]byte, error) {
	var bbuf bytes.Buffer
	enc := gob.NewEncoder(&bbuf)
	err := enc.Encode(&token)
	if err != nil {
		return bbuf.Bytes(), err
	}
	err = enc.Encode(&token)
	return bbuf.Bytes(), err
}

// decrypt decrypts a token
func decrypt(encrypted []byte) (interface{}, error) {
	enc := gob.NewDecoder(bytes.NewBuffer(encrypted))
	var token interface{}
	err := enc.Decode(&token)
	return token, err
}

// toKey is a helper function that converts a token to a key
func toKey(server Server, token interface{}) (Key, error) {
	var (
		bbuf bytes.Buffer
		key  Key
	)

	// Encrypt
	encrypted, err := encrypt(token)
	if err != nil {
		return key, err
	}

	// Add server to key
	enc := gob.NewEncoder(&bbuf)
	err = enc.Encode(server)
	if err != nil {
		return key, err
	}

	// Add encrypted token to key
	err = enc.Encode(encrypted)
	if err != nil {
		return key, err
	}

	// To network transferable format
	return Key(base64.StdEncoding.EncodeToString(bbuf.Bytes())), nil
}

// ToKey transforms an upload token to a key
func (token UploadToken) ToKey(server Server) (Key, error) {
	return toKey(server, token)
}

// ToKey transforms a download token to a key
func (token DownloadToken) ToKey(server Server) (Key, error) {
	return toKey(server, token)
}

func expired(t time.Time) bool {
	return t.Before(time.Now())
}

// Expired returns true if the token has expired
func (token UploadToken) Expired() bool {
	return expired(token.ExpirationTime)
}

// Expired returns true if the token has expired
func (token DownloadToken) Expired() bool {
	return expired(token.ExpirationTime)
}

// Token extracts the original token from a key
func (k Key) Token() (interface{}, error) {
	// Converts key to its original format
	bbuf, err := base64.StdEncoding.DecodeString(k.String())
	if err != nil {
		return nil, err
	}

	// Extract server
	var server Server
	dec := gob.NewDecoder(bytes.NewBuffer(bbuf))
	err = dec.Decode(&server)
	if err != nil {
		return nil, err
	}

	// Extract encrypted token
	var encrypted []byte
	err = dec.Decode(&encrypted)
	if err != nil {
		return nil, err
	}

	// Decrypt token
	return decrypt(encrypted)
}

// Server extracts the server information from a key
func (k Key) Server() (Server, error) {
	var server Server
	// Key to its original format
	bbuf, err := base64.StdEncoding.DecodeString(k.String())
	if err != nil {
		return server, err
	}

	// Extract server
	dec := gob.NewDecoder(bytes.NewBuffer(bbuf))
	err = dec.Decode(&server)
	return server, err
}

// Light wrappers to hide the expiration time stuff

// NewUploadToken returns a new upload token
func NewUploadToken(vsid volumeset.ID, b blob.ID, expiresInSecond time.Duration) UploadToken {
	return UploadToken{vsid, b, time.Now().Add(expiresInSecond * time.Second)}
}

// NewDownloadToken returns a new upload token
func NewDownloadToken(base, target blob.ID, expiresInSecond time.Duration) DownloadToken {
	return DownloadToken{base, target, time.Now().Add(expiresInSecond * time.Second)}
}
