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

package token_test

import (
	"github.com/ClusterHQ/fli/dl/token"
	"github.com/ClusterHQ/fli/meta/blob"

	"github.com/ClusterHQ/fli/meta/volumeset"
	"reflect"
	"testing"
	"time"
)

func TestUploadToken(t *testing.T) {
	server := token.Server("1.2.3.4")
	vsid := volumeset.NewRandomID()
	bid := blob.NewID("blob id")
	originalToken := token.NewUploadToken(vsid, bid, 1)

	// Generate a key from token
	key, err := originalToken.ToKey(server)
	if err != nil {
		t.Error(err)
	}

	// Key to token, compare and verify
	expectedToken, err := key.Token()
	if err != nil {
		t.Error(err)
	}

	// Make sure it is an upload token
	uploadToken, ok := expectedToken.(token.UploadToken)
	if !ok {
		t.Error("Failed to convert key to upload token")
	}

	// Verify volume set id
	if reflect.DeepEqual(uploadToken.VolSetID, vsid) == false {
		t.Errorf("Got wrong token back, volume set id = %v, expected = %v",
			uploadToken.VolSetID.String(), vsid.String())
	}

	// Verify base blob id
	if reflect.DeepEqual(uploadToken.BaseBlobID, bid) == false {
		t.Errorf("Got wrong token back, base id = %v, expected = %v",
			uploadToken.BaseBlobID.String(), bid.String())
	}

	// Verify server
	serverExpected, err := key.Server()
	if err != nil || reflect.DeepEqual(server, serverExpected) == false {
		t.Errorf("Got wrong server %v, expected = %v", serverExpected, server)
	}

	// Verify expiration time
	if uploadToken.Expired() {
		t.Error("Token expired too early")
	}
	time.Sleep(1 * time.Second)
	if !uploadToken.Expired() {
		t.Error("Token expired too late")
	}
}

func TestDownloadToken(t *testing.T) {
	server := token.Server("1.2.3.4")
	base := blob.NewID("base")
	target := blob.NewID("target")
	originalToken := token.NewDownloadToken(base, target, 1)

	// Generate a key from token
	key, err := originalToken.ToKey(server)
	if err != nil {
		t.Error(err)
	}

	// Key to token, compare and verify
	expectedToken, err := key.Token()
	if err != nil {
		t.Error(err)
	}

	// Make sure it is a download token
	downloadToken, ok := expectedToken.(token.DownloadToken)
	if !ok {
		t.Error("Failed to convert key to upload token")
	}

	// Verify blob id
	if reflect.DeepEqual(downloadToken.RemoteBaseBlobID, base) == false {
		t.Errorf("Got wrong token back, blob id = %v, expected = %v",
			downloadToken.RemoteBaseBlobID.String(), base.String())
	}
	if reflect.DeepEqual(downloadToken.RemoteTargetBlobID, target) == false {
		t.Errorf("Got wrong token back, blob id = %v, expected = %v",
			downloadToken.RemoteTargetBlobID.String(), target.String())
	}

	// Verify server
	serverExpected, err := key.Server()
	if err != nil || reflect.DeepEqual(server, serverExpected) == false {
		t.Errorf("Got wrong server %v, expected = %v", serverExpected, server)
	}

	// Verify expiration time
	if downloadToken.Expired() {
		t.Error("Token expired too early")
	}
	time.Sleep(1 * time.Second)
	if !downloadToken.Expired() {
		t.Error("Token expired too late")
	}
}
