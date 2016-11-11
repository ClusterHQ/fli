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

package cauthn

import (
	"errors"
	"io/ioutil"
	"net/http"
	"strings"
	"bytes"
)

const (
	vhutHeaderName = "VH-Authenticate"
)

var (
	// ErrVHUTMissing indicates that the VHUT header is missing
	ErrVHUTMissing = errors.New("VH-Authenticate header absent")
	// ErrVHUTInvalidFormat indicates that the VHUT was not formatted properly
	ErrVHUTInvalidFormat = errors.New("Invalid VHUT format")
)

// VHUT represents a VH User Token
type VHUT struct {
	vhut string
}

// InitFromString initializes member vhut from a string.
func (v *VHUT) InitFromString(token string) error {
	v.vhut = token
	if !v.validFormat() {
		return ErrVHUTInvalidFormat
	}
	return nil
}

func (v *VHUT) validFormat() bool {
	comps := strings.Split(v.vhut, "|")
	if len(comps) != 5 || comps[0] != "V1" {
		return false
	}
	return true
}

// InitFromFile initializes member vhut from contents of the file.
// This will most likley be invoked by a CLI program that stores the VHUT in a file
// Returns non-nil error if the file doesn't exist, can't be read or the data doesn't
// match the VHUT format. It is to be noted that this method doesn't validate the token
func (v *VHUT) InitFromFile(filename string) error {
	bytes, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	bytes = v.removeBomMarker(bytes)
	
	return v.InitFromString(strings.TrimSpace(string(bytes)))
}

// UpdateRequest adds custom HTTP request header "VH-Authenticate" with vhut as value.
// This will most likely be invoked by a client program before making a HTTP(S) request.
// Updates the value if header "VH-Authenticate" already exists in the request r.
// Returns non-nil error if vhut is not initialized.
func (v *VHUT) UpdateRequest(r *http.Request) error {
	if v.vhut == "" {
		return errors.New("VHUT not initialized")
	}
	r.Header.Set(vhutHeaderName, v.vhut)
	return nil
}

// removeBomMarker removes leading and trailing UTF8 BomMarker from the specified content.
func (v *VHUT) removeBomMarker(content []byte) []byte {
	return bytes.Trim(content,"\xef\xbb\xbf")
}
