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

package testutils

import (
	"io/ioutil"
	"net"
	"net/http"
	"regexp"
	"strconv"
)

/* Export constants and test functions that we might need */

//TokUploadResp expected reply from server when uploading token.
const TokUploadResp string = "Token uploading is not yet allowed."

//TokDownloadResp expected reply from server when downloading token.
const TokDownloadResp string = "Token downloading is not yet allowed."

//DownloadResp expected reply from server when downloading
const DownloadResp string = "Downloading is not yet allowed."

// GetAndMatch is a function for testing.
// It fetches the url and compares response against expected output.
func GetAndMatch(url string, expOut string) (bool, error) {
	var found = false
	var err error
	var resp *http.Response
	var respData []byte

	resp, err = http.Get(url)
	if err != nil {
		goto bail
	}
	defer resp.Body.Close()

	respData, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		goto bail
	}

	found, err = regexp.Match(expOut, respData)
	//goto bail

bail:
	return found, err
}

// GetFreePort gest an unused port on localhost and
// returns it as string with a preceding colon.
// For example - ":8080"
func GetFreePort() (string, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return "", err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return "", err
	}
	defer l.Close()

	return ":" + strconv.Itoa(l.Addr().(*net.TCPAddr).Port), nil
}
