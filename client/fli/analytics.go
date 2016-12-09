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

import (
	"bytes"
	"crypto/tls"
	"net/http"
	"net/url"

	"github.com/ClusterHQ/fli/errors"
	"github.com/ClusterHQ/fli/protocols"
	"github.com/ClusterHQ/fli/vh/cauthn"
)

// AnalyticsLogger logs analytics related information to FlockerHub
type AnalyticsLogger struct {
	serverURL string
	tokenfile string
	client    *http.Client
}

// NewAnalyticsLogger ...
func NewAnalyticsLogger(server string, tokenfile string) *AnalyticsLogger {
	defaultTransport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: !protocols.VerifyCert},
	}

	defaultClient := &http.Client{Transport: defaultTransport}

	return &AnalyticsLogger{
		serverURL: server,
		tokenfile: tokenfile,
		client:    defaultClient,
	}
}

func (al *AnalyticsLogger) sendRequest(payload []byte) error {
	serverURL, err := url.Parse(al.serverURL)
	if err != nil {
		return err
	}

	// create the analytics request
	u, err := serverURL.Parse(protocols.HTTPPathAnalytics)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(payload))
	if err != nil {
		return err
	}

	// add user token related information
	fhut := &cauthn.VHUT{}
	if err = fhut.InitFromFile(al.tokenfile); err != nil {
		return err
	}

	if err := fhut.UpdateRequest(req); err != nil {
		return err
	}

	httpResp, err := al.client.Do(req)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		return errors.Errorf("Response status %s", httpResp.Status)
	}

	return nil
}

// LogConfig log fli config event
func (al *AnalyticsLogger) LogConfig() error {
	req := "fli config"
	if err := al.sendRequest([]byte(req)); err != nil {
		return err
	}

	return nil
}
