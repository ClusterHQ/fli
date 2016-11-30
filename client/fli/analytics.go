/*
 * Copyright © 2014-2016 ClusterHQ Inc.  All rights reserved.
 *
 * Proprietary Rights Notice:   These files and their contents contain
 * material that is the proprietary intellectual property of ClusterHQ Inc.
 * or its licensors, and are protected under U.S. and international copyright
 * and other intellectual property laws.  Except as expressly permitted by
 * ClusterHQ Inc. in writing, no part of these files may be reproduced,
 * duplicated, disclosed, redistributed or transmitted in any form or by any
 * means known or unknown without the prior written permission of ClusterHQ Inc.
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
