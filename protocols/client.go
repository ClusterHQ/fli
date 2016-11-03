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

package protocols

import (
	"crypto/tls"
	"net/http"
)

// VerifyCert ..
const VerifyCert = false

// Client is a wrapper to the http.Client.
type Client struct {
	*http.Client
}

var (
	// TODO: For other configs such as timeout.
	defaultTransport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: !VerifyCert},
	}

	defaultClient = &Client{
		Client: &http.Client{Transport: defaultTransport},
	}
)

// GetClient returns a client to be used to send requests. Our code
// should always this function instead of directly creating http.Client{}.
func GetClient() *Client {
	return defaultClient
}

// Do ...
func (c *Client) Do(req *http.Request) (*http.Response, error) {
	// TODO: Commenting out for now until we have a way to pass different logger.
	//       Right now it prints out too many send messages on CLI's screen
	//       start := time.Now()

	resp, err := c.Client.Do(req)
	if err != nil {
		return nil, err
	}

	// elasped := time.Now().Sub(start)
	// logStr := []string{
	// 	"[HTTP-Send]",
	// 	req.Method,
	// 	req.URL.String(),
	// 	elasped.String(),
	// 	resp.Status,
	// 	strconv.Itoa(int(resp.ContentLength)),
	// }
	// log.Printf(strings.Join(logStr, " "))
	return resp, err
}
