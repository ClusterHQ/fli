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

import "github.com/ClusterHQ/go/meta/volumeset"

const (
	// HTTPPathUsage quries the usage given a list of volumesets.
	HTTPPathUsage = "usage"
)

type (
	// ReqUsage ..
	ReqUsage struct {
		Volumesets []volumeset.ID `json:"volumesets"`
	}

	// RespUsage ..
	RespUsage struct {
		TotalBytes string `json:"total_bytes"`
	}
)
