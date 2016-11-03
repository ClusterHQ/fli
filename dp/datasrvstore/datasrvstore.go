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

package datasrvstore

type (
	// Server is the in memory version of the meta information of a data server
	Server struct {
		ID      int
		URL     string
		Current bool
	}

	// Store is the interface for accessing data servers from a persisted storage
	Store interface {
		// Add adds a new server
		Add(*Server) (*Server, error)

		// Get returns a server
		Get(id int) (*Server, error)

		// All returns all servers
		// Note: The return is expected to be sorted(ASC) by the ID
		All() ([]*Server, error)

		// SetCurrent sets the server identified by the given ID to current
		SetCurrent(id int) error

		// GetCurrent returns the server marked as current
		GetCurrent() (*Server, error)
	}
)
