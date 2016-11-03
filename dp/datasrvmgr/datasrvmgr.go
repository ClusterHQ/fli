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

package datasrvmgr

import (
	"sync"

	"github.com/ClusterHQ/fli/dp/datasrvstore"
)

type (
	// Manager manages a data server store and provides an in memory cache of data server's url for quick look ups
	Manager struct {
		store datasrvstore.Store
		cache map[int]string
		lock  *sync.Mutex
	}
)

// New creates a new data server manager
func New(s datasrvstore.Store) *Manager {
	mgr := Manager{
		store: s,
		cache: make(map[int]string),
		lock:  &sync.Mutex{},
	}
	return &mgr
}

// AddServer adds a new data server to the store if it is not already in the store.
// The given data server will be set as 'current' (used for new bushes)
func (mgr *Manager) AddServer(url string) error {
	// lock even while accessing db, may be slow, but this is not a frequent operation
	mgr.lock.Lock()
	defer mgr.lock.Unlock()

	// no need to add if the url already exists
	srvs, err := mgr.store.All()
	if err != nil {
		return err
	}

	for _, s := range srvs {
		if url == s.URL {
			return mgr.store.SetCurrent(s.ID)
		}
	}

	srv, err := mgr.store.Add(&datasrvstore.Server{
		URL: url,
	})
	if err != nil {
		return err
	}

	mgr.cache[srv.ID] = url
	return mgr.store.SetCurrent(srv.ID)
}

// GetURL ...
func (mgr *Manager) GetURL(id int) (string, error) {
	mgr.lock.Lock()
	defer mgr.lock.Unlock()

	url, present := mgr.cache[id]
	if present {
		return url, nil
	}

	// not in cache, go to db
	srv, err := mgr.store.Get(id)
	if err != nil {
		return "", err
	}

	// add to cache
	mgr.cache[srv.ID] = srv.URL
	return srv.URL, nil
}

// GetNextAvailable returns the data server that new bushes are allocated on
func (mgr *Manager) GetNextAvailable() (int, error) {
	srv, err := mgr.store.GetCurrent()
	if err != nil {
		return 0, err
	}

	return srv.ID, nil
}

// GetAllServers returns a list of all data servers
func (mgr *Manager) GetAllServers() ([]*datasrvstore.Server, error) {
	return mgr.store.All()
}
