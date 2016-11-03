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

package inmemdatasrvstore

import (
	"errors"
	"sort"
	"sync"

	"github.com/ClusterHQ/go/dp/datasrvstore"
)

type (
	// Store ...
	Store struct {
		lock    *sync.Mutex
		servers map[int]*datasrvstore.Server
	}
)

var (
	_ datasrvstore.Store = &Store{}
)

// New ...
func New() *Store {
	return &Store{
		lock:    &sync.Mutex{},
		servers: make(map[int]*datasrvstore.Server),
	}
}

// Add ...
func (s *Store) Add(srv *datasrvstore.Server) (*datasrvstore.Server, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	for _, s := range s.servers {
		if s.URL == srv.URL {
			return nil, errors.New("Server already exists")
		}
	}

	id := len(s.servers) + 1
	s.servers[id] = &datasrvstore.Server{
		ID:      id,
		URL:     srv.URL,
		Current: false,
	}
	return &datasrvstore.Server{
		ID:      id,
		URL:     srv.URL,
		Current: false,
	}, nil
}

// Get ...
func (s *Store) Get(id int) (*datasrvstore.Server, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	srv, present := s.servers[id]
	if !present {
		return nil, errors.New("id doesn't exists")
	}

	return &datasrvstore.Server{
		ID:      srv.ID,
		URL:     srv.URL,
		Current: srv.Current,
	}, nil
}

type srvSortedByID []*datasrvstore.Server

func (s srvSortedByID) Len() int {
	return len(s)
}

func (s srvSortedByID) Less(i, j int) bool {
	return s[i].ID < s[j].ID
}

func (s srvSortedByID) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// All ...
func (s *Store) All() ([]*datasrvstore.Server, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	var srvs []*datasrvstore.Server
	for _, srv := range s.servers {
		srvs = append(srvs, &datasrvstore.Server{
			ID:      srv.ID,
			URL:     srv.URL,
			Current: srv.Current,
		})
	}
	orderedSrvs := srvSortedByID(srvs)
	sort.Sort(orderedSrvs)

	return srvs, nil
}

// SetCurrent ...
func (s *Store) SetCurrent(id int) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	srv, present := s.servers[id]
	if !present {
		return errors.New("Data server doesn't exists")
	}

	for _, s := range s.servers {
		s.Current = false
	}

	srv.Current = true
	return nil
}

// GetCurrent ...
func (s *Store) GetCurrent() (*datasrvstore.Server, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	for _, s := range s.servers {
		if s.Current == true {
			return &datasrvstore.Server{
				ID:      s.ID,
				URL:     s.URL,
				Current: s.Current,
			}, nil
		}
	}

	return nil, errors.New("No data server is marked as current")
}
