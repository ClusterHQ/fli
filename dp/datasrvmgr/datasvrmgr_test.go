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

package datasrvmgr_test

import (
	"testing"

	"github.com/ClusterHQ/fli/dp/datasrvmgr"
	"github.com/ClusterHQ/fli/dp/inmemdatasrvstore"
	"github.com/stretchr/testify/require"
)

// TestManager ...
func TestManager(t *testing.T) {
	mgr := datasrvmgr.New(inmemdatasrvstore.New())

	// no data server available
	_, err := mgr.GetNextAvailable()
	require.Error(t, err)

	// Add servers
	err = mgr.AddServer("srv 1")
	require.NoError(t, err)

	err = mgr.AddServer("srv 2")
	require.NoError(t, err)

	err = mgr.AddServer("srv 2")
	require.NoError(t, err)

	lastAddedURL := "last added"
	err = mgr.AddServer(lastAddedURL)
	require.NoError(t, err)

	// get next available, should return the last added
	id, err := mgr.GetNextAvailable()
	require.NoError(t, err)
	url, err := mgr.GetURL(id)
	require.NoError(t, err)
	require.Equal(t, lastAddedURL, url)

	// Get ID of non next available entries
	url, err = mgr.GetURL(2)
	require.NoError(t, err)
	require.NotEqual(t, lastAddedURL, url)
}
