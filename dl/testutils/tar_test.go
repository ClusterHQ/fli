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

package testutils_test

import (
	"path/filepath"
	"testing"

	"github.com/ClusterHQ/fli/dl/testutils"
)

func TestTar(t *testing.T) {
	// relative path to samples
	tarFile := "testdata/sample1.tgz"
	wd := ",scratch"

	tarDir := filepath.Join(wd, "sample1")
	err := testutils.Untar(tarFile, wd)
	if err != nil {
		t.Fatal(err)
	}

	tarFile = filepath.Join(wd, "sample1.tgz")
	err = testutils.Tar(tarDir, tarFile)
	if err != nil {
		t.Fatal(err)
	}
}
