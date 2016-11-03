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

package securefilepath_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClusterHQ/fli/dl/testutils"
	"github.com/ClusterHQ/fli/securefilepath"
)

type ByPath []securefilepath.SecureFilePath

func (s ByPath) Len() int {
	return len(s)
}

func (s ByPath) Less(i, j int) bool {
	return strings.Compare(s[i].Path(), s[j].Path()) == -1
}

func (s ByPath) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// TestNew verifies that New rejects any potentially ambiguously formed or
// non-canonical paths and accepts only canonical, absolute paths.
func TestNew(t *testing.T) {
	filepath, err := securefilepath.New("non-absolute")
	assert.NotEqual(t, err, nil, "Non-absolute path should produce an error.")

	filepath, err = securefilepath.New("/absolute/../with/dots")
	assert.NotEqual(t, err, nil, "Path with .. should produce error.")

	filepath, err = securefilepath.New("//weird/absolute")
	assert.NotEqual(t, err, nil, "Path with duplicate / should produce error.")

	filepath, err = securefilepath.New("/superfluous/./directory/reference")
	assert.NotEqual(t, err, nil, "Path with . segment should produce error.")

	filepath, err = securefilepath.New("/valid/absolute/path")
	assert.Equal(t, err, nil, "Clean absolute path should not produce error.")
	assert.Equal(t, filepath.Path(), "/valid/absolute/path")
}

// TestParent verifies that SecureFilePath.Parent returns a path representing
// the parent path of the given path.
func TestParent(t *testing.T) {
	root, err := securefilepath.New("/")
	require.NoError(t, err)

	a, err := root.Child("a")
	require.NoError(t, err)

	b, err := a.Child("b")
	require.NoError(t, err)

	c, err := b.Child("c")
	require.NoError(t, err)

	assert.Equal(t, b, c.Parent())
	assert.Equal(t, a, b.Parent())
	assert.Equal(t, root, a.Parent())
	assert.Equal(t, root, root.Parent())
}

// TestChild verifies that SecureFilePath.Child allows access only to any paths
// which are children of the parent path (disregarding effects of symbolic
// links).
func TestChild(t *testing.T) {
	filepath, err := securefilepath.New("/parent")
	if err != nil {
		t.Errorf("%s", err)
		return
	}

	child, err := filepath.Child("/not-child")
	assert.NotEqual(
		t, err, nil,
		fmt.Sprintf(
			"Absolute path for child segment should produce error, not '%s'.",
			child))

	child, err = filepath.Child("multiple/segments")
	assert.NotEqual(t, err, nil, "Child argument with multiple segments should produce an error.")

	child, err = filepath.Child("")
	assert.NotEqual(t, err, nil, "Child argument with no segments should produce an error.")

	child, err = filepath.Child("..")
	assert.NotEqual(t, err, nil, "Special parent segment should produce an error.")

	child, err = filepath.Child("valid-child")
	assert.Equal(t, err, nil, "Valid child should return a new SecureFilePath.")

	anotherChild, err := securefilepath.New("/parent/valid-child")
	if err != nil {
		t.Errorf("%s", err)
		return
	}
	assert.Equal(t, child, anotherChild)

	// Nul is not allowed in paths so a child segment containing a nul
	// cannot result in a valid path.
	notChild, err := filepath.Child("hello\000world")
	assert.Nil(t, notChild)
	assert.Error(t, err)
}

// TestPath verifies that SecureFilePath.Path returns a string representing the path.
func TestPath(t *testing.T) {
	for _, path := range []string{"/parent", "/parent/child/grandchild"} {
		filepath, _ := securefilepath.New(path)
		assert.Equal(t, filepath.Path(), path, "Wrong path returned.")
	}
}

// touch creates a file at the given path.
func touch(p securefilepath.SecureFilePath) error {
	f, err := os.Create(p.Path())
	if err != nil {
		return err
	}
	return f.Close()
}

// setupChildren creates and returns a directory with a directory ("dirchild")
// and a normal file ("filechild") as children.
func setupChildren() (securefilepath.SecureFilePath, securefilepath.SecureFilePath, securefilepath.SecureFilePath, error) {
	path, _ := ioutil.TempDir("", "")
	root, _ := securefilepath.New(path)
	os.Mkdir(root.Path(), os.ModePerm)
	dirChild, _ := root.Child("dirchild")
	os.Mkdir(dirChild.Path(), os.ModePerm)
	fileChild, _ := root.Child("filechild")
	err := touch(fileChild)
	if err != nil {
		return nil, nil, nil, err
	}
	return root, dirChild, fileChild, nil
}

// TestChildrenEmpty verifies that SecureFilePath.Children returns an empty
// slice when called on a path representing a directory with no children.
func TestChildrenEmpty(t *testing.T) {
	_, dirChild, _, err := setupChildren()
	if err != nil {
		t.Errorf("%s", err)
		return
	}
	childChildren, err := dirChild.Children()
	if err != nil {
		t.Errorf("%s", err)
		return
	}
	assert.Equal(t, []securefilepath.SecureFilePath{}, childChildren, "Should be empty.")
}

// TestChildren verifies that SecureFilePath.Children returns a slice of
// SecureFilePath with one element for each child of the path represented.
func TestChildren(t *testing.T) {
	root, dirChild, fileChild, err := setupChildren()
	if err != nil {
		t.Errorf("%s", err)
		return
	}
	actualChildren, err := root.Children()
	if err != nil {
		t.Errorf("%s", err)
		return
	}
	expectedChildren := ByPath{dirChild, fileChild}
	sort.Sort(expectedChildren)

	actualSorted := ByPath(actualChildren)
	sort.Sort(actualSorted)

	assert.Equal(t, expectedChildren, actualSorted, "Wrong children returned.")
}

// TestChildrenOfFile verifies that SecureFilePath.Children returns an error when
// used on a path which refers to a regular file.
func TestChildrenOfFile(t *testing.T) {
	_, _, fileChild, err := setupChildren()
	if err != nil {
		t.Errorf("%s", err)
		return
	}
	_, err = fileChild.Children()
	assert.Error(t, err, "Shouldn't be able to list file.")
}

// TestChildrenOfNonExistant verifies that SecureFilePath.Children returns an
// error when used on a path which does not refer to a file which exists.
func TestChildrenOfNonExtant(t *testing.T) {
	nonexistent, _ := securefilepath.New("/nosuchpathIreallyhopereallyreally")
	_, err := nonexistent.Children()
	assert.Error(t, err, "Shouldn't be able to list non-existent directory.")
}

// TestChildrenWithoutPermission verifies that SecureFilePath.Children returns
// an error when used on a path to which we do not have read permission.
func TestChildrenWithoutPermission(t *testing.T) {
	if testutils.RunningAsRoot() {
		t.Skip("Skipped because test running as root.")
	}

	root, _, _, err := setupChildren()
	if err != nil {
		t.Errorf("%s", err)
		return
	}
	err = os.Chmod(root.Path(), 0)
	if err != nil {
		t.Errorf("%s", err)
		return
	}
	defer os.Chmod(root.Path(), os.ModePerm)
	_, err = root.Children()
	assert.Error(t, err, "Shouldn't be able to list unreadable dir.")
}

// setupExists creates a scratch space in the filesytem for a test of Exists to
// play with.
func setupExists() (securefilepath.SecureFilePath, error) {
	parent, err := ioutil.TempDir("", "testexists")
	if err != nil {
		return nil, err
	}
	parentDir, err := securefilepath.New(parent)
	if err != nil {
		return nil, err
	}
	return parentDir, nil
}

// TestExists verifies that SecureFilePath.Exists returns false if no such file
// exists at the filesystem path it represents.
func TestExistsFalse(t *testing.T) {
	parent, err := setupExists()
	if err != nil {
		t.Errorf("%s", err)
		return
	}

	filepath, err := parent.Child("nonexistent")
	if err != nil {
		t.Errorf("%s", err)
		return
	}
	exists, err := filepath.Exists()
	if err != nil {
		t.Errorf("%s", err)
		return
	}
	require.False(t, exists, "%s does not exist but Exists() says it does.", filepath.Path())
}

// TestExists verifies that SecureFilePath.Exists return true if a file does
// exist at the filesystem path it represents.
func TestExistsTrue(t *testing.T) {
	parent, err := setupExists()
	if err != nil {
		t.Errorf("%s", err)
		return
	}

	filepath, err := parent.Child("extant")
	if err != nil {
		t.Errorf("%s", err)
		return
	}
	err = touch(filepath)
	if err != nil {
		t.Errorf("%s", err)
		return
	}

	exists, err := filepath.Exists()
	if err != nil {
		t.Errorf("%s", err)
		return
	}
	require.True(t, exists, "%s does exist but Exists() says it does not.", filepath.Path())
}

// TestExists verifies that SecureFilePath.Exists returns an error if the
// underlying filesystem returns an error when checking for existance.
func TestExistsError(t *testing.T) {
	if testutils.RunningAsRoot() {
		t.Skip("Skipped because test running as root.")
	}

	parent, err := setupExists()
	if err != nil {
		t.Errorf("%s", err)
		return
	}

	// An error encountered trying to determine whether a path has a
	// corresponding file is propagated to the caller.
	defer os.Chmod(parent.Path(), os.FileMode(0700))
	os.Chmod(parent.Path(), os.FileMode(0))

	filepath, err := parent.Child("nonexistent")
	if err != nil {
		t.Errorf("%s", err)
		return
	}

	exists, err := filepath.Exists()
	require.Error(t, err, "Exists() should have failed with a permission denied error, instead succeeded with %v", exists)
	require.True(t, os.IsPermission(err), "Exists() should have failed with a permission denied error, instead failed with %v", err)
}

// TestBase verifies that SecureFilePath.Base returns the "base name" (the last
// path segment; hierarchically, the most inferior path segment) of the path as
// a string.
func TestBase(t *testing.T) {
	for _, testcase := range []struct{ input, expected string }{
		{"/", ""},
		{"/parent", "parent"},
		{"/parent/foo", "foo"},
	} {
		path, err := securefilepath.New(testcase.input)
		if err != nil {
			t.Errorf("Failed to construct SecureFilePath %v: %v", testcase.input, err)
		}
		if path.Base() != testcase.expected {
			t.Errorf("%v != %v", path.Base(), testcase.expected)
		}
	}
}
