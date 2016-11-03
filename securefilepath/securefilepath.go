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

// Package securefilepath offers operations on filesystem paths and files in a
// way which offers additional security properties beyond simple string
// manipulation.
//
// The additional security comes primarily from the SecureFilePath.Child method
// which allows the construction of a new path which is a direct child of an
// existing path, even using untrusted input.  Any input which would create a
// path which is not a child results in an error instead.
package securefilepath

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type internalPath struct {
	path string
}

// SecureFilePath presents functionality related to paths and the files they
// refer to.
type SecureFilePath interface {
	// Create a child path, given a segment (e.g. "child.txt"). Non-segments
	// like "../child.txt" or "/path/child.txt" will result in an error.
	Child(string) (SecureFilePath, error)

	// Return the full path as a string.
	Path() string

	// Parent returns the path to the parent of this path or this path
	// itself if it is the root.
	Parent() SecureFilePath

	// Base returns the basename (the last segment) of the path.
	Base() string

	// Return the children of the directory.
	Children() ([]SecureFilePath, error)

	// Exists determines whether a file exists at this path at the time of
	// the call.
	Exists() (bool, error)
}

// New creates a new SecureFilePath from an absolute, sanitized path.
func New(path string) (SecureFilePath, error) {
	sanitized := filepath.Clean("/" + path)

	if sanitized != path {
		return nil, fmt.Errorf(
			"Can only create SecureFilePath from a clean, absolute path, not '%s'",
			path)
	}
	return &internalPath{sanitized}, nil
}

func (p *internalPath) Child(segment string) (SecureFilePath, error) {
	if filepath.IsAbs(segment) {
		return nil, fmt.Errorf(
			"Segment must be a single path segment, not '%s'.",
			segment)
	}
	if strings.ContainsRune(segment, '\000') {
		return nil, fmt.Errorf(
			"Segment must not contain nul but %#v does.",
			segment)
	}
	candidate := filepath.Join(p.path, segment)
	sanitized := filepath.Clean(candidate)
	if sanitized != candidate {
		return nil, fmt.Errorf(
			"Segment '%s' does not produce a child of '%s'",
			segment, p.path)
	}
	if filepath.Dir(candidate) != p.path {
		return nil, fmt.Errorf(
			"Segment '%s' does not produce a direct child of '%s'",
			segment, p.path)
	}
	return New(candidate)
}

func (p *internalPath) Parent() SecureFilePath {
	return &internalPath{
		path: path.Dir(p.path),
	}
}

func (p *internalPath) Path() string {
	return p.path
}

// Base returns the last element of the path, commonly called the "basename".
func (p *internalPath) Base() string {
	if p.path == "/" {
		return ""
	}
	return path.Base(p.path)
}

func (p *internalPath) Children() ([]SecureFilePath, error) {
	info, err := os.Stat(p.path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("'%s' is not a directory", p.path)
	}

	dir, err := os.Open(p.path)
	if err != nil {
		return nil, err
	}
	contents, err := dir.Readdirnames(0)
	if err != nil {
		return nil, err
	}
	result := make([]SecureFilePath, len(contents))
	for i, filename := range contents {
		child, _ := p.Child(filename)
		result[i] = child
	}
	return result, nil
}

func (p *internalPath) Exists() (bool, error) {
	_, err := os.Stat(p.Path())
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
