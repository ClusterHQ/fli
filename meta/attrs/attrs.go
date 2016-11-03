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

package attrs

import (
	"strings"
)

const (
	// Keys known to CHQ
	// TODO: Block user keys that start with $$$CHQ$$$

	// Description is the key for descriptions used by volume set and snapshot
	Description = "$$$CHQ$$$DESC"

	// Name is the key for names used by volume set and snapshot
	Name = "$$$CHQ$$$NAME"

	// Prefix is the key for prefix used by volume set
	Prefix = "$$$CHQ$$$PREFIX"
)

type (
	// Attrs is collection of non-semantic metadata about an object
	Attrs map[string]string
)

// Copy ...
func (a Attrs) Copy() Attrs {
	clone := make(Attrs, len(a))
	for k, v := range a {
		clone[k] = v
	}
	return clone
}

func (a Attrs) matches(key string, value string) bool {
	v, ok := a[key]
	if !ok {
		return false
	}

	return v == value
}

// Matches return whether if attributes a contains all attribut pair in b.
func (a Attrs) Matches(b Attrs) bool {
	for k, v := range b {
		if !a.matches(k, v) {
			return false
		}
	}

	return true
}

// Contains return if the attributes, key or value, contains a substring s.
func (a Attrs) Contains(s string) bool {
	for k, v := range a {
		if strings.Contains(k, s) || strings.Contains(v, s) {
			return true
		}
	}

	return false
}

// GetKey returns the value of the given key if it exists; returns "" if the key doesn't exist
// Note: The given key is removed from Attrs
func (a Attrs) GetKey(k string) string {
	v, exist := a[k]
	if exist {
		delete(a, k)
	}

	return v
}

// SetKey sets the value of the given key if the value is not emoty; do nothing otherwise
func (a Attrs) SetKey(k string, v string) {
	if len(strings.TrimSpace(v)) != 0 {
		a[k] = v
	}
}
