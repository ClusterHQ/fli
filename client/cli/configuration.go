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

package cli

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

// Default file permissions
const filePerms os.FileMode = 0600

type (
	// Config ...
	Config struct {
		SQLiteDBCur  string
		SQLiteDBInit string
		VhubURL      string
		Tokenfile    string
		ZPool        string
	}

	// JSONFileConfig implements the
	JSONFileConfig struct {
		filename string
	}
)

// ConfigIface interface defines APIs to implement CLI configurations
type ConfigIface interface {
	ReadConfig() (Config, error)
	UpdateConfig(Config) error
}

// FileConfigIface interface defines APIs to implement file based configurations.
type FileConfigIface interface {
	Exists() bool
	CreateFile() error
}

var (
	_ ConfigIface     = &JSONFileConfig{}
	_ FileConfigIface = &JSONFileConfig{}
)

// Exists checks if the file exists
func (jfc *JSONFileConfig) Exists() bool {
	if _, err := os.Stat(jfc.filename); os.IsNotExist(err) {
		return false
	}

	return true
}

// CreateFile creates a configuration file
func (jfc *JSONFileConfig) CreateFile() error {
	cfg := Config{}
	err := jfc.UpdateConfig(cfg)
	if err != nil {
		return err
	}

	return nil
}

// NewJSONFileConfig creates a JSON based file configuration
func NewJSONFileConfig(filepath string) *JSONFileConfig {
	js := &JSONFileConfig{filename: filepath}
	return js
}

// ReadConfig reads the configuration
func (jfc *JSONFileConfig) ReadConfig() (Config, error) {
	cfg := Config{}
	if !jfc.Exists() {
		// TODO If it doesn't exist look at temp file to be safe
		return cfg, fmt.Errorf("%s not found", jfc.filename)
	}

	buf, err := ioutil.ReadFile(jfc.filename)
	if err != nil {
		return cfg, err
	}

	err = json.Unmarshal(buf, &cfg)
	if err != nil {
		return cfg, err
	}

	return cfg, nil
}

// UpdateConfig updates on the on-disk configuration with the in-memory configuration updates
func (jfc *JSONFileConfig) UpdateConfig(cfg Config) error {
	var tmpFile = jfc.filename + ".tmp"

	// remove any old temporary file
	os.Remove(tmpFile)

	buf, err := json.Marshal(cfg)
	if err != nil {
		return err
	}

	// write to temp config file
	if err := ioutil.WriteFile(tmpFile, buf, filePerms); err != nil {
		return err
	}

	// atomically switch the files
	if err = os.Rename(tmpFile, jfc.filename); err != nil {
		return err
	}

	return nil
}
