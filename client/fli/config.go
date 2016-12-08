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

package fli

import (
	"io/ioutil"
	"os"

	"github.com/go-yaml/yaml"
)

type (
	// ConfigParams ...
	ConfigParams struct {
		SQLMdsCurrent string `yaml:"current,omitempty"`
		SQLMdsInitial string `yaml:"initial,omitempty"`
		FlockerHubURL string `yaml:"url,omitempty"`
		AuthTokenFile string `yaml:"token,omitempty"`
		Zpool         string `yaml:"zpool,omitempty"`
		Version       string `yaml:"version,omitempty"`
	}

	// Config ...
	Config struct {
		filename string
	}
)

// Exists ...
func (y *Config) Exists() bool {
	if _, err := os.Stat(y.filename); os.IsNotExist(err) {
		return false
	}

	return true
}

// CreateFile creates a configuration file
func (y *Config) CreateFile() error {
	cfg := ConfigParams{}
	err := y.UpdateConfig(cfg)
	if err != nil {
		return err
	}

	return nil
}

// NewConfig creates a YAML based file configuration
func NewConfig(filepath string) *Config {
	y := Config{filename: filepath}
	return &y
}

// ReadConfig reads the configuration
func (y *Config) ReadConfig() (ConfigParams, error) {
	cfg := ConfigParams{}
	if !y.Exists() {
		// TODO If it doesn't exist look at temp file to be safe
		return cfg, &ErrConfigFileNotFound{}
	}

	buf, err := ioutil.ReadFile(y.filename)
	if err != nil {
		return cfg, err
	}

	err = yaml.Unmarshal(buf, &cfg)
	if err != nil {
		return cfg, err
	}

	return cfg, nil
}

// UpdateConfig updates on the on-disk configuration with the in-memory configuration updates
func (y *Config) UpdateConfig(cfg ConfigParams) error {
	buf, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}

	// write to temp config file
	if err := ioutil.WriteFile(y.filename, buf, 0644); err != nil {
		return err
	}

	return nil
}
