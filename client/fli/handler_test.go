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

package fli_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/ClusterHQ/go/client/fli"
	"github.com/ClusterHQ/go/dl/testutils"
	"github.com/stretchr/testify/suite"
)

type (
	// HandlerSuite ...
	HandlerSuite struct {
		tempDir    string
		cfgFile    string
		mdsCurrent string
		mdsInitial string
		handler    *fli.Handler
		suite.Suite
	}
)

// TestSearcher run the test suite
func TestHandler(t *testing.T) {
	s := HandlerSuite{}
	suite.Run(t, &s)
}

// SetupTest sets up the tests
func (s *HandlerSuite) SetupTest() {
	if !testutils.RunningAsRoot() {
		s.T().Skip("Need zpool 'chq' and SUDO access to run the test")
	}

	var err error

	s.tempDir, err = ioutil.TempDir("", ".fli")
	s.Require().NoError(err, "Failed to create temp dir")
	//s.tempDir = "/tmp/.fli"
	//os.RemoveAll(s.tempDir)
	//os.MkdirAll(s.tempDir, (os.ModeDir | 0755))

	s.mdsCurrent = filepath.Join(s.tempDir, "mds_current")
	s.mdsInitial = filepath.Join(s.tempDir, "mds_initial")
	s.cfgFile = filepath.Join(s.tempDir, "config")

	cfg := fli.NewConfig(s.cfgFile)
	err = cfg.CreateFile()
	s.Require().NoError(err, "Failed to create config file")

	params, err := cfg.ReadConfig()
	s.Require().NoError(err, "Unable to read config file")

	s.handler = fli.NewHandler(params, s.cfgFile, s.mdsCurrent, s.mdsInitial)

	_, err = s.handler.Setup("chq", true, []string{})
	s.Require().NoError(err, "Failed first time setup")
}

func (s *HandlerSuite) TestSetup() {
	var err error

	os.RemoveAll(s.mdsCurrent)
	os.RemoveAll(s.mdsInitial)
	os.RemoveAll(s.cfgFile)

	// first time setup without zpool
	_, err = s.handler.Setup("", false, []string{})
	s.Require().Error(err, "Expected error because no zpool passed")

	// valid setup without force
	_, err = s.handler.Setup("chq", false, []string{})
	s.Require().NoError(err, "Failed first time setup")

	// remove one mds file
	os.RemoveAll(s.mdsCurrent)

	// setup with existing mds files
	_, err = s.handler.Setup("chq", false, []string{})
	s.Require().Error(err, "Expected error because mds files exists")

	// valid setup with force
	_, err = s.handler.Setup("chq", true, []string{})
	s.Require().NoError(err, "Failed to setup with existing mdsfile")

	// remove one mds file
	os.RemoveAll(s.mdsInitial)

	// setup with existing mds files
	_, err = s.handler.Setup("chq", false, []string{})
	s.Require().Error(err, "Expected error because mds files exists")

	// valid setup with force
	_, err = s.handler.Setup("chq", true, []string{})
	s.Require().NoError(err, "Failed to setup with existing mdsfile")
}

func (s *HandlerSuite) TestConfig() {
	tokenfile := filepath.Join(s.tempDir, "token")
	// create a temp tokenfile
	fp, err := os.Create(tokenfile)
	s.Require().NoError(err, "File create failed")
	fp.Close()

	// Update tokenfile in configuration
	_, err = s.handler.Config("", tokenfile, []string{})
	s.Require().NoError(err, "Config update failed")

	// Tokenfile does not exists
	os.RemoveAll(tokenfile)
	_, err = s.handler.Config("", tokenfile, []string{})
	s.Require().Error(err, "Expected an error")

	// Tokenfile is not an absolute path
	_, err = s.handler.Config("", "token", []string{})
	s.Require().Error(err, "Expected an error")

	// Configure URL
	_, err = s.handler.Config("localhost", "", []string{})
	s.Require().NoError(err, "Failed to set flockerhub URL")

	// Without args
	_, err = s.handler.Config("", "", []string{})
	s.Require().NoError(err, "Failed to just show configurations")
}

func (s *HandlerSuite) TestCreateAndInit() {
	// Create volset & vol
	_, err := s.handler.Create("Key=Value", false, []string{"volset", "vol"})
	s.Require().NoError(err, "Don't expect an error here")

	_, err = s.handler.Create("Key=Value1", false, []string{"volest", "vol1"})
	s.Require().NoError(err, "Don't expect an error here")

	_, err = s.handler.Init("Key=Value2", "description", []string{"volset"})
	s.Require().NoError(err, "Don't expect an error here")
}

func (s *HandlerSuite) TestSnapshotAndClone() {
	// Create volset & vol
	_, err := s.handler.Create("Key=Value", false, []string{"volset", "vol"})
	s.Require().NoError(err, "Don't expect an error here")

	// Create snapshot of vol
	_, err = s.handler.Snapshot("test_branch", false, "Key=Value1", "test snapshot", false, []string{"volset:vol", "snap"})
	s.Require().NoError(err, "Don't expect an error here")

	// Create volume from branch
	_, err = s.handler.Clone("Key=Value2", false, []string{"volset:test_branch", "volFromBranch"})
	s.Require().NoError(err, "Don't expect an error here")

	// Create volume from Snapshot
	_, err = s.handler.Clone("Key=Value3", false, []string{"volset:snap", "volFromSnap"})
	s.Require().NoError(err, "Don't expect an error here")
}
