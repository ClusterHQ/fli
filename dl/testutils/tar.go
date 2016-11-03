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

package testutils

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// Tar takes the src directory or file and generates destFile that is tar'ed & gzip'ed
func Tar(src string, destFile string) error {
	_, err := os.Stat(destFile)
	if os.IsExist(err) {
		fmt.Println(destFile, ": Destination already exist")
		return errors.New("Invalid destination")
	}

	_, err = os.Stat(src)
	if err != nil {
		fmt.Println(src, ": Unable to os.Stat()")
		return errors.New("Invalid source")
	}

	// convert to absolute path
	if !filepath.IsAbs(destFile) {
		pwd, err := os.Getwd()
		if err != nil {
			return err
		}

		destFile = filepath.Join(pwd, destFile)
	}

	// save the current working dir to change it later after the tar command is run.
	pwd, err := os.Getwd()
	if err != nil {
		fmt.Println("os.Getwd failed with", err)
		return err
	}

	os.Chdir(filepath.Dir(src))

	// NOTE: Using internal tar package is not used here because of the code complexity effectively. The tar package
	//       works iteratively for each file/directory bottom up, hence you need to walk the complete directory
	//       structure. Seems far more complex than just using a simple shell command.
	cmd := exec.Command("tar", "-czf", destFile, filepath.Base(src))
	err = cmd.Run()
	if err != nil {
		fmt.Println("tar command failed with", err)
		return err
	}

	// revert to previous working dir
	err = os.Chdir(pwd)
	if err != nil {
		fmt.Println("os.Chdir failed with", err)
		return err
	}

	return err

}

// Untar takes the srcFile that is a tar'ed & gzip'ed file and expands in inside the destDir
func Untar(srcFile string, destDir string) error {
	info, err := os.Stat(destDir)
	if err != nil {
		fmt.Println(destDir, ": Unable to os.Stat()")
		return errors.New("Invalid destination")
	}
	if os.IsNotExist(err) {
		fmt.Println(destDir, ": Destination does not exist")
		return errors.New("Invalid destination")
	}

	if !info.IsDir() {
		fmt.Println(destDir, ": Is not a directory")
		return errors.New("Invalid destination")
	}

	info, err = os.Stat(srcFile)
	if err != nil {
		fmt.Println(srcFile, ": Unable to os.Stat()")
		return errors.New("Invalid source")
	}

	// convert to absolute path
	if !filepath.IsAbs(srcFile) {
		pwd, err := os.Getwd()
		if err != nil {
			return err
		}

		srcFile = filepath.Join(pwd, srcFile)
	}

	// save the current working dir to change it later after the tar command is run.
	pwd, err := os.Getwd()
	if err != nil {
		fmt.Println("os.Getwd failed with ", err)
		return err
	}

	os.Chdir(destDir)
	cmd := exec.Command("tar", "-xzf", srcFile)
	err = cmd.Run()
	if err != nil {
		fmt.Println("tar command failed with", err)
		return err
	}

	// revert to previous working dir
	err = os.Chdir(pwd)
	if err != nil {
		fmt.Println("os.Chdir failed with", err)
		return err
	}

	return err
}
