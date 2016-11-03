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

package main

import (
	"fmt"
	"os"
	"path"

	"github.com/ClusterHQ/go/client/cli"
)

func main() {
	// set the log output for dpcli
	var (
		// XXX: Set output stream to /opt/clusterhq/var/log/dpcli.log
		logDir  = path.Join("/opt", "clusterhq", "var", "log", "messages")
		logFile = "dpcli.log"
	)

	fs, err := os.Lstat(logDir)
	if err != nil {
		if os.IsNotExist(err) {
			// directory does not exist
			err = os.MkdirAll(logDir, 0755)
			if err != nil {
				// mkdirall failed
				fmt.Fprintln(os.Stderr, "Failed: ", err)
				os.Exit(1)
			}
		} else {
			// lstat failed
			fmt.Fprintln(os.Stderr, "Failed: ", err)
			os.Exit(1)
		}
	} else if !fs.IsDir() { // logDir previously exists but is not a directory
		fmt.Fprintln(os.Stderr, "Failed: "+logDir+" is not a directory")
		os.Exit(2)
	}

	var fp *os.File

	_, err = os.Lstat(path.Join(logDir, logFile))
	if err != nil {
		if os.IsNotExist(err) {
			// logFile has to be created
			fp, err = os.Create(path.Join(logDir, logFile))
			if err != nil {
				// create failed
				fmt.Fprintln(os.Stderr, "Failed: ", err)
				os.Exit(1)
			}
		} else {
			// lstat failed
			fmt.Fprintln(os.Stderr, "Failed: ", err)
			os.Exit(1)
		}
	} else {
		// file exists, so open it
		fp, err = os.Open(path.Join(logDir, logFile))
		if err != nil {
			// open failed
			fmt.Fprintln(os.Stderr, "Failed: ", err)
			os.Exit(1)
		}
	}

	defer fp.Close()
	// XXX disable redirecting logs log.SetOutput(fp)

	if err := cli.Execute(); err != nil {
		os.Exit(1)
	}
}
