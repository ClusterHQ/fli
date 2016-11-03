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
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/ClusterHQ/fli/client/cligen"
)

var (
	// flags of cligen
	yamlfile = flag.String("yaml", "", "yaml file")
	pkg      = flag.String("package", "", "package to which this command belongs to")
	output   = flag.String("output", "", "output file name")
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("cmdgen:")

	flag.Usage = usage
	flag.Parse()

	// validate flags
	if len(*yamlfile) == 0 {
		flag.Usage()
		os.Exit(2)
	}

	if len(*pkg) == 0 {
		flag.Usage()
		os.Exit(2)
	}

	if len(*output) == 0 {
		flag.Usage()
		os.Exit(2)
	}

	var g = cligen.NewGenerator(*pkg)

	g.Unmarshal(*yamlfile)
	g.Generate()
	g.WriteOut(*output)
}
